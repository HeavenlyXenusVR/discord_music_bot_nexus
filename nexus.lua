--[[
  NEXUS -- swarm music node.

  "A connection point for servers that bridge multiple communities."

  Ported from nexus.py (discord.py + Wavelink + aiomysql) to LuaJIT on the
  shared swarmlua foundation (lib/swarmlua/{bot,gateway,rest,lavalink,pg}.lua,
  copied verbatim -- see lib/swarmlua/ and the fleet README for the protocol
  layer this file builds on).

  This file has full command-surface parity with nexus.py: all 59 of its
  `/nexus_main_*` slash commands are ported (verified name-for-name against
  nexus.py's @bot.tree.command declarations). nexus_error_events IS ported
  (see report_error() below) -- command/button-handler failures,
  process_queue errors, and maintenance-loop errors all land in the shared
  nexus_error_events table (same table Aria/SwarmPanel read from for the
  cross-bot error dashboard) and, if NEXUS_ERROR_WEBHOOK_URL is configured,
  post to a dedicated error webhook, matching nexus.py's
  _persist_error_event()/send_error_webhook_log() pair.

  The deep Python-side defensive/self-healing machinery (Redis cross-process
  locks, yt-dlp audio caching + loudness/BPM analysis, Gemini track
  embeddings, the Aria direct-order bridge, the dozen overlapping
  resilience/watchdog task loops) is intentionally NOT ported. Additional
  documented simplifications made during this port:
    - Smart shuffle is weighted (biased toward server-liked/finished tracks)
      but does not do nexus.py's separate duplicate-track spacing pass.
    - "Beat-matched mixing" (/fade mix) falls back to the same smooth-fade
      curve as /fade fade -- no BPM analysis/matching was ported.
    - "Live playlist sync" (nexus.py's background task that periodically
      re-scans a queued YouTube/Spotify playlist URL for newly-added videos
      and refills them) was not ported -- /play expands a playlist once, in
      full, at queue time via Lavalink's loadType=playlist response.
    - Vote-skip/upvote/downvote quorums use a fixed threshold of 2 rather
      than nexus.py's proportional "listeners in channel" quorum, since this
      file does not maintain a full per-channel member roster.
    - The maintenance loop (heartbeat + position checkpoint + error-event
      retention, all below) is a deliberately simplified stand-in for
      nexus.py's roughly dozen separate @tasks.loop coroutines.
]]

package.path = "./lib/?.lua;./lib/?/init.lua;" .. package.path

local copas = require("copas") -- must load before any socket/ssl usage anywhere in the process
local cjson = require("cjson")
local https = require("ssl.https")
local ltn12 = require("ltn12")
local socket = require("socket")
local bit = require("bit")
-- table.unpack does not exist in this LuaJIT build (confirmed live: `table.unpack`
-- is nil) -- same shim lua-shared/swarmlua/pg.lua needed centrally. dbq() below
-- crashed on every single parameterized query without this until caught by the
-- db_smoke_test.lua run during this port's validation pass.
local unpack = table.unpack or unpack
local Bot = require("swarmlua.bot")

math.randomseed(os.time())

------------------------------------------------------------------------
-- Environment / configuration (mirrors nexus.py's NEXUS_* env reads, same
-- fallback chain, so the existing Music/.env keeps working unchanged).
------------------------------------------------------------------------
local function env(name, default)
  local v = os.getenv(name)
  if v == nil or v == "" then return default end
  return v
end

local BOT_NAME = "nexus"
local TOKEN = env("NEXUS_DISCORD_TOKEN", "")
if TOKEN == "" then
  error("NEXUS_DISCORD_TOKEN is not set")
end

local DB_HOST = env("NEXUS_DB_HOST", env("DB_HOST", env("MYSQL_HOST", "127.0.0.1")))
local DB_PORT = tonumber(env("NEXUS_DB_PORT", env("DB_PORT", env("MYSQL_PORT", "5432"))))
local DB_USER = env("NEXUS_DB_USER", env("DB_USER", env("MYSQL_USER", "botuser")))
local DB_PASSWORD = env("NEXUS_DB_PASSWORD", env("DB_PASSWORD", env("MYSQL_PASSWORD", "")))
local DB_NAME = env("NEXUS_DB_NAME", "discord_music_nexus")

local function parse_lavalink_uri(uri)
  uri = tostring(uri or ""):gsub("%s+$", "")
  local host, port = uri:match("^https?://([^:/]+):?(%d*)")
  if not host then
    host, port = uri:match("^([^:/]+):?(%d*)")
  end
  host = (host and host ~= "") and host or "127.0.0.1"
  port = tonumber(port) or 2333
  return host, port
end

local DEFAULT_LAVALINK_URI = env("LAVALINK_URI", env("LAVALINK_URL", env("LAVALINK_HOST", "127.0.0.1:2333")))
local LAVALINK_URI = env("NEXUS_LAVALINK_URI", env("NEXUS_LAVALINK_URL", env("NEXUS_LAVALINK_HOST", DEFAULT_LAVALINK_URI)))
local LAVALINK_HOST, LAVALINK_PORT = parse_lavalink_uri(LAVALINK_URI)
local LAVALINK_PASSWORD = env("NEXUS_LAVALINK_PASSWORD", env("LAVALINK_PASSWORD", ""))
if LAVALINK_PASSWORD == "" then
  error("Set NEXUS_LAVALINK_PASSWORD or LAVALINK_PASSWORD before starting nexus.")
end

local WEBHOOK_URL = env("NEXUS_WEBHOOK_URL", "")
local ERROR_WEBHOOK_URL = env("NEXUS_ERROR_WEBHOOK_URL", env("SWARM_ERROR_WEBHOOK_URL", ""))

------------------------------------------------------------------------
-- Bot construction
------------------------------------------------------------------------
-- Intents: GUILDS (1) + GUILD_VOICE_STATES (128). Nexus only needs guild
-- create/delete bookkeeping and voice-state plumbing for Lavalink; it does
-- not read message content or need presence/member intents.
local bot = Bot.new({
  token = TOKEN,
  intents = 1 + 128,
  db = { host = DB_HOST, port = DB_PORT, user = DB_USER, password = DB_PASSWORD, database = DB_NAME },
  lavalink = { host = LAVALINK_HOST, port = LAVALINK_PORT, password = LAVALINK_PASSWORD },
})

local START_TIME = socket.gettime()

------------------------------------------------------------------------
-- Postgres helpers
--
-- IMPORTANT (flagged in the final report): pgmoon decodes Postgres `bigint`
-- columns as Lua numbers (doubles). Discord snowflake IDs routinely exceed
-- 2^53 and silently lose precision when converted that way (verified against
-- the live DB during this port). Every SELECT below that returns an
-- id-shaped bigint column (guild_id, channel_id, requester_id, dj_role_id,
-- track/user ids, ...) explicitly casts it `::text` so pgmoon hands back an
-- exact Lua string instead of a lossy double. This is a systemic gotcha for
-- every one of the 13 bots using pg.lua/pgmoon, not just Nexus.
------------------------------------------------------------------------
local function pgval(v)
  if v == nil then return "NULL" end
  local t = type(v)
  if t == "boolean" then return v and "TRUE" or "FALSE" end
  if t == "number" then return tostring(v) end
  local s = tostring(v):gsub("'", "''")
  return "'" .. s .. "'"
end

local function dbq(sql, ...)
  local n = select("#", ...)
  if n > 0 then
    local vals = {}
    for i = 1, n do vals[i] = pgval((select(i, ...))) end
    sql = sql:format(unpack(vals, 1, n))
  end
  local rows, err = bot.db:query(sql)
  if rows == nil and err then
    print("[nexus] db error: " .. tostring(err))
    print("[nexus] failing sql: " .. sql)
  end
  return rows, err
end

local function db_one(sql, ...)
  local rows = dbq(sql, ...)
  if rows and rows[1] then return rows[1] end
  return nil
end

------------------------------------------------------------------------
-- Small utilities
------------------------------------------------------------------------
local function trim(s) return (tostring(s or ""):gsub("^%s+", ""):gsub("%s+$", "")) end

local function fnv1a(str, seed)
  local hash = seed or 2166136261
  for i = 1, #str do
    hash = bit.bxor(hash, str:byte(i))
    hash = bit.tobit(hash * 16777619)
  end
  return bit.band(hash, 0xFFFFFFFF)
end

-- Internal dedupe/lookup key for a track. Doesn't need to match nexus.py's
-- sha1 (nothing cross-reads it) -- just needs to be stable per (url, title).
local function url_key(url, title)
  local raw = trim(url)
  if raw == "" then raw = "title:" .. trim(title or "unknown") end
  raw = raw:lower():gsub("%s+", " ")
  return ("%08x%08x"):format(fnv1a(raw, 2166136261), fnv1a(raw, 84696351))
end

local UID_CHARS = "0123456789abcdef"
local function new_uid()
  local t = {}
  for i = 1, 32 do
    local idx = math.random(1, 16)
    t[i] = UID_CHARS:sub(idx, idx)
  end
  return table.concat(t)
end

local function is_explicit_query(value)
  local text = trim(value):lower()
  if text:match("^https?://") then return true end
  return text:match("^[%a%d_]+search:") ~= nil
end

local function fmt_duration(total)
  total = math.max(0, math.floor(total or 0))
  local m = math.floor(total / 60)
  local s = total % 60
  return ("%d:%02d"):format(m, s)
end

local function progress_bar(current, total, length)
  length = length or 15
  if not total or total <= 0 then
    return ("[%s] %s / Live"):format(("\226\150\172"):rep(length), fmt_duration(current))
  end
  local progress = math.max(0, math.min(length, math.floor((current / total) * length)))
  local bar = ("\226\150\172"):rep(progress) .. "\240\159\148\152" .. ("\226\150\172"):rep(math.max(0, length - progress - 1))
  return ("[%s] %s / %s"):format(bar, fmt_duration(current), fmt_duration(total))
end

local function truncate(s, limit)
  s = tostring(s or "")
  if #s <= limit then return s end
  return s:sub(1, math.max(0, limit - 3)) .. "..."
end

------------------------------------------------------------------------
-- Discord embed / interaction reply helpers
--
-- bot.lua's Bot:reply only sends plain-text content -- nexus.py's UX is
-- built entirely on rich embeds (and, for /panel, message components), so
-- this file talks to the interaction callback / webhook-followup REST
-- endpoints directly via bot.rest, the same client bot.lua itself uses.
------------------------------------------------------------------------
local COLOR = {
  red = 0xE74C3C, green = 0x2ECC71, blue = 0x3498DB, blurple = 0x5865F2,
  gold = 0xF1C40F, orange = 0xE67E22, purple = 0x9B59B6, dark_purple = 0x71368A,
  grey = 0x2B2D31,
}

local function mk_embed(opts)
  local e = { color = opts.color or COLOR.blurple }
  if opts.title then e.title = opts.title end
  if opts.description then e.description = opts.description end
  if opts.fields then e.fields = opts.fields end
  if opts.footer then e.footer = { text = opts.footer } end
  if opts.author then e.author = { name = opts.author } end
  return e
end

local function simple_embed(description, color)
  return mk_embed({ description = description, color = color or COLOR.blurple })
end

local function interaction_callback(interaction, payload)
  local ok, err = bot.rest:post(("/interactions/%s/%s/callback"):format(interaction.id, interaction.token), payload)
  if not ok and err then print("[nexus] interaction callback failed: " .. tostring(err)) end
  return ok, err
end

local function reply(interaction, opts)
  opts = opts or {}
  local data = {}
  if opts.content then data.content = opts.content end
  if opts.embed then data.embeds = { opts.embed } end
  if opts.components then data.components = opts.components end
  if opts.ephemeral then data.flags = 64 end
  return interaction_callback(interaction, { type = 4, data = data })
end

local function defer(interaction, ephemeral)
  local data = nil
  if ephemeral then data = { flags = 64 } end
  return interaction_callback(interaction, { type = 5, data = data })
end

local function update_message(interaction, opts)
  opts = opts or {}
  local data = {}
  if opts.content then data.content = opts.content end
  if opts.embed then data.embeds = { opts.embed } end
  if opts.components then data.components = opts.components end
  return interaction_callback(interaction, { type = 7, data = data })
end

local function followup(interaction, opts)
  opts = opts or {}
  local data = {}
  if opts.content then data.content = opts.content end
  if opts.embed then data.embeds = { opts.embed } end
  if opts.components then data.components = opts.components end
  if opts.ephemeral then data.flags = 64 end
  local ok, err = bot.rest:post(("/webhooks/%s/%s"):format(bot.application_id, interaction.token), data)
  if not ok and err then print("[nexus] followup failed: " .. tostring(err)) end
  return ok, err
end

local function send_channel_message(channel_id, opts)
  if not channel_id then return end
  local data = {}
  if opts.content then data.content = opts.content end
  if opts.embed then data.embeds = { opts.embed } end
  local ok, err = bot.rest:post(("/channels/%s/messages"):format(channel_id), data)
  if not ok and err then print("[nexus] channel send failed: " .. tostring(err)) end
  return ok, err
end

local function send_dm(user_id, opts)
  local channel, err = bot.rest:post("/users/@me/channels", { recipient_id = user_id })
  if not channel or not channel.id then return nil, err or "could not open DM channel" end
  return send_channel_message(channel.id, opts)
end

local function send_webhook_log(title, description, color, username)
  if WEBHOOK_URL == "" or WEBHOOK_URL == "PASTE_YOUR_NEW_WEBHOOK_URL_HERE" then return end
  local ok, body = pcall(function()
    local response_body = {}
    local payload = cjson.encode({
      username = username or ("Node: " .. BOT_NAME:sub(1, 1):upper() .. BOT_NAME:sub(2)),
      embeds = { { title = title, description = description, color = color or COLOR.blurple, footer = { text = "Swarm Network Matrix" } } },
    })
    https.request({
      url = WEBHOOK_URL, method = "POST",
      headers = { ["Content-Type"] = "application/json", ["Content-Length"] = tostring(#payload) },
      source = ltn12.source.string(payload), sink = ltn12.sink.table(response_body),
    })
  end)
  if not ok then print("[nexus] webhook log failed: " .. tostring(body)) end
end

------------------------------------------------------------------------
-- Error events: mirrors nexus.py's _persist_error_event()/send_error_webhook_log()
-- -- writes into the shared nexus_error_events table (same table Aria/SwarmPanel
-- read from for the cross-bot error dashboard) and, if configured, posts to a
-- dedicated error webhook. Ported the same way gws.lua/harmonic.lua added this
-- (see those bots' final reports): command/button handler failures are wired
-- in via bot.on_error (an additive, optional hook -- see lib/swarmlua/bot.lua),
-- and process_queue/maintenance/startup-resume loops call report_error directly.
------------------------------------------------------------------------
local function persist_error_event(guild_id, level, error_type, title, description)
  dbq([[INSERT INTO nexus_error_events (bot_name, guild_id, error_level, error_type, title, description)
        VALUES ('nexus', %s, %s, %s, %s, %s)]],
      guild_id, level, error_type, truncate(title, 255), truncate(description, 5000))
end

local function send_error_webhook(title, description)
  if ERROR_WEBHOOK_URL == "" then return end
  local ok, body = pcall(function()
    local response_body = {}
    local payload = cjson.encode({
      embeds = { { title = "\240\159\148\180 Nexus Error: " .. truncate(title, 200), description = truncate(description, 4000),
                   color = COLOR.red, footer = { text = "Nexus Music Bot" } } },
    })
    https.request({
      url = ERROR_WEBHOOK_URL, method = "POST",
      headers = { ["Content-Type"] = "application/json", ["Content-Length"] = tostring(#payload) },
      source = ltn12.source.string(payload), sink = ltn12.sink.table(response_body),
    })
  end)
  if not ok then print("[nexus] error webhook failed: " .. tostring(body)) end
end

-- guild_id may be nil (process-wide failures, e.g. the maintenance loop).
local function report_error(guild_id, error_type, title, description)
  local ok, err = pcall(persist_error_event, guild_id, "error", error_type, title, description)
  if not ok then print("[nexus] failed to persist error event: " .. tostring(err)) end
  send_error_webhook(title, description)
end

-- Wired into swarmlua.bot's additive/optional on_error hook (see
-- lib/swarmlua/bot.lua) so command handler failures also land in
-- nexus_error_events / the error webhook, not just process_queue/maintenance
-- below.
bot.on_error = function(name, interaction, err)
  report_error(interaction and interaction.guild_id, "command_error", tostring(name), tostring(err))
end

------------------------------------------------------------------------
-- Interaction option / identity helpers
------------------------------------------------------------------------
local function opts_map(interaction)
  local map = {}
  for _, o in ipairs((interaction.data and interaction.data.options) or {}) do
    map[o.name] = o.value
  end
  return map
end

local function user_id_of(interaction)
  if interaction.member and interaction.member.user then return interaction.member.user.id end
  if interaction.user then return interaction.user.id end
  return nil
end

local function display_name_of(interaction)
  local u = (interaction.member and interaction.member.user) or interaction.user
  if not u then return "Someone" end
  if interaction.member and interaction.member.nick and interaction.member.nick ~= cjson.null then
    return interaction.member.nick
  end
  return u.global_name or u.username or ("User " .. tostring(u.id))
end

local function resolved_display_name(interaction, uid)
  local resolved = interaction.data and interaction.data.resolved
  if resolved then
    local member = resolved.members and resolved.members[uid]
    if member and member.nick and member.nick ~= cjson.null then return member.nick end
    local user = resolved.users and resolved.users[uid]
    if user then return user.global_name or user.username end
  end
  return "User " .. tostring(uid)
end

local ADMINISTRATOR_BIT = 8

local guild_owner_cache = {} -- guild_id -> owner user_id

local function get_guild_owner_id(guild_id)
  if not guild_id then return nil end
  local cached = guild_owner_cache[guild_id]
  if cached then return cached end
  local g = bot.rest:get("/guilds/" .. tostring(guild_id))
  local owner_id = g and g.owner_id
  if owner_id then guild_owner_cache[guild_id] = owner_id end
  return owner_id
end

local function is_admin(interaction)
  local perms = interaction.member and tonumber(interaction.member.permissions)
  if perms and math.floor(perms / ADMINISTRATOR_BIT) % 2 == 1 then return true end
  -- The guild owner always effectively has admin, but Discord's resolved
  -- member.permissions bitfield on the interaction has been observed to not
  -- reliably reflect that -- fall back to an explicit ownership check
  -- rather than ever locking the actual owner out of admin-gated commands.
  local uid = interaction.member and interaction.member.user and interaction.member.user.id
  local owner_id = uid and get_guild_owner_id(interaction.guild_id)
  return owner_id ~= nil and tostring(owner_id) == tostring(uid)
end

local function require_admin(interaction)
  if is_admin(interaction) then return true end
  reply(interaction, { embed = simple_embed("You need the Administrator permission to use this command.", COLOR.red), ephemeral = true })
  return false
end

------------------------------------------------------------------------
-- Requester name resolution (best-effort; falls back to a raw id label)
------------------------------------------------------------------------
local requester_name_cache = {}
local function resolve_requester_name(guild_id, requester_id)
  if not requester_id then return "Unknown User" end
  local cache_key = guild_id .. ":" .. requester_id
  local cached = requester_name_cache[cache_key]
  if cached and (socket.gettime() - cached.at) < 900 then return cached.name end
  local member, err = bot.rest:get(("/guilds/%s/members/%s"):format(guild_id, requester_id))
  local name
  if member and member.user then
    name = (member.nick and member.nick ~= cjson.null and member.nick) or member.user.global_name or member.user.username
  else
    name = "User " .. tostring(requester_id)
  end
  requester_name_cache[cache_key] = { name = name, at = socket.gettime() }
  return name
end

------------------------------------------------------------------------
-- Guild settings
------------------------------------------------------------------------
local GUILD_SETTINGS_DEFAULTS = {
  home_vc_id = nil, volume = 100, loop_mode = "queue", filter_mode = "none",
  dj_role_id = nil, feedback_channel_id = nil, transition_mode = "off",
  fade_seconds = 3.0, fade_curve = "smooth", custom_speed = 1.0, custom_pitch = 1.0,
  custom_modifiers_left = 0, dj_only_mode = false, stay_in_vc = false, filter_stack = nil,
}

local function ensure_guild_settings(guild_id)
  dbq("INSERT INTO nexus_guild_settings (guild_id) VALUES (%s) ON CONFLICT (guild_id) DO NOTHING", guild_id)
end

local function get_guild_settings(guild_id)
  local row = db_one([[SELECT home_vc_id::text AS home_vc_id, volume, loop_mode, filter_mode,
                        dj_role_id::text AS dj_role_id, feedback_channel_id::text AS feedback_channel_id,
                        transition_mode, fade_seconds, fade_curve, custom_speed, custom_pitch,
                        custom_modifiers_left, dj_only_mode, stay_in_vc, filter_stack
                        FROM nexus_guild_settings WHERE guild_id = %s]], guild_id)
  local out = {}
  for k, v in pairs(GUILD_SETTINGS_DEFAULTS) do out[k] = v end
  if row then
    for k, v in pairs(row) do
      if v ~= nil then out[k] = v end
    end
  end
  return out
end

local function update_guild_setting(guild_id, column, value)
  ensure_guild_settings(guild_id)
  dbq(("UPDATE nexus_guild_settings SET %s = %%s WHERE guild_id = %%s"):format(column), value, guild_id)
end

local function toggle_guild_setting(guild_id, column)
  ensure_guild_settings(guild_id)
  local row = db_one(("SELECT %s AS v FROM nexus_guild_settings WHERE guild_id = %%s"):format(column), guild_id)
  local new_val = not (row and row.v)
  dbq(("UPDATE nexus_guild_settings SET %s = %%s WHERE guild_id = %%s"):format(column), new_val, guild_id)
  return new_val
end

------------------------------------------------------------------------
-- Home / feedback channel
------------------------------------------------------------------------
local function get_home_channel(guild_id)
  local row = db_one("SELECT home_vc_id::text AS home_vc_id FROM nexus_bot_home_channels WHERE guild_id = %s AND bot_name = 'nexus'", guild_id)
  return row and row.home_vc_id or nil
end

local function set_home_channel(guild_id, channel_id)
  dbq([[INSERT INTO nexus_bot_home_channels (guild_id, bot_name, home_vc_id) VALUES (%s, 'nexus', %s)
        ON CONFLICT (guild_id, bot_name) DO UPDATE SET home_vc_id = EXCLUDED.home_vc_id]], guild_id, channel_id)
end

local function send_feedback(guild_id, embed_table)
  local settings = get_guild_settings(guild_id)
  if settings.feedback_channel_id then
    send_channel_message(settings.feedback_channel_id, { embed = embed_table })
  end
end

------------------------------------------------------------------------
-- DJ check
------------------------------------------------------------------------
local function is_dj(interaction)
  if is_admin(interaction) then return true end
  local settings = get_guild_settings(interaction.guild_id)
  if not settings.dj_only_mode then return true end
  if settings.dj_role_id and interaction.member and interaction.member.roles then
    for _, rid in ipairs(interaction.member.roles) do
      if rid == settings.dj_role_id then return true end
    end
  end
  return false
end

local function require_dj(interaction)
  if is_dj(interaction) then return true end
  reply(interaction, { embed = simple_embed("\226\157\140 **Strict DJ Mode is Active.** You need the DJ Role.", COLOR.red), ephemeral = true })
  return false
end

------------------------------------------------------------------------
-- Autodj toggle (nexus_swarm_toggles)
------------------------------------------------------------------------
local function get_autodj_enabled(guild_id)
  local row = db_one("SELECT auto_dj FROM nexus_swarm_toggles WHERE guild_id = %s", guild_id)
  return row ~= nil and row.auto_dj == true
end

local function set_autodj_enabled(guild_id, enabled)
  dbq([[INSERT INTO nexus_swarm_toggles (guild_id, auto_dj) VALUES (%s, %s)
        ON CONFLICT (guild_id) DO UPDATE SET auto_dj = EXCLUDED.auto_dj]], guild_id, enabled)
end

------------------------------------------------------------------------
-- Track intelligence / Smart Auto-DJ persistence
------------------------------------------------------------------------
local function record_history(guild_id, url, title, requester_id)
  dbq("INSERT INTO nexus_history (guild_id, video_url, title, requester_id) VALUES (%s, %s, %s, %s)",
      guild_id, url, title, requester_id)
end

-- NOTE (found via live-schema introspection against discord_music_nexus,
-- same gotcha class harmonic's port documented): nexus_track_intelligence and
-- nexus_user_track_affinity's counter columns (queued_count, play_count,
-- finish_count, skip_count, like_count, dislike_count, total_listen_seconds,
-- score) are all NOT NULL with NO column default in Postgres -- unlike the
-- old MySQL schema, an INSERT that omits any of them fails outright instead
-- of silently defaulting to 0. Every INSERT into either table below
-- explicitly zero-fills every counter column it isn't setting a real value
-- for, not just the one being bumped.
local function record_track_play_started(guild_id, url, title, requester_id)
  local key = url_key(url, title)
  dbq([[INSERT INTO nexus_track_intelligence (guild_id, url_key, video_url, title,
          queued_count, play_count, finish_count, skip_count, like_count, dislike_count, total_listen_seconds,
          last_requester_id, last_played, source)
        VALUES (%s, %s, %s, %s, 0, 1, 0, 0, 0, 0, 0, %s, NOW(), 'playback')
        ON CONFLICT (guild_id, url_key) DO UPDATE SET
          video_url = EXCLUDED.video_url, title = EXCLUDED.title,
          play_count = nexus_track_intelligence.play_count + 1,
          last_requester_id = EXCLUDED.last_requester_id, last_played = NOW(), source = 'playback']],
      guild_id, key, url, title, requester_id)
  if requester_id then
    dbq([[INSERT INTO nexus_user_track_affinity (guild_id, user_id, url_key, video_url, title,
            queued_count, play_count, finish_count, skip_count, like_count, dislike_count, score, last_requested)
          VALUES (%s, %s, %s, %s, %s, 0, 1, 0, 0, 0, 0, 0.5, NOW())
          ON CONFLICT (guild_id, user_id, url_key) DO UPDATE SET
            video_url = EXCLUDED.video_url, title = EXCLUDED.title,
            play_count = nexus_user_track_affinity.play_count + 1,
            score = nexus_user_track_affinity.score + 0.5, last_requested = NOW()]],
        guild_id, requester_id, key, url, title)
  end
end

local function record_track_outcome(guild_id, url, title, requester_id, finished, listen_seconds)
  if not url and not title then return end
  local key = url_key(url, title)
  local finish_delta = finished and 1 or 0
  local skip_delta = finished and 0 or 1
  local score_delta = finished and 1.25 or -1.75
  listen_seconds = math.max(0, math.floor(listen_seconds or 0))
  dbq([[INSERT INTO nexus_track_intelligence (guild_id, url_key, video_url, title,
          queued_count, play_count, finish_count, skip_count, like_count, dislike_count, total_listen_seconds,
          last_requester_id, last_played, source)
        VALUES (%s, %s, %s, %s, 0, 0, %s, %s, 0, 0, %s, %s, NOW(), 'playback')
        ON CONFLICT (guild_id, url_key) DO UPDATE SET
          video_url = EXCLUDED.video_url, title = EXCLUDED.title,
          finish_count = nexus_track_intelligence.finish_count + EXCLUDED.finish_count,
          skip_count = nexus_track_intelligence.skip_count + EXCLUDED.skip_count,
          total_listen_seconds = nexus_track_intelligence.total_listen_seconds + EXCLUDED.total_listen_seconds,
          last_requester_id = EXCLUDED.last_requester_id, last_played = NOW(), source = 'playback']],
      guild_id, key, url, title, finish_delta, skip_delta, listen_seconds, requester_id)
  if requester_id then
    dbq([[INSERT INTO nexus_user_track_affinity (guild_id, user_id, url_key, video_url, title,
            queued_count, play_count, finish_count, skip_count, like_count, dislike_count, score, last_requested)
          VALUES (%s, %s, %s, %s, %s, 0, 0, %s, %s, 0, 0, %s, NOW())
          ON CONFLICT (guild_id, user_id, url_key) DO UPDATE SET
            video_url = EXCLUDED.video_url, title = EXCLUDED.title,
            finish_count = nexus_user_track_affinity.finish_count + EXCLUDED.finish_count,
            skip_count = nexus_user_track_affinity.skip_count + EXCLUDED.skip_count,
            score = nexus_user_track_affinity.score + EXCLUDED.score, last_requested = NOW()]],
        guild_id, requester_id, key, url, title, finish_delta, skip_delta, score_delta)
  end
end

local function record_track_feedback(guild_id, user_id, url, title, liked)
  local key = url_key(url, title)
  local like_delta = liked and 1 or 0
  local dislike_delta = liked and 0 or 1
  local score_delta = liked and 4.0 or -4.0
  dbq([[INSERT INTO nexus_track_intelligence (guild_id, url_key, video_url, title,
          queued_count, play_count, finish_count, skip_count, like_count, dislike_count, total_listen_seconds,
          last_requester_id, source)
        VALUES (%s, %s, %s, %s, 0, 0, 0, 0, %s, %s, 0, %s, 'feedback')
        ON CONFLICT (guild_id, url_key) DO UPDATE SET
          video_url = EXCLUDED.video_url, title = EXCLUDED.title,
          like_count = nexus_track_intelligence.like_count + EXCLUDED.like_count,
          dislike_count = nexus_track_intelligence.dislike_count + EXCLUDED.dislike_count,
          last_requester_id = EXCLUDED.last_requester_id, source = 'feedback']],
      guild_id, key, url, title, like_delta, dislike_delta, user_id)
  dbq([[INSERT INTO nexus_user_track_affinity (guild_id, user_id, url_key, video_url, title,
          queued_count, play_count, finish_count, skip_count, like_count, dislike_count, score, last_requested)
        VALUES (%s, %s, %s, %s, %s, 0, 0, 0, 0, %s, %s, %s, NOW())
        ON CONFLICT (guild_id, user_id, url_key) DO UPDATE SET
          video_url = EXCLUDED.video_url, title = EXCLUDED.title,
          like_count = nexus_user_track_affinity.like_count + EXCLUDED.like_count,
          dislike_count = nexus_user_track_affinity.dislike_count + EXCLUDED.dislike_count,
          score = nexus_user_track_affinity.score + EXCLUDED.score, last_requested = NOW()]],
      guild_id, user_id, key, url, title, like_delta, dislike_delta, score_delta)
end

------------------------------------------------------------------------
-- Queue plumbing
------------------------------------------------------------------------
local function queue_count(guild_id)
  local row = db_one("SELECT COUNT(*) AS n FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", guild_id)
  return row and math.floor(row.n) or 0
end

local function enqueue_track(guild_id, url, title, requester_id, backup)
  local uid = new_uid()
  dbq("INSERT INTO nexus_queue (guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, 'nexus', %s, %s, %s, %s)",
      guild_id, url, title, requester_id, uid)
  if backup ~= false then
    dbq("INSERT INTO nexus_queue_backup (guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, 'nexus', %s, %s, %s, %s)",
        guild_id, url, title, requester_id, uid)
  end
  return uid
end

local function insert_queue_front(guild_id, url, title, requester_id, track_uid)
  local row = db_one("SELECT COALESCE(MIN(id), 0) AS min_id FROM nexus_queue")
  local new_id = (row and math.floor(row.min_id) or 0) - 1
  dbq("INSERT INTO nexus_queue (id, guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, %s, 'nexus', %s, %s, %s, %s)",
      new_id, guild_id, url, title, requester_id, track_uid or new_uid())
end

local function snapshot_queue_backup(guild_id)
  dbq("DELETE FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus'", guild_id)
  local rows = dbq([[SELECT video_url, title, requester_id::text AS requester_id, track_uid
                      FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC]], guild_id) or {}
  for _, r in ipairs(rows) do
    dbq("INSERT INTO nexus_queue_backup (guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, 'nexus', %s, %s, %s, %s)",
        guild_id, r.video_url, r.title, r.requester_id, r.track_uid)
  end
end

local function restore_queue_from_backup(guild_id)
  if queue_count(guild_id) > 0 then return 0 end
  local rows = dbq([[SELECT video_url, title, requester_id::text AS requester_id, track_uid
                      FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC]], guild_id) or {}
  for _, r in ipairs(rows) do
    dbq("INSERT INTO nexus_queue (guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, 'nexus', %s, %s, %s, %s)",
        guild_id, r.video_url, r.title, r.requester_id, r.track_uid)
  end
  return #rows
end

-- Weighted shuffle: biases toward tracks this server has historically
-- finished/liked, same spirit as nexus.py's smart shuffle, without the
-- duplicate-track spacing pass (documented simplification).
local function shuffle_queue_rows(guild_id, preserve_first)
  local rows = dbq([[SELECT id, video_url, title, requester_id::text AS requester_id, track_uid
                      FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC]], guild_id) or {}
  if #rows <= 1 then return #rows end

  local head = nil
  if preserve_first then
    head = rows[1]
    table.remove(rows, 1)
  end

  local keys = {}
  for _, r in ipairs(rows) do keys[url_key(r.video_url, r.title)] = true end
  local weights = {}
  for k, _ in pairs(keys) do
    local trow = db_one("SELECT play_count, finish_count, skip_count, like_count, dislike_count FROM nexus_track_intelligence WHERE guild_id = %s AND url_key = %s", guild_id, k)
    if trow then
      local plays = math.max(1, trow.play_count or 0)
      local score = 1.0 + ((trow.finish_count or 0) / plays) * 0.4 - ((trow.skip_count or 0) / plays) * 0.5
          + (trow.like_count or 0) * 0.15 - (trow.dislike_count or 0) * 0.2
      weights[k] = math.max(0.15, math.min(2.5, score))
    end
  end

  local pool = {}
  for i, r in ipairs(rows) do pool[i] = r end
  local ordered = {}
  while #pool > 0 do
    local total = 0
    for _, r in ipairs(pool) do total = total + (weights[url_key(r.video_url, r.title)] or 1.0) end
    local target = math.random() * total
    local upto = 0
    local chosen_idx = #pool
    for i, r in ipairs(pool) do
      upto = upto + (weights[url_key(r.video_url, r.title)] or 1.0)
      if upto >= target then chosen_idx = i; break end
    end
    table.insert(ordered, table.remove(pool, chosen_idx))
  end

  if head then table.insert(ordered, 1, head) end

  dbq("DELETE FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", guild_id)
  for _, r in ipairs(ordered) do
    dbq("INSERT INTO nexus_queue (guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, 'nexus', %s, %s, %s, %s)",
        guild_id, r.video_url, r.title, r.requester_id, r.track_uid)
  end
  return #ordered
end

------------------------------------------------------------------------
-- Smart Auto-DJ recommendation
------------------------------------------------------------------------
local FALLBACK_TERMS = { "lofi hip hop", "synthwave mix", "chill electronic", "gaming music", "jazz hop" }
local RADIO_SUFFIXES = { "radio", "audio", "playlist", "mix" }

local function clean_smart_title(title)
  local t = tostring(title or "")
  t = t:gsub("https?://%S+", " ")
  t = t:gsub("%s*%b()", " "):gsub("%s*%b[]", " ")
  t = t:gsub("%s+", " ")
  t = trim(t)
  return t:sub(1, 120)
end

local function load_smart_avoid_keys(guild_id, listener_ids)
  local avoid = {}
  local hist = dbq("SELECT video_url, title FROM nexus_history WHERE guild_id = %s ORDER BY played_at DESC LIMIT 36", guild_id) or {}
  for _, r in ipairs(hist) do avoid[url_key(r.video_url, r.title)] = true end
  local q = dbq("SELECT video_url, title FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC LIMIT 36", guild_id) or {}
  for _, r in ipairs(q) do avoid[url_key(r.video_url, r.title)] = true end
  for _, uid in ipairs(listener_ids or {}) do
    local rows = dbq("SELECT url_key FROM nexus_user_track_affinity WHERE guild_id = %s AND user_id = %s AND dislike_count > like_count", guild_id, uid) or {}
    for _, r in ipairs(rows) do avoid[r.url_key] = true end
  end
  return avoid
end

-- Weighted pick from a list of {title=,video_url=,weight=}
local function weighted_pick(candidates)
  local total = 0
  local pool = {}
  for _, c in ipairs(candidates) do
    local w = tonumber(c.weight) or 0
    if w > 0 and (c.title or c.video_url) then
      total = total + w
      table.insert(pool, c)
    end
  end
  if total <= 0 or #pool == 0 then return nil end
  local target = math.random() * total
  local upto = 0
  for _, c in ipairs(pool) do
    upto = upto + (tonumber(c.weight) or 0)
    if upto >= target then return c end
  end
  return pool[#pool]
end

local function build_smart_recommendation(guild_id, listener_ids)
  local candidates = {}
  local recency = "POWER(0.5, LEAST(GREATEST(EXTRACT(EPOCH FROM (NOW() - COALESCE(last_played, last_queued, first_seen)))/86400.0, 0), 365) / 21.0)"
  local recency_u = "POWER(0.5, LEAST(GREATEST(EXTRACT(EPOCH FROM (NOW() - COALESCE(last_requested, NOW())))/86400.0, 0), 365) / 21.0)"

  if listener_ids and #listener_ids > 0 then
    for _, uid in ipairs(listener_ids) do
      local rows = dbq(([[SELECT title, video_url,
            (score + play_count * 1.35 + finish_count * 1.5 + like_count * 3.0 - dislike_count * 4.0
             - (skip_count::float / (play_count + finish_count + 1)) * 6.0) * %s AS weight
          FROM nexus_user_track_affinity
          WHERE guild_id = %%s AND user_id = %%s AND (dislike_count <= like_count OR like_count > 0)
          ORDER BY weight DESC LIMIT 40]]):format(recency_u), guild_id, uid) or {}
      for _, r in ipairs(rows) do r.reason = "listener taste"; table.insert(candidates, r) end

      local pl = dbq([[SELECT title, video_url, (COUNT(*) * 1.1) AS weight FROM nexus_user_playlists
                        WHERE user_id = %s GROUP BY title, video_url ORDER BY weight DESC LIMIT 20]], uid) or {}
      for _, r in ipairs(pl) do r.reason = "saved playlist"; table.insert(candidates, r) end
    end
  end

  local server = dbq(([[SELECT title, video_url,
        (play_count * 1.1 + finish_count * 1.6 + like_count * 3.0 + queued_count * 0.2 - dislike_count * 4.0
         - (skip_count::float / (play_count + finish_count + 1)) * 5.0) * %s AS weight
      FROM nexus_track_intelligence
      WHERE guild_id = %%s AND (dislike_count <= like_count OR like_count > 0)
      ORDER BY weight DESC LIMIT 40]]):format(recency), guild_id) or {}
  for _, r in ipairs(server) do r.reason = "server taste"; table.insert(candidates, r) end

  local last = db_one("SELECT video_url, title FROM nexus_history WHERE guild_id = %s ORDER BY played_at DESC LIMIT 1", guild_id)
  if last then
    local last_key = url_key(last.video_url, last.title)
    local nextup = dbq([[SELECT ti.title AS title, ti.video_url AS video_url, (tc.weight * 2.0) AS weight
          FROM nexus_track_cooccurrence tc
          JOIN nexus_track_intelligence ti ON ti.guild_id = tc.guild_id AND ti.url_key = tc.url_key_b
          WHERE tc.guild_id = %s AND tc.url_key_a = %s AND (ti.dislike_count <= ti.like_count OR ti.like_count > 0)
          ORDER BY tc.weight DESC LIMIT 8]], guild_id, last_key) or {}
    for _, r in ipairs(nextup) do r.reason = "often played next"; table.insert(candidates, r) end
  end

  if #candidates == 0 then
    local h = dbq("SELECT title, video_url, 1.0 AS weight FROM nexus_history WHERE guild_id = %s ORDER BY played_at DESC LIMIT 40", guild_id) or {}
    for _, r in ipairs(h) do r.reason = "recent history"; table.insert(candidates, r) end
  end

  local seed = weighted_pick(candidates)
  if seed then
    local cleaned = clean_smart_title(seed.title)
    local suffix = RADIO_SUFFIXES[math.random(1, #RADIO_SUFFIXES)]
    local query = (cleaned ~= "") and ("ytmsearch:" .. cleaned .. " " .. suffix) or ("ytmsearch:" .. FALLBACK_TERMS[math.random(1, #FALLBACK_TERMS)])
    return { query = query, seed_title = seed.title, seed_url = seed.video_url, reason = seed.reason or "history" }
  end

  local term = FALLBACK_TERMS[math.random(1, #FALLBACK_TERMS)]
  return { query = "ytmsearch:" .. term, seed_title = term, seed_url = nil, reason = "fallback" }
end

local search_playables -- forward declaration (defined in the playback section)

local function pick_smart_recommendation_track(guild_id, listener_ids)
  local recommendation = build_smart_recommendation(guild_id, listener_ids)
  local avoid = load_smart_avoid_keys(guild_id, listener_ids)
  local entries = search_playables(recommendation.query)
  local chosen, scanned = nil, 0
  for _, entry in ipairs(entries or {}) do
    scanned = scanned + 1
    local title = entry.info and entry.info.title or ""
    local uri = entry.info and entry.info.uri or ""
    if title ~= "" or uri ~= "" then
      if avoid[url_key(uri, title)] and scanned < 12 then
        -- skip, keep scanning
      else
        chosen = entry
        break
      end
    end
  end
  if not chosen and entries and entries[1] then chosen = entries[1] end
  return chosen, recommendation
end

local function record_smart_recommendation(guild_id, requester_id, recommendation, chosen, reason)
  dbq([[INSERT INTO nexus_smart_recommendations (guild_id, requester_id, seed_title, seed_url, query_text, chosen_url, chosen_title, reason)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)]],
      guild_id, requester_id, recommendation.seed_title, recommendation.seed_url, recommendation.query,
      chosen and chosen.info and chosen.info.uri or nil, chosen and chosen.info and chosen.info.title or nil,
      reason or recommendation.reason)
end

------------------------------------------------------------------------
-- Playback runtime state
------------------------------------------------------------------------
local playback = {}        -- guild_id -> { url, title, duration, start_time, offset, paused, requester_id, track_uid, channel_id, speed, filter_mode, volume, encoded, uploader }
local voice_channel = {}   -- guild_id -> channel_id currently joined
local fade_token = {}      -- guild_id -> integer, bumped to cancel in-flight fades
local sleep_token = {}      -- guild_id -> integer, bumped to cancel sleep timers
local vote_skip_sessions = {} -- guild_id -> { [user_id]=true }
local queue_vote_state = {}   -- "guild_id:queue_id" -> { up={}, down={} }

local function current_position(guild_id)
  local data = playback[guild_id]
  if not data then return 0 end
  local pos
  if data.paused then
    pos = data.offset or 0
  else
    local elapsed = socket.gettime() - (data.start_time or socket.gettime())
    pos = (data.offset or 0) + elapsed * (data.speed or 1.0)
  end
  if data.duration and data.duration > 0 then pos = math.min(pos, data.duration) end
  return math.floor(math.max(0, pos))
end

-- NOTE (found live via db_smoke_test.lua): the VALUES(...) placeholder order
-- must match the INSERT column order exactly -- dbq() has no named-parameter
-- binding, it's positional %s substitution. An earlier version of this
-- function passed (channel_id, url, title, position, ...) here, one column
-- out of step with (channel_id, video_url, position_seconds, is_playing,
-- is_paused, title, ...), which silently wrote title text into
-- position_seconds and shifted every column after it -- and threw outright
-- once a non-numeric title hit the integer position_seconds column.
local function persist_playback_state(guild_id, channel_id, url, title, position, is_playing, is_paused, track_uid)
  dbq([[INSERT INTO nexus_playback_state (guild_id, bot_name, channel_id, video_url, position_seconds, is_playing, is_paused, title, track_uid, play_session_key)
        VALUES (%s, 'nexus', %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (guild_id, bot_name) DO UPDATE SET
          channel_id = EXCLUDED.channel_id, video_url = EXCLUDED.video_url, position_seconds = EXCLUDED.position_seconds,
          is_playing = EXCLUDED.is_playing, is_paused = EXCLUDED.is_paused, title = EXCLUDED.title,
          track_uid = EXCLUDED.track_uid, play_session_key = EXCLUDED.play_session_key, last_checkpoint_at = NOW()]],
      guild_id, channel_id, url, math.floor(position or 0), is_playing, is_paused, title, track_uid, track_uid)
end

local function clear_playback_state(guild_id)
  dbq([[UPDATE nexus_playback_state SET channel_id = NULL, video_url = NULL, title = NULL, position_seconds = 0,
        is_playing = FALSE, is_paused = FALSE, play_session_key = NULL, track_uid = NULL
        WHERE guild_id = %s AND bot_name = 'nexus']], guild_id)
end

------------------------------------------------------------------------
-- Voice connect (thin wrapper over Gateway voice-state ops; bot.lua already
-- forwards VOICE_STATE_UPDATE/VOICE_SERVER_UPDATE to Lavalink automatically)
------------------------------------------------------------------------
-- reconnect_attempts is NOT NULL with no column default (found via real-DB
-- validation, same class of bug the counter tables above have -- every
-- INSERT here must zero-fill it or this fails outright on the very first
-- /join).
-- Split out from ensure_voice/leave_voice below so the DB upsert itself
-- (the part that actually hit the reconnect_attempts NOT-NULL bug) is
-- independently callable/testable without a live gateway connection.
local function mark_voice_connected(guild_id, channel_id)
  dbq([[INSERT INTO nexus_voice_state (guild_id, bot_name, connected_channel_id, last_channel_id, desired_connected, reconnect_attempts, last_seen_at)
        VALUES (%s, 'nexus', %s, %s, TRUE, 0, NOW())
        ON CONFLICT (guild_id, bot_name) DO UPDATE SET
          connected_channel_id = EXCLUDED.connected_channel_id, last_channel_id = EXCLUDED.last_channel_id,
          desired_connected = TRUE, reconnect_attempts = 0, last_seen_at = NOW(), disconnected_at = NULL]],
      guild_id, channel_id, channel_id)
end

local function mark_voice_disconnected(guild_id)
  dbq([[INSERT INTO nexus_voice_state (guild_id, bot_name, desired_connected, connected_channel_id, reconnect_attempts, disconnected_at, last_seen_at)
        VALUES (%s, 'nexus', FALSE, NULL, 0, NOW(), NOW())
        ON CONFLICT (guild_id, bot_name) DO UPDATE SET
          desired_connected = FALSE, connected_channel_id = NULL, disconnected_at = NOW(), last_seen_at = NOW()]],
      guild_id)
end

local function ensure_voice(guild_id, channel_id)
  if voice_channel[guild_id] ~= channel_id then
    bot.gateway:join_voice(guild_id, channel_id, false, true) -- self_mute=false, self_deaf=true
    voice_channel[guild_id] = channel_id
    copas.sleep(1.5) -- give the voice-server handshake time to reach Lavalink
  end
  mark_voice_connected(guild_id, channel_id)
end

-- Port of alucard.py's update_stage_topic/clear_voice_channel_status: without
-- this, a stage channel just sits showing "waiting" in Discord's UI and the
-- member list never shows what's playing, even though audio is flowing fine
-- -- these are purely cosmetic Discord-side signals, not required for audio,
-- but users expect them. Channel type is memoized since it never changes.
local channel_kind_cache = {} -- channel_id -> "stage" | "voice"
local last_stage_topic = {}   -- guild_id -> last topic string sent (dedup)

local function get_channel_kind(channel_id)
  if not channel_id then return "voice" end
  local cached = channel_kind_cache[channel_id]
  if cached then return cached end
  local ch = bot.rest:get("/channels/" .. tostring(channel_id))
  local kind = (ch and ch.type == 13) and "stage" or "voice"
  channel_kind_cache[channel_id] = kind
  return kind
end

local function update_stage_topic(guild_id, channel_id, title)
  if not channel_id then return end
  local ok, err = pcall(function()
    local safe_title = tostring(title or "Unknown Track"):gsub("\n", " "):sub(1, 60)
    local topic = "\240\159\142\181 " .. safe_title
    if last_stage_topic[guild_id] == topic then return end
    if get_channel_kind(channel_id) == "stage" then
      local patched = bot.rest:patch("/stage-instances/" .. tostring(channel_id), { topic = topic })
      if not patched then
        bot.rest:post("/stage-instances", { channel_id = tostring(channel_id), topic = topic, privacy_level = 2 })
      end
    else
      bot.rest:patch("/channels/" .. tostring(channel_id), { status = topic })
    end
    last_stage_topic[guild_id] = topic
  end)
  if not ok then print(("[nexus] stage/voice topic update failed for %s: %s"):format(tostring(guild_id), tostring(err))) end
end

local function clear_stage_topic(guild_id, channel_id)
  last_stage_topic[guild_id] = nil
  if not channel_id then return end
  pcall(function()
    if get_channel_kind(channel_id) == "stage" then
      bot.rest:delete("/stage-instances/" .. tostring(channel_id))
    else
      bot.rest:patch("/channels/" .. tostring(channel_id), { status = cjson.null })
    end
  end)
end

local function leave_voice(guild_id)
  clear_stage_topic(guild_id, voice_channel[guild_id])
  bot.gateway:leave_voice(guild_id)
  voice_channel[guild_id] = nil
  if bot.lavalink and bot.lavalink.session_id then
    bot.lavalink:destroy_player(guild_id)
  end
  mark_voice_disconnected(guild_id)
end

------------------------------------------------------------------------
-- Lavalink search / resolve
------------------------------------------------------------------------
local function unwrap_load_result(result)
  if not result then return {}, false, "no result" end
  local lt = result.loadType
  if lt == "track" and result.data then return { result.data }, false end
  if lt == "search" then return result.data or {}, false end
  if lt == "playlist" and result.data then return (result.data.tracks or {}), true end
  local msg = (result.data and result.data.message) or ("load type: " .. tostring(lt))
  return {}, false, msg
end

search_playables = function(query)
  query = trim(query)
  if query == "" then return {}, false end
  if not is_explicit_query(query) then query = "ytmsearch:" .. query end
  local result, err = bot.lavalink:load_tracks(query)
  if not result then return {}, false, err end
  local entries, is_playlist, uerr = unwrap_load_result(result)
  return entries, is_playlist, uerr
end

local function resolve_track_by_url(url)
  local result, err = bot.lavalink:load_tracks(url)
  local entries, _, uerr = unwrap_load_result(result)
  if entries[1] then return entries[1] end
  return nil, err or uerr or "could not resolve track"
end

------------------------------------------------------------------------
-- Audio filter presets (Lavalink v4 filter JSON, mirrors nexus.py's preset
-- table including the loudnorm-style baseline EQ every non-EQ preset gets).
------------------------------------------------------------------------
local FILTER_PRESET_CHOICES = {
  { name = "None (Standard high quality audio)", value = "none" },
  { name = "Bassboost", value = "bassboost" },
  { name = "Nightcore", value = "nightcore" },
  { name = "Vaporwave", value = "vaporwave" },
  { name = "8D Rotation", value = "8d" },
  { name = "Karaoke", value = "karaoke" },
  { name = "Tremolo", value = "tremolo" },
  { name = "Vibrato", value = "vibrato" },
  { name = "Low Pass", value = "lowpass" },
  { name = "Lo-fi", value = "lofi" },
  { name = "Electronic", value = "electronic" },
  { name = "Party", value = "party" },
  { name = "Radio", value = "radio" },
  { name = "Cinema", value = "cinema" },
  { name = "Soft", value = "soft" },
  { name = "Pop", value = "pop" },
  { name = "Rock", value = "rock" },
  { name = "Classical", value = "classical" },
  { name = "Ear Rape", value = "earrape" },
  { name = "Double Time (2x)", value = "doubletime" },
  { name = "Slow Mo (0.5x)", value = "slowmo" },
  { name = "Pitch Up", value = "pitch_up" },
  { name = "Pitch Down", value = "pitch_down" },
  { name = "Deep", value = "deep" },
  { name = "Anime", value = "anime" },
}
local FILTER_PRESET_SET = {}
local FILTER_PRESET_LABEL = {}
for _, c in ipairs(FILTER_PRESET_CHOICES) do FILTER_PRESET_SET[c.value] = true; FILTER_PRESET_LABEL[c.value] = c.name end

local LOUDNORM_EQ = {
  { 0, -0.05 }, { 1, -0.03 }, { 2, 0.0 }, { 3, 0.0 }, { 4, 0.0 }, { 5, 0.0 }, { 6, 0.0 }, { 7, -0.01 },
  { 8, -0.02 }, { 9, -0.02 }, { 10, -0.03 }, { 11, -0.02 }, { 12, 0.01 }, { 13, 0.01 }, { 14, 0.0 },
}

local function blend_loudnorm(preset_bands)
  local base = {}
  for _, p in ipairs(LOUDNORM_EQ) do base[p[1]] = p[2] end
  for _, p in ipairs(preset_bands or {}) do base[p[1]] = (base[p[1]] or 0.0) + p[2] end
  local out = {}
  for band = 0, 14 do
    if base[band] ~= nil then table.insert(out, { band = band, gain = base[band] }) end
  end
  return out
end

local function apply_filter_preset(filters, mode, current_speed)
  mode = tostring(mode or "none"):lower():gsub("%s", "")
  local speed = current_speed or 1.0
  local eq_used = false
  if mode == "nightcore" then
    filters.timescale = { speed = 1.25, pitch = 1.3 }; speed = 1.25
  elseif mode == "vaporwave" then
    filters.timescale = { speed = 0.8, pitch = 0.8 }; speed = 0.8
  elseif mode == "bassboost" then
    eq_used = true; filters.equalizer = blend_loudnorm({ { 0, 0.32 }, { 1, 0.24 }, { 2, 0.12 } })
  elseif mode == "8d" then
    filters.rotation = { rotationHz = 0.18 }
    filters.channelMix = { leftToLeft = 0.8, leftToRight = 0.2, rightToLeft = 0.2, rightToRight = 0.8 }
  elseif mode == "karaoke" then
    filters.karaoke = { level = 1.0, monoLevel = 1.0, filterBand = 220.0, filterWidth = 100.0 }
  elseif mode == "tremolo" then
    filters.tremolo = { frequency = 4.0, depth = 0.45 }
  elseif mode == "vibrato" then
    filters.vibrato = { frequency = 4.5, depth = 0.35 }
  elseif mode == "lowpass" or mode == "lofi" then
    filters.lowPass = { smoothing = (mode == "lowpass") and 20.0 or 35.0 }
    if mode == "lofi" then filters.timescale = { speed = 0.94, pitch = 0.96 }; speed = 0.94 end
  elseif mode == "electronic" then
    eq_used = true; filters.equalizer = blend_loudnorm({ { 0, 0.12 }, { 1, 0.10 }, { 4, -0.05 }, { 8, 0.08 }, { 10, 0.14 } })
  elseif mode == "party" then
    eq_used = true; filters.equalizer = blend_loudnorm({ { 0, 0.25 }, { 1, 0.18 }, { 2, 0.08 }, { 9, 0.10 } })
  elseif mode == "radio" then
    eq_used = true; filters.equalizer = blend_loudnorm({ { 0, -0.18 }, { 1, -0.10 }, { 4, 0.12 }, { 5, 0.12 }, { 10, -0.12 } })
    filters.lowPass = { smoothing = 18.0 }
  elseif mode == "cinema" then
    eq_used = true; filters.equalizer = blend_loudnorm({ { 0, 0.18 }, { 1, 0.12 }, { 8, 0.08 }, { 9, 0.10 } })
  elseif mode == "soft" then
    filters.lowPass = { smoothing = 25.0 }
  elseif mode == "pop" then
    eq_used = true
    filters.equalizer = blend_loudnorm({ { 0, -0.1 }, { 1, 0.1 }, { 2, 0.2 }, { 3, 0.25 }, { 4, 0.2 }, { 5, 0.1 }, { 6, 0.0 },
      { 7, -0.05 }, { 8, -0.1 }, { 9, -0.1 }, { 10, -0.05 }, { 11, 0.0 }, { 12, 0.1 }, { 13, 0.2 }, { 14, 0.1 } })
  elseif mode == "rock" then
    eq_used = true
    filters.equalizer = blend_loudnorm({ { 0, 0.3 }, { 1, 0.25 }, { 2, 0.1 }, { 3, -0.1 }, { 4, -0.15 }, { 5, -0.1 }, { 6, 0.0 },
      { 7, 0.1 }, { 8, 0.3 }, { 9, 0.35 }, { 10, 0.3 }, { 11, 0.2 }, { 12, 0.1 }, { 13, 0.05 }, { 14, 0.0 } })
  elseif mode == "classical" then
    eq_used = true
    filters.equalizer = blend_loudnorm({ { 0, 0.1 }, { 1, 0.1 }, { 2, 0.05 }, { 3, 0.0 }, { 4, -0.05 }, { 5, -0.05 }, { 6, -0.05 },
      { 7, -0.05 }, { 8, -0.05 }, { 9, -0.05 }, { 10, 0.0 }, { 11, 0.05 }, { 12, 0.1 }, { 13, 0.15 }, { 14, 0.2 } })
  elseif mode == "earrape" then
    filters.distortion = { sinOffset = 0.0, sinScale = 1.5, cosOffset = 0.0, cosScale = 1.5, tanOffset = 0.0, tanScale = 1.0, offset = 0.0, scale = 1.5 }
  elseif mode == "doubletime" then
    filters.timescale = { speed = 2.0, pitch = 1.0 }; speed = 2.0
  elseif mode == "slowmo" then
    filters.timescale = { speed = 0.5, pitch = 1.0 }; speed = 0.5
  elseif mode == "pitch_up" then
    filters.timescale = { speed = 1.0, pitch = 1.3 }
  elseif mode == "pitch_down" then
    filters.timescale = { speed = 1.0, pitch = 0.7 }
  elseif mode == "deep" then
    eq_used = true; filters.equalizer = blend_loudnorm({ { 0, 0.5 }, { 1, 0.4 }, { 2, 0.3 } })
    filters.timescale = { speed = 1.0, pitch = 0.8 }
  elseif mode == "anime" then
    filters.timescale = { speed = 1.4, pitch = 1.5 }; speed = 1.4
  end
  if not eq_used then filters.equalizer = blend_loudnorm({}) end
  return speed
end

------------------------------------------------------------------------
-- Fade
------------------------------------------------------------------------
local function fade_curve_progress(progress, curve)
  if curve == "ease_in" then return progress * progress end
  if curve == "ease_out" then return 1 - (1 - progress) * (1 - progress) end
  if curve == "smooth" then return progress * progress * (3 - 2 * progress) end
  return progress
end

local function run_fade(guild_id, start_vol, end_vol, duration, curve, token)
  duration = math.max(0.25, duration or 3.0)
  local steps = math.max(6, math.floor(duration * 8))
  for i = 1, steps do
    if fade_token[guild_id] ~= token then return end
    local progress = i / steps
    local vol = math.floor(start_vol + (end_vol - start_vol) * fade_curve_progress(progress, curve) + 0.5)
    bot.lavalink:set_volume(guild_id, math.max(0, vol))
    copas.sleep(duration / steps)
  end
end

------------------------------------------------------------------------
-- Core playback loop
------------------------------------------------------------------------
local process_queue -- forward declaration (recursive)

local function maybe_enqueue_autodj(guild_id, channel_id)
  if not get_autodj_enabled(guild_id) then return false end
  local ok, chosen, recommendation = pcall(pick_smart_recommendation_track, guild_id, {})
  if not ok or not chosen then return false end
  local uid = enqueue_track(guild_id, chosen.info.uri, chosen.info.title, nil, false)
  record_smart_recommendation(guild_id, nil, recommendation, chosen, "autodj")
  print(("[nexus] Auto-DJ queued '%s' in guild %s (%s)"):format(chosen.info.title, guild_id, recommendation.reason))
  return true
end

local function process_queue_inner(guild_id, channel_id)
  local settings = get_guild_settings(guild_id)
  local row = db_one([[SELECT id, video_url, title, requester_id::text AS requester_id, track_uid
                        FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC LIMIT 1]], guild_id)

  if not row and settings.loop_mode == "queue" then
    if restore_queue_from_backup(guild_id) > 0 then
      row = db_one([[SELECT id, video_url, title, requester_id::text AS requester_id, track_uid
                      FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC LIMIT 1]], guild_id)
    end
  end

  if not row then
    if maybe_enqueue_autodj(guild_id, channel_id) then
      return process_queue(guild_id, channel_id)
    end
    playback[guild_id] = nil
    clear_playback_state(guild_id)
    if not settings.stay_in_vc then
      leave_voice(guild_id)
    end
    return
  end

  local url, title, requester_id, track_uid = row.video_url, row.title, row.requester_id, row.track_uid
  dbq("DELETE FROM nexus_queue WHERE id = %s AND guild_id = %s", row.id, guild_id)

  local track, terr = resolve_track_by_url(url)
  if not track then
    print(("[nexus] resolve failed for '%s' in guild %s: %s"):format(tostring(title), guild_id, tostring(terr)))
    return process_queue(guild_id, channel_id)
  end

  ensure_voice(guild_id, channel_id)

  local filters = {}
  local speed = 1.0
  if (settings.custom_modifiers_left or 0) > 0 then
    filters.timescale = { speed = settings.custom_speed, pitch = settings.custom_pitch }
    speed = settings.custom_speed
    local remaining = settings.custom_modifiers_left - 1
    dbq("UPDATE nexus_guild_settings SET custom_modifiers_left = %s WHERE guild_id = %s", remaining, guild_id)
    if remaining <= 0 then
      dbq("UPDATE nexus_guild_settings SET custom_speed = 1.0, custom_pitch = 1.0 WHERE guild_id = %s", guild_id)
    end
  else
    speed = apply_filter_preset(filters, settings.filter_mode, speed)
    if settings.filter_stack and settings.filter_stack ~= settings.filter_mode and settings.filter_stack ~= "" then
      speed = apply_filter_preset(filters, settings.filter_stack, speed)
    end
  end

  local duration = (track.info and track.info.length or 0) / 1000
  local want_fade = settings.transition_mode == "fade" or settings.transition_mode == "smart" or settings.transition_mode == "mix"
  local start_vol = want_fade and 0 or settings.volume

  bot.lavalink:update_player(guild_id, { volume = start_vol, filters = filters })
  bot.lavalink:play(guild_id, track.encoded)

  update_stage_topic(guild_id, channel_id, title)

  fade_token[guild_id] = (fade_token[guild_id] or 0) + 1
  playback[guild_id] = {
    url = url, title = title, duration = duration, start_time = socket.gettime(), offset = 0,
    paused = false, requester_id = requester_id, track_uid = track_uid, channel_id = channel_id,
    speed = speed, filter_mode = settings.filter_mode, volume = settings.volume, encoded = track.encoded,
    uploader = track.info and track.info.author or "Unknown",
  }

  record_history(guild_id, url, title, requester_id)
  local rok, rerr = pcall(record_track_play_started, guild_id, url, title, requester_id)
  if not rok then print("[nexus] track-intelligence write failed: " .. tostring(rerr)) end
  persist_playback_state(guild_id, channel_id, url, title, 0, true, false, track_uid)

  if want_fade then
    local mytoken = fade_token[guild_id]
    copas.addthread(function() run_fade(guild_id, 0, settings.volume, settings.fade_seconds, settings.fade_curve, mytoken) end)
  end

  local np_fields = { { name = "Track ID", value = "`" .. track_uid:sub(1, 8) .. "`", inline = true } }
  if requester_id then
    table.insert(np_fields, { name = "Requested by", value = resolve_requester_name(guild_id, requester_id), inline = true })
  end
  send_feedback(guild_id, mk_embed({
    title = "\240\159\142\181 Now Playing",
    description = ("**[%s](%s)**\n*By: %s*"):format(title, url, playback[guild_id].uploader),
    color = COLOR.blurple, fields = np_fields,
  }))
end

process_queue = function(guild_id, channel_id)
  local ok, err = pcall(process_queue_inner, guild_id, channel_id)
  if not ok then
    print(("[nexus] process_queue error for guild %s: %s"):format(tostring(guild_id), tostring(err)))
    report_error(guild_id, "runtime", "process_queue error", tostring(err))
  end
end

------------------------------------------------------------------------
-- Lavalink event wiring (track start/end, position updates)
------------------------------------------------------------------------
bot.lavalink:on("event", function(msg)
  local guild_id = msg.guildId
  if not guild_id then return end
  if msg.type == "TrackEndEvent" then
    if msg.reason == "REPLACED" then return end
    local data = playback[guild_id]
    local channel_id = (data and data.channel_id) or voice_channel[guild_id]
    if data then
      local finished = msg.reason == "FINISHED"
      local listen_seconds = current_position(guild_id)
      local ok, err = pcall(record_track_outcome, guild_id, data.url, data.title, data.requester_id, finished, listen_seconds)
      if not ok then print("[nexus] record_track_outcome failed: " .. tostring(err)) end
      if finished then
        local settings = get_guild_settings(guild_id)
        if settings.loop_mode == "song" then
          insert_queue_front(guild_id, data.url, data.title, data.requester_id, data.track_uid)
        elseif settings.loop_mode == "queue" then
          enqueue_track(guild_id, data.url, data.title, data.requester_id, false)
        end
      end
    end
    playback[guild_id] = nil
    if channel_id then
      copas.addthread(function() process_queue(guild_id, channel_id) end)
    end
  elseif msg.type == "TrackExceptionEvent" or msg.type == "TrackStuckEvent" then
    print(("[nexus] playback problem in guild %s: %s (%s)"):format(tostring(guild_id), msg.type,
      (msg.exception and msg.exception.message) or tostring(msg.thresholdMs)))
  end
end)

bot.lavalink:on("playerUpdate", function(msg)
  local data = playback[msg.guildId]
  if data and msg.state and msg.state.position then
    data.offset = msg.state.position / 1000
    data.start_time = socket.gettime()
  end
end)

------------------------------------------------------------------------
-- Lyrics (LRCLIB, no API key -- same source nexus.py uses)
------------------------------------------------------------------------
local function lrc_parse(lrc_text)
  local lines = {}
  for line in (lrc_text or ""):gmatch("[^\r\n]+") do
    local mm, ss, text = line:match("^%[(%d+):(%d+%.?%d*)%](.*)$")
    if mm then
      table.insert(lines, { ts = tonumber(mm) * 60 + tonumber(ss), text = trim(text) })
    end
  end
  table.sort(lines, function(a, b) return a.ts < b.ts end)
  return lines
end

local function fetch_lyrics(title)
  local query = clean_smart_title(title)
  if query == "" then return nil end
  local response_body = {}
  local ok = https.request({
    url = "https://lrclib.net/api/search?q=" .. query:gsub(" ", "%%20"),
    method = "GET",
    sink = ltn12.sink.table(response_body),
  })
  if not ok then return nil end
  local decoded_ok, results = pcall(cjson.decode, table.concat(response_body))
  if not decoded_ok or not results or not results[1] then return nil end
  local best = results[1]
  if best.syncedLyrics and best.syncedLyrics ~= cjson.null then
    return { synced = lrc_parse(best.syncedLyrics), track_name = best.trackName, artist_name = best.artistName }
  elseif best.plainLyrics and best.plainLyrics ~= cjson.null then
    return { plain = best.plainLyrics, track_name = best.trackName, artist_name = best.artistName }
  end
  return nil
end

------------------------------------------------------------------------
-- Panel (embed + buttons)
------------------------------------------------------------------------
local function build_panel_embed(guild_id)
  local settings = get_guild_settings(guild_id)
  local e = mk_embed({ title = "\240\159\142\155\239\184\143 NEXUS Music Control Panel", color = COLOR.grey, footer = "NEXUS Main Music System" })
  local data = playback[guild_id]
  if data then
    local bar = progress_bar(current_position(guild_id), data.duration)
    e.description = nil
    e.fields = {
      { name = "Now Playing", value = ("**[%s](%s)**\n`%s`"):format(data.title, data.url, bar), inline = false },
      { name = "Filter", value = tostring(data.filter_mode or "none"), inline = true },
    }
    if data.requester_id then
      table.insert(e.fields, 2, { name = "Requested by", value = resolve_requester_name(guild_id, data.requester_id), inline = true })
    end
  else
    e.description = "Nothing is playing right now."
    e.fields = {}
  end
  table.insert(e.fields, { name = "Volume", value = tostring(settings.volume) .. "%", inline = true })
  table.insert(e.fields, { name = "Loop", value = tostring(settings.loop_mode), inline = true })
  return e
end

local function panel_components()
  return {
    { type = 1, components = {
      { type = 2, style = 1, label = "\226\143\175\239\184\143 Play/Pause", custom_id = "nexus_panel_playpause" },
      { type = 2, style = 4, label = "\226\143\185\239\184\143 Stop", custom_id = "nexus_panel_stop" },
      { type = 2, style = 2, label = "\226\143\173\239\184\143 Skip", custom_id = "nexus_panel_skip" },
    } },
    { type = 1, components = {
      { type = 2, style = 2, label = "\226\143\170 -10s", custom_id = "nexus_panel_rw10" },
      { type = 2, style = 2, label = "\226\143\169 +10s", custom_id = "nexus_panel_fw10" },
      { type = 2, style = 2, label = "\240\159\148\137 Vol-", custom_id = "nexus_panel_voldown" },
      { type = 2, style = 2, label = "\240\159\148\138 Vol+", custom_id = "nexus_panel_volup" },
    } },
    { type = 1, components = {
      { type = 2, style = 2, label = "\240\159\148\129 Loop", custom_id = "nexus_panel_loop" },
      { type = 2, style = 3, label = "\240\159\148\128 Shuffle", custom_id = "nexus_panel_shuffle" },
      { type = 2, style = 2, label = "\240\159\147\156 View Queue", custom_id = "nexus_panel_queue" },
    } },
  }
end

------------------------------------------------------------------------
-- Slash command registration
------------------------------------------------------------------------
local OPT = { STRING = 3, INTEGER = 4, BOOLEAN = 5, USER = 6, CHANNEL = 7, ROLE = 8, NUMBER = 10 }
local CH_VOICE, CH_STAGE = 2, 13
local CH_TEXT_LIKE = { 0, 5, 10, 11, 12 }

local function cmd(name, description, options)
  return { name = "nexus_main_" .. name, description = description, options = options }
end

------------------------------------------------------------------------
-- Server configuration
------------------------------------------------------------------------
bot:command("nexus_main_sethome", cmd("sethome",
  "Save this bot's default voice or stage channel for join, autoplay, and recovery behavior.",
  { { type = OPT.CHANNEL, name = "channel", description = "Voice or stage channel", required = true, channel_types = { CH_VOICE, CH_STAGE } } }),
  function(_, interaction)
    if not require_admin(interaction) then return end
    local channel_id = opts_map(interaction).channel
    set_home_channel(interaction.guild_id, channel_id)
    reply(interaction, { embed = mk_embed({ title = "\240\159\143\160 Home Set", description = ("Home channel set to <#%s>."):format(channel_id), color = COLOR.green }), ephemeral = true })
  end)

bot:command("nexus_main_setfeedback", cmd("setfeedback",
  "Choose the text channel (or thread) for updates, queue actions, and recovery notices.",
  { { type = OPT.CHANNEL, name = "channel", description = "Text channel or thread", required = true, channel_types = CH_TEXT_LIKE } }),
  function(_, interaction)
    if not require_admin(interaction) then return end
    local channel_id = opts_map(interaction).channel
    update_guild_setting(interaction.guild_id, "feedback_channel_id", channel_id)
    reply(interaction, { embed = mk_embed({ title = "\226\156\133 Feedback Channel Set", description = ("Updates will be sent to <#%s>."):format(channel_id), color = COLOR.green }), ephemeral = true })
  end)

bot:command("nexus_main_djrole", cmd("djrole", "Set the server DJ role that can manage restricted playback, queue, and settings commands.",
  { { type = OPT.ROLE, name = "role", description = "DJ role", required = true } }),
  function(_, interaction)
    if not require_admin(interaction) then return end
    local role_id = opts_map(interaction).role
    update_guild_setting(interaction.guild_id, "dj_role_id", role_id)
    reply(interaction, { embed = simple_embed(("\240\159\142\167 DJ role set to <@&%s>"):format(role_id), COLOR.green), ephemeral = true })
  end)

bot:command("nexus_main_removedj", cmd("removedj", "Clear the configured DJ role so only admins or open-access mode can control restricted commands.", {}),
  function(_, interaction)
    if not require_admin(interaction) then return end
    update_guild_setting(interaction.guild_id, "dj_role_id", nil)
    reply(interaction, { embed = simple_embed("DJ role requirements removed.", COLOR.green), ephemeral = true })
  end)

bot:command("nexus_main_djmode", cmd("djmode", "Enable or disable Strict DJ Mode so only admins and the DJ role can use control commands.", {}),
  function(_, interaction)
    if not require_admin(interaction) then return end
    local new_val = toggle_guild_setting(interaction.guild_id, "dj_only_mode")
    reply(interaction, { embed = simple_embed(("\240\159\142\167 Strict DJ Mode is now **%s**."):format(new_val and "ENABLED" or "DISABLED"), COLOR.green), ephemeral = true })
  end)

bot:command("nexus_main_247", cmd("247", "Keep the bot connected and ready in voice channels even after playback ends until you disable it.", {}),
  function(_, interaction)
    if not require_admin(interaction) then return end
    local new_val = toggle_guild_setting(interaction.guild_id, "stay_in_vc")
    reply(interaction, { embed = simple_embed(("\240\159\149\176\239\184\143 24/7 Mode is now **%s**."):format(new_val and "ENABLED" or "DISABLED"), COLOR.green), ephemeral = true })
  end)

bot:command("nexus_main_restart", cmd("restart", "Restart this bot instance immediately for maintenance or recovery. Administrator only.", {}),
  function(_, interaction)
    if not require_admin(interaction) then return end
    reply(interaction, { content = "Restarting...", ephemeral = true })
    send_webhook_log("\226\153\187\239\184\143 Node Restart", "This node is restarting for **admin_slash_command**. Docker will bring it back online automatically.", COLOR.orange)
    os.exit(0) -- container supervisor (Docker restart policy) brings it back up
  end)

------------------------------------------------------------------------
-- Playback & transport
------------------------------------------------------------------------
local function resolve_target_channel(interaction)
  local home = get_home_channel(interaction.guild_id)
  if home then return home end
  local resolved = interaction.data and interaction.data.resolved
  return nil
end

bot:command("nexus_main_play", cmd("play",
  "Queue a track, URL, livestream, search result, or playlist and start playback if idle.",
  {
    { type = OPT.STRING, name = "search", description = "Track name, URL, or search text", required = true },
    { type = OPT.STRING, name = "source", description = "Where to search (defaults to YouTube for plain text)", required = false,
      choices = { { name = "YouTube", value = "ytmsearch" }, { name = "SoundCloud", value = "scsearch" } } },
  }),
  function(_, interaction)
    defer(interaction, false)
    local o = opts_map(interaction)
    local search, source = o.search, o.source or "ytmsearch"
    if not is_explicit_query(search) then search = source .. ":" .. search end

    local channel_id = get_home_channel(interaction.guild_id)
    if not channel_id then
      channel_id = interaction.member and interaction.member.voice_channel_id
    end
    if not channel_id then
      followup(interaction, { embed = simple_embed("\226\157\140 Join a channel first or set a home channel.", COLOR.red) })
      return
    end

    local entries, is_playlist, err = search_playables(search)
    if not entries or #entries == 0 then
      followup(interaction, { embed = mk_embed({ title = "\226\157\140 Source Error", description = "Could not load that source: " .. tostring(err or "nothing playable came back"), color = COLOR.red }) })
      return
    end

    local requester_id = user_id_of(interaction)
    for _, track in ipairs(entries) do
      enqueue_track(interaction.guild_id, track.info.uri, track.info.title, requester_id, false)
      local ok, ierr = pcall(function()
        dbq([[INSERT INTO nexus_track_intelligence (guild_id, url_key, video_url, title,
                queued_count, play_count, finish_count, skip_count, like_count, dislike_count, total_listen_seconds,
                last_requester_id, last_queued, source)
              VALUES (%s, %s, %s, %s, 1, 0, 0, 0, 0, 0, 0, %s, NOW(), 'queued')
              ON CONFLICT (guild_id, url_key) DO UPDATE SET queued_count = nexus_track_intelligence.queued_count + 1,
                last_requester_id = EXCLUDED.last_requester_id, last_queued = NOW()]],
            interaction.guild_id, url_key(track.info.uri, track.info.title), track.info.uri, track.info.title, requester_id)
      end)
      if not ok then print("[nexus] queued-intelligence write failed: " .. tostring(ierr)) end
    end
    if #entries > 1 then shuffle_queue_rows(interaction.guild_id, true) end
    snapshot_queue_backup(interaction.guild_id)
    local q_len = queue_count(interaction.guild_id)

    local vc_active = playback[interaction.guild_id] ~= nil
    if not vc_active then
      followup(interaction, { embed = mk_embed({ title = "\240\159\142\182 Queued & Starting", description = ("Added **%d** track(s). Starting Lavalink Engine!"):format(#entries), color = COLOR.green }) })
      process_queue(interaction.guild_id, channel_id)
    else
      followup(interaction, { embed = mk_embed({ title = "\240\159\147\165 Added to Queue", description = ("Added **%d** track(s). (Queue size: %d)"):format(#entries, q_len), color = COLOR.blue }) })
    end
  end)

bot:command("nexus_main_playnext", cmd("playnext", "Queue one track to play next, ahead of the existing queue, without clearing current playback.",
  { { type = OPT.STRING, name = "search", description = "Track name or URL", required = true } }),
  function(_, interaction)
    if not require_dj(interaction) then return end
    defer(interaction, true)
    local entries, _, err = search_playables(opts_map(interaction).search)
    if not entries or #entries == 0 then
      followup(interaction, { embed = simple_embed("Could not resolve that source: " .. tostring(err or "not found"), COLOR.red), ephemeral = true })
      return
    end
    local track = entries[1]
    local requester_id = user_id_of(interaction)
    insert_queue_front(interaction.guild_id, track.info.uri, track.info.title, requester_id, new_uid())
    snapshot_queue_backup(interaction.guild_id)
    if not playback[interaction.guild_id] then
      local channel_id = voice_channel[interaction.guild_id] or get_home_channel(interaction.guild_id) or (interaction.member and interaction.member.voice_channel_id)
      if channel_id then process_queue(interaction.guild_id, channel_id) end
    end
    followup(interaction, { embed = simple_embed(("**Playing next:** %s"):format(track.info.title), COLOR.green), ephemeral = true })
  end)

bot:command("nexus_main_skip", cmd("skip", "Skip the current track and move playback to the next queued item or Auto-DJ recommendation.", {}),
  function(_, interaction)
    if not require_dj(interaction) then return end
    if playback[interaction.guild_id] then
      bot.lavalink:stop(interaction.guild_id)
      reply(interaction, { embed = simple_embed("\226\143\173\239\184\143 Skipped", COLOR.blurple), ephemeral = true })
    else
      reply(interaction, { embed = simple_embed("\226\157\140 Nothing is playing.", COLOR.red), ephemeral = true })
    end
  end)

local function do_stop(guild_id)
  playback[guild_id] = nil
  clear_playback_state(guild_id)
  dbq("DELETE FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", guild_id)
  dbq("DELETE FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus'", guild_id)
  leave_voice(guild_id) -- upserts nexus_voice_state (desired_connected = FALSE) itself now
end

bot:command("nexus_main_stop", cmd("stop", "Stop playback, clear the queue, remove recovery state, and reset the bot for this server.", {}),
  function(_, interaction)
    if not require_dj(interaction) then return end
    do_stop(interaction.guild_id)
    reply(interaction, { embed = mk_embed({ title = "\226\143\185\239\184\143 Stopped", description = "Music stopped and cleared.", color = COLOR.red }), ephemeral = true })
  end)

bot:command("nexus_main_sleep", cmd("sleep", "Stop playback and disconnect after N minutes. Use 0 to cancel an active timer.",
  { { type = OPT.INTEGER, name = "minutes", description = "Minutes until playback stops automatically (0 cancels the current timer)", required = true, min_value = 0, max_value = 240 } }),
  function(_, interaction)
    if not require_dj(interaction) then return end
    local minutes = opts_map(interaction).minutes
    local guild_id = interaction.guild_id
    sleep_token[guild_id] = (sleep_token[guild_id] or 0) + 1
    if minutes <= 0 then
      reply(interaction, { embed = simple_embed("\226\143\176 Sleep timer cancelled.", COLOR.orange), ephemeral = true })
      return
    end
    local mytoken = sleep_token[guild_id]
    local channel_id = interaction.channel_id
    copas.addthread(function()
      copas.sleep(minutes * 60)
      if sleep_token[guild_id] ~= mytoken then return end
      do_stop(guild_id)
      send_channel_message(channel_id, { embed = mk_embed({ title = "\240\159\152\180 Sleep Timer", description = "Playback stopped \226\128\148 sleep timer elapsed.", color = COLOR.dark_purple }) })
    end)
    reply(interaction, { embed = simple_embed(("\240\159\152\180 Sleep timer set for **%d** minute(s). Playback will stop automatically."):format(minutes), COLOR.blue), ephemeral = true })
  end)

bot:command("nexus_main_pause", cmd("pause", "Pause the current track without clearing the queue or playback position.", {}),
  function(_, interaction)
    if not require_dj(interaction) then return end
    local data = playback[interaction.guild_id]
    if data and not data.paused then
      data.offset = current_position(interaction.guild_id)
      data.paused = true
      bot.lavalink:set_paused(interaction.guild_id, true)
      reply(interaction, { embed = simple_embed("\226\143\184\239\184\143 Paused", COLOR.blue), ephemeral = true })
    else
      reply(interaction, { embed = simple_embed("\226\157\140 Nothing is currently playing.", COLOR.red), ephemeral = true })
    end
  end)

bot:command("nexus_main_resume", cmd("resume", "Resume the paused track from its current playback position.", {}),
  function(_, interaction)
    if not require_dj(interaction) then return end
    local data = playback[interaction.guild_id]
    if data and data.paused then
      data.paused = false
      data.start_time = socket.gettime()
      bot.lavalink:set_paused(interaction.guild_id, false)
      reply(interaction, { embed = simple_embed("\226\150\182\239\184\143 Resumed", COLOR.green), ephemeral = true })
    else
      reply(interaction, { embed = simple_embed("\226\157\140 Nothing is currently paused.", COLOR.red), ephemeral = true })
    end
  end)

bot:command("nexus_main_clear", cmd("clear", "Clear the upcoming queue, stop playback, and reset stored playback state for this server.", {}),
  function(_, interaction)
    if not require_dj(interaction) then return end
    do_stop(interaction.guild_id)
    reply(interaction, { embed = simple_embed("\240\159\151\145\239\184\143 Playback stopped and queue cleared.", COLOR.red), ephemeral = true })
  end)

bot:command("nexus_main_join", cmd("join", "Force the bot to join your current voice channel, or its configured home channel if one is saved.", {}),
  function(_, interaction)
    defer(interaction, true)
    local channel_id = get_home_channel(interaction.guild_id) or (interaction.member and interaction.member.voice_channel_id)
    if channel_id then
      ensure_voice(interaction.guild_id, channel_id)
      followup(interaction, { embed = simple_embed(("Joined <#%s>."):format(channel_id), COLOR.green), ephemeral = true })
    else
      followup(interaction, { content = "Join a channel first, or set a home channel.", ephemeral = true })
    end
  end)

bot:command("nexus_main_leave", cmd("leave", "Disconnect the bot from voice and clear any pending recovery handoff for this server.", {}),
  function(_, interaction)
    if not require_dj(interaction) then return end
    playback[interaction.guild_id] = nil
    clear_playback_state(interaction.guild_id)
    leave_voice(interaction.guild_id)
    reply(interaction, { embed = simple_embed("\240\159\145\139 Disconnected.", COLOR.blurple), ephemeral = true })
  end)

bot:command("nexus_main_nowplaying", cmd("nowplaying", "Show the current track, progress bar, requester, and live playback status.", {}),
  function(_, interaction)
    local data = playback[interaction.guild_id]
    if not data then
      reply(interaction, { embed = simple_embed("Nothing playing.", COLOR.red), ephemeral = true })
      return
    end
    local fields = { { name = "Filter", value = tostring(data.filter_mode or "none"), inline = true } }
    if data.requester_id then
      table.insert(fields, 1, { name = "Requested by", value = resolve_requester_name(interaction.guild_id, data.requester_id), inline = true })
    end
    reply(interaction, { embed = mk_embed({
      title = "\240\159\142\181 Now Playing",
      description = ("**[%s](%s)**\n\n`%s`"):format(data.title, data.url, progress_bar(current_position(interaction.guild_id), data.duration)),
      color = COLOR.blue, fields = fields,
    }), ephemeral = true })
  end)

------------------------------------------------------------------------
-- Queue management
------------------------------------------------------------------------
bot:command("nexus_main_queue", cmd("queue", "Show the current queue with paging, requester names, and track positions",
  { { type = OPT.INTEGER, name = "page", description = "Page number", required = false, min_value = 1 } }),
  function(_, interaction)
    defer(interaction, true)
    local page = math.max(1, tonumber(opts_map(interaction).page) or 1)
    local per_page = 10
    local offset = (page - 1) * per_page
    local total = queue_count(interaction.guild_id)
    local rows = dbq([[SELECT title, requester_id::text AS requester_id FROM nexus_queue
                        WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC LIMIT %s OFFSET %s]],
        interaction.guild_id, per_page, offset) or {}
    if #rows == 0 then
      followup(interaction, { embed = simple_embed("Queue empty.", COLOR.red), ephemeral = true })
      return
    end
    local lines = {}
    for i, r in ipairs(rows) do
      local rname = r.requester_id and resolve_requester_name(interaction.guild_id, r.requester_id) or "Auto-DJ"
      table.insert(lines, ("**%d.** %s \226\128\148 *%s*"):format(offset + i, r.title, rname))
    end
    local pages = math.max(1, math.ceil(total / per_page))
    followup(interaction, { embed = mk_embed({ title = "\240\159\147\156 Queue", description = table.concat(lines, "\n"), color = COLOR.blurple,
      footer = ("Page %d/%d \226\128\162 %d queued track(s)"):format(page, pages, total) }), ephemeral = true })
  end)

bot:command("nexus_main_shuffle", cmd("shuffle", "Smart-shuffle the upcoming queue while keeping duplicate tracks separated when possible.", {}),
  function(_, interaction)
    if not require_dj(interaction) then return end
    local n = shuffle_queue_rows(interaction.guild_id, false)
    if n <= 0 then
      reply(interaction, { content = "Queue empty.", ephemeral = true })
      return
    end
    snapshot_queue_backup(interaction.guild_id)
    reply(interaction, { embed = simple_embed(("\240\159\148\128 Smart-shuffled %d queued track(s)."):format(n), COLOR.green), ephemeral = true })
  end)

local function queue_row_at(guild_id, index)
  return db_one([[SELECT id, video_url, title, requester_id::text AS requester_id, track_uid
                   FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC LIMIT 1 OFFSET %s]],
      guild_id, index - 1)
end

bot:command("nexus_main_remove", cmd("remove", "Remove a queued track by its queue number so it will not play later.",
  { { type = OPT.INTEGER, name = "index", description = "Queue position", required = true, min_value = 1 } }),
  function(_, interaction)
    if not require_dj(interaction) then return end
    local index = opts_map(interaction).index
    local row = queue_row_at(interaction.guild_id, index)
    if not row then
      reply(interaction, { content = "Invalid index.", ephemeral = true })
      return
    end
    dbq("DELETE FROM nexus_queue WHERE id = %s AND guild_id = %s", row.id, interaction.guild_id)
    dbq("DELETE FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus' AND track_uid = %s", interaction.guild_id, row.track_uid)
    reply(interaction, { embed = simple_embed(("Removed item #%d"):format(index), COLOR.green), ephemeral = true })
  end)

bot:command("nexus_main_skipto", cmd("skipto", "Drop everything before a chosen queue position and jump playback forward to that track.",
  { { type = OPT.INTEGER, name = "index", description = "Queue position", required = true, min_value = 1 } }),
  function(_, interaction)
    if not require_dj(interaction) then return end
    defer(interaction, true)
    local index = opts_map(interaction).index
    local skip_n = math.max(0, index - 1)
    if skip_n > 0 then
      local rows = dbq([[SELECT track_uid FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC LIMIT %s]], interaction.guild_id, skip_n) or {}
      dbq([[DELETE FROM nexus_queue WHERE id IN (
              SELECT id FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC LIMIT %s
            )]], interaction.guild_id, skip_n)
      for _, r in ipairs(rows) do
        dbq("DELETE FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus' AND track_uid = %s", interaction.guild_id, r.track_uid)
      end
    end
    if playback[interaction.guild_id] then bot.lavalink:stop(interaction.guild_id) end
    followup(interaction, { embed = simple_embed(("Skipped to #%d"):format(index), COLOR.green), ephemeral = true })
  end)

bot:command("nexus_main_move", cmd("move", "Move a queued track from one queue slot to another without rebuilding the entire session manually.",
  { { type = OPT.INTEGER, name = "frm", description = "From position", required = true, min_value = 1 },
    { type = OPT.INTEGER, name = "to", description = "To position", required = true, min_value = 1 } }),
  function(_, interaction)
    if not require_dj(interaction) then return end
    defer(interaction, true)
    local o = opts_map(interaction)
    local rows = dbq([[SELECT id, video_url, title, requester_id::text AS requester_id, track_uid
                        FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC]], interaction.guild_id) or {}
    if o.frm > #rows or o.to > #rows or o.frm < 1 or o.to < 1 then
      followup(interaction, { content = "Invalid index", ephemeral = true })
      return
    end
    local item = table.remove(rows, o.frm)
    table.insert(rows, math.max(1, math.min(o.to, #rows + 1)), item)
    dbq("DELETE FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", interaction.guild_id)
    for _, r in ipairs(rows) do
      dbq("INSERT INTO nexus_queue (guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, 'nexus', %s, %s, %s, %s)",
          interaction.guild_id, r.video_url, r.title, r.requester_id, r.track_uid)
    end
    snapshot_queue_backup(interaction.guild_id)
    followup(interaction, { embed = simple_embed(("Moved item from %d to %d"):format(o.frm, o.to), COLOR.green), ephemeral = true })
  end)

bot:command("nexus_main_bump", cmd("bump", "Move a queued track to the front so it plays next",
  { { type = OPT.INTEGER, name = "index", description = "Queue position", required = true, min_value = 1 } }),
  function(_, interaction)
    if not require_dj(interaction) then return end
    local index = opts_map(interaction).index
    local row = queue_row_at(interaction.guild_id, index)
    if not row then
      reply(interaction, { content = "Invalid index.", ephemeral = true })
      return
    end
    dbq("DELETE FROM nexus_queue WHERE id = %s AND guild_id = %s", row.id, interaction.guild_id)
    insert_queue_front(interaction.guild_id, row.video_url, row.title, row.requester_id, row.track_uid)
    snapshot_queue_backup(interaction.guild_id)
    reply(interaction, { embed = simple_embed(("\226\172\134\239\184\143 Moved **%s** to play next."):format(row.title), COLOR.green), ephemeral = true })
  end)

bot:command("nexus_main_clearmine", cmd("clearmine", "Remove your own queued songs without touching other listeners' tracks", {}),
  function(_, interaction)
    defer(interaction, true)
    local uid = user_id_of(interaction)
    local row = db_one("SELECT COUNT(*) AS n FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' AND requester_id = %s", interaction.guild_id, uid)
    local removed = row and math.floor(row.n) or 0
    if removed > 0 then
      dbq("DELETE FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' AND requester_id = %s", interaction.guild_id, uid)
      dbq("DELETE FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus' AND requester_id = %s", interaction.guild_id, uid)
    end
    followup(interaction, { embed = simple_embed(("\240\159\167\185 Removed **%d** of your queued track(s)."):format(removed), COLOR.green), ephemeral = true })
  end)

bot:command("nexus_main_voteskip", cmd("voteskip", "Start or join a vote skip when no DJ is around to skip directly", {}),
  function(_, interaction)
    if not playback[interaction.guild_id] then
      reply(interaction, { content = "Nothing is playing right now.", ephemeral = true })
      return
    end
    if is_dj(interaction) then
      bot.lavalink:stop(interaction.guild_id)
      reply(interaction, { embed = simple_embed("\226\143\173\239\184\143 DJ override used: skipped the current track.", COLOR.green), ephemeral = true })
      return
    end
    local uid = user_id_of(interaction)
    local votes = vote_skip_sessions[interaction.guild_id] or {}
    vote_skip_sessions[interaction.guild_id] = votes
    votes[uid] = true
    local count = 0
    for _ in pairs(votes) do count = count + 1 end
    local required = 2 -- listener roster isn't tracked without a voice-state member cache; fixed small quorum
    if count >= required then
      vote_skip_sessions[interaction.guild_id] = nil
      bot.lavalink:stop(interaction.guild_id)
      reply(interaction, { embed = simple_embed(("\226\143\173\239\184\143 Vote skip passed with **%d/%d** votes."):format(count, required), COLOR.green) })
    else
      reply(interaction, { embed = simple_embed(("\240\159\151\179\239\184\143 Vote recorded: **%d/%d** votes to skip."):format(count, required), COLOR.blurple), ephemeral = true })
    end
  end)

bot:command("nexus_main_upvote", cmd("upvote", "Upvote a queued track; enough listener upvotes bump it to play next.",
  { { type = OPT.INTEGER, name = "index", description = "Queue position", required = true, min_value = 1 } }),
  function(_, interaction)
    local index = opts_map(interaction).index
    local row = queue_row_at(interaction.guild_id, index)
    if not row then
      reply(interaction, { content = "Invalid index.", ephemeral = true })
      return
    end
    local key = interaction.guild_id .. ":" .. row.id
    local votes = queue_vote_state[key] or { up = {}, down = {} }
    queue_vote_state[key] = votes
    votes.down[user_id_of(interaction)] = nil
    votes.up[user_id_of(interaction)] = true
    local up_count = 0
    for _ in pairs(votes.up) do up_count = up_count + 1 end
    local required = 2
    if up_count >= required then
      queue_vote_state[key] = nil
      dbq("DELETE FROM nexus_queue WHERE id = %s AND guild_id = %s", row.id, interaction.guild_id)
      insert_queue_front(interaction.guild_id, row.video_url, row.title, row.requester_id, row.track_uid)
      snapshot_queue_backup(interaction.guild_id)
      reply(interaction, { embed = simple_embed(("\226\172\134\239\184\143 **%s** got enough upvotes and will play next!"):format(row.title), COLOR.green) })
    else
      reply(interaction, { embed = simple_embed(("\240\159\148\186 Upvoted **#%d** (%d/%d) to play next."):format(index, up_count, required), COLOR.blurple), ephemeral = true })
    end
  end)

bot:command("nexus_main_downvote", cmd("downvote", "Downvote a queued track; enough listener downvotes removes it from the queue.",
  { { type = OPT.INTEGER, name = "index", description = "Queue position", required = true, min_value = 1 } }),
  function(_, interaction)
    local index = opts_map(interaction).index
    local row = queue_row_at(interaction.guild_id, index)
    if not row then
      reply(interaction, { content = "Invalid index.", ephemeral = true })
      return
    end
    local key = interaction.guild_id .. ":" .. row.id
    local votes = queue_vote_state[key] or { up = {}, down = {} }
    queue_vote_state[key] = votes
    votes.up[user_id_of(interaction)] = nil
    votes.down[user_id_of(interaction)] = true
    local down_count = 0
    for _ in pairs(votes.down) do down_count = down_count + 1 end
    local required = 2
    if down_count >= required then
      queue_vote_state[key] = nil
      dbq("DELETE FROM nexus_queue WHERE id = %s AND guild_id = %s", row.id, interaction.guild_id)
      dbq("DELETE FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus' AND track_uid = %s", interaction.guild_id, row.track_uid)
      reply(interaction, { embed = simple_embed(("\226\172\135\239\184\143 **%s** got enough downvotes and was removed from the queue."):format(row.title), COLOR.orange) })
    else
      reply(interaction, { embed = simple_embed(("\240\159\148\187 Downvoted **#%d** (%d/%d)."):format(index, down_count, required), COLOR.blurple), ephemeral = true })
    end
  end)

bot:command("nexus_main_loop", cmd("loop", "Choose whether playback loops nothing, the current song, or the full queue.",
  { { type = OPT.STRING, name = "mode", description = "Loop mode", required = true,
    choices = { { name = "Off", value = "off" }, { name = "Song", value = "song" }, { name = "Queue", value = "queue" } } } }),
  function(_, interaction)
    if not require_dj(interaction) then return end
    local mode = opts_map(interaction).mode
    ensure_guild_settings(interaction.guild_id)
    update_guild_setting(interaction.guild_id, "loop_mode", mode)
    reply(interaction, { embed = simple_embed("\240\159\148\129 Looping set to: " .. mode, COLOR.green), ephemeral = true })
  end)

------------------------------------------------------------------------
-- Radio (mood-based multi-queue)
------------------------------------------------------------------------
bot:command("nexus_main_radio", cmd("radio", "Queue several tracks matching a mood or vibe, e.g. 'chill lofi' or 'workout hype'.",
  { { type = OPT.STRING, name = "mood", description = "Mood or vibe", required = true },
    { type = OPT.INTEGER, name = "count", description = "How many tracks", required = false, min_value = 1, max_value = 10 } }),
  function(_, interaction)
    defer(interaction, false)
    local o = opts_map(interaction)
    local mood = trim(o.mood)
    local count = o.count or 5
    local channel_id = interaction.member and interaction.member.voice_channel_id
    if not channel_id then
      followup(interaction, { embed = simple_embed("Join a voice channel first.", COLOR.red) })
      return
    end
    local entries, _, err = search_playables("ytmsearch:" .. mood:sub(1, 100) .. " mix")
    if not entries or #entries == 0 then
      followup(interaction, { embed = simple_embed(("Couldn't find tracks for **%s**: %s"):format(mood, tostring(err or "no results")), COLOR.red) })
      return
    end
    local requester_id = user_id_of(interaction)
    local n = 0
    for i = 1, math.min(count, #entries) do
      enqueue_track(interaction.guild_id, entries[i].info.uri, entries[i].info.title, requester_id, true)
      n = n + 1
    end
    if not playback[interaction.guild_id] then process_queue(interaction.guild_id, channel_id) end
    followup(interaction, { embed = mk_embed({ title = "\240\159\147\187 Mood Radio", description = ("Queued **%d** track(s) for the vibe: *%s*."):format(n, mood), color = COLOR.green }) })
  end)

------------------------------------------------------------------------
-- Smart Auto-DJ & discovery
------------------------------------------------------------------------
bot:command("nexus_main_autodj", cmd("autodj", "Enable or disable smarter Auto-DJ recommendations when the queue runs dry",
  { { type = OPT.BOOLEAN, name = "enabled", description = "Enable Auto-DJ", required = true } }),
  function(_, interaction)
    if not require_dj(interaction) then return end
    defer(interaction, true)
    local enabled = opts_map(interaction).enabled
    set_autodj_enabled(interaction.guild_id, enabled)
    followup(interaction, { embed = simple_embed(("\240\159\147\187 Auto-DJ is now **%s**."):format(enabled and "enabled" or "disabled"), COLOR.green), ephemeral = true })
  end)

bot:command("nexus_main_settings", cmd("settings", "Show the saved playback, DJ, queue, and recovery settings for this server", {}),
  function(_, interaction)
    defer(interaction, true)
    local s = get_guild_settings(interaction.guild_id)
    local autodj_state = get_autodj_enabled(interaction.guild_id)
    followup(interaction, { embed = mk_embed({
      title = "\226\154\153\239\184\143 Server Music Settings", color = COLOR.blurple,
      fields = {
        { name = "Home Channel", value = s.home_vc_id and ("<#" .. s.home_vc_id .. ">") or "Not set", inline = true },
        { name = "Feedback Channel", value = s.feedback_channel_id and ("<#" .. s.feedback_channel_id .. ">") or "Not set", inline = true },
        { name = "DJ Role", value = s.dj_role_id and ("<@&" .. s.dj_role_id .. ">") or "Not set", inline = true },
        { name = "Volume", value = tostring(s.volume), inline = true },
        { name = "Loop Mode", value = tostring(s.loop_mode), inline = true },
        { name = "Filter", value = tostring(s.filter_mode), inline = true },
        { name = "Transitions", value = tostring(s.transition_mode), inline = true },
        { name = "Custom Speed/Pitch", value = ("%sx / %sx (%s left)"):format(s.custom_speed, s.custom_pitch, s.custom_modifiers_left), inline = true },
        { name = "Strict DJ", value = s.dj_only_mode and "Enabled" or "Disabled", inline = true },
        { name = "24/7 Mode", value = s.stay_in_vc and "Enabled" or "Disabled", inline = true },
        { name = "Auto-DJ", value = autodj_state and "Enabled" or "Disabled", inline = true },
      },
    }), ephemeral = true })
  end)

------------------------------------------------------------------------
-- Playlists & history
------------------------------------------------------------------------
bot:command("nexus_main_playlists", cmd("playlists", "List your saved personal playlists and how many tracks each one contains", {}),
  function(_, interaction)
    local uid = user_id_of(interaction)
    local rows = dbq("SELECT playlist_name, COUNT(*) AS n FROM nexus_user_playlists WHERE user_id = %s GROUP BY playlist_name ORDER BY playlist_name ASC", uid) or {}
    if #rows == 0 then
      reply(interaction, { content = "You do not have any saved playlists yet.", ephemeral = true })
      return
    end
    local lines = {}
    for i = 1, math.min(20, #rows) do
      table.insert(lines, ("\226\128\162 **%s** \226\128\148 %d track(s)"):format(rows[i].playlist_name, math.floor(rows[i].n)))
    end
    reply(interaction, { embed = mk_embed({ title = "\240\159\142\188 Your Saved Playlists", description = table.concat(lines, "\n"), color = COLOR.blurple }), ephemeral = true })
  end)

bot:command("nexus_main_deleteplaylist", cmd("deleteplaylist", "Delete one of your saved personal playlists by name",
  { { type = OPT.STRING, name = "name", description = "Playlist name", required = true } }),
  function(_, interaction)
    local uid = user_id_of(interaction)
    local name = opts_map(interaction).name
    local row = db_one("SELECT COUNT(*) AS n FROM nexus_user_playlists WHERE user_id = %s AND playlist_name = %s", uid, name)
    local count = row and math.floor(row.n) or 0
    if count == 0 then
      reply(interaction, { content = "That playlist was not found.", ephemeral = true })
      return
    end
    dbq("DELETE FROM nexus_user_playlists WHERE user_id = %s AND playlist_name = %s", uid, name)
    reply(interaction, { embed = simple_embed(("\240\159\151\145\239\184\143 Deleted **%s** (%d track(s))."):format(name, count), COLOR.green), ephemeral = true })
  end)

bot:command("nexus_main_savequeue", cmd("savequeue", "Save the current queue to one of your personal playlists so you can load it again later.",
  { { type = OPT.STRING, name = "name", description = "Playlist name", required = true } }),
  function(_, interaction)
    defer(interaction, true)
    local uid = user_id_of(interaction)
    local name = opts_map(interaction).name
    local rows = dbq("SELECT video_url, title FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC", interaction.guild_id) or {}
    if #rows == 0 then
      followup(interaction, { content = "Queue is empty!", ephemeral = true })
      return
    end
    for _, r in ipairs(rows) do
      dbq("INSERT INTO nexus_user_playlists (user_id, playlist_name, video_url, title) VALUES (%s, %s, %s, %s)", uid, name, r.video_url, r.title)
    end
    followup(interaction, { embed = simple_embed(("\240\159\146\190 Saved **%d** tracks to your personal playlist: **%s**"):format(#rows, name), COLOR.green), ephemeral = true })
  end)

bot:command("nexus_main_loadqueue", cmd("loadqueue", "Load one of your saved personal playlists into the active queue and start playback if needed.",
  { { type = OPT.STRING, name = "name", description = "Playlist name", required = true } }),
  function(_, interaction)
    defer(interaction, true)
    local uid = user_id_of(interaction)
    local name = opts_map(interaction).name
    local rows = dbq("SELECT video_url, title FROM nexus_user_playlists WHERE user_id = %s AND playlist_name = %s", uid, name) or {}
    if #rows == 0 then
      followup(interaction, { content = "Playlist not found or empty.", ephemeral = true })
      return
    end
    for _, r in ipairs(rows) do
      enqueue_track(interaction.guild_id, r.video_url, r.title, uid, false)
    end
    snapshot_queue_backup(interaction.guild_id)
    followup(interaction, { embed = simple_embed(("\240\159\147\130 Loaded **%d** tracks from **%s** into the queue!"):format(#rows, name), COLOR.green), ephemeral = true })
    if not playback[interaction.guild_id] then
      local channel_id = interaction.member and interaction.member.voice_channel_id
      if channel_id then process_queue(interaction.guild_id, channel_id) end
    end
  end)

bot:command("nexus_main_leaderboard", cmd("leaderboard", "Show the most played tracks from this server based on stored playback history.", {}),
  function(_, interaction)
    defer(interaction, true)
    local rows = dbq("SELECT title, COUNT(*) AS plays FROM nexus_history WHERE guild_id = %s GROUP BY title ORDER BY plays DESC LIMIT 10", interaction.guild_id) or {}
    if #rows == 0 then
      followup(interaction, { content = "No play history yet.", ephemeral = true })
      return
    end
    local lines = {}
    for i, r in ipairs(rows) do table.insert(lines, ("**%d.** %s *(Played %d times)*"):format(i, r.title, math.floor(r.plays))) end
    followup(interaction, { embed = mk_embed({ title = "\240\159\143\134 Server Top Tracks", description = table.concat(lines, "\n"), color = COLOR.gold }), ephemeral = true })
  end)

bot:command("nexus_main_history", cmd("history", "Show the most recent tracks played in this server from playback history.", {}),
  function(_, interaction)
    defer(interaction, true)
    local rows = dbq("SELECT title FROM nexus_history WHERE guild_id = %s ORDER BY played_at DESC LIMIT 5", interaction.guild_id) or {}
    if #rows == 0 then
      followup(interaction, { content = "No history.", ephemeral = true })
      return
    end
    local lines = {}
    for _, r in ipairs(rows) do table.insert(lines, "- " .. r.title) end
    followup(interaction, { embed = mk_embed({ title = "\240\159\147\156 History", description = table.concat(lines, "\n"), color = COLOR.blurple }), ephemeral = true })
  end)

bot:command("nexus_main_userhistory", cmd("userhistory", "Show the most recent tracks requested by a specific user in this server.",
  { { type = OPT.USER, name = "member", description = "Member", required = true } }),
  function(_, interaction)
    local member_id = opts_map(interaction).member
    local rows = dbq([[SELECT id, title, video_url FROM nexus_history WHERE guild_id = %s AND requester_id = %s
                        ORDER BY played_at DESC LIMIT 10]], interaction.guild_id, member_id) or {}
    local name = resolved_display_name(interaction, member_id)
    if #rows == 0 then
      reply(interaction, { embed = simple_embed(("\240\159\147\173 %s hasn't queued any songs yet."):format(name), COLOR.red), ephemeral = true })
      return
    end
    local lines = {}
    for i, r in ipairs(rows) do table.insert(lines, ("**%d.** [%s](%s)"):format(i, r.title, r.video_url)) end
    reply(interaction, { embed = mk_embed({ title = ("\240\159\142\167 %s's Play History"):format(name), description = table.concat(lines, "\n"), color = COLOR.blue,
      footer = "Use /nexus_main_steal <user> <number> to add one to the queue!" }), ephemeral = true })
  end)

bot:command("nexus_main_steal", cmd("steal", "Copy a track from a member's request history and add it back into the queue.",
  { { type = OPT.USER, name = "member", description = "Member", required = true },
    { type = OPT.INTEGER, name = "track_number", description = "History position", required = true, min_value = 1 } }),
  function(_, interaction)
    local o = opts_map(interaction)
    local song = db_one([[SELECT video_url, title FROM nexus_history WHERE guild_id = %s AND requester_id = %s
                           ORDER BY played_at DESC LIMIT 1 OFFSET %s]], interaction.guild_id, o.member, o.track_number - 1)
    if not song then
      reply(interaction, { embed = simple_embed(("\226\157\140 Could not find track #%d in their history."):format(o.track_number), COLOR.red), ephemeral = true })
      return
    end
    local requester_id = user_id_of(interaction)
    enqueue_track(interaction.guild_id, song.video_url, song.title, requester_id, true)
    reply(interaction, { embed = mk_embed({ title = "\240\159\165\183 Song Stolen!", description = ("Added **%s** to the queue from %s's history."):format(song.title, resolved_display_name(interaction, o.member)), color = COLOR.green }) })
    if not playback[interaction.guild_id] then
      local channel_id = interaction.member and interaction.member.voice_channel_id
      if channel_id then process_queue(interaction.guild_id, channel_id) end
    end
  end)

bot:command("nexus_main_grab", cmd("grab", "Send yourself the currently playing track in a direct message for easy saving or sharing.", {}),
  function(_, interaction)
    defer(interaction, true)
    local data = playback[interaction.guild_id]
    if not data then
      followup(interaction, { embed = simple_embed("Nothing is currently playing.", COLOR.red), ephemeral = true })
      return
    end
    local name = display_name_of(interaction)
    local ok = send_dm(user_id_of(interaction), { embed = mk_embed({
      title = "\240\159\142\181 Track Saved!",
      description = ("Hey **%s**!\nHere is the track you wanted to save:\n\n**[%s](%s)**"):format(name, data.title, data.url),
      color = COLOR.blurple,
    }) })
    if ok then
      followup(interaction, { embed = simple_embed("\240\159\147\172 Check your DMs!", COLOR.green), ephemeral = true })
    else
      followup(interaction, { embed = simple_embed("\226\157\140 I can't DM you! Please check your privacy settings.", COLOR.red), ephemeral = true })
    end
  end)

------------------------------------------------------------------------
-- Taste / like / dislike / recommend
------------------------------------------------------------------------
local function current_track_snapshot(guild_id)
  local data = playback[guild_id]
  if data then return { url = data.url, title = data.title, track_uid = data.track_uid } end
  local row = db_one([[SELECT video_url, title, track_uid FROM nexus_playback_state
                        WHERE guild_id = %s AND bot_name = 'nexus' AND (is_playing = TRUE OR is_paused = TRUE)
                        ORDER BY is_playing DESC LIMIT 1]], guild_id)
  if not row then return nil end
  return { url = row.video_url, title = row.title, track_uid = row.track_uid }
end

bot:command("nexus_main_like", cmd("like", "Teach Auto-DJ to play more tracks like the current one for you.", {}),
  function(_, interaction)
    local snap = current_track_snapshot(interaction.guild_id)
    if not snap then
      reply(interaction, { content = "Nothing is playing right now.", ephemeral = true })
      return
    end
    record_track_feedback(interaction.guild_id, user_id_of(interaction), snap.url, snap.title, true)
    reply(interaction, { embed = simple_embed(("Saved your like for **%s**. Auto-DJ will lean toward similar picks."):format(snap.title or "this track"), COLOR.green), ephemeral = true })
  end)

bot:command("nexus_main_dislike", cmd("dislike", "Teach Auto-DJ to avoid the current track for your future recommendations.", {}),
  function(_, interaction)
    local snap = current_track_snapshot(interaction.guild_id)
    if not snap then
      reply(interaction, { content = "Nothing is playing right now.", ephemeral = true })
      return
    end
    record_track_feedback(interaction.guild_id, user_id_of(interaction), snap.url, snap.title, false)
    reply(interaction, { embed = simple_embed(("Saved your dislike for **%s**. Auto-DJ will avoid it for you."):format(snap.title or "this track"), COLOR.orange), ephemeral = true })
  end)

bot:command("nexus_main_recommend", cmd("recommend", "Queue a smart recommendation learned from server taste, playlists, and listener feedback.",
  { { type = OPT.USER, name = "member", description = "Whose taste to use", required = false } }),
  function(_, interaction)
    defer(interaction, false)
    local o = opts_map(interaction)
    local target_id = o.member or user_id_of(interaction)
    local channel_id = voice_channel[interaction.guild_id] or get_home_channel(interaction.guild_id) or (interaction.member and interaction.member.voice_channel_id)
    if not channel_id then
      followup(interaction, { embed = simple_embed("Join a channel first or set a home channel.", COLOR.red) })
      return
    end
    local chosen, recommendation = pick_smart_recommendation_track(interaction.guild_id, { target_id })
    if not chosen then
      followup(interaction, { embed = simple_embed("I could not find a recommendation yet. Play a few tracks or save a playlist first.", COLOR.red) })
      return
    end
    local requester_id = user_id_of(interaction)
    enqueue_track(interaction.guild_id, chosen.info.uri, chosen.info.title, requester_id, true)
    record_smart_recommendation(interaction.guild_id, requester_id, recommendation, chosen, "slash:" .. tostring(target_id))
    local title
    if not playback[interaction.guild_id] then
      process_queue(interaction.guild_id, channel_id)
      title = "Smart Recommendation Starting"
    else
      title = "Smart Recommendation Queued"
    end
    followup(interaction, { embed = mk_embed({ title = title, description = ("Added **%s** based on **%s**. Queue size: %d"):format(chosen.info.title, recommendation.reason or "saved taste", queue_count(interaction.guild_id)), color = COLOR.green }) })
  end)

bot:command("nexus_main_taste", cmd("taste", "Show the saved taste profile Auto-DJ has learned for you or another member.",
  { { type = OPT.USER, name = "member", description = "Member", required = false } }),
  function(_, interaction)
    local o = opts_map(interaction)
    local target_id = o.member or user_id_of(interaction)
    local name = o.member and resolved_display_name(interaction, o.member) or display_name_of(interaction)
    local top = dbq([[SELECT title, video_url, play_count, finish_count, like_count, dislike_count, skip_count, score
                       FROM nexus_user_track_affinity WHERE guild_id = %s AND user_id = %s
                       ORDER BY score DESC, last_requested DESC LIMIT 10]], interaction.guild_id, target_id) or {}
    if #top == 0 then
      reply(interaction, { content = ("No saved taste profile for %s yet."):format(name), ephemeral = true })
      return
    end
    local totals = db_one([[SELECT COALESCE(SUM(play_count),0) AS played, COALESCE(SUM(finish_count),0) AS finished,
                             COALESCE(SUM(like_count),0) AS liked, COALESCE(SUM(dislike_count),0) AS disliked,
                             COALESCE(SUM(skip_count),0) AS skipped FROM nexus_user_track_affinity
                             WHERE guild_id = %s AND user_id = %s]], interaction.guild_id, target_id)
    local lines = {}
    for i = 1, math.min(8, #top) do
      local t = top[i]
      table.insert(lines, ("**%d.** %s *(score %.1f)*"):format(i, clean_smart_title(t.title), tonumber(t.score) or 0))
    end
    reply(interaction, { embed = mk_embed({
      title = "NEXUS Taste Profile: " .. name, description = table.concat(lines, "\n"), color = COLOR.blurple,
      fields = { { name = "Signals", value = ("plays %d | finishes %d | likes %d | dislikes %d | skips %d"):format(
        totals.played, totals.finished, totals.liked, totals.disliked, totals.skipped), inline = false } },
    }), ephemeral = true })
  end)

------------------------------------------------------------------------
-- Audio & filters
------------------------------------------------------------------------
bot:command("nexus_main_volume", cmd("volume", "Set the playback volume for this server from 1 to 200 percent.",
  { { type = OPT.INTEGER, name = "vol", description = "Volume percent", required = true, min_value = 1, max_value = 200 } }),
  function(_, interaction)
    if not require_dj(interaction) then return end
    local vol = math.max(1, math.min(200, opts_map(interaction).vol))
    fade_token[interaction.guild_id] = (fade_token[interaction.guild_id] or 0) + 1 -- cancel any in-flight fade
    ensure_guild_settings(interaction.guild_id)
    update_guild_setting(interaction.guild_id, "volume", vol)
    if playback[interaction.guild_id] then
      playback[interaction.guild_id].volume = vol
      bot.lavalink:set_volume(interaction.guild_id, vol)
    end
    reply(interaction, { embed = simple_embed(("\240\159\148\138 Volume set to %d%%"):format(vol), COLOR.green), ephemeral = true })
  end)

bot:command("nexus_main_filter", cmd("filter", "Apply an audio filter such as nightcore, vaporwave, or bass boost to upcoming playback.",
  { { type = OPT.STRING, name = "mode", description = "Choose an audio filter to apply", required = true, choices = FILTER_PRESET_CHOICES },
    { type = OPT.STRING, name = "stack", description = "Optional second filter to layer on top (e.g. bassboost + nightcore)", required = false, choices = FILTER_PRESET_CHOICES } }),
  function(_, interaction)
    if not require_dj(interaction) then return end
    local o = opts_map(interaction)
    if not FILTER_PRESET_SET[o.mode] then
      reply(interaction, { content = "Invalid filter.", ephemeral = true })
      return
    end
    if o.stack ~= nil and not FILTER_PRESET_SET[o.stack] then
      reply(interaction, { content = "Invalid stacked filter.", ephemeral = true })
      return
    end
    defer(interaction, true)
    ensure_guild_settings(interaction.guild_id)
    local stack_value = (o.stack == nil or o.stack == "none") and nil or o.stack
    if o.stack == nil then
      dbq("UPDATE nexus_guild_settings SET filter_mode = %s, custom_speed = 1.0, custom_pitch = 1.0, custom_modifiers_left = 0 WHERE guild_id = %s", o.mode, interaction.guild_id)
    else
      dbq("UPDATE nexus_guild_settings SET filter_mode = %s, filter_stack = %s, custom_speed = 1.0, custom_pitch = 1.0, custom_modifiers_left = 0 WHERE guild_id = %s", o.mode, stack_value, interaction.guild_id)
    end
    local new_speed = 1.0
    if playback[interaction.guild_id] then
      local filters = {}
      new_speed = apply_filter_preset(filters, o.mode, 1.0)
      if stack_value and stack_value ~= o.mode then new_speed = apply_filter_preset(filters, stack_value, new_speed) end
      bot.lavalink:update_player(interaction.guild_id, { filters = filters })
      playback[interaction.guild_id].speed = new_speed
      playback[interaction.guild_id].filter_mode = o.mode
    end
    local label = FILTER_PRESET_LABEL[o.mode] or o.mode
    if stack_value and stack_value ~= o.mode then
      followup(interaction, { embed = simple_embed(("\240\159\142\155\239\184\143 Filter set to: **%s** + **%s**."):format(label, FILTER_PRESET_LABEL[stack_value] or stack_value), COLOR.blurple), ephemeral = true })
    else
      followup(interaction, { embed = simple_embed(("\240\159\142\155\239\184\143 Filter set to: **%s**."):format(label), COLOR.blurple), ephemeral = true })
    end
  end)

bot:command("nexus_main_fade", cmd("fade", "Customize track fade transitions, let the bot pick smart fade timing, or enable BPM-matched mixing.",
  { { type = OPT.STRING, name = "mode", description = "Fade mode", required = true,
      choices = { { name = "Smart Adaptive Fades", value = "smart" }, { name = "Custom Fades", value = "fade" },
                  { name = "Beat-Matched Mixing", value = "mix" }, { name = "Disable Fades", value = "off" } } },
    { type = OPT.NUMBER, name = "seconds", description = "Fade length in seconds, from 0.5 to 20", required = false, min_value = 0.5, max_value = 20 },
    { type = OPT.STRING, name = "curve", description = "Volume curve", required = false,
      choices = { { name = "Linear", value = "linear" }, { name = "Smooth", value = "smooth" },
                  { name = "Slow Start", value = "ease_in" }, { name = "Soft Land", value = "ease_out" } } } }),
  function(_, interaction)
    if not require_dj(interaction) then return end
    local o = opts_map(interaction)
    defer(interaction, true)
    local curve = o.curve or "smooth"
    local seconds = math.max(0.25, math.min(12.0, tonumber(o.seconds) or 3.0))
    ensure_guild_settings(interaction.guild_id)
    dbq("UPDATE nexus_guild_settings SET transition_mode = %s, fade_seconds = %s, fade_curve = %s WHERE guild_id = %s", o.mode, seconds, curve, interaction.guild_id)
    local message, color
    if o.mode == "smart" then
      message, color = "\240\159\140\138 Smart fades enabled. I will use short, smooth ramps that adapt to track length and active filters.", COLOR.green
    elseif o.mode == "fade" then
      message, color = ("\240\159\140\138 Custom fades enabled: %gs using %s."):format(seconds, curve:gsub("_", " ")), COLOR.green
    elseif o.mode == "mix" then
      message, color = "\240\159\142\154\239\184\143 Beat-matched mixing enabled (best-effort clean-cut fallback in this build; full BPM analysis was not ported).", COLOR.green
    else
      message, color = "\226\143\185\239\184\143 Smooth fades disabled.", COLOR.red
    end
    followup(interaction, { embed = simple_embed(message, color), ephemeral = true })
  end)

bot:command("nexus_main_modify", cmd("modify", "Apply temporary custom speed and pitch modifiers to the next few tracks in the queue.",
  { { type = OPT.NUMBER, name = "speed", description = "Speed multiplier (0.5 to 2.0)", required = false, min_value = 0.5, max_value = 2.0 },
    { type = OPT.NUMBER, name = "pitch", description = "Pitch multiplier (0.5 to 2.0)", required = false, min_value = 0.5, max_value = 2.0 },
    { type = OPT.INTEGER, name = "duration", description = "How many tracks this lasts (default 1)", required = false, min_value = 1 } }),
  function(_, interaction)
    if not require_dj(interaction) then return end
    local o = opts_map(interaction)
    defer(interaction, true)
    ensure_guild_settings(interaction.guild_id)
    local speed = math.max(0.5, math.min(2.0, tonumber(o.speed) or 1.0))
    local pitch = math.max(0.5, math.min(2.0, tonumber(o.pitch) or 1.0))
    local duration = o.duration or 1
    local settings = get_guild_settings(interaction.guild_id)
    if settings.filter_mode ~= "none" then
      followup(interaction, { embed = simple_embed("\226\157\140 **Conflict:** Disable standard Filters via `/nexus_main_filter none` first.", COLOR.red), ephemeral = true })
      return
    end
    dbq("UPDATE nexus_guild_settings SET custom_speed = %s, custom_pitch = %s, custom_modifiers_left = %s WHERE guild_id = %s", speed, pitch, duration, interaction.guild_id)
    if playback[interaction.guild_id] then
      playback[interaction.guild_id].speed = speed
      bot.lavalink:update_player(interaction.guild_id, { filters = { timescale = { speed = speed, pitch = pitch } } })
    end
    followup(interaction, { embed = mk_embed({ title = "\240\159\142\155\239\184\143 Audio Modifiers Set", description = ("**Speed:** %sx\n**Pitch:** %sx\n*Active for the next %d track(s).*"):format(speed, pitch, duration), color = COLOR.gold }), ephemeral = true })
  end)

------------------------------------------------------------------------
-- Scrubbing
------------------------------------------------------------------------
local function do_seek(interaction, target_seconds, response_text)
  if not require_dj(interaction) then return end
  local data = playback[interaction.guild_id]
  if not data then
    reply(interaction, { content = "Nothing playing.", ephemeral = true })
    return
  end
  target_seconds = math.max(0, target_seconds)
  bot.lavalink:seek(interaction.guild_id, math.floor(target_seconds * 1000))
  data.offset = target_seconds
  data.start_time = socket.gettime()
  reply(interaction, { embed = simple_embed(response_text, COLOR.green), ephemeral = true })
end

bot:command("nexus_main_seek", cmd("seek", "Jump to an exact time in the current track using seconds from the start.",
  { { type = OPT.INTEGER, name = "seconds", description = "Seconds from track start", required = true, min_value = 0 } }),
  function(_, interaction)
    local seconds = opts_map(interaction).seconds
    do_seek(interaction, seconds, ("Seeked to %ds"):format(seconds))
  end)

bot:command("nexus_main_forward", cmd("forward", "Jump forward within the current track by the number of seconds you provide.",
  { { type = OPT.INTEGER, name = "seconds", description = "Seconds to jump forward", required = true, min_value = 1 } }),
  function(_, interaction)
    local seconds = opts_map(interaction).seconds
    do_seek(interaction, current_position(interaction.guild_id) + seconds, ("Skipped forward %ds"):format(seconds))
  end)

bot:command("nexus_main_rewind", cmd("rewind", "Jump backward within the current track by the number of seconds you provide.",
  { { type = OPT.INTEGER, name = "seconds", description = "Seconds to jump backward", required = true, min_value = 1 } }),
  function(_, interaction)
    local seconds = opts_map(interaction).seconds
    do_seek(interaction, current_position(interaction.guild_id) - seconds, ("Rewound %ds"):format(seconds))
  end)

bot:command("nexus_main_replay", cmd("replay", "Restart the current track from the beginning without changing the queue.", {}),
  function(_, interaction)
    do_seek(interaction, 0, "Replaying song.")
  end)

------------------------------------------------------------------------
-- Utility & info
------------------------------------------------------------------------
bot:command("nexus_main_panel", cmd("panel", "Post an interactive control panel with playback, queue, and transport buttons.", {}),
  function(_, interaction)
    reply(interaction, { embed = build_panel_embed(interaction.guild_id), components = panel_components() })
  end)

bot:command("nexus_main_lyrics", cmd("lyrics", "Show lyrics for the currently playing track, synced to the current position when available.", {}),
  function(_, interaction)
    local data = playback[interaction.guild_id]
    if not data then
      reply(interaction, { embed = simple_embed("Nothing is playing.", COLOR.red), ephemeral = true })
      return
    end
    defer(interaction, false)
    local result = fetch_lyrics(data.title)
    if not result then
      followup(interaction, { embed = simple_embed(("No lyrics found for **%s**."):format(data.title), COLOR.red) })
      return
    end
    local e = mk_embed({ title = "\240\159\147\156 " .. (result.track_name or data.title), color = COLOR.blue })
    if result.author then e.author = { name = result.artist_name } end
    if result.synced then
      local pos = current_position(interaction.guild_id)
      local current_idx = 1
      for i, line in ipairs(result.synced) do
        if line.ts <= pos then current_idx = i else break end
      end
      local body = {}
      for i = math.max(1, current_idx - 2), math.min(#result.synced, current_idx + 4) do
        local line = result.synced[i]
        table.insert(body, i == current_idx and ("**" .. line.text .. "**") or line.text)
      end
      e.description = truncate(table.concat(body, "\n"), 4000)
      e.footer = { text = "Synced to current playback position \226\128\148 rerun to refresh." }
    else
      e.description = truncate(result.plain or "No lyrics text available.", 4000)
    end
    followup(interaction, { embed = e })
  end)

bot:command("nexus_main_ping", cmd("ping", "Show the bot websocket latency so you can quickly check responsiveness.", {}),
  function(_, interaction)
    reply(interaction, { embed = simple_embed("\240\159\143\147 Pong!", COLOR.green), ephemeral = true })
  end)

bot:command("nexus_main_uptime", cmd("uptime", "Show how long this bot process has been running since its last startup.", {}),
  function(_, interaction)
    local secs = math.floor(socket.gettime() - START_TIME)
    local h = math.floor(secs / 3600)
    local m = math.floor((secs % 3600) / 60)
    local s = secs % 60
    reply(interaction, { embed = simple_embed(("\226\143\177\239\184\143 Uptime: %d:%02d:%02d"):format(h, m, s), COLOR.green), ephemeral = true })
  end)

bot:command("nexus_main_stats", cmd("stats", "Show quick bot statistics such as guild count and active player count.", {}),
  function(_, interaction)
    local n_playback = 0
    for _ in pairs(playback) do n_playback = n_playback + 1 end
    reply(interaction, { embed = simple_embed(("\240\159\147\138 Active Players: %d"):format(n_playback), COLOR.green), ephemeral = true })
  end)

bot:command("nexus_main_help", cmd("help", "Show a categorized help menu for all NEXUS music commands and utilities", {}),
  function(_, interaction)
    local names = {}
    for name, _ in pairs(bot.commands) do table.insert(names, name) end
    table.sort(names)
    reply(interaction, { embed = mk_embed({
      title = "\240\159\147\154 NEXUS Command List",
      description = table.concat(names, ", "),
      color = COLOR.blue,
      footer = "NEXUS -- swarm node. Connection point for servers that bridge multiple communities.",
    }), ephemeral = true })
  end)

------------------------------------------------------------------------
-- Panel button interactions (MESSAGE_COMPONENT, interaction type 3)
--
-- Registered directly on the gateway (bot.lua's own INTERACTION_CREATE
-- handler only dispatches type 2 APPLICATION_COMMAND interactions and
-- ignores everything else, so this is a second, additive listener).
------------------------------------------------------------------------
local function panel_refresh(interaction)
  update_message(interaction, { embed = build_panel_embed(interaction.guild_id), components = panel_components() })
end

local function adjust_volume(interaction, delta)
  ensure_guild_settings(interaction.guild_id)
  local s = get_guild_settings(interaction.guild_id)
  local vol = math.max(1, math.min(200, (s.volume or 100) + delta))
  update_guild_setting(interaction.guild_id, "volume", vol)
  if playback[interaction.guild_id] then
    playback[interaction.guild_id].volume = vol
    bot.lavalink:set_volume(interaction.guild_id, vol)
  end
end

bot.gateway:on("INTERACTION_CREATE", function(interaction)
  if interaction.type ~= 3 then return end -- MESSAGE_COMPONENT
  local id = interaction.data and interaction.data.custom_id
  local guild_id = interaction.guild_id
  local ok, err = pcall(function()
    if id == "nexus_panel_playpause" then
      if not is_dj(interaction) then reply(interaction, { content = "DJ permission required.", ephemeral = true }); return end
      local data = playback[guild_id]
      if not data then reply(interaction, { content = "Nothing is playing.", ephemeral = true }); return end
      data.paused = not data.paused
      if data.paused then data.offset = current_position(guild_id) else data.start_time = socket.gettime() end
      bot.lavalink:set_paused(guild_id, data.paused)
      panel_refresh(interaction)
    elseif id == "nexus_panel_stop" then
      if not is_dj(interaction) then reply(interaction, { content = "DJ permission required.", ephemeral = true }); return end
      fade_token[guild_id] = (fade_token[guild_id] or 0) + 1
      do_stop(guild_id)
      panel_refresh(interaction)
    elseif id == "nexus_panel_skip" then
      if not is_dj(interaction) then reply(interaction, { content = "DJ permission required.", ephemeral = true }); return end
      if playback[guild_id] then
        fade_token[guild_id] = (fade_token[guild_id] or 0) + 1
        bot.lavalink:stop(guild_id)
        reply(interaction, { content = "\226\143\173\239\184\143 Skipped to next track", ephemeral = true })
      else
        reply(interaction, { content = "Nothing to skip.", ephemeral = true })
      end
    elseif id == "nexus_panel_rw10" or id == "nexus_panel_fw10" then
      if not is_dj(interaction) then reply(interaction, { content = "DJ permission required.", ephemeral = true }); return end
      local data = playback[guild_id]
      if not data then reply(interaction, { content = "Nothing playing.", ephemeral = true }); return end
      local delta = (id == "nexus_panel_fw10") and 10 or -10
      local new_pos = math.max(0, current_position(guild_id) + delta)
      bot.lavalink:seek(guild_id, new_pos * 1000)
      data.offset = new_pos
      data.start_time = socket.gettime()
      panel_refresh(interaction)
    elseif id == "nexus_panel_voldown" then
      if not is_dj(interaction) then reply(interaction, { content = "DJ permission required.", ephemeral = true }); return end
      adjust_volume(interaction, -10); panel_refresh(interaction)
    elseif id == "nexus_panel_volup" then
      if not is_dj(interaction) then reply(interaction, { content = "DJ permission required.", ephemeral = true }); return end
      adjust_volume(interaction, 10); panel_refresh(interaction)
    elseif id == "nexus_panel_loop" then
      if not is_dj(interaction) then reply(interaction, { content = "DJ permission required.", ephemeral = true }); return end
      local s = get_guild_settings(guild_id)
      local next_mode = ({ off = "queue", queue = "song", song = "off" })[s.loop_mode] or "queue"
      update_guild_setting(guild_id, "loop_mode", next_mode)
      panel_refresh(interaction)
    elseif id == "nexus_panel_shuffle" then
      if not is_dj(interaction) then reply(interaction, { content = "DJ permission required.", ephemeral = true }); return end
      local n = shuffle_queue_rows(guild_id, false)
      if n <= 0 then reply(interaction, { content = "Queue empty.", ephemeral = true }); return end
      snapshot_queue_backup(guild_id)
      reply(interaction, { content = "\240\159\148\128 Queue successfully shuffled!", ephemeral = true })
    elseif id == "nexus_panel_queue" then
      local rows = dbq("SELECT title FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC LIMIT 10", guild_id) or {}
      if #rows == 0 then
        reply(interaction, { content = "Queue is empty.", ephemeral = true })
      else
        local lines = {}
        for i, r in ipairs(rows) do table.insert(lines, ("%d. %s"):format(i, r.title)) end
        reply(interaction, { content = "**Current Queue:**\n" .. table.concat(lines, "\n"), ephemeral = true })
      end
    end
  end)
  if not ok then
    print("[nexus] panel interaction error: " .. tostring(err))
    report_error(guild_id, "command_error", "panel:" .. tostring(id), tostring(err))
  end
end)

------------------------------------------------------------------------
-- Voice-state bookkeeping: track each guild member's current voice channel
-- so /play, /radio, /join etc. can find "the channel the caller is in"
-- without a REST round-trip. Also drives the queue-empty auto-disconnect
-- already handled inside process_queue.
------------------------------------------------------------------------
local guild_voice_members = {} -- guild_id -> { [user_id] = channel_id }

bot.gateway:on("VOICE_STATE_UPDATE", function(d)
  if not d.guild_id then return end
  guild_voice_members[d.guild_id] = guild_voice_members[d.guild_id] or {}
  if d.channel_id and d.channel_id ~= cjson.null then
    guild_voice_members[d.guild_id][d.user_id] = d.channel_id
  else
    guild_voice_members[d.guild_id][d.user_id] = nil
  end
end)

-- interaction.member.voice_channel_id is not part of the raw Discord
-- interaction payload; synthesize it from the voice-state cache above so
-- the command handlers above (which read that field) work as written.
bot.gateway:on("INTERACTION_CREATE", function(interaction)
  if not interaction.guild_id or not interaction.member then return end
  local uid = user_id_of(interaction)
  local gmap = guild_voice_members[interaction.guild_id]
  interaction.member.voice_channel_id = gmap and gmap[uid] or nil
end)

------------------------------------------------------------------------
-- Guild lifecycle: swarm-health visibility + leave cleanup
------------------------------------------------------------------------
local known_guilds = {}

bot.gateway:on("GUILD_CREATE", function(g)
  if g and g.id then known_guilds[g.id] = true end
end)

bot.gateway:on("GUILD_DELETE", function(g)
  if not g or not g.id then return end
  known_guilds[g.id] = nil
  if g.unavailable then return end -- outage, not an actual removal
  local guild_id = g.id
  dbq("DELETE FROM nexus_playback_state WHERE guild_id = %s AND bot_name = 'nexus'", guild_id)
  dbq("DELETE FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", guild_id)
  dbq("DELETE FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus'", guild_id)
  dbq("DELETE FROM nexus_active_playlists WHERE guild_id = %s AND bot_name = 'nexus'", guild_id)
  dbq("DELETE FROM nexus_active_playlist_tracks WHERE guild_id = %s AND bot_name = 'nexus'", guild_id)
  dbq("DELETE FROM nexus_guild_settings WHERE guild_id = %s", guild_id)
  dbq("DELETE FROM nexus_voice_state WHERE guild_id = %s AND bot_name = 'nexus'", guild_id)
  playback[guild_id] = nil
  voice_channel[guild_id] = nil
end)

------------------------------------------------------------------------
-- Background maintenance: a single lightweight loop covering swarm-health
-- heartbeat + coarse position checkpointing. nexus.py runs roughly a dozen
-- separate @tasks.loop coroutines for this (resilience/watchdog/zombie-
-- reaper/DB-janitor/etc.); this is a deliberately-simplified stand-in that
-- keeps SwarmPanel/Aria-visible state fresh without the elaborate self-heal
-- machinery (see the final report).
------------------------------------------------------------------------
local function maintenance_tick()
  dbq([[INSERT INTO swarm_health (bot_name, last_pulse, status) VALUES ('nexus', NOW(), 'online')
        ON CONFLICT (bot_name) DO UPDATE SET last_pulse = NOW(), status = EXCLUDED.status]])
  for guild_id, data in pairs(playback) do
    local pos = current_position(guild_id)
    persist_playback_state(guild_id, data.channel_id, data.url, data.title, pos, not data.paused, data.paused, data.track_uid)
    local backup_row = db_one("SELECT COUNT(*) AS n FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus'", guild_id)
    local backup_n = backup_row and math.floor(backup_row.n) or 0
    -- NOTE (found live via db_smoke_test.lua-style call-count auditing): this
    -- INSERT has 9 %s placeholders (guild_id, connected_channel_id,
    -- player_playing, player_paused, queue_count, backup_queue_count,
    -- is_paused_db, position_seconds, duration_seconds) -- an earlier version
    -- of this call passed only 8 args, one short, which throws
    -- "bad argument to 'format'" (too few values) on every maintenance tick
    -- with an active player, and additionally mapped `pos`/`duration` into
    -- the wrong (is_paused_db/position_seconds) columns.
    dbq([[INSERT INTO nexus_metrics (guild_id, bot_name, voice_connected, connected_channel_id, player_connected,
            player_playing, player_paused, queue_count, backup_queue_count, is_playing_db, is_paused_db,
            position_seconds, duration_seconds, lavalink_ready, updated_at)
          VALUES (%s, 'nexus', TRUE, %s, TRUE, %s, %s, %s, %s, TRUE, %s, %s, %s, TRUE, NOW())
          ON CONFLICT (guild_id, bot_name) DO UPDATE SET
            voice_connected = EXCLUDED.voice_connected, connected_channel_id = EXCLUDED.connected_channel_id,
            player_connected = EXCLUDED.player_connected, player_playing = EXCLUDED.player_playing,
            player_paused = EXCLUDED.player_paused, queue_count = EXCLUDED.queue_count,
            backup_queue_count = EXCLUDED.backup_queue_count, is_playing_db = EXCLUDED.is_playing_db,
            is_paused_db = EXCLUDED.is_paused_db, position_seconds = EXCLUDED.position_seconds,
            duration_seconds = EXCLUDED.duration_seconds, lavalink_ready = EXCLUDED.lavalink_ready, updated_at = NOW()]],
        guild_id, data.channel_id, not data.paused, data.paused, queue_count(guild_id), backup_n,
        data.paused, pos, math.floor(data.duration or 0))
  end
end

------------------------------------------------------------------------
-- Startup: on READY, try to resume any guild that has a home channel and a
-- non-empty queue (or a live-but-idle playback_state row) left over from
-- before a restart. This is a deliberately simple stand-in for nexus.py's
-- elaborate multi-stage recovery/backoff/checkpoint system.
------------------------------------------------------------------------
bot.gateway:on("READY", function(_)
  copas.addthread(function()
    copas.sleep(3) -- let the gateway settle / guild cache populate
    local homes = dbq("SELECT guild_id::text AS guild_id, home_vc_id::text AS home_vc_id FROM nexus_bot_home_channels WHERE bot_name = 'nexus'") or {}
    for _, h in ipairs(homes) do
      if queue_count(h.guild_id) > 0 and not playback[h.guild_id] then
        local ok, err = pcall(process_queue, h.guild_id, h.home_vc_id)
        if not ok then
          print("[nexus] startup resume failed for guild " .. h.guild_id .. ": " .. tostring(err))
          report_error(h.guild_id, "runtime", "startup resume failed", tostring(err))
        end
      end
    end
  end)

  copas.addthread(function()
    while true do
      local ok, err = pcall(maintenance_tick)
      if not ok then
        print("[nexus] maintenance tick failed: " .. tostring(err))
        report_error(nil, "runtime", "maintenance tick error", tostring(err))
      end
      copas.sleep(15)
    end
  end)

  -- Mirrors nexus.py's "Database Janitor" retention pass for error_events
  -- (30-day retention); everything else that task did (history pruning,
  -- cache cleanup) is covered by the documented simplifications above.
  copas.addthread(function()
    while true do
      copas.sleep(6 * 3600)
      local ok, err = pcall(dbq, "DELETE FROM nexus_error_events WHERE created_at < NOW() - INTERVAL '30 days'")
      if not ok then print("[nexus] error_events cleanup failed: " .. tostring(err)) end
    end
  end)

  print("[nexus] swarm node online.")
  send_webhook_log("\240\159\159\162 Node Online", "NEXUS is online and syncing with the swarm.", COLOR.green)
end)

------------------------------------------------------------------------
-- Run
------------------------------------------------------------------------
if env("NEXUS_DRY_RUN") == "test" then
  -- Exposes internals for the DB/logic smoke test (db_smoke_test.lua); not
  -- reached in normal operation (that script sets this env var itself).
  return {
    dbq = dbq, db_one = db_one, url_key = url_key,
    ensure_guild_settings = ensure_guild_settings, get_guild_settings = get_guild_settings,
    update_guild_setting = update_guild_setting, toggle_guild_setting = toggle_guild_setting,
    enqueue_track = enqueue_track, insert_queue_front = insert_queue_front,
    snapshot_queue_backup = snapshot_queue_backup, restore_queue_from_backup = restore_queue_from_backup,
    shuffle_queue_rows = shuffle_queue_rows, queue_count = queue_count,
    record_track_play_started = record_track_play_started, record_track_outcome = record_track_outcome,
    record_track_feedback = record_track_feedback, build_smart_recommendation = build_smart_recommendation,
    load_smart_avoid_keys = load_smart_avoid_keys, get_autodj_enabled = get_autodj_enabled,
    set_autodj_enabled = set_autodj_enabled, persist_playback_state = persist_playback_state,
    clear_playback_state = clear_playback_state,
    mark_voice_connected = mark_voice_connected, mark_voice_disconnected = mark_voice_disconnected,
    report_error = report_error, persist_error_event = persist_error_event, bot = bot,
    maintenance_tick = maintenance_tick, playback = playback,
  }
elseif env("NEXUS_DRY_RUN") then
  print(("[nexus] dry run OK: %d commands registered, db/lavalink/gateway objects constructed."):format((function()
    local n = 0; for _ in pairs(bot.commands) do n = n + 1 end; return n
  end)()))
  os.exit(0)
end

bot:run()
