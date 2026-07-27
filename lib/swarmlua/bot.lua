-- Thin application framework tying gateway + rest + lavalink + pg together.
-- Each bot's own file uses this to register slash commands and handlers;
-- everything protocol-level lives in this shared library so behavior stays
-- consistent across all 13 bots while each bot's command set stays unique.

-- LuaJIT fully block-buffers stdout when it isn't a TTY (true for every
-- Docker container), so `docker logs` can sit there showing nothing -- or a
-- stale snapshot -- long after the bot has actually connected, including
-- hiding real crash output. Force line buffering so logs/print show up as
-- they happen. This module is required near the top of every bot's
-- entrypoint, before any real connection logic runs.
io.stdout:setvbuf("line")
io.stderr:setvbuf("line")

local Gateway = require("swarmlua.gateway")
local Rest = require("swarmlua.rest")
local Lavalink = require("swarmlua.lavalink")
local Pg = require("swarmlua.pg")
local cjson = require("cjson")

local Bot = {}
Bot.__index = Bot

function Bot.new(config)
  local self = setmetatable({}, Bot)
  self.token = config.token
  self.gateway = Gateway.new({ token = config.token, intents = config.intents })
  self.rest = Rest.new(config.token)
  self.db = Pg.new(config.db)
  self.lavalink = config.lavalink and Lavalink.new(config.lavalink) or nil
  self.commands = {}
  self.voice_state = {} -- guild_id -> { session_id = ... }
  self.application_id = nil
  self.user_id = nil
  self.on_error = nil -- optional fn(err, context) for bot-specific error-event/webhook reporting
  return self
end

function Bot:command(name, spec, handler)
  spec.name = name
  self.commands[name] = { spec = spec, handler = handler }
end

function Bot:sync_commands()
  local defs = {}
  for _, entry in pairs(self.commands) do
    table.insert(defs, entry.spec)
  end
  local ok, err = self.rest:put(("/applications/%s/commands"):format(self.application_id), defs)
  if not ok then
    print("[bot] command sync failed: " .. tostring(err))
  else
    print(("[bot] synced %d slash commands"):format(#defs))
  end
end

function Bot:reply(interaction, content, ephemeral)
  self.rest:post(
    ("/interactions/%s/%s/callback"):format(interaction.id, interaction.token),
    { type = 4, data = { content = content, flags = ephemeral and 64 or nil } }
  )
end

function Bot:_wire_voice_to_lavalink()
  self.gateway:on("VOICE_STATE_UPDATE", function(d)
    if d.user_id == self.user_id then
      self.voice_state[d.guild_id] = self.voice_state[d.guild_id] or {}
      self.voice_state[d.guild_id].session_id = d.session_id
      if d.channel_id and d.channel_id ~= cjson.null then
        self.voice_state[d.guild_id].channel_id = d.channel_id
      end
      -- Joining a stage channel (as opposed to a regular voice channel)
      -- drops the bot in as a suppressed audience member by default -- no
      -- audio goes out even though Lavalink is playing fine -- unless it's
      -- explicitly unsuppressed. Regular voice channels never set
      -- suppress=true, so this is a no-op there.
      if d.suppress and d.channel_id and d.channel_id ~= cjson.null then
        self.rest:patch(("/guilds/%s/voice-states/@me"):format(d.guild_id), {
          channel_id = d.channel_id, suppress = false,
        })
      end
    end
  end)
  self.gateway:on("VOICE_SERVER_UPDATE", function(d)
    local vs = self.voice_state[d.guild_id]
    if vs and vs.session_id and self.lavalink then
      local ok, err = self.lavalink:send_voice_update(d.guild_id, {
        token = d.token,
        endpoint = d.endpoint,
        session_id = vs.session_id,
        channel_id = vs.channel_id,
      })
      if not ok then
        print(("[bot] voice update failed for guild %s: %s"):format(tostring(d.guild_id), tostring(err)))
      end
    end
  end)
end

function Bot:run()
  self.gateway:on("READY", function(d)
    self.application_id = d.application and d.application.id
    self.user_id = d.user and d.user.id
    print(("[bot] ready as %s"):format(d.user and d.user.username or "?"))
    if self.application_id then self:sync_commands() end
    if self.lavalink then
      self.lavalink.user_id = self.user_id
      -- READY refires on every fresh gateway IDENTIFY (invalid session,
      -- post-reconnect re-auth, etc), which happens often. Lavalink:connect()
      -- opens a brand new session each call without closing the previous
      -- one, so calling it again here would leave old orphaned sessions
      -- around and make self.lavalink.session_id race between them --
      -- update_player/play would then target whichever session's "ready"
      -- op happened to land last, which may have no live voice connection,
      -- so playback silently fails even with a full queue. Connect once.
      if not self.lavalink_connected then
        self.lavalink_connected = true
        self.lavalink:connect()
      end
    end
  end)

  self.gateway:on("INTERACTION_CREATE", function(interaction)
    if interaction.type ~= 2 then return end -- APPLICATION_COMMAND
    local entry = self.commands[interaction.data.name]
    if entry then
      local ok, err = pcall(entry.handler, self, interaction)
      if not ok then
        print("[bot] handler error: " .. tostring(err))
        if self.on_error then pcall(self.on_error, err, { command = interaction.data.name }) end
        self:reply(interaction, "Something went wrong running that command.", true)
      end
    end
  end)

  self:_wire_voice_to_lavalink()
  self.gateway:run()
end

return Bot
