-- Thin application framework tying gateway + rest + lavalink + pg together.
-- Each bot's own file uses this to register slash commands and handlers;
-- everything protocol-level lives in this shared library so behavior stays
-- consistent across all 13 bots while each bot's command set stays unique.
-- LuaJIT fully block-buffers stdout when it isn't a TTY (true for every
-- Docker container), so `docker logs` can sit there showing nothing -- or a
-- stale snapshot -- long after the bot has actually connected, including
-- hiding real crash output. Force line buffering so logs/print show up as
-- they happen.
io.stdout:setvbuf("line")
io.stderr:setvbuf("line")

local Gateway = require("swarmlua.gateway")
local Rest = require("swarmlua.rest")
local Lavalink = require("swarmlua.lavalink")
local Pg = require("swarmlua.pg")

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
    end
  end)
  self.gateway:on("VOICE_SERVER_UPDATE", function(d)
    local vs = self.voice_state[d.guild_id]
    if vs and vs.session_id and self.lavalink then
      self.lavalink:send_voice_update(d.guild_id, {
        token = d.token,
        endpoint = d.endpoint,
        session_id = vs.session_id,
      })
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
      self.lavalink:connect()
    end
  end)

  self.gateway:on("INTERACTION_CREATE", function(interaction)
    if interaction.type ~= 2 then return end -- APPLICATION_COMMAND
    local entry = self.commands[interaction.data.name]
    if entry then
      local ok, err = pcall(entry.handler, self, interaction)
      if not ok then
        print("[bot] handler error: " .. tostring(err))
        -- ADDITIVE, backward-compatible extension (mirrors the same hook vendored
        -- into gws's/harmonic's copy of this file, not yet merged into the canonical
        -- lua-shared/swarmlua/bot.lua): if the owning bot sets self.on_error, it
        -- gets a chance to log/report the failure (DB error-events table, error
        -- webhook, etc). Any bot that never sets on_error sees no behavior change.
        if self.on_error then pcall(self.on_error, interaction.data.name, interaction, err) end
        self:reply(interaction, "Something went wrong running that command.", true)
      end
    end
  end)

  self:_wire_voice_to_lavalink()
  self.gateway:run()
end

return Bot
