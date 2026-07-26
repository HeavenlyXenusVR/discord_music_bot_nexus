-- Discord Gateway (v10) client: identify, heartbeat, resume, dispatch.
local copas = require("copas")
local websocket = require("websocket")
local ws_sync = require("websocket.sync")
local socket = require("socket")
local cjson = require("cjson")
local https = require("ssl.https")
local ltn12 = require("ltn12")

-- lua-websockets' bundled copas backend (websocket.client.copas()) only ever
-- does a plain TCP connect, and websocket.sync's own connect() hard-rejects
-- any URL scheme other than the literal string "ws" ("bad protocol"). Discord's
-- real gateway is wss:// (TLS), so we build our own minimal sync-extended
-- client here: plain copas.connect() for the TCP leg, then copas.dohandshake()
-- to upgrade to TLS before the WS handshake, when the caller asks for it.
local function new_ws_client(use_tls)
  local self = {}
  self.sock_connect = function(this, host, port)
    this.sock = socket.tcp()
    local _, err = copas.connect(this.sock, host, port)
    if err and err ~= "already connected" then
      this.sock:close()
      return nil, err
    end
    if use_tls then
      local wrapped, err2 = copas.dohandshake(this.sock, { mode = "client", protocol = "any", verify = "none", options = "all" })
      if not wrapped then
        return nil, "tls handshake failed: " .. tostring(err2)
      end
      this.sock = wrapped
    end
    return true
  end
  self.sock_send = function(this, ...) return copas.send(this.sock, ...) end
  self.sock_receive = function(this, ...) return copas.receive(this.sock, ...) end
  self.sock_close = function(this) this.sock:close() end
  return ws_sync.extend(self)
end

local OP = {
  DISPATCH = 0, HEARTBEAT = 1, IDENTIFY = 2, PRESENCE_UPDATE = 3,
  VOICE_STATE_UPDATE = 4, RESUME = 6, RECONNECT = 7,
  INVALID_SESSION = 9, HELLO = 10, HEARTBEAT_ACK = 11,
}

local Gateway = {}
Gateway.__index = Gateway

function Gateway.new(opts)
  local self = setmetatable({}, Gateway)
  self.token = opts.token
  self.intents = opts.intents or 3243773 -- guilds, members, messages, message content, voice states, etc (bot should narrow per its needs)
  self.handlers = {}
  self.seq = nil
  self.session_id = nil
  self.resume_url = nil
  self.ws = nil
  self.heartbeat_interval = nil
  self.should_run = true
  return self
end

function Gateway:on(event_name, fn)
  self.handlers[event_name] = self.handlers[event_name] or {}
  table.insert(self.handlers[event_name], fn)
end

function Gateway:emit(event_name, data)
  for _, fn in ipairs(self.handlers[event_name] or {}) do
    local ok, err = pcall(fn, data)
    if not ok then
      print(("[gateway] handler error for %s: %s"):format(event_name, err))
    end
  end
end

function Gateway:fetch_gateway_url()
  local response_body = {}
  local ok = https.request({
    url = "https://discord.com/api/v10/gateway/bot",
    method = "GET",
    headers = { ["Authorization"] = "Bot " .. self.token },
    sink = ltn12.sink.table(response_body),
  })
  if not ok then return "wss://gateway.discord.gg" end
  local decoded = cjson.decode(table.concat(response_body))
  return decoded.url or "wss://gateway.discord.gg"
end

function Gateway:send(op, d)
  self.ws:send(cjson.encode({ op = op, d = d }))
end

function Gateway:identify()
  self:send(OP.IDENTIFY, {
    token = self.token,
    intents = self.intents,
    properties = { ["os"] = "linux", ["browser"] = "swarmlua", ["device"] = "swarmlua" },
  })
end

function Gateway:resume()
  self:send(OP.RESUME, { token = self.token, session_id = self.session_id, seq = self.seq })
end

function Gateway:join_voice(guild_id, channel_id, self_mute, self_deaf)
  self:send(OP.VOICE_STATE_UPDATE, {
    guild_id = guild_id,
    channel_id = channel_id,
    self_mute = self_mute or false,
    self_deaf = self_deaf or false,
  })
end

function Gateway:leave_voice(guild_id)
  self:join_voice(guild_id, cjson.null)
end

function Gateway:start_heartbeat()
  copas.addthread(function()
    while self.should_run and self.ws do
      copas.sleep(self.heartbeat_interval / 1000)
      if self.ws then
        local ok = pcall(function() self:send(OP.HEARTBEAT, self.seq) end)
        if not ok then break end
      end
    end
  end)
end

function Gateway:handle_message(raw)
  local msg = cjson.decode(raw)
  if msg.s and msg.s ~= cjson.null then self.seq = msg.s end

  if msg.op == OP.HELLO then
    self.heartbeat_interval = msg.d.heartbeat_interval
    self:start_heartbeat()
    if self.session_id then
      self:resume()
    else
      self:identify()
    end
  elseif msg.op == OP.DISPATCH then
    if msg.t == "READY" then
      self.session_id = msg.d.session_id
      self.resume_url = msg.d.resume_gateway_url
    end
    self:emit(msg.t, msg.d)
    self:emit("*", msg)
  elseif msg.op == OP.INVALID_SESSION then
    self.session_id = nil
    self.seq = nil
    copas.sleep(2)
    self:identify()
  elseif msg.op == OP.RECONNECT then
    self:reconnect(true)
  end
  -- HEARTBEAT_ACK: no-op, presence of traffic is enough liveness signal here.
end

function Gateway:reconnect(resume)
  if self.ws then pcall(function() self.ws:close() end) end
  self.ws = nil
  if not resume then
    self.session_id = nil
    self.seq = nil
  end
  self:connect()
end

function Gateway:connect()
  local raw_url = (self.resume_url or self:fetch_gateway_url()) .. "/?v=10&encoding=json"
  local use_tls = raw_url:match("^wss://") ~= nil
  local ws_url = raw_url:gsub("^wss://", "ws://")
  if use_tls and not ws_url:match("^ws://[^/]+:%d+") then
    ws_url = ws_url:gsub("^(ws://[^/]+)", "%1:443")
  end
  self.ws = new_ws_client(use_tls)
  local ok, err = self.ws:connect(ws_url)
  if not ok then
    print("[gateway] connect failed: " .. tostring(err) .. ", retrying in 5s")
    copas.sleep(5)
    return self:connect()
  end

  copas.addthread(function()
    while self.should_run do
      local message, opcode, was_clean = self.ws:receive()
      if not message then
        print("[gateway] connection lost, reconnecting")
        self:reconnect(true)
        return
      end
      self:handle_message(message)
    end
  end)
end

function Gateway:run()
  -- connect() (and its retry-loop copas.sleep()) must run inside a copas
  -- coroutine, not the bare main chunk -- calling it directly here crashes
  -- with "attempt to yield across C-call boundary" the first time a connect
  -- attempt fails and falls into copas.sleep().
  copas.addthread(function() self:connect() end)
  copas.loop()
end

return Gateway
