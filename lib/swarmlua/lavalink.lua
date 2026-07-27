-- Lavalink v4 client: session websocket + REST player/track control.
local copas = require("copas") -- must load before socket.http/ssl.https anywhere in the process
local websocket = require("websocket")
local ws_sync = require("websocket.sync")
local ws_tools = require("websocket.tools")
local ws_handshake = require("websocket.handshake")
local socket = require("socket")
local cjson = require("cjson")
local http = require("socket.http")
local ltn12 = require("ltn12")

-- lua-websockets' websocket.sync connect() builds the HTTP upgrade request
-- itself (see handshake.upgrade_request) with NO extension point for extra
-- headers -- so the Authorization/User-Id/Client-Name headers Lavalink
-- requires were silently never sent, and Lavalink correctly rejected every
-- handshake (returning a plain HTTP error response with no
-- Sec-WebSocket-Accept header, surfacing here as "Invalid Sec-Websocket-Accept
-- ... got nil"). This never ran end-to-end before this bot was first run in a
-- real container with a real Lavalink instance. Fixed by hand-rolling the
-- upgrade request with the required headers, reusing sync.extend() only for
-- post-handshake frame send/receive/close.
local function connect_with_headers(self, ws_url, extra_headers)
  local protocol, host, port, uri = ws_tools.parse_url(ws_url)
  if protocol ~= "ws" then return nil, "bad protocol" end
  local ok, err = self:sock_connect(host, port)
  if not ok then return nil, err end
  local key = ws_tools.generate_key()
  local lines = {
    ("GET %s HTTP/1.1"):format(uri or "/"),
    ("Host: %s"):format(host),
    "Upgrade: websocket",
    "Connection: Upgrade",
    ("Sec-WebSocket-Key: %s"):format(key),
    "Sec-WebSocket-Version: 13",
  }
  for hname, hval in pairs(extra_headers or {}) do
    table.insert(lines, ("%s: %s"):format(hname, hval))
  end
  table.insert(lines, "")
  table.insert(lines, "")
  local req = table.concat(lines, "\r\n")
  local n, err2 = self:sock_send(req)
  if n ~= #req then return nil, err2 end
  local resp = {}
  repeat
    local line, err3 = self:sock_receive("*l")
    resp[#resp + 1] = line
    if err3 then return nil, err3 end
  until line == ""
  local headers = ws_handshake.http_headers(table.concat(resp, "\r\n"))
  local expected_accept = ws_handshake.sec_websocket_accept(key)
  if headers["sec-websocket-accept"] ~= expected_accept then
    return nil, ("Websocket Handshake failed: Invalid Sec-Websocket-Accept (expected %s got %s)")
      :format(expected_accept, headers["sec-websocket-accept"] or "nil")
  end
  self.state = "OPEN"
  return true
end

local function new_ws_client()
  local self = {}
  self.sock_connect = function(this, host, port)
    this.sock = socket.tcp()
    local _, err = copas.connect(this.sock, host, port)
    if err and err ~= "already connected" then
      this.sock:close()
      return nil, err
    end
    return true
  end
  self.sock_send = function(this, ...) return copas.send(this.sock, ...) end
  self.sock_receive = function(this, ...) return copas.receive(this.sock, ...) end
  self.sock_close = function(this) this.sock:close() end
  local obj = ws_sync.extend(self)
  obj.connect = connect_with_headers
  return obj
end

local Lavalink = {}
Lavalink.__index = Lavalink

function Lavalink.new(opts)
  local self = setmetatable({}, Lavalink)
  self.host = opts.host or "127.0.0.1"
  self.port = opts.port or 2333
  self.password = opts.password
  self.user_id = opts.user_id
  self.session_id = nil
  self.handlers = {}
  return self
end

function Lavalink:on(event_name, fn)
  self.handlers[event_name] = self.handlers[event_name] or {}
  table.insert(self.handlers[event_name], fn)
end

function Lavalink:emit(event_name, data)
  for _, fn in ipairs(self.handlers[event_name] or {}) do
    pcall(fn, data)
  end
end

function Lavalink:rest(method, path, body)
  local response_body = {}
  local request_body = body and cjson.encode(body) or nil
  local ok, status = http.request({
    url = ("http://%s:%d%s"):format(self.host, self.port, path),
    method = method,
    headers = {
      ["Authorization"] = self.password,
      ["Content-Type"] = "application/json",
      ["Content-Length"] = request_body and tostring(#request_body) or "0",
    },
    source = request_body and ltn12.source.string(request_body) or nil,
    sink = ltn12.sink.table(response_body),
  })
  local raw = table.concat(response_body)
  local decoded = nil
  if #raw > 0 then
    local ok2, parsed = pcall(cjson.decode, raw)
    if ok2 then decoded = parsed end
  end
  if not ok or (type(status) == "number" and status >= 400) then
    return nil, "lavalink http " .. tostring(status) .. ": " .. raw
  end
  return decoded
end

-- Search/resolve a track. identifier can be "ytsearch:query" or a direct URL.
function Lavalink:load_tracks(identifier)
  return self:rest("GET", "/v4/loadtracks?identifier=" .. identifier:gsub(" ", "%%20"))
end

function Lavalink:update_player(guild_id, patch, no_replace)
  local path = ("/v4/sessions/%s/players/%s"):format(self.session_id, guild_id)
  if no_replace then path = path .. "?noReplace=true" end
  return self:rest("PATCH", path, patch)
end

function Lavalink:destroy_player(guild_id)
  return self:rest("DELETE", ("/v4/sessions/%s/players/%s"):format(self.session_id, guild_id))
end

-- Called with the {token, endpoint, session_id} assembled from the bot's
-- VOICE_SERVER_UPDATE + VOICE_STATE_UPDATE gateway events.
function Lavalink:send_voice_update(guild_id, voice)
  -- Lavalink 4.2.2's VoiceState schema requires channelId in addition to
  -- token/endpoint/sessionId (older versions didn't). Without it this PATCH
  -- 400s server-side (HttpMessageNotReadableException: "Field 'channelId' is
  -- required") and Lavalink never gets valid voice credentials at all -- no
  -- real UDP voice connection is ever established, so nothing plays, even
  -- though the bot's own queue/playback-state bookkeeping has no idea
  -- anything went wrong.
  return self:update_player(guild_id, {
    voice = {
      token = voice.token, endpoint = voice.endpoint,
      sessionId = voice.session_id, channelId = voice.channel_id,
    },
  })
end

function Lavalink:play(guild_id, encoded_track)
  return self:update_player(guild_id, { encodedTrack = encoded_track })
end

function Lavalink:stop(guild_id)
  return self:update_player(guild_id, { encodedTrack = cjson.null })
end

function Lavalink:set_paused(guild_id, paused)
  return self:update_player(guild_id, { paused = paused })
end

function Lavalink:seek(guild_id, position_ms)
  return self:update_player(guild_id, { position = position_ms })
end

function Lavalink:set_volume(guild_id, volume)
  return self:update_player(guild_id, { volume = volume })
end

function Lavalink:connect()
  copas.addthread(function()
    local ws = new_ws_client()
    local url = ("ws://%s:%d/v4/websocket"):format(self.host, self.port)
    local ok, err = ws:connect(url, {
      ["Authorization"] = self.password,
      ["User-Id"] = self.user_id,
      ["Client-Name"] = "swarmlua/1.0",
    })
    if not ok then
      print("[lavalink] connect failed: " .. tostring(err) .. ", retrying in 5s")
      copas.sleep(5)
      return self:connect()
    end
    while true do
      local message = ws:receive()
      if not message then
        print("[lavalink] connection lost, reconnecting")
        copas.sleep(5)
        return self:connect()
      end
      local ok2, msg = pcall(cjson.decode, message)
      if ok2 then
        if msg.op == "ready" then
          self.session_id = msg.sessionId
          print("[lavalink] session ready: " .. tostring(self.session_id))
        end
        self:emit(msg.op, msg)
      end
    end
  end)
end

return Lavalink
