-- Discord REST API client (blocking; see README for the async trade-off note).
require("copas") -- must load before socket.http/ssl.https anywhere in the process
local https = require("ssl.https")
local ltn12 = require("ltn12")
local cjson = require("cjson")

local API_BASE = "https://discord.com/api/v10"

local Rest = {}
Rest.__index = Rest

function Rest.new(token)
  return setmetatable({ token = token }, Rest)
end

function Rest:request(method, path, body)
  local response_body = {}
  local request_body = body and cjson.encode(body) or nil
  local headers = {
    ["Authorization"] = "Bot " .. self.token,
    ["User-Agent"] = "SwarmLua (https://github.com/, 1.0)",
    ["Content-Type"] = "application/json",
  }
  if request_body then
    headers["Content-Length"] = tostring(#request_body)
  end

  local ok, status = https.request({
    url = API_BASE .. path,
    method = method,
    headers = headers,
    source = request_body and ltn12.source.string(request_body) or nil,
    sink = ltn12.sink.table(response_body),
  })

  local raw = table.concat(response_body)
  local decoded = nil
  if #raw > 0 then
    local ok2, parsed = pcall(cjson.decode, raw)
    if ok2 then decoded = parsed end
  end

  if not ok then
    return nil, "request failed: " .. tostring(status)
  end
  if type(status) == "number" and status == 429 and decoded then
    -- Discord rate limit. Caller should back off retry_after seconds and retry.
    return nil, "rate_limited", decoded.retry_after
  end
  if type(status) == "number" and status >= 400 then
    return nil, "http " .. status .. ": " .. raw
  end
  return decoded, nil
end

function Rest:get(path) return self:request("GET", path) end
function Rest:post(path, body) return self:request("POST", path, body) end
function Rest:patch(path, body) return self:request("PATCH", path, body) end
function Rest:delete(path) return self:request("DELETE", path) end
function Rest:put(path, body) return self:request("PUT", path, body) end

return Rest
