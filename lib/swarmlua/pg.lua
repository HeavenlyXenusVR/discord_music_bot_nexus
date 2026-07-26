-- Postgres connection helper (pgmoon-backed), auto-reconnecting.
local pgmoon = require("pgmoon")
local unpack = table.unpack or unpack

local Pg = {}
Pg.__index = Pg

function Pg.new(opts)
  local self = setmetatable({}, Pg)
  self.opts = {
    host = opts.host or "127.0.0.1",
    port = tonumber(opts.port or 5432),
    user = opts.user,
    password = opts.password,
    database = opts.database,
  }
  self.conn = nil
  return self
end

function Pg:ensure()
  if self.conn and self.conn.sock then
    return true
  end
  local conn = pgmoon.new(self.opts)
  local ok, err = conn:connect()
  if not ok then
    return nil, "postgres connect failed: " .. tostring(err)
  end
  -- Discord snowflake IDs (guild/user/channel/role) live in BIGINT columns and routinely
  -- exceed 2^53, the largest integer a Lua double can represent exactly. pgmoon's default
  -- OID 20 (int8) deserializer runs every bigint value through tonumber(), which silently
  -- corrupts them, e.g. 1304564041863266347 comes back as 1.3045640418633e+18. Force bigint
  -- columns to come back as the raw wire-format string instead, which preserves full
  -- precision; safe to hand straight back into another bigint column or WHERE clause since
  -- Postgres implicitly casts untyped string literals to the target numeric type.
  conn:set_type_deserializer(20, "string")
  self.conn = conn
  return true
end

-- query(sql, ...) -> rows, err. Params are escaped via pgmoon's built-in escaping.
function Pg:query(sql, ...)
  local ok, err = self:ensure()
  if not ok then return nil, err end

  local n = select('#', ...)
  if n > 0 then
    local args = { ... }
    for i = 1, n do
      local v = args[i]
      args[i] = (v == nil) and "NULL" or self.conn:escape_literal(v)
    end
    sql = sql:format(unpack(args, 1, n))
  end

  local res, err2 = self.conn:query(sql)
  if res == nil then
    -- connection may have dropped; retry once
    self.conn = nil
    local ok2 = self:ensure()
    if not ok2 then return nil, err2 end
    res, err2 = self.conn:query(sql)
    if res == nil then return nil, err2 end
  end
  return res
end

return Pg
