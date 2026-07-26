FROM alpine:3.20

# NOTE: Alpine 3.20's unversioned "luarocks" package is an empty stub (zero
# files) -- the real binary is luarocks-5.1, from the luarocks5.1 package.
# Confirmed by building this image; do not revert to plain "luarocks".
RUN apk add --no-cache \
      luajit luajit-dev luarocks5.1 \
      build-base \
      libsodium-dev opus-dev openssl-dev \
      git ca-certificates

# luarocks-5.1's own Lua-detection defaults don't find LuaJIT's headers/libs
# (it looks for /usr/include/lua/5.1, not where luajit-dev actually installs
# them), so point it at the real paths before installing anything.
RUN luarocks-5.1 config variables.LUA_INCDIR /usr/include/luajit-2.1 \
 && luarocks-5.1 config variables.LUA_LIBDIR /usr/lib

# Same package set as the swarm host (lua-shared/README.md), pinned to Lua 5.1
# semantics (LuaJIT's native ABI) so pgmoon/luasec/etc build against luajit.
# copas is listed separately from lua-websockets on purpose -- it is its own
# rock (lua-websockets only *supports* a copas client, it does not pull copas
# in as a dependency), and swarmlua's gateway/rest/lavalink modules all
# require("copas") directly.
RUN luarocks-5.1 install luasocket \
 && luarocks-5.1 install luasec \
 && luarocks-5.1 install lua-cjson \
 && luarocks-5.1 install pgmoon \
 && luarocks-5.1 install luasodium \
 && luarocks-5.1 install lua-websockets \
 && luarocks-5.1 install copas \
 && luarocks-5.1 install luaopus

WORKDIR /app

# Copy bot source (including the vendored lib/swarmlua/ shared library, copied
# verbatim from lua-shared/swarmlua/ plus this bot's local fixes -- see
# lib/swarmlua/gateway.lua and lib/swarmlua/bot.lua for the NOTE comments);
# secrets and runtime state stay in env files or mounted volumes.
COPY . .

# Start the bot
# CMD handled by compose -- see this bot's port report for the exact
# `command:` line: luajit nexus.lua
