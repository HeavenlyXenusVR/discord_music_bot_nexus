# Nexus

![Nexus bot icon](assets/bot-icon.png)

Nexus is one node in the 13-bot Discord music fleet. A connection point for servers that bridge multiple communities. It owns its queue tables, reports playback state to the shared swarm database, and is controlled directly from Discord, Aria, and SwarmPanel.

## Identity

- Discord display name: **GWS Music Bot (Nexus)**
- Internal node name: `nexus`
- Command namespace: `/nexus_main_*`

## What It Does

- Plays links, playlists, search results, and livestreams through Lavalink, picking the best available audio quality.
- Persists the live queue, backup queue, current track, requester, playtime, and recovery state to MySQL.
- Restores playback automatically from database state after disconnects, restarts, or voice connection stalls.
- Uses smooth fade logic with many small volume steps instead of a low-volume pause followed by a jump, with optional "smart" fade timing based on track length and active filters.
- Applies audio filters (nightcore, vaporwave, bass boost, and more) by replacing active filters instead of stacking them.
- Supports temporary per-track speed/pitch modifiers that automatically expire after a set number of tracks.
- Keeps auto-shuffle from placing duplicate tracks next to each other when avoidable.
- Runs a Smart Auto-DJ layer that learns server and per-user taste from likes, dislikes, and playback history, and can queue recommendations when the queue runs dry.
- Lets users save, list, load, and delete personal playlists, save the current queue as a playlist, and "steal" tracks from another listener's history.
- Tracks per-server leaderboards and play history, and can grab the currently playing track via DM.
- Cleans local, pre-cache, and Lavalink-adjacent cache state on the built-in 10-hour maintenance timer.
- Reports heartbeat, queue depth, backup depth, track state, and errors for Aria and SwarmPanel.

## Commands

All commands are namespaced as `/nexus_main_<command>`.

**Playback & Transport**
`play`, `playnext`, `skip`, `skipto`, `stop`, `pause`, `resume`, `replay`, `seek`, `forward`, `rewind`, `join`, `leave`, `nowplaying`

**Queue Management**
`queue`, `remove`, `move`, `bump`, `clearmine`, `clear`, `shuffle`, `voteskip`, `loop`

**Audio & Filters**
`volume`, `filter`, `fade`, `modify`

**Smart Auto-DJ & Discovery**
`autodj`, `recommend`, `like`, `dislike`, `taste`

**Personal Playlists & History**
`playlists`, `savequeue`, `loadqueue`, `deleteplaylist`, `history`, `userhistory`, `leaderboard`, `steal`, `grab`

**Server Configuration**
`sethome`, `setfeedback`, `djrole`, `removedj`, `djmode`, `settings`, `restart`

**Utility**
`panel`, `ping`, `uptime`, `stats`, `help`

## Fleet Role

- Node: `nexus`
- Database schema: `discord_music_nexus`
- Panel visibility: SwarmPanel bot dashboard, queue controls, recovery views, and health feed.
- Aria visibility: Medic drift checks, voice timeout triage, stale node checks, and recovery probes.

## Tech Stack

- [discord.py](https://github.com/Rapptz/discord.py) 2.7
- [Wavelink](https://github.com/PythonistaGuild/Wavelink) 3.5 + a shared Lavalink node (youtube, lavasrc, sponsorblock plugins)
- aiomysql against the shared swarm MySQL/MariaDB instance
- Dockerized via the fleet's `docker-compose.yml`, with persistent log and cache volumes

## Guardrails

- Owner-only command access keeps the bot private.
- Cookie jars, tokens, and environment secrets are ignored and should never be committed.
- Queue recovery prefers the persisted live queue first, then the backup queue when the live queue leaks or fails.

## Copyright

(c) HeavenlyXenusVR. Discord: <https://discord.com/users/1304564041863266347>
