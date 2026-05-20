# Nexus

![Nexus bot icon](assets/bot-icon.png)

Nexus is one node in the 12-bot Discord music fleet. It owns its queue tables, reports playback state to the shared swarm database, and is controlled directly from Discord, Aria, and SwarmPanel.

## What It Does

- Plays links, playlists, and queued tracks through Lavalink.
- Persists live queue, backup queue, current track, requester, playtime, and recovery state.
- Restores playback from database state after disconnects, restarts, or voice connection stalls.
- Uses smooth fade logic with many small volume steps instead of a low-volume pause followed by a jump.
- Replaces active filters instead of stacking old filters under new ones.
- Keeps auto shuffle from placing identical tracks next to each other when avoidable.
- Cleans local, pre-cache, and Lavalink-adjacent cache state on the built-in 10-hour maintenance timer.
- Reports heartbeat, queue depth, backup depth, track state, and errors for Aria and SwarmPanel.

## Fleet Role

- Node: `nexus`
- Database schema: `discord_music_nexus`
- Panel visibility: SwarmPanel bot dashboard, queue controls, recovery views, and health feed.
- Aria visibility: Medic drift checks, voice timeout triage, stale node checks, and recovery probes.

## Guardrails

- Owner-only command access keeps the bot private.
- Cookie jars, tokens, and environment secrets are ignored and should never be committed.
- Queue recovery prefers the persisted live queue first, then the backup queue when the live queue leaks or fails.

## Copyright

(c) HeavenlyXenusVR. Discord: <https://discord.com/users/1304564041863266347>
