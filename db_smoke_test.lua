package.path = "./lib/?.lua;./lib/?/init.lua;" .. package.path

local N = dofile("nexus.lua")

local TEST_GUILD = "999900001"
local TEST_USER = "999900002"
local TEST_USER2 = "999900003"

local function check(label, ok, err)
  if ok then
    print("OK   " .. label)
  else
    print("FAIL " .. label .. " -> " .. tostring(err))
  end
end

-- guild settings upsert/read/write
N.ensure_guild_settings(TEST_GUILD)
local settings = N.get_guild_settings(TEST_GUILD)
check("get_guild_settings returns row", settings ~= nil)
N.update_guild_setting(TEST_GUILD, "volume", 77)
N.update_guild_setting(TEST_GUILD, "loop_mode", "song")
settings = N.get_guild_settings(TEST_GUILD)
check("update_guild_setting volume persisted", tonumber(settings.volume) == 77, tostring(settings.volume))
check("update_guild_setting loop_mode persisted", settings.loop_mode == "song")

-- queue lifecycle
local uid1 = N.enqueue_track(TEST_GUILD, "https://example.com/a", "Track A", TEST_USER)
local uid2 = N.enqueue_track(TEST_GUILD, "https://example.com/b", "Track B", TEST_USER)
N.insert_queue_front(TEST_GUILD, "https://example.com/z", "Track Z (front)", TEST_USER)
check("queue_count after 3 inserts", N.queue_count(TEST_GUILD) == 3, N.queue_count(TEST_GUILD))
local rows = N.dbq("SELECT video_url, title FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC", TEST_GUILD)
check("insert_queue_front puts Z first", rows and rows[1] and rows[1].title == "Track Z (front)")

N.snapshot_queue_backup(TEST_GUILD)
local backup_count = N.db_one("SELECT COUNT(*) AS n FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus'", TEST_GUILD)
check("backup_count matches after snapshot", backup_count and math.floor(backup_count.n) == 3)

local n = N.shuffle_queue_rows(TEST_GUILD, true)
check("shuffle_queue_rows returns count", n == 3, n)

-- clear queue, then restore from backup
N.dbq("DELETE FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", TEST_GUILD)
check("queue emptied", N.queue_count(TEST_GUILD) == 0)
local restored = N.restore_queue_from_backup(TEST_GUILD)
check("restore_queue_from_backup restores 3", restored == 3 and N.queue_count(TEST_GUILD) == 3, restored)

-- track intelligence / affinity (the NOT-NULL counter-column gotcha)
local ok2, err2 = pcall(N.record_track_play_started, TEST_GUILD, "https://example.com/a", "Track A", TEST_USER)
check("record_track_play_started (was: NOT NULL violation before fix)", ok2, err2)
local ok4, err4 = pcall(N.record_track_outcome, TEST_GUILD, "https://example.com/a", "Track A", TEST_USER, true, 120)
check("record_track_outcome finished (was: NOT NULL violation before fix)", ok4, err4)
local ok5, err5 = pcall(N.record_track_outcome, TEST_GUILD, "https://example.com/b", "Track B", TEST_USER, false, 5)
check("record_track_outcome skipped", ok5, err5)
local ok6, err6 = pcall(N.record_track_feedback, TEST_GUILD, TEST_USER, "https://example.com/a", "Track A", true)
check("record_track_feedback like (was: NOT NULL violation before fix)", ok6, err6)
local ok7, err7 = pcall(N.record_track_feedback, TEST_GUILD, TEST_USER2, "https://example.com/b", "Track B", false)
check("record_track_feedback dislike", ok7, err7)

local key_a = N.url_key("https://example.com/a", "Track A")
local ti = N.db_one("SELECT queued_count, play_count, finish_count, like_count, dislike_count, skip_count, total_listen_seconds FROM nexus_track_intelligence WHERE guild_id = %s AND url_key = %s", TEST_GUILD, key_a)
check("track_intelligence row has all counters non-null",
  ti and ti.queued_count ~= nil and ti.play_count ~= nil and ti.finish_count ~= nil
    and ti.like_count ~= nil and ti.dislike_count ~= nil and ti.skip_count ~= nil and ti.total_listen_seconds ~= nil,
  tostring(ti))
check("track_intelligence play_count == 1 after one play", ti and tonumber(ti.play_count) == 1, ti and tostring(ti.play_count))
check("track_intelligence finish_count == 1 after one finish", ti and tonumber(ti.finish_count) == 1, ti and tostring(ti.finish_count))

local aff = N.db_one("SELECT queued_count, play_count, finish_count, skip_count, like_count, dislike_count, score FROM nexus_user_track_affinity WHERE guild_id = %s AND user_id = %s AND url_key = %s", TEST_GUILD, TEST_USER, key_a)
check("user_track_affinity row has all counters non-null",
  aff and aff.queued_count ~= nil and aff.play_count ~= nil and aff.finish_count ~= nil
    and aff.skip_count ~= nil and aff.like_count ~= nil and aff.dislike_count ~= nil)

-- /play's queued-count track_intelligence upsert (same INSERT shape as
-- record_track_play_started/outcome/feedback, exercised directly here since
-- it lives inline in the play command handler rather than its own function)
local ok_q, err_q = pcall(N.dbq, [[INSERT INTO nexus_track_intelligence (guild_id, url_key, video_url, title,
        queued_count, play_count, finish_count, skip_count, like_count, dislike_count, total_listen_seconds,
        last_requester_id, last_queued, source)
      VALUES (%s, %s, %s, %s, 1, 0, 0, 0, 0, 0, 0, %s, NOW(), 'queued')
      ON CONFLICT (guild_id, url_key) DO UPDATE SET queued_count = nexus_track_intelligence.queued_count + 1,
        last_requester_id = EXCLUDED.last_requester_id, last_queued = NOW()]],
    TEST_GUILD, N.url_key("https://example.com/c", "Track C"), "https://example.com/c", "Track C", TEST_USER)
check("/play queued-count track_intelligence upsert", ok_q, err_q)

-- autodj toggle
N.set_autodj_enabled(TEST_GUILD, true)
check("autodj enabled true", N.get_autodj_enabled(TEST_GUILD) == true)
N.set_autodj_enabled(TEST_GUILD, false)
check("autodj enabled false", N.get_autodj_enabled(TEST_GUILD) == false)

-- smart recommendation query (exercises the full weighted-decay SQL, incl. LEFT JOIN cooccurrence)
local ok9, rec = pcall(N.build_smart_recommendation, TEST_GUILD, { TEST_USER })
check("build_smart_recommendation runs", ok9 and rec and rec.query ~= nil, rec)

local ok10, avoid = pcall(N.load_smart_avoid_keys, TEST_GUILD, { TEST_USER2 })
check("load_smart_avoid_keys runs", ok10, avoid)

-- playback_state upsert/clear
local ok11, err11 = pcall(N.persist_playback_state, TEST_GUILD, "111", "https://example.com/a", "Track A", 42, true, false, uid1)
check("persist_playback_state upsert", ok11, err11)
local ps = N.db_one("SELECT position_seconds, is_playing FROM nexus_playback_state WHERE guild_id = %s AND bot_name = 'nexus'", TEST_GUILD)
check("playback_state position persisted", ps and tonumber(ps.position_seconds) == 42)
N.clear_playback_state(TEST_GUILD)
ps = N.db_one("SELECT video_url, is_playing FROM nexus_playback_state WHERE guild_id = %s AND bot_name = 'nexus'", TEST_GUILD)
check("clear_playback_state clears", ps and ps.video_url == nil and ps.is_playing == false)

-- voice_state upsert (the reconnect_attempts NOT-NULL gotcha)
local ok12, err12 = pcall(N.mark_voice_connected, TEST_GUILD, "222")
check("mark_voice_connected (was: NOT NULL violation before fix)", ok12, err12)
local vs = N.db_one("SELECT connected_channel_id::text AS connected_channel_id, desired_connected, reconnect_attempts FROM nexus_voice_state WHERE guild_id = %s AND bot_name = 'nexus'", TEST_GUILD)
check("voice_state connected row correct", vs and vs.connected_channel_id == "222" and vs.desired_connected == true and tonumber(vs.reconnect_attempts) == 0)
local ok13, err13 = pcall(N.mark_voice_disconnected, TEST_GUILD)
check("mark_voice_disconnected", ok13, err13)
vs = N.db_one("SELECT connected_channel_id, desired_connected FROM nexus_voice_state WHERE guild_id = %s AND bot_name = 'nexus'", TEST_GUILD)
check("voice_state disconnected row correct", vs and vs.connected_channel_id == nil and vs.desired_connected == false)

-- maintenance_tick / nexus_metrics (was: too few args for the %s placeholder
-- count -> "bad argument to 'format'" on every tick with an active player)
N.playback[TEST_GUILD] = {
  channel_id = "111", url = "https://example.com/a", title = "Track A",
  duration = 200, start_time = 0, offset = 55, paused = false, track_uid = uid1,
}
N.enqueue_track(TEST_GUILD, "https://example.com/c", "Track C", TEST_USER) -- so queue_count > 0
local ok15, err15 = pcall(N.maintenance_tick)
check("maintenance_tick runs with an active player (was: format() crash)", ok15, err15)
local metrics = N.db_one("SELECT queue_count, backup_queue_count, is_paused_db, position_seconds, duration_seconds FROM nexus_metrics WHERE guild_id = %s AND bot_name = 'nexus'", TEST_GUILD)
check("nexus_metrics row written", metrics ~= nil)
check("nexus_metrics is_paused_db is boolean false, not a position/duration value", metrics and metrics.is_paused_db == false, metrics and tostring(metrics.is_paused_db))
check("nexus_metrics position_seconds is a plausible position (not the duration/title)", metrics and tonumber(metrics.position_seconds) ~= nil and tonumber(metrics.position_seconds) < 300, metrics and tostring(metrics.position_seconds))
N.playback[TEST_GUILD] = nil

-- error_events (report_error)
local ok14, err14 = pcall(N.report_error, TEST_GUILD, "smoke_test", "smoke test error", "this is a test error event, safe to ignore")
check("report_error / persist_error_event", ok14, err14)
local ee = N.db_one("SELECT title, error_type FROM nexus_error_events WHERE guild_id = %s AND error_type = 'smoke_test' ORDER BY created_at DESC LIMIT 1", TEST_GUILD)
check("error_events row persisted", ee and ee.title == "smoke test error")

-- cleanup test rows
N.dbq("DELETE FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", TEST_GUILD)
N.dbq("DELETE FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus'", TEST_GUILD)
N.dbq("DELETE FROM nexus_playback_state WHERE guild_id = %s AND bot_name = 'nexus'", TEST_GUILD)
N.dbq("DELETE FROM nexus_guild_settings WHERE guild_id = %s", TEST_GUILD)
N.dbq("DELETE FROM nexus_swarm_toggles WHERE guild_id = %s", TEST_GUILD)
N.dbq("DELETE FROM nexus_track_intelligence WHERE guild_id = %s", TEST_GUILD)
N.dbq("DELETE FROM nexus_user_track_affinity WHERE guild_id = %s", TEST_GUILD)
N.dbq("DELETE FROM nexus_voice_state WHERE guild_id = %s AND bot_name = 'nexus'", TEST_GUILD)
N.dbq("DELETE FROM nexus_error_events WHERE guild_id = %s", TEST_GUILD)
N.dbq("DELETE FROM nexus_metrics WHERE guild_id = %s AND bot_name = 'nexus'", TEST_GUILD)
print("Cleanup complete.")
