import copy
import json
import asyncio
import discord
import aiohttp
import os
import time
import logging
import warnings
import random
import datetime
import sys
import yt_dlp
import aiomysql
import wavelink
from discord.ext import commands, tasks
from discord import app_commands
import re

warnings.filterwarnings("ignore", message=".*already exists.*", category=Warning, module=r"aiomysql\..*")
warnings.filterwarnings("ignore", message="Can't create database .*; database exists", category=Warning, module=r"aiomysql\..*")

# --- DAVE PROTOCOL MONKEYPATCH (FIXES LAVALINK 4.2.2 E2EE ENCRYPTION) ---
original_request = aiohttp.ClientSession.request
original__request = aiohttp.ClientSession._request
pending_voice_channels = {}

def _resolve_channel_id_for_guild(guild_id: int):
    try:
        global bot
        guild = bot.get_guild(guild_id) if bot else None
    except Exception:
        guild = None

    if guild:
        try:
            voice_client = guild.voice_client
            if voice_client and getattr(voice_client, "channel", None):
                return str(voice_client.channel.id)
        except Exception:
            pass
        try:
            if guild.me and guild.me.voice and guild.me.voice.channel:
                return str(guild.me.voice.channel.id)
        except Exception:
            pass

    fallback = pending_voice_channels.get(guild_id)
    return str(fallback) if fallback else None

def _inject_lavalink_channel_id(method, url, kwargs):
    if str(method).upper() != 'PATCH':
        return kwargs
    
    try:
        payload = kwargs.get("json")
        if not isinstance(payload, dict) or "voice" not in payload: 
            return kwargs
            
        voice_data = payload["voice"]
        if not isinstance(voice_data, dict) or "endpoint" not in voice_data or "channelId" in voice_data: 
            return kwargs
            
        url_str = str(url)
        match = re.search(r'/players/(\d+)', url_str)
        if not match: 
            return kwargs
            
        guild_id = int(match.group(1))
        resolved_channel_id = _resolve_channel_id_for_guild(guild_id)
        
        if resolved_channel_id:
            new_kwargs = copy.copy(kwargs)
            new_payload = copy.deepcopy(payload)
            new_payload["voice"]["channelId"] = resolved_channel_id
            new_kwargs["json"] = new_payload
            return new_kwargs
    except Exception:
        pass
        
    return kwargs

def patched_request(self, method, url, *args, **kwargs):
    kwargs = _inject_lavalink_channel_id(method, url, kwargs)
    return original_request(self, method, url, *args, **kwargs)

async def patched__request(self, method, url, *args, **kwargs):
    kwargs = _inject_lavalink_channel_id(method, url, kwargs)
    return await original__request(self, method, url, *args, **kwargs)

aiohttp.ClientSession.request = patched_request
aiohttp.ClientSession._request = patched__request

# --- ENHANCED ROBUST WEBHOOK DISPATCHER ---
WEBHOOK_URL = os.getenv('NEXUS_WEBHOOK_URL', '').strip()

async def send_webhook_log(bot_name, title, description, color, retries=3, image_url=None, fields=None):
    if not WEBHOOK_URL or WEBHOOK_URL == 'PASTE_YOUR_NEW_WEBHOOK_URL_HERE':
        return

    for attempt in range(retries):
        try:
            async with HTTPSessionManager() as session:
                webhook = discord.Webhook.from_url(WEBHOOK_URL, session=session)
                embed = discord.Embed(title=title, description=description, color=color, timestamp=discord.utils.utcnow())
                embed.set_footer(text="Swarm Network Matrix")
                if image_url: embed.set_thumbnail(url=image_url)
                if fields:
                    for name, value, inline in fields:
                        embed.add_field(name=name, value=value, inline=inline)

                await webhook.send(embed=embed, username=f"Node: {bot_name.capitalize()}")
                return
        except discord.errors.NotFound:
            logger.error("❌ WEBHOOK KILLED: Discord deleted your webhook. Create a new one.")
            return
        except Exception as e:
            if attempt < retries - 1: await asyncio.sleep(2 ** attempt)
            else: logger.error(f"❌ Webhook Dispatch Failed: {e}")

async def ensure_database_exists():
    """Create this bot's MariaDB schema before opening the normal pooled connection."""
    db_name = str(DB_CONFIG.get("db") or "").strip()
    if not db_name:
        raise RuntimeError("DB_CONFIG['db'] is empty; cannot create bot database.")
    if not re.fullmatch(r"[A-Za-z0-9_]+", db_name):
        raise RuntimeError(f"Unsafe database name in DB_CONFIG['db']: {db_name!r}")
    conn = await aiomysql.connect(
        host=DB_CONFIG.get("host", "host.docker.internal"),
        port=int(DB_CONFIG.get("port", 3306)),
        user=DB_CONFIG.get("user", "botuser"),
        password=DB_CONFIG.get("password", ""),
        autocommit=True,
        connect_timeout=10,
    )
    try:
        async with conn.cursor() as cur:
            await cur.execute("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = %s", (db_name,))
            if not await cur.fetchone():
                await cur.execute(f"CREATE DATABASE `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
    finally:
        conn.close()

class DBPoolManager:
    _pool = None
    _lock = asyncio.Lock()

    async def __aenter__(self):
        async with DBPoolManager._lock:
            pool = DBPoolManager._pool
            if pool is None or getattr(pool, "closed", False):
                await ensure_database_exists()
                DBPoolManager._pool = await aiomysql.create_pool(minsize=1, maxsize=10, **DB_CONFIG)
            else:
                try:
                    async with pool.acquire() as conn:
                        await conn.ping(reconnect=True)
                except Exception:
                    try:
                        pool.close()
                        await pool.wait_closed()
                    except Exception:
                        pass
                    await ensure_database_exists()
                    DBPoolManager._pool = await aiomysql.create_pool(minsize=1, maxsize=10, **DB_CONFIG)
        return DBPoolManager._pool

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False

class HTTPSessionManager:
    _session = None
    async def __aenter__(self):
        if not HTTPSessionManager._session or HTTPSessionManager._session.closed:
            HTTPSessionManager._session = aiohttp.ClientSession()
        return HTTPSessionManager._session
    async def __aexit__(self, exc_type, exc_val, exc_tb): pass

# --- LOGGING SETUP (Per-bot logs) ---
import os
from logging.handlers import RotatingFileHandler

BOT_ENV_PREFIX = "NEXUS"
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = os.path.join(log_dir, f"{BOT_ENV_PREFIX.lower()}.log")
file_handler = RotatingFileHandler(filename=log_filename, encoding="utf-8", mode="a", maxBytes=10*1024*1024, backupCount=5)
discord.utils.setup_logging(handler=file_handler, level=logging.INFO)
logger = logging.getLogger("discord")

def _player_is_playing(player):
    """Robust Wavelink/discord.py playback detector.

    Fixes false idle presence by treating a connected player with a current
    track as active even when Wavelink's boolean flag is stale/unimplemented.
    """
    if not player:
        return False
    current_track = _player_current_track(player)
    if current_track is None:
        return False
    paused = False
    for attr in ("paused", "is_paused"):
        value = getattr(player, attr, None)
        try:
            if callable(value):
                value = value()
        except TypeError:
            pass
        except Exception:
            value = None
        if isinstance(value, bool):
            paused = value
            break
    if paused:
        return False
    for attr in ("playing", "is_playing"):
        value = getattr(player, attr, None)
        try:
            if callable(value):
                value = value()
        except TypeError:
            pass
        except Exception:
            value = None
        if isinstance(value, bool) and value:
            return True
    return True

def _player_is_paused(player):
    if not player:
        return False
    if _player_current_track(player) is None:
        return False
    for attr in ("paused", "is_paused"):
        value = getattr(player, attr, None)
        try:
            if callable(value):
                value = value()
        except TypeError:
            pass
        except Exception:
            value = None
        if isinstance(value, bool):
            return value
    return False

def _player_current_track(player):
    if not player:
        return None
    for attr in ("current", "track", "source", "playing_track"):
        try:
            value = getattr(player, attr, None)
            if callable(value):
                value = value()
            if value:
                return value
        except Exception:
            continue
    return None

def _track_title_from_obj(track):
    if not track:
        return None
    if isinstance(track, str):
        return track.strip() or None
    for attr in ("title", "name", "raw_title"):
        try:
            value = getattr(track, attr, None)
            if callable(value):
                value = value()
            if value:
                return str(value).strip()
        except Exception:
            continue
    try:
        value = str(track).strip()
        if value and value.lower() not in {"none", "unknown"}:
            return value
    except Exception:
        pass
    return None

def _voice_client_connected(vc):
    if not vc:
        return False
    try:
        value = getattr(vc, "is_connected", None)
        if callable(value):
            return bool(value())  # Return definitive result; False means disconnected
        if isinstance(value, bool):
            return value
    except Exception:
        pass
    return bool(getattr(vc, "channel", None))

def _player_is_active(player):
    return _player_is_playing(player) or _player_is_paused(player) or (_voice_client_connected(player) and _player_current_track(player) is not None)
# --- OPTIMIZATION MAP: startup, DB bootstrap, runtime recovery, slash commands, and swarm bridge are intentionally separated below. ---
# --- CONFIGURATION ---
BOT_ENV_PREFIX = "NEXUS"
TOKEN = os.getenv(f"{BOT_ENV_PREFIX}_DISCORD_TOKEN", "").strip()
DB_CONFIG = {
    'host': os.getenv(f"{BOT_ENV_PREFIX}_DB_HOST") or os.getenv("DB_HOST") or os.getenv("MYSQL_HOST") or "host.docker.internal",
    'user': os.getenv(f"{BOT_ENV_PREFIX}_DB_USER") or os.getenv("DB_USER") or os.getenv("MYSQL_USER") or "botuser",
    'password': os.getenv(f"{BOT_ENV_PREFIX}_DB_PASSWORD") or os.getenv("DB_PASSWORD") or os.getenv("MYSQL_PASSWORD") or "",
    'db': os.getenv(f"{BOT_ENV_PREFIX}_DB_NAME") or "discord_music_nexus",
    'autocommit': True,
    'pool_recycle': 280
}
DEFAULT_LAVALINK_URI = os.getenv("LAVALINK_URI") or os.getenv("LAVALINK_URL") or os.getenv("LAVALINK_HOST") or "http://127.0.0.1:2333"
DEFAULT_LAVALINK_PASSWORD = os.getenv("LAVALINK_PASSWORD", "gws_swarm_secret_password")
LAVALINK_URI = os.getenv(f"{BOT_ENV_PREFIX}_LAVALINK_URI", DEFAULT_LAVALINK_URI).strip()
LAVALINK_PASSWORD = os.getenv(f"{BOT_ENV_PREFIX}_LAVALINK_PASSWORD", DEFAULT_LAVALINK_PASSWORD).strip()

ERROR_WEBHOOK_URL = (
    os.getenv(f"{BOT_ENV_PREFIX}_ERROR_WEBHOOK_URL", "").strip()
    or os.getenv("SWARM_ERROR_WEBHOOK_URL", "").strip()
    or os.getenv("ERROR_WEBHOOK_URL", "").strip()
    or os.getenv("SWARM_WEBHOOK_ERROR_URL", "").strip()
)
error_reporting_installed = False


def _shorten_error_text(value, limit=1800):
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


async def _persist_error_event(title, description, traceback_text=None, guild_id=None, error_type="runtime", level="error"):
    table_name = f"{BOT_ENV_PREFIX.lower()}_error_events"
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            bot_name VARCHAR(50) NOT NULL,
                            guild_id BIGINT NULL,
                            error_level VARCHAR(20) NOT NULL DEFAULT 'error',
                            error_type VARCHAR(50) NOT NULL DEFAULT 'runtime',
                            title VARCHAR(255) NOT NULL,
                            description TEXT NULL,
                            traceback_text MEDIUMTEXT NULL,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                        """
                    )
                    await cur.execute(
                        f"INSERT INTO {table_name} (bot_name, guild_id, error_level, error_type, title, description, traceback_text) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                        (BOT_ENV_PREFIX.lower(), guild_id, level, error_type, _shorten_error_text(title, 255), _shorten_error_text(description, 5000), _shorten_error_text(traceback_text, 20000) if traceback_text else None),
                    )
    except Exception as db_exc:
        print(f"[{BOT_ENV_PREFIX}] Failed to persist error event: {db_exc}", file=sys.stderr)


async def send_error_webhook_log(bot_name, title, description, color=discord.Color.red(), retries=3, fields=None, traceback_text=None):
    if not ERROR_WEBHOOK_URL or ERROR_WEBHOOK_URL == 'PASTE_YOUR_NEW_WEBHOOK_URL_HERE':
        return

    for attempt in range(retries):
        try:
            async with HTTPSessionManager() as session:
                webhook = discord.Webhook.from_url(ERROR_WEBHOOK_URL, session=session)
                embed = discord.Embed(
                    title=_shorten_error_text(title, 256),
                    description=_shorten_error_text(description, 3500),
                    color=color,
                    timestamp=discord.utils.utcnow(),
                )
                embed.set_footer(text="Swarm Error Matrix")
                if fields:
                    for name, value, inline in fields:
                        embed.add_field(name=name, value=_shorten_error_text(value, 1024), inline=inline)
                if traceback_text:
                    embed.add_field(name="Traceback", value="```py\n{}\n```".format(_shorten_error_text(traceback_text, 900)), inline=False)
                await webhook.send(embed=embed, username=f"Error Node: {bot_name.capitalize()}")
                return
        except discord.errors.NotFound:
            print(f"[{BOT_ENV_PREFIX}] Error webhook no longer exists.", file=sys.stderr)
            return
        except Exception as exc:
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)
            else:
                print(f"[{BOT_ENV_PREFIX}] Error webhook dispatch failed: {exc}", file=sys.stderr)


async def report_runtime_error(title, error=None, *, description=None, traceback_text=None, guild_id=None, error_type="runtime", level="error"):
    if description is None:
        description = str(error or title)
    if traceback_text is None and error is not None:
        traceback_text = ''.join(__import__('traceback').format_exception(type(error), error, error.__traceback__))
    field_rows = []
    if guild_id:
        field_rows.append(("Guild ID", str(guild_id), True))
    if error_type:
        field_rows.append(("Type", str(error_type), True))
    await _persist_error_event(title, description, traceback_text=traceback_text, guild_id=guild_id, error_type=error_type, level=level)
    await send_error_webhook_log(
        bot.user.name if getattr(bot, 'user', None) else BOT_ENV_PREFIX,
        title,
        description,
        color=discord.Color.red(),
        fields=field_rows,
        traceback_text=traceback_text,
    )


def dispatch_runtime_error(title, error=None, *, description=None, traceback_text=None, guild_id=None, error_type="runtime", level="error"):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = getattr(bot, 'loop', None)
    if loop and loop.is_running():
        loop.create_task(
            report_runtime_error(
                title,
                error,
                description=description,
                traceback_text=traceback_text,
                guild_id=guild_id,
                error_type=error_type,
                level=level,
            )
        )


class SwarmErrorWebhookHandler(logging.Handler):
    def emit(self, record):
        try:
            message = record.getMessage()
        except Exception:
            message = str(record.msg)
        if not message:
            return
        lowered = message.lower()
        if "error webhook dispatch failed" in lowered or "failed to persist error event" in lowered:
            return
        traceback_text = None
        if record.exc_info:
            traceback_text = ''.join(__import__('traceback').format_exception(*record.exc_info))
        dispatch_runtime_error(
            f"Python Log Error [{record.name}]",
            description=message,
            traceback_text=traceback_text,
            error_type="python_log",
            level="error",
        )


def _asyncio_exception_handler(loop, context):
    error = context.get('exception')
    message = context.get('message') or 'Unhandled asyncio exception'
    traceback_text = None
    if error is not None:
        traceback_text = ''.join(__import__('traceback').format_exception(type(error), error, error.__traceback__))
    elif context:
        traceback_text = repr(context)
    dispatch_runtime_error(
        'Asyncio Runtime Error',
        error,
        description=message,
        traceback_text=traceback_text,
        error_type='asyncio',
        level='error',
    )


def install_error_reporting():
    global error_reporting_installed
    if error_reporting_installed:
        return
    handler = SwarmErrorWebhookHandler(level=logging.ERROR)
    logger.addHandler(handler)
    logging.getLogger().addHandler(handler)
    sys.excepthook = lambda exc_type, exc, tb: dispatch_runtime_error(
        'Uncaught Python Exception',
        exc,
        description=str(exc),
        traceback_text=''.join(__import__('traceback').format_exception(exc_type, exc, tb)),
        error_type='uncaught_exception',
        level='critical',
    )
    error_reporting_installed = True


def _normalize_lavalink_uri(uri: str) -> str:
    value = str(uri or "").strip()
    if value and "://" not in value:
        value = f"http://{value}"
    return value or "http://127.0.0.1:2333"

LAVALINK_URI = _normalize_lavalink_uri(LAVALINK_URI)
POSITION_UPDATER_INTERVAL = max(5.0, float(os.getenv(f"{BOT_ENV_PREFIX}_POSITION_UPDATER_INTERVAL", "10")))
POSITION_PERSIST_INTERVAL = max(POSITION_UPDATER_INTERVAL, float(os.getenv(f"{BOT_ENV_PREFIX}_POSITION_PERSIST_INTERVAL", "15")))
PLAYLIST_SYNC_INTERVAL = max(45.0, float(os.getenv(f"{BOT_ENV_PREFIX}_PLAYLIST_SYNC_INTERVAL", "90")))
AUTO_HEAL_INTERVAL = max(15.0, float(os.getenv(f"{BOT_ENV_PREFIX}_AUTO_HEAL_INTERVAL", "20")))
AUTO_IMPORT_IDLE_SECONDS = max(45.0, float(os.getenv(f"{BOT_ENV_PREFIX}_AUTO_IMPORT_IDLE_SECONDS", "45")))
RECOVERY_RETRY_BASE_DELAY = max(3.0, float(os.getenv(f"{BOT_ENV_PREFIX}_RECOVERY_RETRY_BASE_DELAY", "5")))
RECOVERY_RETRY_MAX_DELAY = max(RECOVERY_RETRY_BASE_DELAY, float(os.getenv(f"{BOT_ENV_PREFIX}_RECOVERY_RETRY_MAX_DELAY", "30")))
MAX_RECOVERY_RETRIES = max(3, int(os.getenv(f"{BOT_ENV_PREFIX}_MAX_RECOVERY_RETRIES", "6")))
WATCHDOG_REVIVAL_COOLDOWN = max(10.0, float(os.getenv(f"{BOT_ENV_PREFIX}_WATCHDOG_REVIVAL_COOLDOWN", "15")))
WATCHDOG_MAX_REVIVALS = max(3, int(os.getenv(f"{BOT_ENV_PREFIX}_WATCHDOG_MAX_REVIVALS", "6")))
AUTO_RESTORE_SNOOZE_SECONDS = max(60.0, float(os.getenv(f"{BOT_ENV_PREFIX}_AUTO_RESTORE_SNOOZE_SECONDS", "180")))
RECOVERY_EXHAUSTED_COOLDOWN_SECONDS = max(120.0, float(os.getenv(f"{BOT_ENV_PREFIX}_RECOVERY_EXHAUSTED_COOLDOWN_SECONDS", "600")))
PERIODIC_RESTART_HOURS = max(1.0, float(os.getenv(f"{BOT_ENV_PREFIX}_PERIODIC_RESTART_HOURS", os.getenv("PERIODIC_RESTART_HOURS", "5"))))

intents = discord.Intents.default()
intents.message_content = False
bot = commands.Bot(command_prefix="/", intents=intents)
bot.start_time = time.time()
playback_tracking = {}
guild_states = {}
auto_heal_initialized = False
recovering_guilds = set()
process_queue_locks = {}
voice_connect_locks = {}
last_position_persist = {}
playlist_db_initialized = False
playlist_db_lock = asyncio.Lock()
recovery_retry_tasks = {}
recovery_retry_counts = {}
recovery_exhausted_until = {}
idle_voice_since = {}
auto_restore_snooze_until = {}
startup_task_registry = {}
MAX_RUNTIME_GUILD_CACHE_ENTRIES = max(64, int(os.getenv(f"{BOT_ENV_PREFIX}_RUNTIME_GUILD_CACHE_MAX", os.getenv("RUNTIME_GUILD_CACHE_MAX", "512"))))

def _runtime_key(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return value

def prune_runtime_state_cache():
    try:
        active_guild_ids = {_runtime_key(g.id) for g in bot.guilds}
    except Exception:
        active_guild_ids = set()
    protected = {_runtime_key(key) for key in playback_tracking.keys()} | {_runtime_key(key) for key in guild_states.keys()} | {_runtime_key(key) for key in recovering_guilds} | {_runtime_key(key) for key in recovery_retry_tasks.keys()}

    def prune_mapping(mapping):
        for key in list(mapping.keys()):
            normalized = _runtime_key(key)
            if active_guild_ids and normalized not in active_guild_ids and normalized not in protected:
                mapping.pop(key, None)
        while len(mapping) > MAX_RUNTIME_GUILD_CACHE_ENTRIES:
            removable = None
            for key in list(mapping.keys()):
                normalized = _runtime_key(key)
                if normalized not in protected:
                    removable = key
                    break
            if removable is None:
                break
            mapping.pop(removable, None)

    for mapping in (process_queue_locks, voice_connect_locks, last_position_persist, recovery_retry_counts, recovery_exhausted_until, idle_voice_since, auto_restore_snooze_until, vote_skip_sessions, metrics_last_errors):
        prune_mapping(mapping)



def schedule_named_task(name, coro):
    """Prevent duplicate startup/recovery tasks across reconnecting on_ready events."""
    existing = startup_task_registry.get(name)
    if existing and not existing.done():
        try:
            coro.close()
        except Exception:
            pass
        return existing
    task = asyncio.create_task(coro)
    startup_task_registry[name] = task

    def _cleanup(done_task):
        if startup_task_registry.get(name) is done_task:
            startup_task_registry.pop(name, None)
        try:
            done_task.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Background task %s failed", name)

    task.add_done_callback(_cleanup)
    return task


async def request_supervisor_restart(reason: str, *, announce: bool = True):
    logger.warning("[%s] Restart requested (%s); exiting for container supervisor restart.", BOT_ENV_PREFIX.lower(), reason)
    if announce:
        try:
            await send_webhook_log(
                bot.user.name if bot.user else BOT_ENV_PREFIX.capitalize(),
                "♻️ Node Restart",
                f"This node is restarting for **{reason.replace('_', ' ')}**. Docker will bring it back online automatically.",
                discord.Color.orange(),
            )
        except Exception:
            logger.debug("[%s] Failed to publish restart webhook.", BOT_ENV_PREFIX.lower(), exc_info=True)
    await bot.close()
    sys.exit(0)


vote_skip_sessions = {}
metrics_last_errors = {}
METRICS_HEARTBEAT_INTERVAL = max(5, int(os.getenv(f"{BOT_ENV_PREFIX}_METRICS_HEARTBEAT_INTERVAL", os.getenv("METRICS_HEARTBEAT_INTERVAL", "15"))))
VOICE_REJOIN_DELAY_SECONDS = max(1, int(os.getenv(f"{BOT_ENV_PREFIX}_VOICE_REJOIN_DELAY_SECONDS", os.getenv("VOICE_REJOIN_DELAY_SECONDS", "5"))))
VOICE_CONNECT_TIMEOUT_SECONDS = max(10.0, float(os.getenv(f"{BOT_ENV_PREFIX}_VOICE_CONNECT_TIMEOUT_SECONDS", os.getenv("VOICE_CONNECT_TIMEOUT_SECONDS", "30"))))
VOICE_CONNECT_TIMEOUT_BACKOFF_SECONDS = max(120.0, float(os.getenv(f"{BOT_ENV_PREFIX}_VOICE_CONNECT_TIMEOUT_BACKOFF_SECONDS", os.getenv("VOICE_CONNECT_TIMEOUT_BACKOFF_SECONDS", "30"))))

lavalink_connect_task = None
lavalink_connect_lock = asyncio.Lock()
lavalink_recovery_bootstrapped = False
BOT_RECOVERY_ORDER = ["GWS", "HARMONIC", "MAESTRO", "MELODIC", "NEXUS", "RHYTHM", "SYMPHONY", "TUNESTREAM", "ALUCARD", "SAPPHIRE"]
BOT_RECOVERY_STAGGER_SECONDS = max(0.0, float(os.getenv(f"{BOT_ENV_PREFIX}_RECOVERY_STARTUP_STAGGER_SECONDS", os.getenv("RECOVERY_STARTUP_STAGGER_SECONDS", str((BOT_RECOVERY_ORDER.index(BOT_ENV_PREFIX) if BOT_ENV_PREFIX in BOT_RECOVERY_ORDER else 0) * 8)))))

def _get_pool_nodes():
    nodes = getattr(wavelink.Pool, "nodes", None)
    if isinstance(nodes, dict):
        return list(nodes.values())
    if isinstance(nodes, (list, tuple, set)):
        return list(nodes)
    return []

def _has_connected_lavalink_node() -> bool:
    for node in _get_pool_nodes():
        if getattr(node, "available", False) or getattr(node, "connected", False):
            return True
        status = str(getattr(node, "status", ""))
        if "CONNECTED" in status.upper():
            return True
    return False

async def _connect_lavalink_forever():
    await bot.wait_until_ready()
    async with lavalink_connect_lock:
        while not _has_connected_lavalink_node():
            try:
                logger.info(f"Connecting to Lavalink at {LAVALINK_URI}")
                await wavelink.Pool.connect(nodes=[wavelink.Node(uri=LAVALINK_URI, password=LAVALINK_PASSWORD)], client=bot, cache_capacity=100)
            except Exception as exc:
                logger.warning(f"Waiting for Lavalink to boot or authenticate... Retrying in 5s ({exc})")
                await asyncio.sleep(5)
            else:
                for _ in range(40):
                    if _has_connected_lavalink_node():
                        return
                    await asyncio.sleep(0.5)
                logger.warning("Lavalink connection attempt did not become ready within 20s; retrying.")

def ensure_lavalink_connection_task():
    global lavalink_connect_task
    if lavalink_connect_task is None or lavalink_connect_task.done():
        lavalink_connect_task = asyncio.create_task(_connect_lavalink_forever())
    return lavalink_connect_task

async def ensure_lavalink_ready(timeout: float = 20.0) -> bool:
    if _has_connected_lavalink_node():
        return True
    ensure_lavalink_connection_task()
    deadline = asyncio.get_running_loop().time() + max(1.0, timeout)
    while asyncio.get_running_loop().time() < deadline:
        if _has_connected_lavalink_node():
            return True
        await asyncio.sleep(0.5)
    return _has_connected_lavalink_node()

@tasks.loop(seconds=30.0)
async def lavalink_health_monitor():
    try:
        if not _has_connected_lavalink_node():
            logger.warning("[nexus] Lavalink health check failed; reconnect task armed.")
            ensure_lavalink_connection_task()
            for guild in bot.guilds:
                vc = guild.voice_client
                if vc and not _player_is_active(vc):
                    state = await derive_recovery_state_from_db(guild.id)
                    channel_id = (state or {}).get("voice_channel_id") or getattr(getattr(vc, "channel", None), "id", None)
                    if channel_id:
                        position = int((state or {}).get("position", 0) or 0)
                        await remember_recovery_state(guild.id, channel_id, position)
                        schedule_recovery_retry(guild.id, channel_id, start_position=position, reason="lavalink_health")
    except Exception:
        logger.exception("[nexus] Lavalink health monitor failed.")

async def on_ready_lavalink_health():
    ensure_lavalink_connection_task()
    if not lavalink_health_monitor.is_running():
        lavalink_health_monitor.start()
bot.add_listener(on_ready_lavalink_health, 'on_ready')


def _safe_display_name(member_or_user):
    if not member_or_user:
        return "Unknown User"
    return getattr(member_or_user, "display_name", None) or getattr(member_or_user, "global_name", None) or getattr(member_or_user, "name", None) or "Unknown User"

async def resolve_requester_name(guild, requester_id):
    if not requester_id:
        return "Unknown User"
    try:
        requester_id = int(requester_id)
    except Exception:
        return "Unknown User"
    member = guild.get_member(requester_id) if guild else None
    if member:
        return _safe_display_name(member)
    user = bot.get_user(requester_id)
    if user:
        return _safe_display_name(user)
    try:
        user = await bot.fetch_user(requester_id)
        return _safe_display_name(user)
    except Exception:
        return f"User {requester_id}"

async def get_autodj_enabled(guild_id):
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute("SELECT auto_dj FROM nexus_swarm_toggles WHERE guild_id = %s", (guild_id,))
                    row = await cur.fetchone()
                    return bool(row and row.get('auto_dj'))
    except Exception:
        return False

async def set_autodj_enabled(guild_id, enabled):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute("INSERT INTO nexus_swarm_toggles (guild_id, auto_dj) VALUES (%s, %s) ON DUPLICATE KEY UPDATE auto_dj = VALUES(auto_dj)", (guild_id, bool(enabled)))

async def build_autodj_query(cur, guild_id):
    await cur.execute("SELECT title FROM nexus_history WHERE guild_id = %s ORDER BY played_at DESC LIMIT 8", (guild_id,))
    recent_rows = await cur.fetchall()
    _t = lambda r: r.get("title") if isinstance(r, dict) else r[0]  # DictCursor-safe
    recent_titles = [_t(row) for row in recent_rows if row and _t(row)]
    for title in recent_titles:
        cleaned = re.sub(r"\s*\([^)]*\)|\s*\[[^\]]*\]", "", title).strip()
        if cleaned:
            return f"ytmsearch:{cleaned} audio"
    fallback_terms = ["lofi hip hop", "synthwave mix", "chill electronic", "gaming music", "jazz hop"]
    return f"ytmsearch:{random.choice(fallback_terms)}"

async def maybe_enqueue_autodj(cur, guild, channel_id):
    if not await get_autodj_enabled(guild.id):
        return False
    query = await build_autodj_query(cur, guild.id)
    try:
        entries, _playlist_result = await search_playables(query)
        history_titles = set()
        await cur.execute("SELECT title FROM nexus_history WHERE guild_id = %s ORDER BY played_at DESC LIMIT 12", (guild.id,))
        for row in await cur.fetchall():
            _rv = row.get("title") if isinstance(row, dict) else row[0]  # DictCursor-safe
            if row and _rv:
                history_titles.add(str(_rv).strip().lower())
        chosen = None
        for entry in entries:
            title_key = str(getattr(entry, 'title', '')).strip().lower()
            if title_key and title_key not in history_titles:
                chosen = entry
                break
        if not chosen and entries:
            chosen = entries[0]
        if not chosen:
            return False
        await enqueue_track(cur, guild.id, chosen.uri, chosen.title, bot.user.id if bot.user else None)
        schedule_named_task(f"autodj_process_queue:{guild.id}", process_queue(guild, channel_id))
        return True
    except Exception as exc:
        logger.warning(f"[{guild.id}] Auto-DJ recommendation failed: {exc}")
        return False

async def get_saved_settings_summary(guild_id):
    await ensure_guild_settings(guild_id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT home_vc_id, volume, loop_mode, filter_mode, dj_role_id, feedback_channel_id, transition_mode, custom_speed, custom_pitch, custom_modifiers_left, dj_only_mode, stay_in_vc FROM nexus_guild_settings WHERE guild_id = %s", (guild_id,))
                return await cur.fetchone()

ytdl_format_options = {
    'format': 'bestaudio/best', 'outtmpl': '%(extractor)s-%(id)s-%(title)s.%(ext)s',
    'restrictfilenames': True, 'noplaylist': True, 'nocheckcertificate': True,
    'ignoreerrors': True, 'logtostderr': False, 'quiet': True,
    'no_warnings': True, 'default_search': 'auto', 'source_address': '0.0.0.0'
}

# --- DATABASE INITIALIZATION ---
async def init_db():
    global playlist_db_initialized
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_playback_state (guild_id BIGINT, bot_name VARCHAR(50), channel_id BIGINT, video_url TEXT, position_seconds INT DEFAULT 0, is_playing BOOLEAN DEFAULT FALSE, is_paused BOOLEAN DEFAULT FALSE, title TEXT, PRIMARY KEY (guild_id, bot_name))")
                try: await cur.execute("ALTER TABLE nexus_playback_state ADD COLUMN title TEXT")
                except: pass
                try: await cur.execute("ALTER TABLE nexus_playback_state ADD COLUMN is_paused BOOLEAN DEFAULT FALSE")
                except: pass
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_guild_settings (guild_id BIGINT PRIMARY KEY, home_vc_id BIGINT, volume INT DEFAULT 100, loop_mode VARCHAR(10) DEFAULT 'queue', filter_mode VARCHAR(20) DEFAULT 'none', dj_role_id BIGINT DEFAULT NULL, feedback_channel_id BIGINT DEFAULT NULL, transition_mode VARCHAR(10) DEFAULT 'off', custom_speed FLOAT DEFAULT 1.0, custom_pitch FLOAT DEFAULT 1.0, custom_modifiers_left INT DEFAULT 0, dj_only_mode BOOLEAN DEFAULT FALSE, stay_in_vc BOOLEAN DEFAULT FALSE)")
                try: await cur.execute("ALTER TABLE nexus_guild_settings MODIFY loop_mode VARCHAR(10) DEFAULT 'queue'")
                except: pass
                try: await cur.execute("UPDATE nexus_guild_settings SET loop_mode = 'queue' WHERE loop_mode IS NULL OR loop_mode NOT IN ('off', 'song', 'queue')")
                except: pass
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_queue (id INT AUTO_INCREMENT PRIMARY KEY, guild_id BIGINT, bot_name VARCHAR(50), video_url TEXT, title TEXT, requester_id BIGINT DEFAULT NULL)")
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_queue_backup (id INT AUTO_INCREMENT PRIMARY KEY, guild_id BIGINT, bot_name VARCHAR(50), video_url TEXT, title TEXT, requester_id BIGINT DEFAULT NULL)")
                try: await cur.execute("ALTER TABLE nexus_queue ADD COLUMN bot_name VARCHAR(50)")
                except: pass
                try: await cur.execute("ALTER TABLE nexus_queue_backup ADD COLUMN bot_name VARCHAR(50)")
                except: pass
                try: await cur.execute("ALTER TABLE nexus_queue ADD COLUMN requester_id BIGINT DEFAULT NULL")
                except: pass
                try: await cur.execute("ALTER TABLE nexus_queue_backup ADD COLUMN requester_id BIGINT DEFAULT NULL")
                except: pass
                try: await cur.execute("ALTER TABLE nexus_history ADD COLUMN requester_id BIGINT DEFAULT NULL")
                except: pass
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_history (id INT AUTO_INCREMENT PRIMARY KEY, guild_id BIGINT, video_url TEXT, title TEXT, played_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, requester_id BIGINT DEFAULT NULL)")
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_user_playlists (id INT AUTO_INCREMENT PRIMARY KEY, user_id BIGINT, playlist_name VARCHAR(255), video_url TEXT, title TEXT)")
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_bot_home_channels (guild_id BIGINT, bot_name VARCHAR(50), home_vc_id BIGINT, PRIMARY KEY (guild_id, bot_name))")
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_voice_state (guild_id BIGINT, bot_name VARCHAR(50), last_channel_id BIGINT NULL, connected_channel_id BIGINT NULL, text_channel_id BIGINT NULL, disconnected_at TIMESTAMP NULL, last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, desired_connected BOOLEAN DEFAULT FALSE, reconnect_attempts INT NOT NULL DEFAULT 0, last_error TEXT NULL, PRIMARY KEY (guild_id, bot_name))")
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_metrics (guild_id BIGINT, bot_name VARCHAR(50), voice_connected BOOLEAN DEFAULT FALSE, connected_channel_id BIGINT NULL, player_connected BOOLEAN DEFAULT FALSE, player_playing BOOLEAN DEFAULT FALSE, player_paused BOOLEAN DEFAULT FALSE, queue_count INT DEFAULT 0, backup_queue_count INT DEFAULT 0, is_playing_db BOOLEAN DEFAULT FALSE, is_paused_db BOOLEAN DEFAULT FALSE, position_seconds INT DEFAULT 0, recovery_pending BOOLEAN DEFAULT FALSE, heartbeat_age_seconds INT DEFAULT 0, lavalink_ready BOOLEAN DEFAULT FALSE, last_error TEXT NULL, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (guild_id, bot_name))")
                try: await cur.execute("ALTER TABLE nexus_voice_state ADD COLUMN reconnect_attempts INT NOT NULL DEFAULT 0")
                except: pass
                try: await cur.execute("ALTER TABLE nexus_voice_state ADD COLUMN last_error TEXT NULL")
                except: pass
                try: await cur.execute("CREATE INDEX nexus_voice_state_rejoin_idx ON nexus_voice_state (bot_name, desired_connected, last_seen_at)")
                except: pass
                try: await cur.execute("CREATE INDEX nexus_metrics_status_idx ON nexus_metrics (bot_name, updated_at)")
                except: pass
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_active_playlists (guild_id BIGINT, bot_name VARCHAR(50), playlist_url TEXT, known_track_count INT DEFAULT 0, requester_id BIGINT, channel_id BIGINT DEFAULT NULL, PRIMARY KEY (guild_id, bot_name))")
                try: await cur.execute("ALTER TABLE nexus_active_playlists ADD COLUMN channel_id BIGINT DEFAULT NULL")
                except: pass
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_swarm_toggles (guild_id BIGINT PRIMARY KEY, auto_dj BOOLEAN DEFAULT FALSE, audio_filter VARCHAR(20) DEFAULT 'normal')")
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_swarm_overrides (guild_id BIGINT, bot_name VARCHAR(50), command VARCHAR(20), attempts INT NOT NULL DEFAULT 0, last_error TEXT NULL, PRIMARY KEY(guild_id, bot_name))")
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_swarm_direct_orders (id INT AUTO_INCREMENT PRIMARY KEY, bot_name VARCHAR(50), guild_id BIGINT, vc_id BIGINT, text_channel_id BIGINT, command VARCHAR(50), data TEXT, attempts INT NOT NULL DEFAULT 0, last_error TEXT NULL)")
                await cur.execute("CREATE TABLE IF NOT EXISTS swarm_health (bot_name VARCHAR(50) PRIMARY KEY, last_pulse TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, status VARCHAR(20))")
                await cur.execute(f"CREATE TABLE IF NOT EXISTS {BOT_ENV_PREFIX.lower()}_error_events (id INT AUTO_INCREMENT PRIMARY KEY, bot_name VARCHAR(50) NOT NULL, guild_id BIGINT NULL, error_level VARCHAR(20) NOT NULL DEFAULT 'error', error_type VARCHAR(50) NOT NULL DEFAULT 'runtime', title VARCHAR(255) NOT NULL, description TEXT NULL, traceback_text MEDIUMTEXT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                try: await cur.execute("CREATE INDEX nexus_queue_lookup_idx ON nexus_queue (guild_id, bot_name, id)")
                except: pass
                try: await cur.execute("CREATE INDEX nexus_queue_backup_lookup_idx ON nexus_queue_backup (guild_id, bot_name, id)")
                except: pass
                try: await cur.execute("CREATE INDEX nexus_history_guild_played_idx ON nexus_history (guild_id, played_at)")
                except: pass
                try: await cur.execute("CREATE INDEX nexus_history_requester_idx ON nexus_history (guild_id, requester_id, played_at)")
                except: pass
                try: await cur.execute("CREATE INDEX nexus_playback_resume_idx ON nexus_playback_state (bot_name, is_playing, guild_id)")
                except: pass
                try: await cur.execute("CREATE INDEX nexus_playlist_bot_idx ON nexus_active_playlists (bot_name, guild_id)")
                except: pass
                try: await cur.execute("CREATE INDEX nexus_user_playlist_lookup_idx ON nexus_user_playlists (user_id, playlist_name)")
                except: pass
                try: await cur.execute("ALTER TABLE nexus_playback_state ADD COLUMN bot_name VARCHAR(50) DEFAULT 'nexus'")
                except: pass
                try: await cur.execute("ALTER TABLE nexus_active_playlists ADD COLUMN bot_name VARCHAR(50) DEFAULT 'nexus'")
                except: pass
                try: await cur.execute("ALTER TABLE nexus_bot_home_channels ADD COLUMN bot_name VARCHAR(50) DEFAULT 'nexus'")
                except: pass
                playlist_db_initialized = True
                logger.info("Database tables verified/created for NEXUS.")

async def init_db_with_retries(attempts=12, delay=5):
    for attempt in range(1, attempts + 1):
        try:
            await init_db()
            return
        except Exception as exc:
            logger.exception("Database initialization failed on attempt %s/%s: %s", attempt, attempts, exc)
            if attempt >= attempts:
                raise
            await asyncio.sleep(delay)

# --- RUNTIME HOT PATH: keep helpers small; queue, recovery, and panel sync call these frequently. ---
# --- CORE LOGIC & HELPERS ---
async def save_state(guild_id):
    state = guild_states.get(guild_id)
    if not state: return
    try:
        with open(f"state_{guild_id}.json", "w") as f: json.dump(state, f)
    except Exception:
        pass

async def load_states():
    states = {}
    for file in os.listdir():
        if file.startswith("state_") and file.endswith(".json"):
            try:
                gid = file.replace("state_", "").replace(".json", "")
                with open(file) as f: states[gid] = json.load(f)
            except Exception:
                pass
    return states

async def delete_state(guild_id):
    try:
        os.remove(f"state_{guild_id}.json")
    except FileNotFoundError:
        pass
    except Exception:
        pass

async def ensure_guild_settings(guild_id):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("INSERT IGNORE INTO nexus_guild_settings (guild_id) VALUES (%s)", (guild_id,))

def _scalar_from_row(row, default=0):
    if row is None:
        return default
    if isinstance(row, dict):
        return next(iter(row.values()), default)
    if isinstance(row, (tuple, list)):
        return row[0] if row else default
    return row

def _row_value(row, key_or_index, default=None):
    if row is None:
        return default
    if isinstance(row, dict):
        return row.get(key_or_index, default)
    if isinstance(key_or_index, int) and isinstance(row, (tuple, list)):
        return row[key_or_index] if len(row) > key_or_index else default
    return default

def get_process_queue_lock(guild_id):
    lock = process_queue_locks.get(guild_id)
    if lock is None:
        lock = asyncio.Lock()
        process_queue_locks[guild_id] = lock
    return lock

def get_voice_connect_lock(guild_id):
    lock = voice_connect_locks.get(guild_id)
    if lock is None:
        lock = asyncio.Lock()
        voice_connect_locks[guild_id] = lock
    return lock

def invalidate_position_persist(guild_id):
    last_position_persist.pop(guild_id, None)

def current_track_position(guild_id, now=None):
    data = playback_tracking.get(guild_id)
    if not data:
        return int(guild_states.get(guild_id, {}).get("position", 0))
    # Critical: do not let the saved position keep advancing while paused.
    # Pause/resume sync uses this helper, so drift here causes the panel, Aria,
    # and recovery logic to resume too far into the song.
    if data.get('paused'):
        return max(0, int(data.get('offset', 0)))
    now = now or time.time()
    return max(0, int((now - data.get('start_time', now)) * data.get('speed', 1.0) + data.get('offset', 0)))

def clear_auto_restore_snooze(guild_id):
    auto_restore_snooze_until.pop(guild_id, None)

def snooze_auto_restore(guild_id, seconds=AUTO_RESTORE_SNOOZE_SECONDS):
    auto_restore_snooze_until[guild_id] = time.time() + seconds

def recovery_backoff_remaining(guild_id):
    remaining = recovery_exhausted_until.get(guild_id, 0) - time.time()
    if remaining <= 0:
        recovery_exhausted_until.pop(guild_id, None)
        return 0
    return int(max(1, remaining))

def clear_recovery_backoff(guild_id):
    recovery_exhausted_until.pop(guild_id, None)

def arm_recovery_backoff(guild_id, *, seconds=RECOVERY_EXHAUSTED_COOLDOWN_SECONDS, reason="recovery_exhausted"):
    cooldown = max(30.0, float(seconds))
    recovery_exhausted_until[guild_id] = time.time() + cooldown
    playback_tracking.pop(guild_id, None)
    recovering_guilds.discard(guild_id)
    snooze_auto_restore(guild_id, cooldown)
    logger.warning(f"[{guild_id}] Recovery paused for {int(cooldown)}s after {reason}.")

def clear_idle_restore_state(guild_id):
    idle_voice_since.pop(guild_id, None)

def clear_recovery_retry(guild_id):
    recovery_retry_counts.pop(guild_id, None)
    retry_task = recovery_retry_tasks.pop(guild_id, None)
    current_task = asyncio.current_task()
    if retry_task and retry_task is not current_task and not retry_task.done():
        retry_task.cancel()

async def remember_recovery_state(guild_id, channel_id, position=0):
    if not channel_id:
        return
    guild_states[guild_id] = {"voice_channel_id": channel_id, "position": max(0, int(position))}
    invalidate_position_persist(guild_id)
    await save_state(guild_id)



async def persist_voice_state(guild_id, channel_id=None, *, text_channel_id=None, desired_connected=True, connected=True, last_error=None):
    """Persist desired/actual voice channel so restarts and Discord voice drops can recover safely."""
    if not guild_id:
        return
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        INSERT INTO nexus_voice_state
                            (guild_id, bot_name, last_channel_id, connected_channel_id, text_channel_id, disconnected_at, desired_connected, reconnect_attempts, last_error)
                        VALUES (%s, 'nexus', %s, %s, %s, %s, %s, 0, %s)
                        ON DUPLICATE KEY UPDATE
                            last_channel_id = COALESCE(VALUES(last_channel_id), last_channel_id),
                            connected_channel_id = VALUES(connected_channel_id),
                            text_channel_id = COALESCE(VALUES(text_channel_id), text_channel_id),
                            disconnected_at = VALUES(disconnected_at),
                            desired_connected = VALUES(desired_connected),
                            reconnect_attempts = IF(VALUES(connected_channel_id) IS NULL, reconnect_attempts, 0),
                            last_error = VALUES(last_error),
                            last_seen_at = CURRENT_TIMESTAMP
                        """,
                        (
                            guild_id,
                            channel_id,
                            channel_id if connected else None,
                            text_channel_id,
                            None if connected else datetime.datetime.utcnow(),
                            bool(desired_connected),
                            str(last_error)[:2000] if last_error else None,
                        ),
                    )
    except Exception:
        logger.exception("[nexus] Failed to persist voice state for guild %s", guild_id)


async def mark_voice_disconnected(guild_id, channel_id=None, *, desired_connected=True, reason="voice_disconnect"):
    await persist_voice_state(guild_id, channel_id, desired_connected=desired_connected, connected=False, last_error=reason)
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "UPDATE nexus_playback_state SET is_playing = FALSE, is_paused = FALSE WHERE guild_id = %s AND bot_name = 'nexus'",
                        (guild_id,),
                    )
    except Exception:
        logger.exception("[nexus] Failed to mark playback disconnected for guild %s", guild_id)


async def reconcile_runtime_playback_state(guild):
    """Force DB playback truth to follow the real Discord/Lavalink player."""
    if not guild:
        return
    vc = guild.voice_client
    actual_connected = bool(vc and getattr(vc, "is_connected", lambda: False)())
    actual_channel_id = getattr(getattr(vc, "channel", None), "id", None)
    actual_playing = bool(vc and _player_is_playing(vc))
    actual_paused = bool(vc and _player_is_paused(vc))
    live_position = current_track_position(guild.id) if (actual_playing or actual_paused or guild.id in playback_tracking) else 0
    if actual_connected and actual_channel_id:
        await persist_voice_state(guild.id, actual_channel_id, desired_connected=True, connected=True)
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    if not actual_connected:
                        await cur.execute(
                            "UPDATE nexus_playback_state SET is_playing = FALSE, is_paused = FALSE WHERE guild_id = %s AND bot_name = 'nexus'",
                            (guild.id,),
                        )
                    else:
                        await cur.execute(
                            "UPDATE nexus_playback_state SET channel_id = COALESCE(%s, channel_id), is_playing = %s, is_paused = %s, position_seconds = %s WHERE guild_id = %s AND bot_name = 'nexus'",
                            (actual_channel_id, actual_playing, actual_paused, live_position, guild.id),
                        )
    except Exception:
        logger.exception("[nexus] Failed to reconcile playback state for guild %s", guild.id)


async def collect_and_persist_metrics(guild=None):
    guilds = [guild] if guild else list(bot.guilds)
    try:
        lavalink_ready = await ensure_lavalink_ready(timeout=1.0)
    except Exception:
        lavalink_ready = False
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    for g in guilds:
                        if not g:
                            continue
                        vc = g.voice_client
                        channel_id = getattr(getattr(vc, "channel", None), "id", None)
                        voice_connected = bool(vc and getattr(vc, "is_connected", lambda: False)())
                        player_playing = bool(vc and _player_is_playing(vc))
                        player_paused = bool(vc and _player_is_paused(vc))
                        live_position = current_track_position(g.id) if (player_playing or player_paused or g.id in playback_tracking) else 0
                        await cur.execute("SELECT COUNT(*) AS total FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", (g.id,))
                        queue_row = await cur.fetchone() or {}
                        await cur.execute("SELECT COUNT(*) AS total FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus'", (g.id,))
                        backup_row = await cur.fetchone() or {}
                        await cur.execute("SELECT is_playing, is_paused, position_seconds FROM nexus_playback_state WHERE guild_id = %s AND bot_name = 'nexus' LIMIT 1", (g.id,))
                        playback_row = await cur.fetchone() or {}
                        if not voice_connected and (playback_row.get("is_playing") or playback_row.get("is_paused")):
                            await cur.execute("UPDATE nexus_playback_state SET is_playing = FALSE, is_paused = FALSE WHERE guild_id = %s AND bot_name = 'nexus'", (g.id,))
                        elif voice_connected and (player_playing or player_paused or g.id in playback_tracking):
                            await cur.execute(
                                "UPDATE nexus_playback_state SET channel_id = COALESCE(%s, channel_id), is_playing = %s, is_paused = %s, position_seconds = %s WHERE guild_id = %s AND bot_name = 'nexus'",
                                (channel_id, player_playing, player_paused, int(live_position or playback_row.get("position_seconds") or 0), g.id),
                            )
                        await cur.execute(
                            """
                            REPLACE INTO nexus_metrics
                                (guild_id, bot_name, voice_connected, connected_channel_id, player_connected, player_playing, player_paused,
                                 queue_count, backup_queue_count, is_playing_db, is_paused_db, position_seconds, recovery_pending,
                                 heartbeat_age_seconds, lavalink_ready, last_error)
                            VALUES (%s, 'nexus', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, %s, %s)
                            """,
                            (
                                g.id,
                                voice_connected,
                                channel_id,
                                voice_connected,
                                player_playing,
                                player_paused,
                                int(queue_row.get("total") or 0),
                                int(backup_row.get("total") or 0),
                                bool(playback_row.get("is_playing")) if voice_connected else False,
                                bool(playback_row.get("is_paused")) if voice_connected else False,
                                int(live_position or playback_row.get("position_seconds") or 0),
                                g.id in recovering_guilds or str(g.id) in guild_states,
                                bool(lavalink_ready),
                                metrics_last_errors.get(g.id),
                            ),
                        )
    except Exception as exc:
        logger.exception("[nexus] Metrics collection failed: %s", exc)


async def restore_persistent_voice_states():
    """Rejoin desired voice channels after bot restart / Discord voice reconnect edges."""
    try:
        if BOT_RECOVERY_STAGGER_SECONDS:
            await asyncio.sleep(BOT_RECOVERY_STAGGER_SECONDS)
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute(
                        "SELECT guild_id, COALESCE(connected_channel_id, last_channel_id) AS channel_id, reconnect_attempts FROM nexus_voice_state WHERE bot_name = 'nexus' AND desired_connected = TRUE AND COALESCE(connected_channel_id, last_channel_id) IS NOT NULL"
                    )
                    rows = list(await cur.fetchall() or [])
        for row in rows:
            try:
                guild_id = int(row["guild_id"])
                raw_channel_id = row.get("channel_id") if isinstance(row, dict) else row[1]
                channel_id = int(raw_channel_id) if raw_channel_id else 0
                attempts = int((row.get("reconnect_attempts") if isinstance(row, dict) else 0) or 0)
                if attempts >= int(os.getenv(f"{BOT_ENV_PREFIX}_VOICE_REJOIN_MAX_ATTEMPTS", "8")):
                    continue
                guild = bot.get_guild(guild_id)
                if not guild or not channel_id:
                    continue
                channel = guild.get_channel(channel_id)
                if channel is None:
                    await persist_voice_state(guild_id, channel_id, desired_connected=False, connected=False, last_error="voice_channel_missing")
                    continue
                if guild.voice_client and getattr(guild.voice_client, "is_connected", lambda: False)():
                    await persist_voice_state(guild.id, getattr(guild.voice_client.channel, "id", channel_id), desired_connected=True, connected=True)
                    continue
                state = await derive_recovery_state_from_db(guild.id)
                stay_in_vc = False
                try:
                    async with DBPoolManager() as pool:
                        async with pool.acquire() as conn:
                            async with conn.cursor() as cur:
                                await cur.execute("SELECT stay_in_vc FROM nexus_guild_settings WHERE guild_id = %s", (guild.id,))
                                stay_row = await cur.fetchone()
                                stay_in_vc = bool(stay_row[0]) if stay_row else False
                except Exception:
                    logger.debug("[nexus] Failed checking 24/7 state for persistent voice restore.", exc_info=True)
                if not state and not stay_in_vc:
                    await persist_voice_state(guild.id, channel_id, desired_connected=False, connected=False, last_error="no_recoverable_playback")
                    continue
                await asyncio.sleep(VOICE_REJOIN_DELAY_SECONDS)
                vc = await ensure_voice_connection(guild, channel_id, respect_recovery_backoff=True)
                if vc:
                    await persist_voice_state(guild.id, channel_id, desired_connected=bool(stay_in_vc or state), connected=True)
                    if state:
                        schedule_recovery_retry(guild.id, channel_id, start_position=state.get("position", 0), reason="persistent_voice_restore")
                else:
                    async with DBPoolManager() as pool:
                        async with pool.acquire() as conn:
                            async with conn.cursor() as cur:
                                await cur.execute("UPDATE nexus_voice_state SET reconnect_attempts = reconnect_attempts + 1, last_error = %s WHERE guild_id = %s AND bot_name = 'nexus'", ("rejoin_failed", guild.id))
            except Exception as row_exc:
                logger.warning("[nexus] Persistent voice row recovery failed: %s", row_exc)
    except Exception as exc:
        logger.exception("[nexus] Persistent voice restore failed: %s", exc)


async def sync_pause_state(guild_id, paused: bool):
    """Keep Discord/player pause state mirrored in DB for panel + Aria."""
    try:
        position = current_track_position(guild_id)
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "UPDATE nexus_playback_state SET is_paused = %s, is_playing = %s, position_seconds = %s WHERE guild_id = %s AND bot_name = 'nexus'",
                        (bool(paused), not bool(paused), position, guild_id),
                    )
        if guild_id in playback_tracking:
            playback_tracking[guild_id]["offset"] = position
            playback_tracking[guild_id]["start_time"] = time.time()
            playback_tracking[guild_id]["paused"] = bool(paused)
        invalidate_position_persist(guild_id)
    except Exception:
        logger.exception("[nexus] Failed to sync pause state for guild %s.", guild_id)

async def insert_queue_front(cur, table_name, guild_id, bot_name, video_url, title, requester_id, max_attempts=5):
    if not re.fullmatch(r"[A-Za-z0-9_]+", table_name):
        raise ValueError(f"Unsafe table name: {table_name}")

    for attempt in range(max_attempts):
        await cur.execute(f"SELECT COALESCE(MIN(id), 0) AS min_id FROM {table_name}")
        min_row = await cur.fetchone()
        new_id = (_scalar_from_row(min_row, 0) or 0) - 1
        try:
            await cur.execute(
                f"INSERT INTO {table_name} (id, guild_id, bot_name, video_url, title, requester_id) VALUES (%s, %s, %s, %s, %s, %s)",
                (new_id, guild_id, bot_name, video_url, title, requester_id)
            )
            return new_id
        except aiomysql.IntegrityError as e:
            if e.args and e.args[0] == 1062 and attempt < max_attempts - 1:
                await asyncio.sleep(0.05 * (attempt + 1))
                continue
            raise

async def snapshot_queue_backup(cur, guild_id):
    await cur.execute("SELECT video_url, title, requester_id FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC", (guild_id,))
    rows = await cur.fetchall()
    if not rows:
        return 0
    await cur.execute("DELETE FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus'", (guild_id,))
    for url, title, requester_id in rows:
        await cur.execute(
            "INSERT INTO nexus_queue_backup (guild_id, bot_name, video_url, title, requester_id) VALUES (%s, 'nexus', %s, %s, %s)",
            (guild_id, url, title, requester_id),
        )
    return len(rows)

async def backup_track(cur, guild_id, video_url, title, requester_id):
    await cur.execute(
        "INSERT INTO nexus_queue_backup (guild_id, bot_name, video_url, title, requester_id) VALUES (%s, 'nexus', %s, %s, %s)",
        (guild_id, video_url, title, requester_id),
    )

async def enqueue_track(cur, guild_id, video_url, title, requester_id, *, backup=True):
    await cur.execute(
        "INSERT INTO nexus_queue (guild_id, bot_name, video_url, title, requester_id) VALUES (%s, 'nexus', %s, %s, %s)",
        (guild_id, video_url, title, requester_id),
    )
    if backup:
        await backup_track(cur, guild_id, video_url, title, requester_id)
    return 1

async def restore_queue_from_backup(cur, guild_id, requester_id=None):
    await cur.execute("SELECT COUNT(*) FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", (guild_id,))
    queue_count_row = await cur.fetchone()
    queue_count = queue_count_row[0] if queue_count_row else 0
    if queue_count:
        return 0

    await cur.execute("SELECT video_url, title, requester_id FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC", (guild_id,))
    rows = await cur.fetchall()
    if not rows:
        return 0

    restored = 0
    for row in rows:
        url = _row_value(row, "video_url", _row_value(row, 0))
        title = _row_value(row, "title", _row_value(row, 1))
        backup_requester_id = _row_value(row, "requester_id", _row_value(row, 2))
        await cur.execute(
            "INSERT INTO nexus_queue (guild_id, bot_name, video_url, title, requester_id) VALUES (%s, 'nexus', %s, %s, %s)",
            (guild_id, url, title, requester_id if requester_id is not None else backup_requester_id),
        )
        restored += 1
    return restored

async def restore_active_playback_entry(cur, guild_id, requester_id=None):
    await cur.execute(
        "SELECT video_url, title FROM nexus_playback_state "
        "WHERE guild_id = %s AND bot_name = 'nexus' "
        "AND video_url IS NOT NULL AND video_url <> '' "
        "AND (is_playing = TRUE OR position_seconds > 0 OR title IS NOT NULL) "
        "ORDER BY is_playing DESC, position_seconds DESC LIMIT 1",
        (guild_id,),
    )
    row = await cur.fetchone()
    if not row:
        return 0

    video_url = _row_value(row, "video_url", _row_value(row, 0))
    title = _row_value(row, "title", _row_value(row, 1))
    await cur.execute(
        "SELECT video_url FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC LIMIT 1",
        (guild_id,),
    )
    existing = await cur.fetchone()
    existing_url = _row_value(existing, "video_url", _row_value(existing, 0))
    if existing_url and str(existing_url).strip() == str(video_url).strip():
        return 0

    await insert_queue_front(
        cur,
        "nexus_queue",
        guild_id,
        "nexus",
        video_url,
        title or "Resumed Track",
        requester_id if requester_id is not None else (bot.user.id if bot.user else None),
    )
    return 1

def schedule_recovery_retry(guild_id, channel_id, *, start_position=0, reason="recovery"):
    if not channel_id:
        return False

    if recovery_backoff_remaining(guild_id) > 0:
        return False

    existing = recovery_retry_tasks.get(guild_id)
    if existing and not existing.done():
        return False

    attempts = recovery_retry_counts.get(guild_id, 0) + 1
    if attempts > MAX_RECOVERY_RETRIES:
        clear_recovery_retry(guild_id)
        arm_recovery_backoff(guild_id, reason=f"{reason}_exhausted")
        logger.error(f"[{guild_id}] Exhausted recovery retries after {MAX_RECOVERY_RETRIES} attempts ({reason}).")
        return False

    recovery_retry_counts[guild_id] = attempts
    delay = min(RECOVERY_RETRY_BASE_DELAY * attempts, RECOVERY_RETRY_MAX_DELAY)
    current_task = None

    async def _retry():
        try:
            await asyncio.sleep(delay)
            if recovery_backoff_remaining(guild_id) > 0:
                recovery_retry_counts.pop(guild_id, None)
                return

            guild = bot.get_guild(guild_id)
            if not guild:
                recovery_retry_counts.pop(guild_id, None)
                return

            vc = guild.voice_client
            if vc and (_player_is_active(vc)):
                clear_recovery_retry(guild_id)
                return

            recovery_retry_tasks.pop(guild_id, None)
            logger.warning(f"[{guild_id}] Recovery retry {attempts}/{MAX_RECOVERY_RETRIES} armed ({reason}).")
            await process_queue(guild, channel_id, start_position=start_position, allow_recovery_restore=True)
        except asyncio.CancelledError:
            raise
        finally:
            running = recovery_retry_tasks.get(guild_id)
            if running is current_task:
                recovery_retry_tasks.pop(guild_id, None)

    current_task = asyncio.create_task(_retry())
    recovery_retry_tasks[guild_id] = current_task
    return True

async def requeue_failed_track(cur, guild_id, channel_id, url, title, requester_id, *, position=0, reason="recovery"):
    await insert_queue_front(cur, "nexus_queue", guild_id, "nexus", url, title, requester_id)
    # FIX 6: Re-add the failed track to the backup so it survives a crash.
    # After FIX 1 the backup loses the track the moment it is popped to play;
    # if playback then fails we must restore backup parity.
    await cur.execute(
        "INSERT INTO nexus_queue_backup (guild_id, bot_name, video_url, title, requester_id) VALUES (%s, 'nexus', %s, %s, %s)",
        (guild_id, url, title, requester_id),
    )
    await cur.execute(
        "UPDATE nexus_playback_state SET is_playing = FALSE, is_paused = FALSE, position_seconds = %s WHERE guild_id = %s AND bot_name = 'nexus'",
        (position, guild_id),
    )
    await remember_recovery_state(guild_id, channel_id, position)
    schedule_recovery_retry(guild_id, channel_id, start_position=position, reason=reason)

async def get_home_channel(guild):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT home_vc_id FROM nexus_bot_home_channels WHERE guild_id = %s AND bot_name = 'nexus'", (guild.id,))
                res = await cur.fetchone()
    if res and res[0]:
        return guild.get_channel(res[0])
    return None

async def _fade_volume(voice_client, start_volume, end_volume, duration=5.0, steps=10):
    if not voice_client:
        return
    step_delay = duration / steps if steps > 0 else duration
    for step in range(steps + 1):
        volume = int(round(start_volume + (end_volume - start_volume) * (step / steps)))
        try:
            await voice_client.set_volume(max(0, min(200, volume)))
        except Exception:
            return
        if step < steps:
            await asyncio.sleep(step_delay)

async def update_stage_topic(guild, title, requester_id):
    """Update Stage topic AND Voice/Stage channel status for the current song.

    Stage channels use StageInstance.topic. Newer Discord voice channel
    statuses use channel.edit(status=...). Older discord.py builds may not
    expose that keyword, so this helper is best-effort and must never stop
    playback.
    """
    try:
        vc = guild.voice_client
        if not vc or not getattr(vc, "channel", None):
            return
        channel = vc.channel
        requester_name = await resolve_requester_name(guild, requester_id)
        stage_topic = f"🎵 {title[:60]} | 👤 Req: {requester_name}"
        voice_status = f"🎵 {title[:80]}"

        if isinstance(channel, discord.StageChannel):
            try:
                if channel.instance is None:
                    await channel.create_instance(topic=stage_topic)
                else:
                    await channel.instance.edit(topic=stage_topic)
            except Exception as e:
                logger.warning(f"[{guild.id}] Stage topic update failed: {e}")

        if not isinstance(channel, discord.StageChannel):
            try:
                await channel.edit(status=voice_status)
            except TypeError:
                pass
            except discord.Forbidden:
                logger.warning(f"[{guild.id}] Missing Manage Channels permission to update voice channel status.")
            except discord.HTTPException as e:
                if getattr(e, "code", None) != 50024:
                    logger.warning(f"[{guild.id}] Voice channel status update failed: {e}")
            except Exception as e:
                logger.warning(f"[{guild.id}] Voice channel status update failed: {e}")
    except Exception as e:
        logger.error(f"[STAGE/VOICE STATUS ERROR] {e}")

async def clear_voice_channel_status(guild):
    try:
        vc = guild.voice_client
        channel = getattr(vc, "channel", None) if vc else None
        if not channel:
            return
        try:
            await channel.edit(status=None)
        except TypeError:
            pass
        except Exception:
            pass
    except Exception:
        pass

async def send_or_update_status_message(guild, embed):
    """Maintain one live now-playing/status message per guild."""
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_status_messages (guild_id BIGINT, bot_name VARCHAR(50), feedback_channel_id BIGINT, message_id BIGINT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (guild_id, bot_name))")
                await cur.execute("SELECT feedback_channel_id FROM nexus_guild_settings WHERE guild_id = %s", (guild.id,))
                res = await cur.fetchone()
                if not res or not res[0]:
                    return
                channel = guild.get_channel(int(res[0]))
                if not channel:
                    return
                await cur.execute("SELECT message_id FROM nexus_status_messages WHERE guild_id = %s AND bot_name = 'nexus' LIMIT 1", (guild.id,))
                msg_row = await cur.fetchone()
                message_id = int(msg_row[0]) if msg_row and msg_row[0] else None
                message = None
                if message_id:
                    try:
                        message = await channel.fetch_message(message_id)
                    except Exception:
                        message = None
                if message:
                    await message.edit(embed=embed)
                    new_message_id = message.id
                else:
                    message = await channel.send(embed=embed)
                    new_message_id = message.id
                await cur.execute("REPLACE INTO nexus_status_messages (guild_id, bot_name, feedback_channel_id, message_id) VALUES (%s, 'nexus', %s, %s)", (guild.id, channel.id, new_message_id))

async def send_feedback(guild, embed):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT feedback_channel_id FROM nexus_guild_settings WHERE guild_id = %s", (guild.id,))
                res = await cur.fetchone()
                if res and res[0]:
                    channel = guild.get_channel(res[0])
                    if channel:
                        try: await channel.send(embed=embed)
                        except discord.Forbidden: pass

async def ensure_self_deaf(guild, voice_client):
    channel = getattr(voice_client, "channel", None)
    me = guild.me
    if not channel:
        return
    if me and me.voice and me.voice.self_deaf and getattr(me.voice, "channel", None) == channel:
        return
    try:
        await guild.change_voice_state(channel=channel, self_deaf=True)
    except Exception as e:
        logger.warning(f"[{guild.id}] Failed to self-deafen voice connection: {e}")

async def ensure_voice_connection(guild, channel_id, *, respect_recovery_backoff=False):
    channel = guild.get_channel(channel_id)
    if not channel: return None
    lock = get_voice_connect_lock(guild.id)
    async with lock:
        if respect_recovery_backoff and recovery_backoff_remaining(guild.id) > 0:
            return None
        if not await ensure_lavalink_ready():
            logger.warning(f"[{guild.id}] Lavalink not ready yet; deferring voice connection.")
            return None
        voice_client = guild.voice_client
        pending_voice_channels[guild.id] = channel_id
        try:
            if voice_client and (
                not _voice_client_connected(voice_client)
                or (
                    recovery_retry_counts.get(guild.id)
                    and getattr(voice_client, "channel", None)
                    and voice_client.channel.id == channel_id
                    and not _player_is_active(voice_client)
                )
            ):
                try:
                    await voice_client.disconnect()
                except Exception:
                    pass
                voice_client = None

            if not voice_client:
                if respect_recovery_backoff and BOT_RECOVERY_STAGGER_SECONDS:
                    await asyncio.sleep(min(BOT_RECOVERY_STAGGER_SECONDS, 45.0))
                voice_client = await channel.connect(cls=wavelink.Player, timeout=VOICE_CONNECT_TIMEOUT_SECONDS, self_deaf=True)
            elif voice_client.channel.id != channel_id:
                await voice_client.move_to(channel)
            await ensure_self_deaf(guild, voice_client)
            if getattr(voice_client, "channel", None):
                pending_voice_channels[guild.id] = voice_client.channel.id

            if isinstance(channel, discord.StageChannel):
                if guild.me.voice and guild.me.voice.suppress:
                    try: await guild.me.edit(suppress=False)
                    except Exception: pass

            await persist_voice_state(guild.id, channel_id, desired_connected=True, connected=True)
            clear_recovery_backoff(guild.id)

            return voice_client
        except Exception as e:
            logger.error(f"[{guild.id}] Voice connect error: {e}")
            message = str(e).lower()
            if isinstance(e, asyncio.TimeoutError) or "exceeded the timeout" in message or "timed out" in message:
                arm_recovery_backoff(guild.id, seconds=VOICE_CONNECT_TIMEOUT_BACKOFF_SECONDS, reason="voice_connect_timeout")
                try:
                    await mark_voice_disconnected(guild.id, channel_id, desired_connected=True, reason="voice_connect_timeout")
                except Exception:
                    logger.debug(f"[{guild.id}] Failed to persist voice timeout state.", exc_info=True)
            return None

async def is_dj(interaction: discord.Interaction, silent=False):
    if interaction.user.guild_permissions.administrator: return True
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT dj_role_id, dj_only_mode FROM nexus_guild_settings WHERE guild_id = %s", (interaction.guild.id,))
                res = await cur.fetchone()
                if res and res[1]:
                    if res[0] and discord.utils.get(interaction.user.roles, id=res[0]): return True
                    if not silent:
                        await interaction.response.send_message(embed=discord.Embed(description="❌ **Strict DJ Mode is Active.** You need the DJ Role.", color=discord.Color.red()), ephemeral=True)
                    return False
    return True

def make_progress_bar(current, total, length=15):
    if total <= 0: return f"[{'▬'*length}] {current//60}:{current%60:02d} / Live"
    progress = max(0, min(length, int((current / total) * length)))
    bar = "▬" * progress + "🔘" + "▬" * (length - progress - 1)
    return f"[{bar}] {current//60}:{current%60:02d} / {total//60}:{total%60:02d}"

def _has_human_listeners(voice_client):
    if not voice_client or not getattr(voice_client, "channel", None): return False
    return any(not member.bot for member in voice_client.channel.members)

def _should_auto_disconnect(guild, stay_in_vc=False):
    if stay_in_vc: return False
    return not _has_human_listeners(guild.voice_client)

@bot.event
async def on_wavelink_track_end(payload: wavelink.TrackEndEventPayload):
    # FIX: Properly cast reason to upper string to handle API changes safely
    if payload.player and str(getattr(payload, 'reason', '')).upper() != "REPLACED":
        try:
            reason = str(payload.reason).upper()
            if payload.track:
                async with DBPoolManager() as pool:
                    async with pool.acquire() as conn:
                        async with conn.cursor() as cur:
                            await cur.execute("SELECT loop_mode FROM nexus_guild_settings WHERE guild_id = %s", (payload.player.guild.id,))
                            mode_row = await cur.fetchone()
                            loop_mode = mode_row[0] if mode_row and mode_row[0] else 'queue'
                            track_data = playback_tracking.get(payload.player.guild.id, {})
                            original_requester = track_data.get('requester_id', bot.user.id if bot.user else None)

                            if reason == "FINISHED":
                                if loop_mode == 'queue':
                                    await enqueue_track(cur, payload.player.guild.id, payload.track.uri, payload.track.title, original_requester, backup=False)
                                elif loop_mode == 'song':
                                    await insert_queue_front(cur, "nexus_queue", payload.player.guild.id, "nexus", payload.track.uri, payload.track.title, original_requester)
        except Exception as e:
            logger.error(f"[{payload.player.guild.id}] Looping logic DB error: {e}")
        _te_channel_id = (
            getattr(payload.player.channel, 'id', None)
            or playback_tracking.get(payload.player.guild.id, {}).get('channel_id')
            or guild_states.get(payload.player.guild.id, {}).get('voice_channel_id')
        )
        if _te_channel_id:
            coro = process_queue(payload.player.guild, _te_channel_id)
            schedule_named_task(f"track_end_process_queue:{payload.player.guild.id}", coro)

async def _process_queue_inner(guild, channel_id, start_position=0, *, allow_recovery_restore=False):
    recovering_guilds.discard(guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT volume, loop_mode, filter_mode, transition_mode, custom_speed, custom_pitch, custom_modifiers_left, stay_in_vc FROM nexus_guild_settings WHERE guild_id = %s", (guild.id,))
                res = await cur.fetchone()
                vol, loop_mode, filter_mode, trans_mode, c_speed, c_pitch, c_mod_left, stay_in_vc = res if res else (100, 'queue', 'none', 'off', 1.0, 1.0, 0, False)

                await cur.execute("SELECT id, video_url, title, requester_id FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC LIMIT 1", (guild.id,))
                next_song = await cur.fetchone()

                if not next_song and allow_recovery_restore:
                    restored_active = await restore_active_playback_entry(cur, guild.id)
                    restored_backup = 0 if restored_active else await restore_queue_from_backup(cur, guild.id)
                    if restored_active or restored_backup:
                        logger.info(f"[{guild.id}] Rebuilt live queue from persisted playback/backup state (active={restored_active}, backup={restored_backup}).")
                        await cur.execute("SELECT id, video_url, title, requester_id FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC LIMIT 1", (guild.id,))
                        next_song = await cur.fetchone()

                if not next_song:
                    await cur.execute(
                        "UPDATE nexus_playback_state SET channel_id = NULL, video_url = NULL, title = NULL, position_seconds = 0, is_playing = FALSE, is_paused = FALSE WHERE guild_id = %s AND bot_name = 'nexus'",
                        (guild.id,),
                    )
                    playback_tracking.pop(guild.id, None)
                    guild_states.pop(guild.id, None)
                    invalidate_position_persist(guild.id)
                    clear_recovery_retry(guild.id)
                    clear_idle_restore_state(guild.id)
                    await delete_state(guild.id)
                    await bot.change_presence(status=discord.Status.online)
                    if not allow_recovery_restore:
                        await cur.execute("DELETE FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus'", (guild.id,))
                    if await maybe_enqueue_autodj(cur, guild, channel_id):
                        return

                    if _should_auto_disconnect(guild, stay_in_vc) and guild.voice_client:
                        await mark_voice_disconnected(guild.id, getattr(getattr(guild.voice_client, "channel", None), "id", channel_id), desired_connected=False, reason="queue_complete")
                        await guild.voice_client.disconnect()
                    elif guild.voice_client and getattr(guild.voice_client, "channel", None):
                        await persist_voice_state(guild.id, guild.voice_client.channel.id, desired_connected=bool(stay_in_vc), connected=True)
                    return

                song_id, url, title, requester_id = next_song
                await cur.execute("DELETE FROM nexus_queue WHERE id = %s AND guild_id = %s AND bot_name = 'nexus'", (song_id, guild.id))
                # FIX 1: Keep backup in sync — remove the track we are about to play
                await cur.execute(
                    "DELETE FROM nexus_queue_backup WHERE video_url = %s AND title = %s AND guild_id = %s AND bot_name = 'nexus' LIMIT 1",
                    (url, title, guild.id),
                )

                try:
                    track, resolved_source = await resolve_queue_track(url, title)
                    duration = track.length / 1000
                    uploader = track.author
                    if resolved_source != url:
                        logger.info(f"[{guild.id}] Recovered '{title}' using alternate source {resolved_source}.")
                        url = track.uri
                        title = track.title
                except Exception as e:
                    logger.error(f"[{guild.id}] Lavalink search failed for '{title}': {e}")
                    if _is_direct_media_url(url):
                        playback_tracking.pop(guild.id, None)
                        await cur.execute(
                            "UPDATE nexus_playback_state SET video_url = NULL, title = NULL, is_playing = FALSE, is_paused = FALSE, position_seconds = 0 WHERE guild_id = %s AND bot_name = 'nexus'",
                            (guild.id,),
                        )
                        await remember_recovery_state(guild.id, channel_id, 0)
                        clear_recovery_retry(guild.id)
                        schedule_named_task(f"skip_unavailable:{guild.id}", process_queue(guild, channel_id))
                        return
                    await requeue_failed_track(cur, guild.id, channel_id, url, title, requester_id, position=start_position, reason="search_failure")
                    return

                voice_client = await ensure_voice_connection(guild, channel_id, respect_recovery_backoff=allow_recovery_restore)
                if not voice_client:
                    logger.warning(f"[{guild.id}] Voice connection unavailable. Requeueing '{title}' for recovery.")
                    await requeue_failed_track(cur, guild.id, channel_id, url, title, requester_id, position=start_position, reason="voice_connect")
                    return

                wav_filters = wavelink.Filters()
                try:
                    await voice_client.set_volume(vol)

                    if c_mod_left > 0:
                        wav_filters.timescale.set(speed=c_speed, pitch=c_pitch)
                        c_mod_left -= 1
                        await cur.execute("UPDATE nexus_guild_settings SET custom_modifiers_left = %s WHERE guild_id = %s", (c_mod_left, guild.id))
                        if c_mod_left == 0: await cur.execute("UPDATE nexus_guild_settings SET custom_speed = 1.0, custom_pitch = 1.0 WHERE guild_id = %s", (guild.id,))

                    if filter_mode == 'nightcore':
                        wav_filters.timescale.set(speed=1.25, pitch=1.3)
                        c_speed = 1.25
                    elif filter_mode == 'vaporwave':
                        wav_filters.timescale.set(speed=0.8, pitch=0.8)
                        c_speed = 0.8
                    elif filter_mode == 'bassboost':
                        wav_filters.equalizer.set(bands=[(0, 0.3), (1, 0.2), (2, 0.1)])

                    await voice_client.set_filters(wav_filters)

                    if trans_mode == 'fade' and start_position <= 0:
                        await voice_client.set_volume(0)
                except Exception as e:
                    logger.error(f"[{guild.id}] Player preparation failed for '{title}': {e}")
                    if _is_stale_lavalink_player_error(e) and guild.voice_client:
                        try:
                            await guild.voice_client.disconnect()
                        except Exception:
                            logger.debug(f"[{guild.id}] Failed to recycle stale Lavalink player cleanly.", exc_info=True)
                    await requeue_failed_track(cur, guild.id, channel_id, url, title, requester_id, position=start_position, reason="player_prepare")
                    return

                try:
                    await voice_client.play(track)
                    if start_position > 0:
                        await voice_client.seek(int(start_position * 1000))
                    elif trans_mode == 'fade':
                        schedule_named_task(f"fade_volume:{guild.id}", _fade_volume(voice_client, 0, vol))
                except Exception as e:
                    logger.error(f"[{guild.id}] Playback start failed for '{title}': {e}")
                    await requeue_failed_track(cur, guild.id, channel_id, url, title, requester_id, position=start_position, reason="playback_start")
                    return

                # FIX: Execute auto-stage updater
                schedule_named_task(f"stage_topic:{guild.id}", update_stage_topic(guild, title, requester_id))

                await cur.execute("INSERT INTO nexus_history (guild_id, video_url, title, requester_id) VALUES (%s, %s, %s, %s)", (guild.id, url, title, requester_id))
                await bot.change_presence(status=discord.Status.online, activity=discord.Activity(type=discord.ActivityType.listening, name=str(title).replace("\\n", " ").strip()[:120]))
                playback_tracking[guild.id] = {'start_time': time.time(), 'offset': start_position, 'url': url, 'channel_id': channel_id, 'title': title, 'duration': duration, 'speed': c_speed, 'current_filter': filter_mode, 'requester_id': requester_id, 'transition_mode': trans_mode, 'volume': vol, 'paused': False}

                bot_n = os.path.basename(__file__).replace('.py', '')
                await cur.execute(f"REPLACE INTO {bot_n}_playback_state (guild_id, bot_name, channel_id, video_url, position_seconds, is_playing, is_paused, title) VALUES (%s, '{bot_n}', %s, %s, %s, TRUE, FALSE, %s)", (guild.id, channel_id, url, start_position, title))
                # Update persistent state
                guild_states[guild.id] = {"voice_channel_id": channel_id, "position": start_position}
                invalidate_position_persist(guild.id)
                clear_recovery_retry(guild.id)
                clear_recovery_backoff(guild.id)
                clear_auto_restore_snooze(guild.id)
                clear_idle_restore_state(guild.id)
                await save_state(guild.id)

                embed = discord.Embed(title="🎵 Now Playing", description=f"**[{title}]({url})**\n*By: {uploader}*", color=discord.Color.from_rgb(88, 101, 242))
                if requester_id:
                    requester_name = await resolve_requester_name(guild, requester_id)
                    embed.add_field(name="Requested by", value=requester_name, inline=True)
                await send_or_update_status_message(guild, embed)

async def process_queue(guild, channel_id, start_position=0, *, allow_recovery_restore=False):
    lock = get_process_queue_lock(guild.id)
    async with lock:
        vc = guild.voice_client
        current_track = _player_current_track(vc) if vc else None
        if vc and current_track is not None:
            if _player_is_playing(vc):
                return
            if _player_is_paused(vc) and start_position <= 0:
                return
        return await _process_queue_inner(guild, channel_id, start_position=start_position, allow_recovery_restore=allow_recovery_restore)

async def stop_playback(guild):
    playback_tracking.pop(guild.id, None)
    guild_states.pop(guild.id, None)
    recovering_guilds.discard(guild.id)
    invalidate_position_persist(guild.id)
    clear_recovery_retry(guild.id)
    clear_idle_restore_state(guild.id)
    await delete_state(guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("UPDATE nexus_playback_state SET channel_id = NULL, video_url = NULL, title = NULL, position_seconds = 0, is_playing = FALSE, is_paused = FALSE WHERE guild_id = %s AND bot_name = 'nexus'", (guild.id,))
                await cur.execute("DELETE FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", (guild.id,))
                await cur.execute("DELETE FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus'", (guild.id,))
                await cur.execute("UPDATE nexus_voice_state SET desired_connected = FALSE, connected_channel_id = NULL, disconnected_at = CURRENT_TIMESTAMP WHERE guild_id = %s AND bot_name = 'nexus'", (guild.id,))
    await clear_voice_channel_status(guild)
    if guild.voice_client: await guild.voice_client.disconnect()
    await bot.change_presence(status=discord.Status.online, activity=discord.Activity(type=discord.ActivityType.watching, name="the Swarm | Idle"))

async def restore_guild_state(guild_id, state, *, override_backoff=False):
    target_guild_id = int(guild_id)
    guild = bot.get_guild(target_guild_id)
    vc = guild.voice_client if guild else None

    if target_guild_id in recovering_guilds:
        logger.debug(f"[{target_guild_id}] Recovery already active; aborting overlapping heal task.")
        return

    if not override_backoff and recovery_backoff_remaining(target_guild_id) > 0 and not (vc and _player_is_active(vc)):
        return

    if target_guild_id in playback_tracking:
        if vc and _player_is_active(vc):
            return
        logger.warning(f"[{target_guild_id}] Clearing stale playback_tracking so recovery can continue.")
        playback_tracking.pop(target_guild_id, None)

    recovering_guilds.add(target_guild_id)
    handoff = False
    try:
        if not guild:
            return

        vc_id = state.get("voice_channel_id")
        channel = None

        if vc_id:
            channel = guild.get_channel(vc_id)

        # NEW: Fallback to home channel if no saved voice channel
        if not channel:
            channel = await get_home_channel(guild)
            if channel:
                logger.info(f"[{target_guild_id}] No saved voice channel — falling back to home channel {channel.id}")

        if not channel:
            logger.warning(f"[{target_guild_id}] Could not resolve any voice channel for recovery.")
            return

        vc_id = channel.id

        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    restored_active = await restore_active_playback_entry(cur, target_guild_id)
                    if not restored_active:
                        await restore_queue_from_backup(cur, target_guild_id)
                    await cur.execute(
                        "SELECT position_seconds FROM nexus_playback_state WHERE guild_id = %s AND bot_name = 'nexus' AND is_playing = TRUE LIMIT 1",
                        (target_guild_id,),
                    )
                    pos_row = await cur.fetchone()
                    db_position = pos_row[0] if pos_row and pos_row[0] is not None else None

        resume_position = max(0, int(db_position if db_position is not None else state.get("position", 0)))
        await remember_recovery_state(target_guild_id, vc_id, resume_position)

        vc = await ensure_voice_connection(guild, vc_id, respect_recovery_backoff=not override_backoff)
        if not vc:
            schedule_recovery_retry(target_guild_id, vc_id, start_position=resume_position, reason="restore_voice")
            return

        schedule_named_task(f"process_queue:{target_guild_id}", process_queue(guild, vc_id, start_position=resume_position, allow_recovery_restore=True))
        handoff = True
    except Exception as e:
        logger.error(f"[RESTORE ERROR] {guild_id}: {e}")
        schedule_recovery_retry(target_guild_id, state.get("voice_channel_id"), start_position=state.get("position", 0), reason="restore_exception")
    finally:
        if not handoff:
            recovering_guilds.discard(target_guild_id)

async def derive_recovery_state_from_db(guild_id):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT channel_id, position_seconds, is_playing, video_url, title "
                    "FROM nexus_playback_state WHERE guild_id = %s AND bot_name = 'nexus' LIMIT 1",
                    (guild_id,),
                )
                playback = await cur.fetchone()

                await cur.execute(
                    "SELECT home_vc_id FROM nexus_bot_home_channels WHERE guild_id = %s AND bot_name = 'nexus' LIMIT 1",
                    (guild_id,),
                )
                home_row = await cur.fetchone()

                await cur.execute(
                    "SELECT COALESCE(connected_channel_id, last_channel_id) FROM nexus_voice_state WHERE guild_id = %s AND bot_name = 'nexus' AND desired_connected = TRUE LIMIT 1",
                    (guild_id,),
                )
                voice_row = await cur.fetchone()

                await cur.execute(
                    "SELECT COUNT(*) FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'",
                    (guild_id,),
                )
                queue_row = await cur.fetchone()

                await cur.execute(
                    "SELECT COUNT(*) FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus'",
                    (guild_id,),
                )
                backup_row = await cur.fetchone()

    playback_channel_id = playback[0] if playback and playback[0] else None
    playback_position = playback[1] if playback and playback[1] is not None else 0
    playback_is_playing = bool(playback[2]) if playback else False
    playback_url = playback[3] if playback else None
    playback_title = playback[4] if playback else None
    home_channel_id = home_row[0] if home_row and home_row[0] else None
    voice_channel_id = voice_row[0] if voice_row and voice_row[0] else None
    queue_count = queue_row[0] if queue_row else 0
    backup_count = backup_row[0] if backup_row else 0

    channel_id = playback_channel_id or voice_channel_id or home_channel_id
    has_playback_state = bool((playback_is_playing or playback_position > 0) and (playback_url or playback_title))
    has_recoverable_state = bool(channel_id and (has_playback_state or queue_count > 0))
    if not has_recoverable_state:
        return None

    start_position = int(playback_position or 0)
    if not playback_is_playing and queue_count > 0:
        start_position = 0

    return {
        "voice_channel_id": int(channel_id),
        "position": max(0, start_position),
    }

async def bootstrap_recovery_states_from_db():
    guild_ids = set()
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                for table_name in ("nexus_playback_state", "nexus_queue", "nexus_queue_backup", "nexus_bot_home_channels"):
                    await cur.execute(f"SELECT DISTINCT guild_id FROM {table_name}")
                    rows = await cur.fetchall()
                    for row in rows or []:
                        gid = row[0] if isinstance(row, (tuple, list)) else next(iter(row.values()), None)
                        if gid is not None:
                            guild_ids.add(int(gid))

    recovered = {}
    for guild_id in sorted(guild_ids):
        state = await derive_recovery_state_from_db(guild_id)
        if state:
            recovered[str(guild_id)] = state
    return recovered

async def bootstrap_recovery_after_ready():
    try:
        if BOT_RECOVERY_STAGGER_SECONDS:
            await asyncio.sleep(BOT_RECOVERY_STAGGER_SECONDS)
        states = await bootstrap_recovery_states_from_db()
        for gid, state in states.items():
            schedule_named_task(f"restore_guild_state:{gid}", restore_guild_state(gid, state))
    except Exception as e:
        logger.error(f"Recovery bootstrap error: {e}")

@tasks.loop(seconds=AUTO_HEAL_INTERVAL)
async def auto_heal_loop():
    global auto_heal_initialized

    if not auto_heal_initialized:
        db_states = await bootstrap_recovery_states_from_db()
        for gid, state in db_states.items():
            schedule_named_task(f"restore_guild_state:{gid}", restore_guild_state(gid, state))
        auto_heal_initialized = True

    for gid, state in list(guild_states.items()):
        try:
            normalized_gid = int(gid)
            if recovery_backoff_remaining(normalized_gid) > 0:
                continue
            retry_task = recovery_retry_tasks.get(normalized_gid)
            if normalized_gid in recovering_guilds or (retry_task and not retry_task.done()):
                continue
            guild = bot.get_guild(normalized_gid)
            if guild:
                vc = guild.voice_client
                if (not vc or not vc.is_connected()) or (vc and not _player_is_active(vc) and state.get("voice_channel_id")):
                    logger.info(f"[HEAL] Rejoining/restarting playback for {gid}")
                    if vc and not _player_is_active(vc):
                        playback_tracking.pop(normalized_gid, None)
                    schedule_named_task(f"restore_guild_state:{gid}", restore_guild_state(normalized_gid, state))
        except Exception:
            pass

# --- BOT EVENTS & LAVALINK CONNECTION ---
@bot.event
async def setup_hook():
    await init_db_with_retries()

@bot.event
async def on_wavelink_node_ready(payload: wavelink.NodeReadyEventPayload):
    global lavalink_recovery_bootstrapped
    logger.info(f"🔥 Lavalink Bridge Officially Connected and Locked! (Node: {payload.node.identifier})")
    if lavalink_recovery_bootstrapped:
        return
    lavalink_recovery_bootstrapped = True
    if BOT_RECOVERY_STAGGER_SECONDS:
        await asyncio.sleep(BOT_RECOVERY_STAGGER_SECONDS)
    logger.info("Checking for orphaned playback states to auto-resume...")
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute("SELECT guild_id, channel_id, position_seconds, video_url, title FROM nexus_playback_state WHERE is_playing = TRUE AND bot_name = 'nexus'")
                    orphans = await cur.fetchall()
                    for orphan in orphans:
                        guild = bot.get_guild(orphan['guild_id'])
                        if guild:
                            vc = guild.voice_client
                            if vc and _player_is_playing(vc):
                                continue
                            # FIX 7: Skip guilds already being healed by another loop
                            if guild.id in recovering_guilds:
                                continue
                            if guild.id in playback_tracking and not (vc and _player_is_active(vc)):
                                playback_tracking.pop(guild.id, None)
                            recovering_guilds.add(guild.id)
                            await restore_active_playback_entry(cur, guild.id)
                            schedule_named_task(f"orphan_restore_process_queue:{guild.id}", process_queue(guild, orphan['channel_id'], start_position=orphan['position_seconds'], allow_recovery_restore=True))
    except Exception as e:
        logger.error(f"Auto-resume error: {e}")

@bot.event
async def on_wavelink_node_closed(node: wavelink.Node, disconnected):
    global lavalink_recovery_bootstrapped
    lavalink_recovery_bootstrapped = False
    logger.warning("⚠️ Lavalink Connection Lost! Native self-healing activated...")
    ensure_lavalink_connection_task()
    for guild_id, state in list(guild_states.items()):
        schedule_recovery_retry(int(guild_id), state.get("voice_channel_id"), start_position=state.get("position", 0), reason="node_closed")

@bot.event
async def on_ready():
    logger.info(f'Logged in as {bot.user}')

    ensure_lavalink_connection_task()

    try:
        await bot.tree.sync()
    except Exception as e:
        logger.error(f"Slash command sync failed: {e}")

    if not position_updater.is_running():
        position_updater.start()
    if not metrics_heartbeat_loop.is_running():
        metrics_heartbeat_loop.start()

    schedule_named_task("restore_persistent_voice_states", restore_persistent_voice_states())
    schedule_named_task("bootstrap_recovery_after_ready", bootstrap_recovery_after_ready())

@bot.event
async def on_voice_state_update(member, before, after):
    if member == bot.user:
        guild_id = member.guild.id
        if after.channel is not None:
            pending_voice_channels[guild_id] = after.channel.id
            await persist_voice_state(guild_id, after.channel.id, desired_connected=True, connected=True)
            await reconcile_runtime_playback_state(member.guild)
            return
    if member == bot.user and before.channel is not None and after.channel is None:
        guild_id = member.guild.id
        pending_voice_channels.pop(guild_id, None)

        tracked = playback_tracking.get(guild_id)
        remembered_channel_id = (
            (tracked or {}).get('channel_id')
            or guild_states.get(guild_id, {}).get("voice_channel_id")
            or getattr(before.channel, "id", None)
        )

        if (tracked or guild_id in guild_states) and remembered_channel_id:
            position = current_track_position(guild_id)
            playback_tracking.pop(guild_id, None)
            recovering_guilds.discard(guild_id)
            await remember_recovery_state(guild_id, remembered_channel_id, position)
            await mark_voice_disconnected(guild_id, remembered_channel_id, desired_connected=True, reason="voice_disconnect")
            scheduled = schedule_recovery_retry(guild_id, remembered_channel_id, start_position=position, reason="voice_disconnect")
            if scheduled:
                logger.warning(f"[{guild_id}] Voice link dropped unexpectedly. Queued recovery from {position}s.")
            else:
                remaining = recovery_backoff_remaining(guild_id)
                if remaining > 0:
                    logger.warning(f"[{guild_id}] Voice link dropped unexpectedly, but recovery is paused for another {remaining}s.")
            return

        playback_tracking.pop(guild_id, None)
        guild_states.pop(guild_id, None)
        recovering_guilds.discard(guild_id)
        invalidate_position_persist(guild_id)
        clear_recovery_retry(guild_id)
        clear_idle_restore_state(guild_id)
        await mark_voice_disconnected(guild_id, getattr(before.channel, "id", None), desired_connected=False, reason="manual_or_idle_disconnect")
        await delete_state(guild_id)

@tasks.loop(seconds=METRICS_HEARTBEAT_INTERVAL)
async def metrics_heartbeat_loop():
    prune_runtime_state_cache()
    for guild in list(bot.guilds):
        try:
            await reconcile_runtime_playback_state(guild)
        except Exception:
            logger.exception("[nexus] Metrics reconcile failed for guild %s", getattr(guild, 'id', None))
    try:
        await collect_and_persist_metrics()
    except Exception:
        logger.exception("[nexus] Metrics heartbeat failed.")

@tasks.loop(seconds=POSITION_UPDATER_INTERVAL)
async def position_updater():
    now = time.time()
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                for guild_id, data in list(playback_tracking.items()):
                    last_persist = last_position_persist.get(guild_id, 0)
                    if now - last_persist < POSITION_PERSIST_INTERVAL:
                        continue
                    guild = bot.get_guild(guild_id)
                    if guild and guild.voice_client and _player_is_playing(guild.voice_client):
                        pos = int((time.time() - data['start_time']) * data.get('speed', 1.0) + data['offset'])
                        await cur.execute("REPLACE INTO nexus_playback_state (guild_id, bot_name, channel_id, video_url, position_seconds, is_playing, is_paused, title) VALUES (%s, 'nexus', %s, %s, %s, TRUE, FALSE, %s)", (guild_id, data['channel_id'], data['url'], pos, data['title']))
                        last_position_persist[guild_id] = now

# --- SETTINGS COMMANDS ---
@bot.tree.command(name="nexus_main_sethome", description="Save this bot's default voice or stage channel for join, autoplay, and recovery behavior.")
@commands.has_permissions(administrator=True)
async def sethome(interaction: discord.Interaction, channel: discord.VoiceChannel | discord.StageChannel):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("REPLACE INTO nexus_bot_home_channels (guild_id, bot_name, home_vc_id) VALUES (%s, %s, %s)", (interaction.guild.id, 'nexus', channel.id))
    await interaction.response.send_message(embed=discord.Embed(title="🏠 Home Set", description=f"Home channel set to {channel.mention}.", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_setfeedback", description="Choose the text channel for updates, queue actions, and recovery notices.")
@commands.has_permissions(administrator=True)
async def setfeedback(interaction: discord.Interaction, channel: discord.TextChannel):
    await ensure_guild_settings(interaction.guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("UPDATE nexus_guild_settings SET feedback_channel_id = %s WHERE guild_id = %s", (channel.id, interaction.guild.id))
    await interaction.response.send_message(embed=discord.Embed(title="✅ Feedback Channel Set", description=f"Updates will be sent to {channel.mention}.", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_djrole", description="Set the server DJ role that can manage restricted playback, queue, and settings commands.")
@commands.has_permissions(administrator=True)
async def djrole(interaction: discord.Interaction, role: discord.Role):
    await ensure_guild_settings(interaction.guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("UPDATE nexus_guild_settings SET dj_role_id = %s WHERE guild_id = %s", (role.id, interaction.guild.id))
    await interaction.response.send_message(embed=discord.Embed(description=f"🎧 DJ role set to {role.mention}", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_removedj", description="Clear the configured DJ role so only admins or open-access mode can control restricted commands.")
@commands.has_permissions(administrator=True)
async def removedj(interaction: discord.Interaction):
    await ensure_guild_settings(interaction.guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("UPDATE nexus_guild_settings SET dj_role_id = NULL WHERE guild_id = %s", (interaction.guild.id,))
    await interaction.response.send_message(embed=discord.Embed(description="DJ role requirements removed.", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_djmode", description="Enable or disable Strict DJ Mode so only admins and the DJ role can use control commands.")
@commands.has_permissions(administrator=True)
async def toggle_djmode(interaction: discord.Interaction):
    await ensure_guild_settings(interaction.guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT dj_only_mode FROM nexus_guild_settings WHERE guild_id = %s", (interaction.guild.id,))
                res = await cur.fetchone()
                new_val = not res[0] if res else True
                await cur.execute("UPDATE nexus_guild_settings SET dj_only_mode = %s WHERE guild_id = %s", (new_val, interaction.guild.id))
    state = "ENABLED" if new_val else "DISABLED"
    await interaction.response.send_message(embed=discord.Embed(description=f"🎧 Strict DJ Mode is now **{state}**.", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_247", description="Keep the bot connected and ready in voice channels even after playback ends until you disable it.")
@commands.has_permissions(administrator=True)
async def toggle_247(interaction: discord.Interaction):
    await ensure_guild_settings(interaction.guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT stay_in_vc FROM nexus_guild_settings WHERE guild_id = %s", (interaction.guild.id,))
                res = await cur.fetchone()
                new_val = not res[0] if res else True
                await cur.execute("UPDATE nexus_guild_settings SET stay_in_vc = %s WHERE guild_id = %s", (new_val, interaction.guild.id))
    state = "ENABLED" if new_val else "DISABLED"
    await interaction.response.send_message(embed=discord.Embed(description=f"🕰️ 24/7 Mode is now **{state}**.", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_restart", description="Restart this bot instance immediately for maintenance or recovery. Administrator only.")
@commands.has_permissions(administrator=True)
async def restart_bot(interaction: discord.Interaction):
    await interaction.response.send_message("Restarting...", ephemeral=True)
    await request_supervisor_restart("admin_slash_command", announce=False)

# --- COMMAND SURFACE: slash handlers should stay thin and delegate to runtime helpers above. ---
# --- PLAYBACK COMMANDS ---
@bot.tree.command(name="nexus_main_play", description="Queue a track, URL, livestream, search result, or playlist and start playback if idle.")
async def play(interaction: discord.Interaction, search: str):
    interaction_token_valid = True
    try:
        await interaction.response.defer()
    except discord.NotFound: interaction_token_valid = False
    except discord.InteractionResponded: interaction_token_valid = True
    except Exception: interaction_token_valid = False

    async def send_play_feedback(embed: discord.Embed):
        if interaction_token_valid:
            try: return await interaction.followup.send(embed=embed)
            except Exception: pass
        if interaction.channel:
            try: return await interaction.channel.send(embed=embed)
            except Exception: pass
        return None

    # FIX: Priority #1 is the home channel, not just a fallback.
    channel = None
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT home_vc_id FROM nexus_bot_home_channels WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
                res = await cur.fetchone()
                if res and res[0]: channel = interaction.guild.get_channel(res[0])

    if not channel:
        channel = interaction.user.voice.channel if interaction.user.voice else None

    if not channel:
        await send_play_feedback(discord.Embed(title="❌ Error", description="Join a channel first or set a home channel.", color=discord.Color.red()))
        return

    try:
        entries_to_add, playlist_result = await search_playables(search)
        if not entries_to_add: raise ValueError("Nothing playable came back for that search.")
    except Exception as e:
        message = str(e)
        if "No nodes are currently assigned to the wavelink.Pool in a CONNECTED state" in message:
            message = "Lavalink is not connected yet. Give the music node a few seconds to finish booting, then try again."
            ensure_lavalink_connection_task()
        await send_play_feedback(discord.Embed(title="❌ Source Error", description=f"Could not load that source: {message}", color=discord.Color.red()))
        return

    is_playlist_request = bool(playlist_result) or ('list=' in search and len(entries_to_add) > 1)
    playlist_url = resolve_playlist_source(search, playlist_result) if is_playlist_request else None
    added_count = 0

    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                for track in entries_to_add:
                    await enqueue_track(cur, interaction.guild.id, track.uri, track.title, interaction.user.id)
                    added_count += 1
                await cur.execute("SELECT COUNT(*) FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
                q_len = (await cur.fetchone())[0]
    if playlist_url:
        await set_active_playlist(interaction.guild.id, playlist_url, len(entries_to_add), interaction.user.id, channel.id)

    vc = interaction.guild.voice_client
    if not vc or (not _player_is_active(vc)):
        await send_play_feedback(discord.Embed(title="🎶 Queued & Starting", description=f"Added **{added_count}** tracks. Starting Lavalink Engine!", color=discord.Color.green()))
        await process_queue(interaction.guild, channel.id)
    else:
        await send_play_feedback(discord.Embed(title="📥 Added to Queue", description=f"Added **{added_count}** tracks. (Queue size: {q_len})", color=discord.Color.blue()))

@bot.tree.command(name="nexus_main_playnext", description="Queue one track to play next, ahead of the existing queue, without clearing current playback.")
async def playnext(interaction: discord.Interaction, search: str):
    if not await is_dj(interaction): return
    await interaction.response.defer()

    try:
        entries, _playlist_result = await search_playables(search)
        if not entries: raise ValueError("Track could not be found.")
        track = entries[0]
    except Exception as e:
        return await interaction.followup.send(embed=discord.Embed(description=f"Could not resolve that source: {e}", color=discord.Color.red()))

    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id, video_url, title, requester_id FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC", (interaction.guild.id,))
                q = await cur.fetchall()
                await cur.execute("DELETE FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
                await enqueue_track(cur, interaction.guild.id, track.uri, track.title, interaction.user.id)
                for r in q:
                    await enqueue_track(cur, interaction.guild.id, r[1], r[2], r[3], backup=False)
                await snapshot_queue_backup(cur, interaction.guild.id)
    vc = interaction.guild.voice_client
    if not vc or (not _player_is_active(vc)):
        channel = vc.channel if vc and getattr(vc, 'channel', None) else await get_home_channel(interaction.guild)
        if not channel:
            channel = interaction.user.voice.channel if interaction.user.voice else None
        if channel:
            await process_queue(interaction.guild, channel.id)
    await interaction.followup.send(embed=discord.Embed(description=f"**Playing next:** {track.title}", color=discord.Color.green()))

@bot.tree.command(name="nexus_main_skip", description="Skip the current track and move playback to the next queued item or Auto-DJ recommendation.")
async def skip(interaction: discord.Interaction):
    if not await is_dj(interaction): return
    # FIX: Account for silent failures when nothing is playing
    if interaction.guild.voice_client and (_player_is_active(interaction.guild.voice_client)):
        await interaction.guild.voice_client.stop()
        await interaction.response.send_message(embed=discord.Embed(description="⏭️ Skipped", color=discord.Color.blurple()), ephemeral=True)
    else:
        await interaction.response.send_message(embed=discord.Embed(description="❌ Nothing is playing.", color=discord.Color.red()), ephemeral=True)

@bot.tree.command(name="nexus_main_stop", description="Stop playback, clear the queue, remove recovery state, and reset the bot for this server.")
async def stop(interaction: discord.Interaction):
    if not await is_dj(interaction): return
    snooze_auto_restore(interaction.guild.id)
    await stop_playback(interaction.guild)
    await clear_active_playlist(interaction.guild.id)
    await interaction.response.send_message(embed=discord.Embed(title="⏹️ Stopped", description="Music stopped and cleared.", color=discord.Color.red()), ephemeral=True)

@bot.tree.command(name="nexus_main_pause", description="Pause the current track without clearing the queue or playback position.")
async def pause(interaction: discord.Interaction):
    if not await is_dj(interaction): return
    if interaction.guild.voice_client and _player_is_playing(interaction.guild.voice_client):
        await interaction.guild.voice_client.pause(True)
        await sync_pause_state(interaction.guild.id, True)
        await interaction.response.send_message(embed=discord.Embed(description="⏸️ Paused", color=discord.Color.blue()), ephemeral=True)
    else:
        await interaction.response.send_message(embed=discord.Embed(description="❌ Nothing is currently playing.", color=discord.Color.red()), ephemeral=True)

@bot.tree.command(name="nexus_main_resume", description="Resume the paused track from its current playback position.")
async def resume(interaction: discord.Interaction):
    if not await is_dj(interaction): return
    if interaction.guild.voice_client and _player_is_paused(interaction.guild.voice_client):
        await interaction.guild.voice_client.pause(False)
        await sync_pause_state(interaction.guild.id, False)
        await interaction.response.send_message(embed=discord.Embed(description="▶️ Resumed", color=discord.Color.green()), ephemeral=True)
    else:
        await interaction.response.send_message(embed=discord.Embed(description="❌ Nothing is currently paused.", color=discord.Color.red()), ephemeral=True)

@bot.tree.command(name="nexus_main_clear", description="Clear the upcoming queue, stop playback, and reset stored playback state for this server.")
async def clear(interaction: discord.Interaction):
    if not await is_dj(interaction): return
    snooze_auto_restore(interaction.guild.id)
    vc = interaction.guild.voice_client
    if vc and _player_is_active(vc):
        try: await vc.stop()
        except Exception: pass
    playback_tracking.pop(interaction.guild.id, None)
    guild_states.pop(interaction.guild.id, None)
    recovering_guilds.discard(interaction.guild.id)
    invalidate_position_persist(interaction.guild.id)
    clear_recovery_retry(interaction.guild.id)
    clear_idle_restore_state(interaction.guild.id)
    await delete_state(interaction.guild.id)
    await clear_active_playlist(interaction.guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("DELETE FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
                await cur.execute("DELETE FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
                await cur.execute("UPDATE nexus_playback_state SET channel_id = NULL, video_url = NULL, title = NULL, position_seconds = 0, is_playing = FALSE, is_paused = FALSE WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
                await cur.execute("UPDATE nexus_voice_state SET desired_connected = FALSE, connected_channel_id = NULL, disconnected_at = CURRENT_TIMESTAMP WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
    await bot.change_presence(status=discord.Status.online)
    await interaction.response.send_message(embed=discord.Embed(description="🗑️ Playback stopped and queue cleared.", color=discord.Color.red()), ephemeral=True)

@bot.tree.command(name="nexus_main_join", description="Force the bot to join your current voice channel, or its configured home channel if one is saved.")
async def join(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    # FIX: Prioritize home channel over user channel globally
    channel = None
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT home_vc_id FROM nexus_bot_home_channels WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
                res = await cur.fetchone()
                if res and res[0]: channel = interaction.guild.get_channel(res[0])

    if not channel:
        channel = interaction.user.voice.channel if interaction.user.voice else None

    if channel:
        await ensure_voice_connection(interaction.guild, channel.id)
        await interaction.followup.send(embed=discord.Embed(description=f"Joined {channel.mention}.", color=discord.Color.green()))
    else:
        await interaction.followup.send("Join a channel first, or set a home channel.", ephemeral=True)

@bot.tree.command(name="nexus_main_leave", description="Disconnect the bot from voice and clear any pending recovery handoff for this server.")
async def leave(interaction: discord.Interaction):
    if not await is_dj(interaction): return
    if interaction.guild.voice_client:
        snooze_auto_restore(interaction.guild.id)
        playback_tracking.pop(interaction.guild.id, None)
        guild_states.pop(interaction.guild.id, None)
        recovering_guilds.discard(interaction.guild.id)
        invalidate_position_persist(interaction.guild.id)
        clear_recovery_retry(interaction.guild.id)
        clear_idle_restore_state(interaction.guild.id)
        await delete_state(interaction.guild.id)
        await clear_active_playlist(interaction.guild.id)
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("DELETE FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
                    await cur.execute("DELETE FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
                    await cur.execute("UPDATE nexus_playback_state SET channel_id = NULL, video_url = NULL, title = NULL, position_seconds = 0, is_playing = FALSE, is_paused = FALSE WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
                    await cur.execute("UPDATE nexus_voice_state SET desired_connected = FALSE, connected_channel_id = NULL, disconnected_at = CURRENT_TIMESTAMP WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
        await interaction.guild.voice_client.disconnect()
        await interaction.response.send_message(embed=discord.Embed(description="Left the channel.", color=discord.Color.orange()), ephemeral=True)
    else:
        await interaction.response.send_message(embed=discord.Embed(description="❌ I'm not in a voice channel.", color=discord.Color.red()), ephemeral=True)

@bot.tree.command(name="nexus_main_queue", description="Show the current queue with paging, requester names, and track positions")
async def queue_cmd(interaction: discord.Interaction, page: int = 1):
    page = max(1, page)
    per_page = 10
    offset = (page - 1) * per_page
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
                total_row = await cur.fetchone()
                total = total_row[0] if total_row else 0
                await cur.execute("SELECT title, requester_id FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC LIMIT %s OFFSET %s", (interaction.guild.id, per_page, offset))
                songs = await cur.fetchall()
    if not songs:
        return await interaction.response.send_message(embed=discord.Embed(description="Queue empty.", color=discord.Color.red()), ephemeral=True)
    lines = []
    for idx, row in enumerate(songs, start=offset + 1):
        title, requester_id = row
        requester_name = await resolve_requester_name(interaction.guild, requester_id)
        lines.append(f"**{idx}.** {title} — *{requester_name}*")
    pages = max(1, (total + per_page - 1) // per_page)
    embed = discord.Embed(title="📜 Queue", description="\n".join(lines), color=discord.Color.blurple())
    embed.set_footer(text=f"Page {page}/{pages} • {total} queued track(s)")
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="nexus_main_shuffle", description="Randomize the order of the upcoming queue while keeping the currently playing track untouched.")
async def shuffle(interaction: discord.Interaction):
    if not await is_dj(interaction): return
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id, video_url, title, requester_id FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
                q = await cur.fetchall()
                if not q: return await interaction.response.send_message("Queue empty.", ephemeral=True)
                l = list(q); random.shuffle(l)
                await cur.execute("DELETE FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
                for row in l:
                    await enqueue_track(cur, interaction.guild.id, row[1], row[2], row[3], backup=False)
    await interaction.response.send_message(embed=discord.Embed(description="🔀 Queue shuffled.", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_remove", description="Remove a queued track by its queue number so it will not play later.")
async def remove(interaction: discord.Interaction, index: int):
    if not await is_dj(interaction): return
    if index < 1:
        return await interaction.response.send_message("Invalid index.", ephemeral=True)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                # FIX 8: Fetch url+title so we can mirror the deletion in the backup
                await cur.execute("SELECT id, video_url, title FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC LIMIT 1 OFFSET %s", (interaction.guild.id, index-1))
                row = await cur.fetchone()
                if row:
                    await cur.execute("DELETE FROM nexus_queue WHERE id = %s AND guild_id = %s AND bot_name = 'nexus'", (row[0], interaction.guild.id))
                    await cur.execute(
                        "DELETE FROM nexus_queue_backup WHERE video_url = %s AND title = %s AND guild_id = %s AND bot_name = 'nexus' LIMIT 1",
                        (row[1], row[2], interaction.guild.id),
                    )
                    await interaction.response.send_message(embed=discord.Embed(description=f"Removed item #{index}", color=discord.Color.green()), ephemeral=True)
                else: await interaction.response.send_message("Invalid index.", ephemeral=True)

@bot.tree.command(name="nexus_main_skipto", description="Drop everything before a chosen queue position and jump playback forward to that track.")
async def skipto(interaction: discord.Interaction, index: int):
    if not await is_dj(interaction): return
    if index < 1:
        return await interaction.response.send_message("Invalid index.", ephemeral=True)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                # FIX 9: Fetch url+title so skipped tracks can be removed from backup too
                await cur.execute("SELECT id, video_url, title FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC LIMIT %s", (interaction.guild.id, index-1))
                rows = await cur.fetchall()
                for r in rows:
                    await cur.execute("DELETE FROM nexus_queue WHERE id = %s AND guild_id = %s AND bot_name = 'nexus'", (r[0], interaction.guild.id))
                    await cur.execute(
                        "DELETE FROM nexus_queue_backup WHERE video_url = %s AND title = %s AND guild_id = %s AND bot_name = 'nexus' LIMIT 1",
                        (r[1], r[2], interaction.guild.id),
                    )
    if interaction.guild.voice_client: await interaction.guild.voice_client.stop()
    await interaction.response.send_message(embed=discord.Embed(description=f"Skipped to #{index}", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_move", description="Move a queued track from one queue slot to another without rebuilding the entire session manually.")
async def move(interaction: discord.Interaction, frm: int, to: int):
    if not await is_dj(interaction): return
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id, video_url, title, requester_id FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC", (interaction.guild.id,))
                q = list(await cur.fetchall())
                if frm > len(q) or to > len(q) or frm < 1 or to < 1:
                    return await interaction.response.send_message("Invalid index", ephemeral=True)
                item = q.pop(frm - 1)
                # FIX 5: Always use (to - 1) as the 0-indexed insertion point.
                # The old `to - 2` branch when frm < to was off by one and
                # placed the item one slot before the user's intended position.
                insert_at = to - 1
                q.insert(max(0, min(insert_at, len(q))), item)
                await cur.execute("DELETE FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
                for r in q:
                    await enqueue_track(cur, interaction.guild.id, r[1], r[2], r[3], backup=False)
    await interaction.response.send_message(embed=discord.Embed(description=f"Moved item from {frm} to {to}", color=discord.Color.green()), ephemeral=True)



@bot.tree.command(name="nexus_main_bump", description="Move a queued track to the front so it plays next")
async def bump(interaction: discord.Interaction, index: int):
    if not await is_dj(interaction): return
    if index < 1:
        return await interaction.response.send_message("Invalid index.", ephemeral=True)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id, video_url, title, requester_id FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC LIMIT 1 OFFSET %s", (interaction.guild.id, index-1))
                row = await cur.fetchone()
                if not row:
                    return await interaction.response.send_message("Invalid index.", ephemeral=True)
                await cur.execute("DELETE FROM nexus_queue WHERE id = %s AND guild_id = %s AND bot_name = 'nexus'", (row[0], interaction.guild.id))
                await insert_queue_front(cur, "nexus_queue", interaction.guild.id, "nexus", row[1], row[2], row[3])
    await interaction.response.send_message(embed=discord.Embed(description=f"⬆️ Moved **{row[2]}** to play next.", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_clearmine", description="Remove your own queued songs without touching other listeners' tracks")
async def clearmine(interaction: discord.Interaction):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' AND requester_id = %s", (interaction.guild.id, interaction.user.id))
                row = await cur.fetchone()
                removed = row[0] if row else 0
                if removed:
                    await cur.execute("DELETE FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' AND requester_id = %s", (interaction.guild.id, interaction.user.id))
                    # FIX 10: Mirror deletion in the backup queue
                    await cur.execute("DELETE FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus' AND requester_id = %s", (interaction.guild.id, interaction.user.id))
    await interaction.response.send_message(embed=discord.Embed(description=f"🧹 Removed **{removed}** of your queued track(s).", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_voteskip", description="Start or join a vote skip when no DJ is around to skip directly")
async def voteskip(interaction: discord.Interaction):
    vc = interaction.guild.voice_client
    if not vc or not getattr(vc, 'channel', None) or interaction.guild.id not in playback_tracking:
        return await interaction.response.send_message("Nothing is playing right now.", ephemeral=True)
    if await is_dj(interaction, silent=True):
        await vc.stop()
        return await interaction.response.send_message(embed=discord.Embed(description="⏭️ DJ override used: skipped the current track.", color=discord.Color.green()), ephemeral=True)
    listeners = [m for m in vc.channel.members if not m.bot]
    if interaction.user not in listeners:
        return await interaction.response.send_message("Join the same voice channel first.", ephemeral=True)
    required = max(2, (len(listeners) // 2) + 1)
    votes = vote_skip_sessions.setdefault(interaction.guild.id, set())
    votes.add(interaction.user.id)
    if len(votes) >= required:
        vote_skip_sessions.pop(interaction.guild.id, None)
        await vc.stop()
        return await interaction.response.send_message(embed=discord.Embed(description=f"⏭️ Vote skip passed with **{len(votes)}/{required}** votes.", color=discord.Color.green()))
    await interaction.response.send_message(embed=discord.Embed(description=f"🗳️ Vote recorded: **{len(votes)}/{required}** votes to skip.", color=discord.Color.blurple()), ephemeral=True)

@bot.tree.command(name="nexus_main_autodj", description="Enable or disable smarter Auto-DJ recommendations when the queue runs dry")
async def autodj(interaction: discord.Interaction, enabled: bool):
    if not await is_dj(interaction): return
    await set_autodj_enabled(interaction.guild.id, enabled)
    state = "enabled" if enabled else "disabled"
    await interaction.response.send_message(embed=discord.Embed(description=f"📻 Auto-DJ is now **{state}**.", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_settings", description="Show the saved playback, DJ, queue, and recovery settings for this server")
async def settings_cmd(interaction: discord.Interaction):
    row = await get_saved_settings_summary(interaction.guild.id)
    home_vc_id, volume, loop_mode, filter_mode, dj_role_id, feedback_channel_id, transition_mode, custom_speed, custom_pitch, custom_modifiers_left, dj_only_mode, stay_in_vc = row if row else (None, 100, 'queue', 'none', None, None, 'off', 1.0, 1.0, 0, False, False)
    autodj_state = await get_autodj_enabled(interaction.guild.id)
    embed = discord.Embed(title="⚙️ Server Music Settings", color=discord.Color.blurple())
    embed.add_field(name="Home Channel", value=f"<#{home_vc_id}>" if home_vc_id else "Not set", inline=True)
    embed.add_field(name="Feedback Channel", value=f"<#{feedback_channel_id}>" if feedback_channel_id else "Not set", inline=True)
    embed.add_field(name="DJ Role", value=f"<@&{dj_role_id}>" if dj_role_id else "Not set", inline=True)
    embed.add_field(name="Volume", value=str(volume), inline=True)
    embed.add_field(name="Loop Mode", value=str(loop_mode), inline=True)
    embed.add_field(name="Filter", value=str(filter_mode), inline=True)
    embed.add_field(name="Transitions", value=str(transition_mode), inline=True)
    embed.add_field(name="Custom Speed/Pitch", value=f"{custom_speed}x / {custom_pitch}x ({custom_modifiers_left} left)", inline=True)
    embed.add_field(name="Strict DJ", value="Enabled" if dj_only_mode else "Disabled", inline=True)
    embed.add_field(name="24/7 Mode", value="Enabled" if stay_in_vc else "Disabled", inline=True)
    embed.add_field(name="Auto-DJ", value="Enabled" if autodj_state else "Disabled", inline=True)
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="nexus_main_playlists", description="List your saved personal playlists and how many tracks each one contains")
async def playlists(interaction: discord.Interaction):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT playlist_name, COUNT(*) FROM nexus_user_playlists WHERE user_id = %s GROUP BY playlist_name ORDER BY playlist_name ASC", (interaction.user.id,))
                rows = await cur.fetchall()
    if not rows:
        return await interaction.response.send_message("You do not have any saved playlists yet.", ephemeral=True)
    desc = "\n".join(f"• **{name}** — {count} track(s)" for name, count in rows[:20])
    await interaction.response.send_message(embed=discord.Embed(title="🎼 Your Saved Playlists", description=desc, color=discord.Color.blurple()), ephemeral=True)

@bot.tree.command(name="nexus_main_deleteplaylist", description="Delete one of your saved personal playlists by name")
async def deleteplaylist(interaction: discord.Interaction, name: str):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM nexus_user_playlists WHERE user_id = %s AND playlist_name = %s", (interaction.user.id, name))
                row = await cur.fetchone()
                count = row[0] if row else 0
                if count:
                    await cur.execute("DELETE FROM nexus_user_playlists WHERE user_id = %s AND playlist_name = %s", (interaction.user.id, name))
    if not count:
        return await interaction.response.send_message("That playlist was not found.", ephemeral=True)
    await interaction.response.send_message(embed=discord.Embed(description=f"🗑️ Deleted **{name}** ({count} track(s)).", color=discord.Color.green()), ephemeral=True)


# --- PLAYLISTS & HISTORY ---
@bot.tree.command(name="nexus_main_savequeue", description="Save the current queue to one of your personal playlists so you can load it again later.")
async def savequeue(interaction: discord.Interaction, name: str):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT video_url, title FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC", (interaction.guild.id,))
                q = await cur.fetchall()
                if not q: return await interaction.response.send_message("Queue is empty!", ephemeral=True)
                for url, title in q:
                    await cur.execute("INSERT INTO nexus_user_playlists (user_id, playlist_name, video_url, title) VALUES (%s, %s, %s, %s)", (interaction.user.id, name, url, title))
    await interaction.response.send_message(embed=discord.Embed(description=f"💾 Saved **{len(q)}** tracks to your personal playlist: **{name}**", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_loadqueue", description="Load one of your saved personal playlists into the active queue and start playback if needed.")
async def loadqueue(interaction: discord.Interaction, name: str):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT video_url, title FROM nexus_user_playlists WHERE user_id = %s AND playlist_name = %s", (interaction.user.id, name))
                q = await cur.fetchall()
                if not q: return await interaction.response.send_message("Playlist not found or empty.", ephemeral=True)
                for url, title in q:
                    await enqueue_track(cur, interaction.guild.id, url, title, interaction.user.id)
    await interaction.response.send_message(embed=discord.Embed(description=f"📂 Loaded **{len(q)}** tracks from **{name}** into the queue!", color=discord.Color.green()))
    vc = interaction.guild.voice_client
    if not vc or (not _player_is_active(vc)):
        channel = interaction.user.voice.channel if interaction.user.voice else None
        if channel: await process_queue(interaction.guild, channel.id)

@bot.tree.command(name="nexus_main_leaderboard", description="Show the most played tracks from this server based on stored playback history.")
async def leaderboard(interaction: discord.Interaction):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT title, COUNT(*) as plays FROM nexus_history WHERE guild_id = %s GROUP BY title ORDER BY plays DESC LIMIT 10", (interaction.guild.id,))
                songs = await cur.fetchall()
    if not songs: return await interaction.response.send_message("No play history yet.", ephemeral=True)
    desc = "\n".join(f"**{i+1}.** {s[0]} *(Played {s[1]} times)*" for i, s in enumerate(songs))
    await interaction.response.send_message(embed=discord.Embed(title="🏆 Server Top Tracks", description=desc, color=discord.Color.gold()))

@bot.tree.command(name="nexus_main_history", description="Show the most recent tracks played in this server from playback history.")
async def history(interaction: discord.Interaction):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT title FROM nexus_history WHERE guild_id = %s ORDER BY played_at DESC LIMIT 5", (interaction.guild.id,))
                songs = await cur.fetchall()
    if songs: await interaction.response.send_message(embed=discord.Embed(title="📜 History", description="\n".join(f"- {s[0]}" for s in songs), color=discord.Color.blurple()), ephemeral=True)
    else: await interaction.response.send_message("No history.", ephemeral=True)

@bot.tree.command(name="nexus_main_userhistory", description="Show the most recent tracks requested by a specific user in this server.")
async def userhistory(interaction: discord.Interaction, member: discord.Member):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id, title, video_url FROM nexus_history WHERE guild_id = %s AND requester_id = %s ORDER BY played_at DESC LIMIT 10", (interaction.guild.id, member.id))
                songs = await cur.fetchall()
    if not songs: return await interaction.response.send_message(embed=discord.Embed(description=f"📭 {member.display_name} hasn't queued any songs yet.", color=discord.Color.red()), ephemeral=True)
    desc = "\n".join([f"**{idx + 1}.** [{song[1]}]({song[2]})" for idx, song in enumerate(songs)])
    embed = discord.Embed(title=f"🎧 {member.display_name}'s Play History", description=desc, color=discord.Color.blue())
    embed.set_footer(text="Use /nexus_main_steal <user> <number> to add one to the queue!")
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="nexus_main_steal", description="Copy a track from a member's request history and add it back into the queue.")
async def steal(interaction: discord.Interaction, member: discord.Member, track_number: int):
    if track_number < 1:
        return await interaction.response.send_message(embed=discord.Embed(description="❌ Track number must be 1 or greater.", color=discord.Color.red()), ephemeral=True)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT video_url, title FROM nexus_history WHERE guild_id = %s AND requester_id = %s ORDER BY played_at DESC LIMIT 1 OFFSET %s", (interaction.guild.id, member.id, track_number - 1))
                song = await cur.fetchone()
                if not song: return await interaction.response.send_message(embed=discord.Embed(description=f"❌ Could not find track #{track_number} in their history.", color=discord.Color.red()), ephemeral=True)
                url, title = song
                await enqueue_track(cur, interaction.guild.id, url, title, interaction.user.id)
    await interaction.response.send_message(embed=discord.Embed(title="🥷 Song Stolen!", description=f"Added **{title}** to the queue from {member.display_name}'s history.", color=discord.Color.green()))
    vc = interaction.guild.voice_client
    if not vc or (not _player_is_active(vc)):
        channel = interaction.user.voice.channel if interaction.user.voice else None
        if channel: await process_queue(interaction.guild, channel.id)

@bot.tree.command(name="nexus_main_grab", description="Send yourself the currently playing track in a direct message for easy saving or sharing.")
async def grab(interaction: discord.Interaction):
    if interaction.guild.id in playback_tracking:
        data = playback_tracking[interaction.guild.id]
        dm_embed = discord.Embed(
            title="🎵 Track Saved!",
            description=f"Hey **{interaction.user.display_name}**!\nHere is the track you wanted to save:\n\n**[{data.get('title', 'Unknown Title')}]({data['url']})**",
            color=discord.Color.from_rgb(88, 101, 242)
        )
        try:
            await interaction.user.send(embed=dm_embed)
            await interaction.response.send_message(embed=discord.Embed(description="📬 Check your DMs!", color=discord.Color.green()), ephemeral=True)
        except discord.Forbidden:
            await interaction.response.send_message(embed=discord.Embed(description="❌ I can't DM you! Please check your privacy settings.", color=discord.Color.red()), ephemeral=True)
    else:
        await interaction.response.send_message(embed=discord.Embed(description="Nothing is currently playing.", color=discord.Color.red()), ephemeral=True)

# --- MODIFIERS & FILTERS ---
@bot.tree.command(name="nexus_main_volume", description="Set the playback volume for this server from 1 to 200 percent.")
async def volume(interaction: discord.Interaction, vol: int):
    if not await is_dj(interaction): return
    vol = max(1, min(200, vol))
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("INSERT INTO nexus_guild_settings (guild_id, volume) VALUES (%s, %s) ON DUPLICATE KEY UPDATE volume = %s", (interaction.guild.id, vol, vol))
    if interaction.guild.voice_client:
        try: await interaction.guild.voice_client.set_volume(vol)
        except Exception: pass
    await interaction.response.send_message(embed=discord.Embed(description=f"🔊 Volume set to {vol}%", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_loop", description="Choose whether playback loops nothing, the current song, or the full queue.")
async def loop_cmd(interaction: discord.Interaction, mode: str):
    if not await is_dj(interaction): return
    if mode not in ['off', 'song', 'queue']: return await interaction.response.send_message("Invalid mode.", ephemeral=True)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("INSERT INTO nexus_guild_settings (guild_id, loop_mode) VALUES (%s, %s) ON DUPLICATE KEY UPDATE loop_mode = %s", (interaction.guild.id, mode, mode))
    await interaction.response.send_message(embed=discord.Embed(description=f"🔁 Looping set to: {mode}", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_filter", description="Apply an audio filter such as nightcore, vaporwave, or bass boost to upcoming playback.")
@app_commands.describe(mode="Choose an audio filter to apply")
@app_commands.choices(mode=[
    app_commands.Choice(name="None (Standard high quality audio)", value="none"),
    app_commands.Choice(name="Bassboost (Enhances low-end frequencies)", value="bassboost"),
    app_commands.Choice(name="Nightcore (Speeds up and raises pitch)", value="nightcore"),
    app_commands.Choice(name="Vaporwave (Slows down and adds reverb/low pitch)", value="vaporwave")
])
async def filter_cmd(interaction: discord.Interaction, mode: str):
    if not await is_dj(interaction): return
    await ensure_guild_settings(interaction.guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                if mode != 'none': await cur.execute("UPDATE nexus_guild_settings SET filter_mode = %s, custom_modifiers_left = 0 WHERE guild_id = %s", (mode, interaction.guild.id))
                else: await cur.execute("UPDATE nexus_guild_settings SET filter_mode = %s WHERE guild_id = %s", (mode, interaction.guild.id))
    if interaction.guild.voice_client:
        wav_filters = wavelink.Filters()
        if mode == 'nightcore': wav_filters.timescale.set(speed=1.25, pitch=1.3)
        elif mode == 'vaporwave': wav_filters.timescale.set(speed=0.8, pitch=0.8)
        elif mode == 'bassboost': wav_filters.equalizer.set(bands=[(0, 0.3), (1, 0.2), (2, 0.1)])
        try: await interaction.guild.voice_client.set_filters(wav_filters)
        except Exception: pass
    await interaction.response.send_message(embed=discord.Embed(description=f"🎛️ Filter set to: **{mode}**.", color=discord.Color.blurple()), ephemeral=True)

@bot.tree.command(name="nexus_main_fade", description="Enable or disable smooth fade transitions between tracks for softer playback changes.")
@app_commands.describe(mode="Enable or disable smooth fades")
@app_commands.choices(mode=[
    app_commands.Choice(name="Enable 5s Fades", value="fade"),
    app_commands.Choice(name="Disable Fades (Standard)", value="off")
])
async def toggle_fade(interaction: discord.Interaction, mode: str):
    if not await is_dj(interaction): return
    await ensure_guild_settings(interaction.guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("UPDATE nexus_guild_settings SET transition_mode = %s WHERE guild_id = %s", (mode, interaction.guild.id))
    if mode == "fade": await interaction.response.send_message(embed=discord.Embed(description="🌊 Smooth **5-second Fades** have been enabled.", color=discord.Color.green()), ephemeral=True)
    else: await interaction.response.send_message(embed=discord.Embed(description="⏹️ Smooth Fades have been disabled.", color=discord.Color.red()), ephemeral=True)

@bot.tree.command(name="nexus_main_modify", description="Apply temporary custom speed and pitch modifiers to the next few tracks in the queue.")
@app_commands.describe(speed="Speed multiplier (0.5 to 2.0)", pitch="Pitch multiplier (0.5 to 2.0)", duration="How many tracks this lasts (default 1)")
async def modify_audio(interaction: discord.Interaction, speed: float = 1.0, pitch: float = 1.0, duration: int = 1):
    if not await is_dj(interaction): return
    await ensure_guild_settings(interaction.guild.id)
    speed = max(0.5, min(2.0, speed))
    pitch = max(0.5, min(2.0, pitch))
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT filter_mode FROM nexus_guild_settings WHERE guild_id = %s", (interaction.guild.id,))
                res = await cur.fetchone()
                if res and res[0] != 'none': return await interaction.response.send_message(embed=discord.Embed(description="❌ **Conflict:** Disable standard Filters via `/nexus_main_filter none` first.", color=discord.Color.red()), ephemeral=True)
                await cur.execute("UPDATE nexus_guild_settings SET custom_speed = %s, custom_pitch = %s, custom_modifiers_left = %s WHERE guild_id = %s", (speed, pitch, duration, interaction.guild.id))
    if interaction.guild.voice_client:
        wav_filters = wavelink.Filters()
        wav_filters.timescale.set(speed=speed, pitch=pitch)
        try: await interaction.guild.voice_client.set_filters(wav_filters)
        except Exception: pass
    await interaction.response.send_message(embed=discord.Embed(title="🎛️ Audio Modifiers Set", description=f"**Speed:** {speed}x\n**Pitch:** {pitch}x\n*Active for the next {duration} track(s).* ", color=discord.Color.gold()), ephemeral=True)

# --- SCRUBBING ---
@bot.tree.command(name="nexus_main_seek", description="Jump to an exact time in the current track using seconds from the start.")
async def seek(interaction: discord.Interaction, seconds: int):
    if not await is_dj(interaction): return
    if interaction.guild.id not in playback_tracking: return await interaction.response.send_message("Nothing playing.", ephemeral=True)
    if interaction.guild.voice_client:
        await interaction.guild.voice_client.seek(seconds * 1000)
        playback_tracking[interaction.guild.id]['offset'] = seconds
        playback_tracking[interaction.guild.id]['start_time'] = time.time()
        invalidate_position_persist(interaction.guild.id)
    await interaction.response.send_message(embed=discord.Embed(description=f"Seeked to {seconds}s", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_forward", description="Jump forward within the current track by the number of seconds you provide.")
async def forward(interaction: discord.Interaction, seconds: int):
    if not await is_dj(interaction): return
    if interaction.guild.id not in playback_tracking: return await interaction.response.send_message("Nothing playing.", ephemeral=True)
    data = playback_tracking[interaction.guild.id]
    current = current_track_position(interaction.guild.id)
    new_pos = current + seconds
    if interaction.guild.voice_client:
        await interaction.guild.voice_client.seek(new_pos * 1000)
        playback_tracking[interaction.guild.id]['offset'] = new_pos
        playback_tracking[interaction.guild.id]['start_time'] = time.time()
        invalidate_position_persist(interaction.guild.id)
    await interaction.response.send_message(embed=discord.Embed(description=f"Skipped forward {seconds}s", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_rewind", description="Jump backward within the current track by the number of seconds you provide.")
async def rewind(interaction: discord.Interaction, seconds: int):
    if not await is_dj(interaction): return
    if interaction.guild.id not in playback_tracking: return await interaction.response.send_message("Nothing playing.", ephemeral=True)
    data = playback_tracking[interaction.guild.id]
    current = current_track_position(interaction.guild.id)
    new_pos = max(0, current - seconds)
    if interaction.guild.voice_client:
        await interaction.guild.voice_client.seek(new_pos * 1000)
        playback_tracking[interaction.guild.id]['offset'] = new_pos
        playback_tracking[interaction.guild.id]['start_time'] = time.time()
        invalidate_position_persist(interaction.guild.id)
    await interaction.response.send_message(embed=discord.Embed(description=f"Rewound {seconds}s", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_replay", description="Restart the current track from the beginning without changing the queue.")
async def replay(interaction: discord.Interaction):
    if not await is_dj(interaction): return
    if interaction.guild.id not in playback_tracking: return await interaction.response.send_message("Nothing playing.", ephemeral=True)
    if interaction.guild.voice_client:
        await interaction.guild.voice_client.seek(0)
        playback_tracking[interaction.guild.id]['offset'] = 0
        playback_tracking[interaction.guild.id]['start_time'] = time.time()
        invalidate_position_persist(interaction.guild.id)
    await interaction.response.send_message(embed=discord.Embed(description="Replaying song.", color=discord.Color.green()), ephemeral=True)

# --- UTILITY & INFO ---
@bot.tree.command(name="nexus_main_panel", description="Post an interactive control panel with playback, queue, and transport buttons.")
async def panel(interaction: discord.Interaction):
    class AdvancedPanel(discord.ui.View):
        def __init__(self): super().__init__(timeout=None)
        @discord.ui.button(label="⏯️ Play/Pause", style=discord.ButtonStyle.primary, row=0)
        async def pr(self, i: discord.Interaction, b: discord.ui.Button):
            if not await is_dj(i): return
            vc = i.guild.voice_client
            if vc:
                if _player_is_playing(vc):
                    await vc.pause(True)
                    await sync_pause_state(i.guild.id, True)
                    await i.response.send_message("⏸️ Playback Paused", ephemeral=True)
                else:
                    await vc.pause(False)
                    await sync_pause_state(i.guild.id, False)
                    await i.response.send_message("▶️ Playback Resumed", ephemeral=True)
            else: await i.response.send_message("Nothing is playing.", ephemeral=True)
        @discord.ui.button(label="⏹️ Stop", style=discord.ButtonStyle.danger, row=0)
        async def st(self, i: discord.Interaction, b: discord.ui.Button):
            if not await is_dj(i): return
            snooze_auto_restore(i.guild.id)
            await stop_playback(i.guild)
            await i.response.send_message("⏹️ Stopped and cleared state", ephemeral=True)
        @discord.ui.button(label="⏭️ Skip", style=discord.ButtonStyle.secondary, row=0)
        async def sk(self, i: discord.Interaction, b: discord.ui.Button):
            if not await is_dj(i): return
            if i.guild.voice_client:
                await i.guild.voice_client.stop()
                await i.response.send_message("⏭️ Skipped to next track", ephemeral=True)
            else: await i.response.send_message("Nothing to skip.", ephemeral=True)
        @discord.ui.button(label="⏪ -10s", style=discord.ButtonStyle.secondary, row=1)
        async def rw(self, i: discord.Interaction, b: discord.ui.Button):
            if not await is_dj(i): return
            if i.guild.id not in playback_tracking: return await i.response.send_message("Nothing playing.", ephemeral=True)
            data = playback_tracking[i.guild.id]
            current = current_track_position(i.guild.id)
            new_pos = max(0, current - 10)
            if i.guild.voice_client:
                await i.guild.voice_client.seek(new_pos * 1000)
                playback_tracking[i.guild.id]['offset'] = new_pos
                playback_tracking[i.guild.id]['start_time'] = time.time()
                invalidate_position_persist(i.guild.id)
            await i.response.send_message("Rewound 10 seconds.", ephemeral=True)
        @discord.ui.button(label="⏩ +10s", style=discord.ButtonStyle.secondary, row=1)
        async def fw(self, i: discord.Interaction, b: discord.ui.Button):
            if not await is_dj(i): return
            if i.guild.id not in playback_tracking: return await i.response.send_message("Nothing playing.", ephemeral=True)
            data = playback_tracking[i.guild.id]
            current = current_track_position(i.guild.id)
            new_pos = current + 10
            if i.guild.voice_client:
                await i.guild.voice_client.seek(new_pos * 1000)
                playback_tracking[i.guild.id]['offset'] = new_pos
                playback_tracking[i.guild.id]['start_time'] = time.time()
                invalidate_position_persist(i.guild.id)
            await i.response.send_message("Skipped forward 10 seconds.", ephemeral=True)
        @discord.ui.button(label="🔀 Shuffle", style=discord.ButtonStyle.success, row=2)
        async def shuf(self, i: discord.Interaction, b: discord.ui.Button):
            if not await is_dj(i): return
            await i.response.defer(ephemeral=True)
            async with DBPoolManager() as pool:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("SELECT id, video_url, title, requester_id FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", (i.guild.id,))
                        q = await cur.fetchall()
                        if not q: return await i.followup.send("Queue empty.")
                        l = list(q); random.shuffle(l)
                        await cur.execute("DELETE FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", (i.guild.id,))
                        for row in l:
                            await enqueue_track(cur, i.guild.id, row[1], row[2], row[3], backup=False)
            await i.followup.send("🔀 Queue successfully shuffled!")
        @discord.ui.button(label="📜 View Queue", style=discord.ButtonStyle.secondary, row=2)
        async def vq(self, i: discord.Interaction, b: discord.ui.Button):
            await i.response.defer(ephemeral=True)
            async with DBPoolManager() as pool:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("SELECT title FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC LIMIT 10", (i.guild.id,))
                        songs = await cur.fetchall()
            if songs: await i.followup.send("**Current Queue:**\n" + "\n".join(f"{idx+1}. {s[0]}" for idx, s in enumerate(songs)))
            else: await i.followup.send("Queue is empty.")

    embed = discord.Embed(title="🎛️ NEXUS Music Control Panel", description="Manage your audio playback directly from these buttons.", color=discord.Color.from_rgb(43, 45, 49))
    embed.set_footer(text="NEXUS Main Music System")
    await interaction.response.send_message(embed=embed, view=AdvancedPanel())

@bot.tree.command(name="nexus_main_nowplaying", description="Show the current track, progress bar, requester, and live playback status.")
async def nowplaying(interaction: discord.Interaction):
    if interaction.guild.id in playback_tracking:
        data = playback_tracking[interaction.guild.id]
        cur_t = current_track_position(interaction.guild.id)
        dur = data.get('duration', 0)
        p_bar = make_progress_bar(cur_t, dur)
        embed = discord.Embed(title="🎵 Now Playing", description=f"**[{data.get('title', 'Playing')}]({data['url']})**\n\n`{p_bar}`", color=discord.Color.blue())
        requester_id = data.get('requester_id')
        if requester_id:
            requester_name = await resolve_requester_name(interaction.guild, requester_id)
            embed.add_field(name="Requested by", value=requester_name, inline=True)
        embed.add_field(name="Filter", value=str(data.get('current_filter', 'none')), inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=True)
    else:
        await interaction.response.send_message(embed=discord.Embed(description="Nothing playing.", color=discord.Color.red()), ephemeral=True)

@bot.tree.command(name="nexus_main_ping", description="Show the bot websocket latency so you can quickly check responsiveness.")
async def ping(interaction: discord.Interaction):
    latency = bot.latency
    latency_ms = 0 if not isinstance(latency, (int, float)) or latency != latency else round(latency * 1000)
    await interaction.response.send_message(embed=discord.Embed(description=f"🏓 Pong! {latency_ms}ms", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_uptime", description="Show how long this bot process has been running since its last startup.")
async def uptime(interaction: discord.Interaction):
    up = str(datetime.timedelta(seconds=int(time.time() - bot.start_time)))
    await interaction.response.send_message(embed=discord.Embed(description=f"⏱️ Uptime: {up}", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_stats", description="Show quick bot statistics such as guild count and active player count.")
async def stats(interaction: discord.Interaction):
    await interaction.response.send_message(embed=discord.Embed(description=f"📊 Servers: {len(bot.guilds)}\n🎧 Active Players: {len(playback_tracking)}", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_help", description="Show a categorized help menu for all NEXUS music commands and utilities")
async def help_cmd(interaction: discord.Interaction):
    cmds = [c.name for c in bot.tree.get_commands() if c.name.startswith("nexus_main_")]
    await interaction.response.send_message(embed=discord.Embed(title="📚 Command List", description=", ".join(cmds), color=discord.Color.blue()), ephemeral=True)

@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: discord.app_commands.AppCommandError):
    if isinstance(error, app_commands.CheckFailure):
        error_msg = str(error) if str(error) else "You don't have permission to use this command."
        if not interaction.response.is_done(): await interaction.response.send_message(error_msg, ephemeral=True)
        else: await interaction.followup.send(error_msg, ephemeral=True)
        return
    logger.error(f"Command {getattr(interaction.command, 'name', 'unknown')} failed: {error}", exc_info=True)
    error_msg = f"An error occurred: `{error}`"
    if not interaction.response.is_done(): await interaction.response.send_message(error_msg, ephemeral=True)
    else: await interaction.followup.send(error_msg, ephemeral=True)

# --- LIVE PLAYLIST SYNC FEATURE ---
async def init_playlist_db():
    global playlist_db_initialized
    if playlist_db_initialized:
        return
    async with playlist_db_lock:
        if playlist_db_initialized:
            return
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute('''CREATE TABLE IF NOT EXISTS nexus_active_playlists (guild_id BIGINT, bot_name VARCHAR(50), playlist_url TEXT, known_track_count INT DEFAULT 0, requester_id BIGINT, channel_id BIGINT DEFAULT NULL, PRIMARY KEY (guild_id, bot_name))''')
                    try: await cur.execute("ALTER TABLE nexus_active_playlists ADD COLUMN channel_id BIGINT DEFAULT NULL")
                    except: pass
                    try: await cur.execute("CREATE INDEX nexus_playlist_bot_idx ON nexus_active_playlists (bot_name, guild_id)")
                    except: pass
                    try: await cur.execute("ALTER TABLE nexus_playback_state ADD COLUMN bot_name VARCHAR(50) DEFAULT 'nexus'")
                    except: pass
                    try: await cur.execute("ALTER TABLE nexus_active_playlists ADD COLUMN bot_name VARCHAR(50) DEFAULT 'nexus'")
                    except: pass
                    try: await cur.execute("ALTER TABLE nexus_bot_home_channels ADD COLUMN bot_name VARCHAR(50) DEFAULT 'nexus'")
                    except: pass
        playlist_db_initialized = True

def resolve_playlist_source(search, playlist=None):
    candidates = [getattr(playlist, 'url', None), getattr(playlist, 'uri', None), search]
    for candidate in candidates:
        if isinstance(candidate, str):
            cleaned = candidate.strip()
            if cleaned.startswith("http://") or cleaned.startswith("https://"):
                return cleaned
    return None

def unwrap_search_results(results):
    if isinstance(results, wavelink.Playlist):
        return [track for track in getattr(results, 'tracks', []) if track], results
    if results is None or isinstance(results, (str, bytes, dict)):
        return [], None
    if isinstance(results, (list, tuple)):
        return [track for track in results if track], None

    tracks_attr = getattr(results, 'tracks', None)
    if isinstance(tracks_attr, (list, tuple)):
        entries = [track for track in tracks_attr if track]
        playlist_like = results if getattr(results, 'url', None) or getattr(results, 'uri', None) else None
        return entries, playlist_like

    try:
        iterator = iter(results)
    except TypeError:
        return ([results] if results else []), None

    entries = [track for track in iterator if track]
    playlist_like = results if getattr(results, 'url', None) or getattr(results, 'uri', None) else None
    return entries, playlist_like

def _is_explicit_lavalink_query(value):
    text = str(value or "").strip().lower()
    if text.startswith(("http://", "https://")):
        return True
    return bool(re.match(r"^[a-z0-9_]+search:", text))

async def search_playables(query):
    cleaned = str(query or "").strip()
    if not cleaned:
        return [], None
    if not _is_explicit_lavalink_query(cleaned):
        cleaned = f"ytmsearch:{cleaned}"
    if not await ensure_lavalink_ready():
        raise RuntimeError("Lavalink is still starting up. Try again in a few seconds.")
    results = await wavelink.Playable.search(cleaned)
    return unwrap_search_results(results)

def _is_direct_media_url(value):
    text = str(value or "").strip().lower()
    return text.startswith("http://") or text.startswith("https://")

def _build_track_title_search(title):
    cleaned = re.sub(r"\s+", " ", str(title or "").strip())
    return f"ytmsearch:{cleaned}" if cleaned else None

async def _expand_media_url(url):
    candidate = str(url or "").strip()
    if not _is_direct_media_url(candidate):
        return candidate
    if not any(host in candidate for host in ("on.soundcloud.com", "soundcloud.app.goo.gl", "youtu.be", "music.youtube.com")):
        return candidate
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with HTTPSessionManager() as session:
            async with session.get(candidate, allow_redirects=True, timeout=timeout) as response:
                return str(response.url)
    except Exception:
        return candidate

async def resolve_queue_track(source, fallback_title=None):
    candidate = await _expand_media_url(source)
    attempts = [candidate]
    title_search = _build_track_title_search(fallback_title)
    if title_search and _is_direct_media_url(candidate):
        attempts.append(title_search)

    last_error = None
    for attempt in attempts:
        try:
            entries, _playlist_result = await search_playables(attempt)
            if entries:
                return entries[0], attempt
            last_error = ValueError("No stream found.")
        except Exception as exc:
            last_error = exc

    if last_error:
        raise last_error
    raise ValueError("No stream found.")

def _is_stale_lavalink_player_error(error):
    text = str(error or "").lower()
    return (
        "failed to fulfill request to lavalink" in text
        and "status=404" in text
        and "/players/" in text
    )

async def set_active_playlist(guild_id, playlist_url, known_track_count, requester_id, channel_id):
    if not playlist_url:
        return
    await init_playlist_db()
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("REPLACE INTO nexus_active_playlists (guild_id, bot_name, playlist_url, known_track_count, requester_id, channel_id) VALUES (%s, 'nexus', %s, %s, %s, %s)", (guild_id, playlist_url, known_track_count, requester_id, channel_id))

async def clear_active_playlist(guild_id):
    await init_playlist_db()
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("DELETE FROM nexus_active_playlists WHERE guild_id = %s AND bot_name = 'nexus'", (guild_id,))

@tasks.loop(seconds=PLAYLIST_SYNC_INTERVAL)
async def playlist_sync_loop():
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await init_playlist_db()
                await cur.execute("SELECT guild_id, playlist_url, known_track_count, requester_id, channel_id FROM nexus_active_playlists WHERE bot_name = 'nexus'")
                playlists = await cur.fetchall()

    if not playlists: return

    opts = ytdl_format_options.copy()
    opts['extract_flat'] = True
    opts['playlistend'] = None  # FIX 4: Remove yt-dlp's default ~150-track pagination cap
    ydl = yt_dlp.YoutubeDL(opts)
    loop = asyncio.get_running_loop()

    for guild_id, url, known_count, req_id, channel_id in playlists:
        try:
            data = await loop.run_in_executor(None, lambda playlist_url=url: ydl.extract_info(playlist_url, download=False))
            if not data or 'entries' not in data: continue

            entries = [e for e in data['entries'] if e is not None]
            current_count = len(entries)

            if current_count > known_count:
                new_tracks = entries[known_count:]
                added_count = 0
                async with DBPoolManager() as pool:
                    async with pool.acquire() as conn:
                        async with conn.cursor() as cur:
                            for entry in new_tracks:
                                t_title = entry.get('title', 'Unknown Track')
                                t_url = entry.get('url') or entry.get('webpage_url')
                                if t_url and not t_url.startswith('http'): t_url = f"https://www.youtube.com/watch?v={entry.get('id')}"
                                if t_url:
                                    await enqueue_track(cur, guild_id, t_url, t_title, req_id)
                                    added_count += 1
                            await cur.execute("UPDATE nexus_active_playlists SET known_track_count = %s WHERE guild_id = %s AND bot_name = 'nexus'", (current_count, guild_id))

                guild = bot.get_guild(guild_id)
                if guild:
                    vc = guild.voice_client
                    if added_count > 0 and (not vc or (not _player_is_active(vc))):
                        target_channel = vc.channel if vc and getattr(vc, 'channel', None) else guild.get_channel(channel_id) if channel_id else await get_home_channel(guild)
                        if target_channel:
                            schedule_named_task(f"playlist_sync_process_queue:{guild.id}", process_queue(guild, target_channel.id))
                    embed = discord.Embed(title="📡 Playlist Updated", description=f"Detected **{current_count - known_count}** new tracks added to the monitored playlist! Auto-queued them.", color=discord.Color.green())
                    await send_or_update_status_message(guild, embed)
                    await send_webhook_log(bot.user.name if bot.user else "Unknown Node", "📡 Playlist Sync", f"Detected **{current_count - known_count}** new tracks for guild {guild.name} and queued them automatically.", discord.Color.green())
        except Exception as e:
            logger.error(f"Sync Loop Error: {e}")

@bot.event
async def on_ready_sync():
    await init_playlist_db()
    if not playlist_sync_loop.is_running(): playlist_sync_loop.start()
bot.add_listener(on_ready_sync, 'on_ready')

# --- ARIA SWARM OVERRIDE LISTENER ---
@tasks.loop(seconds=2.0)
async def aria_command_listener():
    try:
        await ensure_swarm_command_tables()
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute("SELECT guild_id, command, COALESCE(attempts, 0) AS attempts FROM nexus_swarm_overrides WHERE bot_name = %s", ('nexus',))
                    commands = await cur.fetchall()

        if not commands: return

        for row in commands:
            guild_id = row['guild_id']
            cmd = str(row['command'] or '').upper()
            attempts = int(row.get('attempts', 0) or 0)

            if cmd == 'RESTART':
                async with DBPoolManager() as pool:
                    async with pool.acquire() as conn:
                        async with conn.cursor() as cur:
                            await cur.execute("DELETE FROM nexus_swarm_overrides WHERE guild_id = %s AND bot_name = %s", (guild_id, 'nexus'))
                await request_supervisor_restart("aria_override")
                continue  # Bot is restarting; skip remaining processing for this row

            guild = bot.get_guild(guild_id)
            vc = guild.voice_client if guild else None
            executed = False

            if guild:
                if cmd == 'PAUSE' and vc and _player_is_playing(vc):
                    await vc.pause(True); await sync_pause_state(guild_id, True); executed = True
                elif cmd == 'RESUME' and vc and _player_is_paused(vc):
                    await vc.pause(False); await sync_pause_state(guild_id, False); executed = True
                elif cmd == 'SKIP' and vc:
                    await vc.stop(); executed = True
                elif cmd == 'STOP':
                    snooze_auto_restore(guild_id)
                    await clear_active_playlist(guild_id)
                    await stop_playback(guild)
                    executed = True
                elif cmd == 'UPDATE_FILTER':
                    if vc:
                        async with DBPoolManager() as _pool:
                            async with _pool.acquire() as _conn:
                                async with _conn.cursor() as _cur:
                                    await _cur.execute("SELECT filter_mode FROM nexus_guild_settings WHERE guild_id = %s", (guild_id,))
                                    res = await _cur.fetchone()
                                    if res:
                                        f_mode = res[0]
                                        wav_filters = wavelink.Filters()
                                        if f_mode == 'nightcore': wav_filters.timescale.set(speed=1.25, pitch=1.3)
                                        elif f_mode == 'vaporwave': wav_filters.timescale.set(speed=0.8, pitch=0.8)
                                        elif f_mode == 'bassboost': wav_filters.equalizer.set(bands=[(0, 0.3), (1, 0.2), (2, 0.1)])
                                        try: await vc.set_filters(wav_filters)
                                        except Exception: pass
                    executed = True

                if executed:
                    try: await send_webhook_log(bot.user.name if bot.user else "Unknown Node", "🤖 Aria Override", f"Aria forcefully executed a **{cmd}** command in `{guild.name}`.", discord.Color.purple())
                    except Exception:
                        logger.exception("[nexus] Failed sending Aria override webhook for guild %s.", guild_id)
                else:
                    logger.info("[nexus] Ignored Aria override %s for guild %s because the player state did not match.", cmd, guild_id)
            else:
                logger.warning("[nexus] Received Aria override %s for unknown guild %s.", cmd, guild_id)

            async with DBPoolManager() as pool:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        if executed or attempts >= 2 or not guild:
                            await cur.execute("DELETE FROM nexus_swarm_overrides WHERE guild_id = %s AND bot_name = %s", (guild_id, 'nexus'))
                        else:
                            await cur.execute("UPDATE nexus_swarm_overrides SET attempts = COALESCE(attempts, 0) + 1, last_error = %s WHERE guild_id = %s AND bot_name = %s", (f"state_mismatch:{cmd}", guild_id, 'nexus'))
    except Exception:
        logger.exception("Aria override listener failed for nexus.")

@bot.event
async def on_ready_aria_listener():
    if not aria_command_listener.is_running(): aria_command_listener.start()
bot.add_listener(on_ready_aria_listener, 'on_ready')


async def ensure_swarm_command_tables():
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("CREATE TABLE IF NOT EXISTS nexus_swarm_overrides (guild_id BIGINT, bot_name VARCHAR(50), command VARCHAR(20), attempts INT NOT NULL DEFAULT 0, last_error TEXT NULL, PRIMARY KEY(guild_id, bot_name))")
                    await cur.execute("CREATE TABLE IF NOT EXISTS nexus_swarm_direct_orders (id INT AUTO_INCREMENT PRIMARY KEY, bot_name VARCHAR(50), guild_id BIGINT, vc_id BIGINT NULL, text_channel_id BIGINT NULL, command VARCHAR(50), data TEXT NULL, attempts INT NOT NULL DEFAULT 0, last_error TEXT NULL)")
                    for stmt in [
                        "ALTER TABLE nexus_swarm_overrides ADD COLUMN bot_name VARCHAR(50) DEFAULT 'nexus'",
                        "ALTER TABLE nexus_swarm_overrides ADD COLUMN command VARCHAR(20) NULL",
                        "ALTER TABLE nexus_swarm_direct_orders ADD COLUMN vc_id BIGINT NULL",
                        "ALTER TABLE nexus_swarm_direct_orders ADD COLUMN text_channel_id BIGINT NULL",
                        "ALTER TABLE nexus_swarm_direct_orders ADD COLUMN command VARCHAR(50) NULL",
                        "ALTER TABLE nexus_swarm_direct_orders ADD COLUMN data TEXT NULL",
                        "ALTER TABLE nexus_swarm_overrides ADD COLUMN attempts INT NOT NULL DEFAULT 0",
                        "ALTER TABLE nexus_swarm_overrides ADD COLUMN last_error TEXT NULL",
                        "ALTER TABLE nexus_swarm_direct_orders ADD COLUMN attempts INT NOT NULL DEFAULT 0",
                        "ALTER TABLE nexus_swarm_direct_orders ADD COLUMN last_error TEXT NULL",
                    ]:
                        try:
                            await cur.execute(stmt)
                        except Exception:
                            pass
    except Exception:
        logger.exception("Failed ensuring swarm command tables for nexus.")

# --- AUTOMATED BACKGROUND MAINTENANCE ---
@tasks.loop(seconds=15.0)
async def resilience_loop():
    now = time.time()
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    for guild in bot.guilds:
                        vc = guild.voice_client
                        is_active = bool(vc and (_player_is_active(vc)))

                        await cur.execute("SELECT COUNT(*) FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", (guild.id,))
                        queue_count_row = await cur.fetchone()
                        queue_count = queue_count_row[0] if queue_count_row else 0

                        if queue_count > 0:
                            clear_idle_restore_state(guild.id)

                        if is_active or queue_count > 0:
                            continue

                        if now < auto_restore_snooze_until.get(guild.id, 0):
                            continue

                        await cur.execute("SELECT home_vc_id FROM nexus_bot_home_channels WHERE guild_id = %s AND bot_name = 'nexus'", (guild.id,))
                        home_row = await cur.fetchone()
                        home_vc_id = home_row[0] if home_row and home_row[0] else None
                        current_channel_id = vc.channel.id if vc and getattr(vc, 'channel', None) else None

                        remembered_channel_id = guild_states.get(guild.id, {}).get("voice_channel_id")
                        preferred_channel_id = home_vc_id or remembered_channel_id
                        target_channel_id = preferred_channel_id or current_channel_id

                        if target_channel_id:
                            idle_voice_since.setdefault(guild.id, now)
                        else:
                            clear_idle_restore_state(guild.id)

                        long_idle = bool(target_channel_id) and now - idle_voice_since.get(guild.id, now) >= AUTO_IMPORT_IDLE_SECONDS
                        disconnected_from_target = bool(preferred_channel_id) and not current_channel_id

                        if not (long_idle or disconnected_from_target):
                            continue

                        await cur.execute(
                            "SELECT is_playing, is_paused, position_seconds, video_url, title FROM nexus_playback_state WHERE guild_id = %s AND bot_name = 'nexus' LIMIT 1",
                            (guild.id,),
                        )
                        playback_row = await cur.fetchone()
                        has_recovery_playback = bool(
                            playback_row
                            and (bool(playback_row[0]) or bool(playback_row[1]) or int(playback_row[2] or 0) > 0)
                            and (playback_row[3] or playback_row[4])
                        )
                        if not has_recovery_playback:
                            await cur.execute("DELETE FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus'", (guild.id,))
                            clear_idle_restore_state(guild.id)
                            continue

                        restored = await restore_queue_from_backup(cur, guild.id)
                        if restored <= 0 or not target_channel_id:
                            continue

                        await remember_recovery_state(guild.id, target_channel_id, 0)
                        clear_idle_restore_state(guild.id)
                        schedule_recovery_retry(guild.id, target_channel_id, start_position=0, reason="idle_restore")
                        logger.info(f"[{guild.id}] Restored {restored} backup tracks after idle/home recovery.")
                        schedule_named_task(f"idle_restore_process_queue:{guild.id}", process_queue(guild, target_channel_id, allow_recovery_restore=True))
    except Exception as e:
        logger.error(f"Resilience Loop Error: {e}")

@tasks.loop(minutes=5.0)
async def zombie_reaper_loop():
    for guild in bot.guilds:
        vc = guild.voice_client
        if vc and not _player_is_active(vc):
            try:
                async with DBPoolManager() as pool:
                    async with pool.acquire() as conn:
                        async with conn.cursor() as cur:
                            await cur.execute("SELECT COUNT(*) FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", (guild.id,))
                            res = await cur.fetchone()
                            if res and res[0] == 0:
                                await cur.execute("SELECT COUNT(*) FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus'", (guild.id,))
                                backup_row = await cur.fetchone()
                                backup_count = backup_row[0] if backup_row else 0
                                if backup_count > 0:
                                    await cur.execute(
                                        "SELECT is_playing, is_paused, position_seconds, video_url, title FROM nexus_playback_state WHERE guild_id = %s AND bot_name = 'nexus' LIMIT 1",
                                        (guild.id,),
                                    )
                                    playback_row = await cur.fetchone()
                                    has_recovery_playback = bool(
                                        playback_row
                                        and (bool(playback_row[0]) or bool(playback_row[1]) or int(playback_row[2] or 0) > 0)
                                        and (playback_row[3] or playback_row[4])
                                    )
                                    recovery_channel_id = getattr(getattr(vc, "channel", None), "id", None) or guild_states.get(guild.id, {}).get("voice_channel_id")
                                    if has_recovery_playback and recovery_channel_id:
                                        await remember_recovery_state(guild.id, recovery_channel_id, 0)
                                        restored = await restore_queue_from_backup(cur, guild.id)
                                        if restored > 0:
                                            clear_idle_restore_state(guild.id)
                                            schedule_recovery_retry(guild.id, recovery_channel_id, start_position=0, reason="zombie_restore")
                                            schedule_named_task(f"zombie_restore_process_queue:{guild.id}", process_queue(guild, recovery_channel_id, allow_recovery_restore=True))
                                    else:
                                        await cur.execute("DELETE FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus'", (guild.id,))
                                    playback_tracking.pop(guild.id, None)
                                    await cur.execute("UPDATE nexus_playback_state SET channel_id = NULL, video_url = NULL, title = NULL, position_seconds = 0, is_playing = FALSE, is_paused = FALSE WHERE guild_id = %s AND bot_name = 'nexus'", (guild.id,))
                                    continue
                                await cur.execute("SELECT stay_in_vc FROM nexus_guild_settings WHERE guild_id = %s", (guild.id,))
                                cfg = await cur.fetchone()
                                stay_in_vc = bool(cfg[0]) if cfg else False
                                if _should_auto_disconnect(guild, stay_in_vc):
                                    await stop_playback(guild)
                                else:
                                    playback_tracking.pop(guild.id, None)
                                    await cur.execute("UPDATE nexus_playback_state SET channel_id = NULL, video_url = NULL, title = NULL, position_seconds = 0, is_playing = FALSE, is_paused = FALSE WHERE guild_id = %s AND bot_name = 'nexus'", (guild.id,))
            except Exception:
                logger.exception("[nexus] Zombie reaper failed for guild %s.", guild.id)

@tasks.loop(hours=24.0)
async def database_janitor_loop():
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("DELETE FROM nexus_history WHERE played_at < NOW() - INTERVAL 30 DAY")
                    deleted_rows = cur.rowcount
                    if deleted_rows > 0:
                        logger.info(f"🧹 Janitor cleared {deleted_rows} old history records.")
                        await send_webhook_log(bot.user.name if bot.user else "Unknown Node", "🧹 Database Janitor", f"Successfully cleared **{deleted_rows}** old song history records to optimize database speed.", discord.Color.blurple())
    except Exception as e:
        logger.error(f"Janitor Error: {e}")

@tasks.loop(hours=PERIODIC_RESTART_HOURS)
async def periodic_restart_loop():
    await request_supervisor_restart(f"periodic_{PERIODIC_RESTART_HOURS:g}h_cycle")

@periodic_restart_loop.before_loop
async def before_periodic_restart_loop():
    await bot.wait_until_ready()
    await asyncio.sleep(PERIODIC_RESTART_HOURS * 60 * 60)

@bot.event
async def on_ready_maintenance():
    if not resilience_loop.is_running(): resilience_loop.start()
    if not zombie_reaper_loop.is_running(): zombie_reaper_loop.start()
    if not database_janitor_loop.is_running(): database_janitor_loop.start()
    if not periodic_restart_loop.is_running(): periodic_restart_loop.start()
bot.add_listener(on_ready_maintenance, 'on_ready')

# --- ARIA DIRECT DRONE CONTROL ---
@tasks.loop(seconds=2.0)
async def direct_order_listener():
    try:
        await ensure_swarm_command_tables()
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute("SELECT id, bot_name, guild_id, vc_id, text_channel_id, command, data, COALESCE(attempts, 0) AS attempts FROM nexus_swarm_direct_orders WHERE bot_name = %s ORDER BY id ASC", ('nexus',))
                    orders = await cur.fetchall()

        if not orders: return

        for order in orders:
            oid = order['id']
            guild = bot.get_guild(order['guild_id'])
            cmd = str(order['command'] or '').upper()
            data = order['data']
            attempts = int(order.get('attempts', 0) or 0)
            executed = False

            if guild:
                vc_target = guild.get_channel(order['vc_id']) if order.get('vc_id') is not None and order['vc_id'] else None

                if cmd == 'PLAY':
                    if not vc_target:
                        vc_target = await get_home_channel(guild)

                    if vc_target:
                        await ensure_voice_connection(guild, vc_target.id)
                        try:
                            entries, playlist_result = await search_playables(data)
                            if entries:
                                is_playlist_request = bool(playlist_result) or (isinstance(data, str) and 'list=' in data and len(entries) > 1)
                                playlist_url = resolve_playlist_source(data, playlist_result) if is_playlist_request else None
                                added_count = 0
                                async with DBPoolManager() as pool:
                                    async with pool.acquire() as conn:
                                        async with conn.cursor() as cur:
                                            for track in entries:
                                                await enqueue_track(cur, guild.id, track.uri, track.title, bot.user.id)
                                                added_count += 1
                                if playlist_url:
                                    await set_active_playlist(guild.id, playlist_url, len(entries), bot.user.id if bot.user else None, vc_target.id)

                                if added_count > 0:
                                    executed = True
                                    try:
                                        await send_feedback(guild, discord.Embed(title="🎶 Direct Order Received", description=f"Aria successfully deposited **{added_count}** tracks into my matrix. Booting audio engine...", color=discord.Color.green()))
                                        await send_webhook_log(bot.user.name if bot.user else "Unknown Node", "📥 Matrix Loaded", f"Aria routed a payload of **{added_count}** tracks directly into `{guild.name}`.", discord.Color.blue())
                                    except Exception:
                                        pass
                        except Exception as e:
                            logger.error(f"Direct Play Extractor Error: {e}")

                        if executed and guild.voice_client and not _player_is_active(guild.voice_client):
                            schedule_named_task(f"direct_play_process_queue:{guild.id}", process_queue(guild, vc_target.id))
                    else:
                        logger.warning("[nexus] Dropped direct PLAY order %s for guild %s because no voice channel was resolved.", oid, guild.id)

                elif cmd == 'PAUSE':
                    vc = guild.voice_client
                    if vc and _player_is_playing(vc):
                        await vc.pause(True)
                        await sync_pause_state(guild.id, True)
                        executed = True
                    else:
                        logger.info(f"[{guild.id}] Direct PAUSE ignored because nothing is currently playing.")

                elif cmd == 'RESUME':
                    vc = guild.voice_client
                    if vc and _player_is_paused(vc):
                        await vc.pause(False)
                        await sync_pause_state(guild.id, False)
                        executed = True
                    else:
                        logger.info(f"[{guild.id}] Direct RESUME ignored because the player is not paused.")

                elif cmd == 'SKIP':
                    vc = guild.voice_client
                    if vc and (_player_is_active(vc)):
                        await vc.stop()
                        executed = True
                    else:
                        logger.info(f"[{guild.id}] Direct SKIP ignored because no active player exists.")

                elif cmd == 'STOP':
                    snooze_auto_restore(guild.id)
                    await clear_active_playlist(guild.id)
                    await stop_playback(guild)
                    executed = True

                elif cmd == 'RECOVER':
                    force_recover = isinstance(data, str) and data.strip().lower() in {'force', 'override', 'admin'}
                    remaining_backoff = recovery_backoff_remaining(guild.id)
                    if remaining_backoff > 0 and not force_recover:
                        logger.info("[%s] Dropped RECOVER order for guild %s because recovery is paused for another %ss.", BOT_ENV_PREFIX.lower(), guild.id, remaining_backoff)
                        executed = True
                    else:
                        state = await derive_recovery_state_from_db(guild.id)
                        recover_channel_id = order.get('vc_id') or (state or {}).get('voice_channel_id')
                        if recover_channel_id:
                            recover_state = dict(state or {})
                            recover_state['voice_channel_id'] = recover_channel_id
                            recover_state['position'] = int((state or {}).get('position', 0) or 0)
                            if force_recover:
                                clear_recovery_backoff(guild.id)
                                clear_auto_restore_snooze(guild.id)
                            await restore_guild_state(guild.id, recover_state, override_backoff=force_recover)
                            executed = True
                        else:
                            logger.warning("[%s] Could not resolve RECOVER target for guild %s.", BOT_ENV_PREFIX.lower(), guild.id)

                elif cmd == 'LEAVE':
                    force_leave = isinstance(data, str) and data.strip().lower() in {'force', 'override', 'admin'}
                    if guild.voice_client and _has_human_listeners(guild.voice_client) and not force_leave:
                        logger.info(f"[{guild.id}] Ignoring non-forced LEAVE order while human listeners are present.")
                    else:
                        snooze_auto_restore(guild.id)
                        await clear_active_playlist(guild.id)
                        await stop_playback(guild)
                        executed = True

                else:
                    logger.warning(f"[nexus] Unsupported direct order {cmd!r} for guild {guild.id}.")

                if executed:
                    try:
                        details = f" command=`{cmd}`"
                        if cmd == 'RECOVER' and data:
                            details += f" | payload={str(data)[:160]}"
                        await send_webhook_log(bot.user.name if bot.user else "Unknown Node", "🤖 Direct Drone Execution", f"Received and executed direct `{cmd}` order from Aria in `{guild.name}`.{details}", discord.Color.purple())
                    except Exception:
                        logger.exception("[nexus] Failed sending direct order webhook for guild %s.", guild.id)
                else:
                    logger.info("[nexus] Direct order %s in guild %s completed without state changes.", cmd, guild.id)
            else:
                logger.warning("[nexus] Received direct order %s for unknown guild %s.", cmd, order['guild_id'])

            async with DBPoolManager() as pool:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        if executed or attempts >= 2 or not guild:
                            await cur.execute("DELETE FROM nexus_swarm_direct_orders WHERE id = %s", (oid,))
                        else:
                            await cur.execute("UPDATE nexus_swarm_direct_orders SET attempts = COALESCE(attempts, 0) + 1, last_error = %s WHERE id = %s", (f"unexecuted:{cmd}", oid))

    except Exception:
        logger.exception("Direct order listener failed for nexus.")

@bot.event
async def on_ready_direct_order():
    if not direct_order_listener.is_running(): direct_order_listener.start()
bot.add_listener(on_ready_direct_order, 'on_ready')

# --- SWARM INTELLIGENCE MODULE ---
class SwarmIntelligence(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.bot_name = 'nexus'
        self.status_updater.start()
        self.heartbeat.start()
        self.watchdog.start()

    def cog_unload(self):
        self.status_updater.cancel()
        self.heartbeat.cancel()
        self.watchdog.cancel()

    async def _best_presence_title(self):
        """Choose live track title for bot status without falling back to idle too early."""
        reconnect_candidate = None
        for guild in self.bot.guilds:
            vc = guild.voice_client
            connected = _voice_client_connected(vc)
            tracked = playback_tracking.get(guild.id) or {}
            if connected:
                title = tracked.get("title") or _track_title_from_obj(_player_current_track(vc))
                if title:
                    return str(title).replace("\n", " ").strip(), guild.name
                try:
                    async with DBPoolManager() as pool:
                        async with pool.acquire() as conn:
                            async with conn.cursor(aiomysql.DictCursor) as cur:
                                await cur.execute(
                                    f"SELECT title, is_playing, is_paused FROM {self.bot_name}_playback_state WHERE guild_id = %s AND bot_name = %s AND (is_playing = TRUE OR is_paused = TRUE) ORDER BY is_playing DESC LIMIT 1",
                                    (guild.id, self.bot_name),
                                )
                                row = await cur.fetchone()
                                if row and row.get("title"):
                                    return str(row["title"]).replace("\n", " ").strip(), guild.name
                except Exception as exc:
                    logger.debug("[%s] Presence DB lookup failed for guild %s: %s", self.bot_name, guild.id, exc)
            elif tracked.get("title"):
                try:
                    started = float(tracked.get("start_time") or 0)
                except Exception:
                    started = 0
                if time.time() - started < 120:
                    reconnect_candidate = (str(tracked["title"]).replace("\n", " ").strip(), guild.name)
        return reconnect_candidate or (None, None)

    @tasks.loop(seconds=15)
    async def status_updater(self):
        try:
            title, guild_name = await self._best_presence_title()
            if title:
                await self.bot.change_presence(status=discord.Status.online, activity=discord.Activity(type=discord.ActivityType.listening, name=str(title).replace("\n", " ").strip()[:120]))
                return
            await self.bot.change_presence(status=discord.Status.online, activity=discord.Activity(type=discord.ActivityType.watching, name="the Swarm | Idle"))
        except (aiohttp.ClientConnectionResetError, ConnectionResetError):
            logger.debug("[%s] Presence update skipped while the gateway transport was closing.", self.bot_name)
        except Exception:
            logger.exception("[%s] Status updater failed.", self.bot_name)

    @tasks.loop(seconds=30)
    async def heartbeat(self):
        try:
            async with DBPoolManager() as pool:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("INSERT INTO swarm_health (bot_name, status, last_pulse) VALUES (%s, 'HEALTHY', NOW()) ON DUPLICATE KEY UPDATE status=VALUES(status), last_pulse=NOW()", (self.bot_name,))
        except Exception:
            logger.exception("[nexus] Heartbeat update failed.")

    @tasks.loop(seconds=15)
    async def watchdog(self):
        try:
            for guild in self.bot.guilds:
                if recovery_backoff_remaining(guild.id) > 0:
                    continue
                # FIX 3: Never let the watchdog interfere with an in-progress recovery
                if guild.id in recovering_guilds:
                    continue
                vc = guild.voice_client
                if vc and not _player_is_active(vc):
                    if guild.id in playback_tracking:
                        track_info = playback_tracking[guild.id]
                        now = time.time()
                        if now - track_info.get('start_time', 0) > 10:
                            if now - track_info.get('last_watchdog_revival', 0) < WATCHDOG_REVIVAL_COOLDOWN: continue
                            async with DBPoolManager() as pool:
                                async with pool.acquire() as conn:
                                    async with conn.cursor() as cur:
                                        revival_attempts = track_info.get('watchdog_revival_attempts', 0)
                                        if revival_attempts >= WATCHDOG_MAX_REVIVALS:
                                            playback_tracking.pop(guild.id, None)
                                            await cur.execute(f"UPDATE {self.bot_name}_playback_state SET is_playing = FALSE WHERE guild_id = %s AND bot_name = %s", (guild.id, self.bot_name))
                                            await send_webhook_log(self.bot.user.name if self.bot.user else "Unknown Node", "⚙️ Watchdog Cooldown", f"Stall persisted in `{guild.name}`; watchdog parked to prevent revival loop.", discord.Color.orange())
                                            continue
                                        current_pos = int((now - track_info.get('start_time', now)) * track_info.get('speed', 1.0) + track_info.get('offset', 0))
                                        await requeue_failed_track(
                                            cur,
                                            guild.id,
                                            track_info.get('channel_id'),
                                            track_info.get('url', ''),
                                            track_info.get('title', 'Recovered Track'),
                                            track_info.get('requester_id', self.bot.user.id if self.bot.user else None),
                                            position=current_pos,
                                            reason="watchdog_stall",
                                        )
                                        track_info['watchdog_revival_attempts'] = revival_attempts + 1
                                        track_info['last_watchdog_revival'] = now
                                        track_info['start_time'] = now
                                        track_info['offset'] = current_pos
                                        await send_webhook_log(self.bot.user.name if self.bot.user else "Unknown Node", "⚙️ Watchdog Revival", f"Detected playback stall in `{guild.name}`. Recovering track safely at {current_pos}s.", discord.Color.orange())
                                        schedule_named_task(f"watchdog_recovery_process_queue:{guild.id}", process_queue(guild, track_info.get('channel_id'), start_position=current_pos))
        except Exception:
            logger.exception("[nexus] Watchdog tick failed.")

    @status_updater.before_loop
    @heartbeat.before_loop
    @watchdog.before_loop
    async def before_loops(self):
        await self.bot.wait_until_ready()

async def setup_intelligence(bot):
    await bot.add_cog(SwarmIntelligence(bot))

@bot.event
async def on_ready_intelligence():
    if not bot.get_cog("SwarmIntelligence"): await setup_intelligence(bot)
bot.add_listener(on_ready_intelligence, 'on_ready')

@bot.event
async def on_ready_auto_heal():
    if not auto_heal_loop.is_running():
        auto_heal_loop.start()
bot.add_listener(on_ready_auto_heal, 'on_ready')

@bot.tree.interaction_check
async def global_proximity_shield(interaction: discord.Interaction):
    admin_only = ['sethome', 'setfeedback', 'djrole', 'removedj', 'djmode', '247', 'restart']
    protected = ['play', 'stop', 'pause', 'resume', 'skip', 'join', 'leave', 'playnext', 'shuffle', 'clear', 'skipto', 'move', 'remove', 'seek', 'forward', 'rewind', 'replay']
    if not interaction.command: return True
    if any(interaction.command.name.endswith(s) for s in admin_only) and not interaction.user.guild_permissions.administrator:
        raise discord.app_commands.CheckFailure("You need administrator permission to use this command.")
    if not any(interaction.command.name.endswith(s) for s in protected): return True
    vc = interaction.guild.voice_client if interaction.guild else None
    if not vc: return True
    if getattr(vc, 'channel', None) and (not interaction.user.voice or interaction.user.voice.channel != vc.channel):
        raise discord.app_commands.AppCommandError("You must be in the active voice channel to issue commands.")
    return True

# --- BOT RUN TRIGGER ---
def validate_runtime_config():
    required = {
        f"{BOT_ENV_PREFIX}_DISCORD_TOKEN": TOKEN,
        f"{BOT_ENV_PREFIX}_LAVALINK_PASSWORD": LAVALINK_PASSWORD,
    }
    missing = [name for name, value in required.items() if not value]
    if missing: raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")


@bot.event
async def on_command_error(ctx, error):
    dispatch_runtime_error(
        'Discord Command Error',
        error,
        description=f"{ctx.command.qualified_name if ctx.command else 'unknown_command'} failed in guild {getattr(ctx.guild, 'id', 'DM')}",
        guild_id=getattr(ctx.guild, 'id', None),
        error_type='discord_command',
    )


@bot.event
async def on_error(event_method, *args, **kwargs):
    exc_type, exc, tb = sys.exc_info()
    dispatch_runtime_error(
        f'Discord Event Error: {event_method}',
        exc,
        description=f'Unhandled Discord event failure in {event_method}',
        traceback_text=''.join(__import__('traceback').format_exception(exc_type, exc, tb)) if exc_type else None,
        error_type='discord_event',
    )


@bot.event
async def on_ready_error_reporting():
    install_error_reporting()
    try:
        asyncio.get_running_loop().set_exception_handler(_asyncio_exception_handler)
    except Exception:
        pass
bot.add_listener(on_ready_error_reporting, 'on_ready')


def main():
    validate_runtime_config()
    install_error_reporting()
    bot.run(TOKEN)

if __name__ == "__main__":
    main()
