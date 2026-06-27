import copy
import json
import math
import asyncio
from collections import Counter, deque
import discord
import aiohttp
import os
import time
import logging
import random
import secrets
import datetime
import sys
import signal
import yt_dlp
import aiomysql
import wavelink
from discord.ext import commands, tasks
from discord import app_commands
import re
import urllib.parse
import hashlib
import shutil
import subprocess
import fcntl
from types import SimpleNamespace

# --- DAVE PROTOCOL MONKEYPATCH (FIXES LAVALINK 4.2.2 E2EE ENCRYPTION) ---
original_request = aiohttp.ClientSession.request
original__request = aiohttp.ClientSession._request
pending_voice_channels = {}

def _resolve_channel_id_for_guild(guild_id: int):
    try:
        global bot
        guild = bot.get_guild(guild_id) if bot else None
    except Exception as tx_error:
        guild = None

    if guild:
        try:
            voice_client = guild.voice_client
            if voice_client and getattr(voice_client, "channel", None):
                return str(voice_client.channel.id)
        except Exception as tx_error:
            pass
        try:
            if guild.me and guild.me.voice and guild.me.voice.channel:
                return str(guild.me.voice.channel.id)
        except Exception as tx_error:
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
    except Exception as tx_error:
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


def _redact_secret_text(value):
    text = str(value or "")
    if not text:
        return ""
    redacted = re.sub(r"https://discord(?:app)?[.]com/api/webhooks/[0-9]+/[^ )\x27\"]+", "https://discord.com/api/webhooks/[REDACTED]", text, flags=re.I)
    redacted = re.sub(r"https://api[.]telegram[.]org/bot[^/ )\x27\"]+", "https://api.telegram.org/bot[REDACTED]", redacted, flags=re.I)
    redacted = re.sub(r"(?i)\b([A-Z0-9_]*(?:TOKEN|SECRET|PASSWORD|API_KEY|WEBHOOK)[A-Z0-9_]*=)([^\s]+)", r"\1[REDACTED]", redacted)
    return redacted

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
        except ValueError:
            logger.error("❌ WEBHOOK ERROR: Malformed Webhook URL.")
            return
        except discord.errors.NotFound:
            logger.error("❌ WEBHOOK KILLED: Discord deleted your webhook. Create a new one.")
            return
        except Exception as e:
            if attempt < retries - 1: await asyncio.sleep(2 ** attempt)
            else: logger.error(f"❌ Webhook Dispatch Failed: {_redact_secret_text(e)}")

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
            await cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
    finally:
        conn.close()

class DBPoolManager:
    _pool = None
    _create_lock = asyncio.Lock()
    _last_ping = 0.0

    @staticmethod
    def _pool_closed(pool):
        return pool is None or getattr(pool, "closed", False) or getattr(pool, "_closing", False)

    @classmethod
    async def _open_pool(cls):
        await ensure_database_exists()
        cls._pool = await aiomysql.create_pool(
            minsize=DB_POOL_MINSIZE,
            maxsize=DB_POOL_MAXSIZE,
            **DB_CONFIG,
        )
        cls._last_ping = time.time()
        return cls._pool

    @classmethod
    async def _ping_pool(cls, pool):
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
        cls._last_ping = time.time()

    @classmethod
    async def _close_pool(cls, pool):
        try:
            pool.close()
            await pool.wait_closed()
        except Exception as tx_error:
            pass

    async def __aenter__(self):
        async with DBPoolManager._create_lock:
            pool = DBPoolManager._pool
        needs_ping = (
            not DBPoolManager._pool_closed(pool)
            and time.time() - DBPoolManager._last_ping >= DB_POOL_PING_INTERVAL_SECONDS
        )
        if DBPoolManager._pool_closed(pool) or needs_ping:
            async with DBPoolManager._create_lock:
                pool = DBPoolManager._pool
            if DBPoolManager._pool_closed(pool):
                pool = await DBPoolManager._open_pool()
            elif time.time() - DBPoolManager._last_ping >= DB_POOL_PING_INTERVAL_SECONDS:
                try:
                    await DBPoolManager._ping_pool(pool)
                except Exception as tx_error:
                    logger.warning("[%s] DB pool keepalive failed; reopening pool.", BOT_ENV_PREFIX.lower(), exc_info=True)
                    await DBPoolManager._close_pool(pool)
                    pool = await DBPoolManager._open_pool()
        return pool

    async def __aexit__(self, _exc_type, _exc_val, _exc_tb):
        return False

class HTTPSessionManager:
    _session = None
    async def __aenter__(self):
        if not hasattr(HTTPSessionManager, "_lock") or HTTPSessionManager._lock is None: HTTPSessionManager._lock = asyncio.Lock()
        async with HTTPSessionManager._lock:
            if not HTTPSessionManager._session or HTTPSessionManager._session.closed:
            # One bounded shared session prevents webhook/status helpers from leaking sockets
            # or hanging the bot during Discord/API/network stalls.
                    timeout = aiohttp.ClientTimeout(total=float(os.getenv(f"{BOT_ENV_PREFIX}_HTTP_TIMEOUT_SECONDS", os.getenv("MUSIC_BOT_HTTP_TIMEOUT_SECONDS", "20")))) if "BOT_ENV_PREFIX" in globals() else aiohttp.ClientTimeout(total=20)
                    connector = aiohttp.TCPConnector(limit=int(os.getenv(f"{BOT_ENV_PREFIX}_HTTP_CONNECTOR_LIMIT", os.getenv("MUSIC_BOT_HTTP_CONNECTOR_LIMIT", "32"))) if "BOT_ENV_PREFIX" in globals() else 32, ttl_dns_cache=300)
                    HTTPSessionManager._session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return HTTPSessionManager._session
    async def __aexit__(self, _exc_type, _exc_val, _exc_tb): pass

# --- LOGGING SETUP (console + host-visible per-bot rotating file) ---
from logging.handlers import RotatingFileHandler

try:
    _bot_name = os.path.basename(__file__).replace(".py", "").lower()
except NameError:
    _bot_name = "bot"

_log_env_prefix = _bot_name.upper()
log_dir = os.getenv(f"{_log_env_prefix}_LOG_DIR") or os.getenv("MUSIC_BOT_LOG_DIR") or "/app/logs"
log_dir = os.path.abspath(log_dir)
os.makedirs(log_dir, exist_ok=True)

log_filename = os.path.join(log_dir, f"{_bot_name}.log")
# Create the file immediately so `ls logs/` proves logging is mounted/writable even before the first warning/error.
with open(log_filename, "a", encoding="utf-8"):
    pass

_log_formatter = logging.Formatter(
    fmt="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(_log_formatter)

file_handler = RotatingFileHandler(
    filename=log_filename,
    encoding="utf-8",
    mode="a",
    maxBytes=int(os.getenv("MUSIC_BOT_LOG_MAX_BYTES", str(10 * 1024 * 1024))),
    backupCount=int(os.getenv("MUSIC_BOT_LOG_BACKUP_COUNT", "5")),
)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(_log_formatter)

# Replace only normal console/file handlers so repeated imports or discord.setup_logging do not duplicate lines.
for _handler in list(root_logger.handlers):
    if isinstance(_handler, (logging.StreamHandler, RotatingFileHandler)):
        root_logger.removeHandler(_handler)
root_logger.addHandler(console_handler)
root_logger.addHandler(file_handler)

for _logger_name in ("discord", "wavelink", "aiohttp", "asyncio"):
    _lib_logger = logging.getLogger(_logger_name)
    _lib_logger.setLevel(logging.INFO)
    # discord.py may install its own StreamHandler. Remove library-local console/file
    # handlers and let the root logger own output, otherwise every line prints twice.
    for _handler in list(_lib_logger.handlers):
        if isinstance(_handler, (logging.StreamHandler, RotatingFileHandler)):
            _lib_logger.removeHandler(_handler)
    _lib_logger.propagate = True

logger = logging.getLogger("discord")
logger.info("[%s] File logging active at %s", _bot_name, log_filename)

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
        except Exception as tx_error:
            value = None
        if isinstance(value, bool):
            paused = value
            break
    if paused:
        return False
    explicit_false_seen = False
    for attr in ("playing", "is_playing"):
        value = getattr(player, attr, None)
        try:
            if callable(value):
                value = value()
        except TypeError:
            pass
        except Exception as tx_error:
            value = None
        if isinstance(value, bool):
            if value:
                return True
            explicit_false_seen = True
    # Wavelink can briefly expose a current track before/after the boolean updates,
    # but a disconnected stale player should not block queue recovery forever.
    return _voice_client_connected(player) and (_player_current_track(player) is not None or not explicit_false_seen)

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
        except Exception as tx_error:
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
        except Exception as tx_error:
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
        except Exception as tx_error:
            continue
    try:
        value = str(track).strip()
        if value and value.lower() not in {"none", "unknown"}:
            return value
    except Exception as tx_error:
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
    except Exception as tx_error:
        pass
    return bool(getattr(vc, "channel", None))

def _player_is_active(player):
    return _player_is_playing(player) or _player_is_paused(player) or (_voice_client_connected(player) and _player_current_track(player) is not None)

def _wavelink_event_reason(reason) -> str:
    """Normalize Wavelink/Lavalink end reasons across str/enum-like payloads."""
    raw = getattr(reason, "name", None) or getattr(reason, "value", None) or reason
    text = str(raw or "").strip().upper()
    if "." in text:
        text = text.rsplit(".", 1)[-1]
    return text


def _is_retryable_mysql_error(exc):
    return _mysql_error_code(exc) in {1205, 1213, 2006, 2013}

# --- OPTIMIZATION MAP: startup, DB bootstrap, runtime recovery, slash commands, and swarm bridge are intentionally separated below. ---
# --- CONFIGURATION ---
BOT_ENV_PREFIX = "NEXUS"
TOKEN = os.getenv(f"{BOT_ENV_PREFIX}_DISCORD_TOKEN", "").strip()
DB_CONFIG = {
    'host': os.getenv(f"{BOT_ENV_PREFIX}_DB_HOST") or os.getenv("DB_HOST") or os.getenv("MYSQL_HOST") or "host.docker.internal",
    'port': int(os.getenv(f"{BOT_ENV_PREFIX}_DB_PORT") or os.getenv("DB_PORT") or os.getenv("MYSQL_PORT") or 3306),
    'user': os.getenv(f"{BOT_ENV_PREFIX}_DB_USER") or os.getenv("DB_USER") or os.getenv("MYSQL_USER") or "botuser",
    'password': os.getenv(f"{BOT_ENV_PREFIX}_DB_PASSWORD") or os.getenv("DB_PASSWORD") or os.getenv("MYSQL_PASSWORD") or "",
    'db': os.getenv(f"{BOT_ENV_PREFIX}_DB_NAME") or "discord_music_nexus",
    'autocommit': True,
    'pool_recycle': int(os.getenv(f"{BOT_ENV_PREFIX}_DB_POOL_RECYCLE_SECONDS", os.getenv("DB_POOL_RECYCLE_SECONDS", "280"))),
    'connect_timeout': int(os.getenv(f"{BOT_ENV_PREFIX}_DB_CONNECT_TIMEOUT_SECONDS", os.getenv("DB_CONNECT_TIMEOUT_SECONDS", "10"))),
}
DB_POOL_MINSIZE = max(1, int(os.getenv(f"{BOT_ENV_PREFIX}_DB_POOL_MINSIZE", os.getenv("DB_POOL_MINSIZE", "3"))))
DB_POOL_MAXSIZE = max(DB_POOL_MINSIZE, int(os.getenv(f"{BOT_ENV_PREFIX}_DB_POOL_MAXSIZE", os.getenv("DB_POOL_MAXSIZE", "32"))))
DB_POOL_PING_INTERVAL_SECONDS = max(5.0, float(os.getenv(f"{BOT_ENV_PREFIX}_DB_POOL_PING_INTERVAL_SECONDS", os.getenv("DB_POOL_PING_INTERVAL_SECONDS", "30"))))
DEFAULT_LAVALINK_URI = os.getenv("LAVALINK_URI") or os.getenv("LAVALINK_URL") or os.getenv("LAVALINK_HOST") or "http://127.0.0.1:2333"
DEFAULT_LAVALINK_PASSWORD = os.getenv("LAVALINK_PASSWORD", "").strip()
LAVALINK_URI = (
    os.getenv(f"{BOT_ENV_PREFIX}_LAVALINK_URI")
    or os.getenv(f"{BOT_ENV_PREFIX}_LAVALINK_URL")
    or os.getenv(f"{BOT_ENV_PREFIX}_LAVALINK_HOST")
    or DEFAULT_LAVALINK_URI
).strip()
LAVALINK_PASSWORD = os.getenv(f"{BOT_ENV_PREFIX}_LAVALINK_PASSWORD", DEFAULT_LAVALINK_PASSWORD).strip()
if not LAVALINK_PASSWORD:
    raise RuntimeError(f"Set {BOT_ENV_PREFIX}_LAVALINK_PASSWORD or LAVALINK_PASSWORD before starting {BOT_ENV_PREFIX.lower()}.")

ERROR_WEBHOOK_URL = (
    os.getenv(f"{BOT_ENV_PREFIX}_ERROR_WEBHOOK_URL", "").strip()
    or os.getenv("SWARM_ERROR_WEBHOOK_URL", "").strip()
    or os.getenv("ERROR_WEBHOOK_URL", "").strip()
    or os.getenv("SWARM_WEBHOOK_ERROR_URL", "").strip()
)
error_reporting_installed = False
ERROR_REPORT_THROTTLE_SECONDS = max(
    60.0,
    float(os.getenv(f"{BOT_ENV_PREFIX}_ERROR_REPORT_THROTTLE_SECONDS", os.getenv("MUSIC_BOT_ERROR_REPORT_THROTTLE_SECONDS", "300"))),
)
_error_report_last_sent = {}
_bg_tasks = set()


def _error_report_key(message, traceback_text=None):
    message_key = str(message or "").strip().splitlines()[0][:240]
    traceback_key = ""
    if traceback_text:
        traceback_key = str(traceback_text).strip().splitlines()[-1][:240]
    return f"{message_key}|{traceback_key}"


def _should_throttle_error_report(message, traceback_text=None):
    if ERROR_REPORT_THROTTLE_SECONDS <= 0:
        return False
    now = time.monotonic()
    key = _error_report_key(message, traceback_text)
    last_sent = _error_report_last_sent.get(key, 0.0)
    if now - last_sent < ERROR_REPORT_THROTTLE_SECONDS:
        return True
    _error_report_last_sent[key] = now
    if len(_error_report_last_sent) > 256:
        stale_before = now - (ERROR_REPORT_THROTTLE_SECONDS * 2)
        for old_key, sent_at in list(_error_report_last_sent.items()):
            if sent_at < stale_before:
                _error_report_last_sent.pop(old_key, None)
    return False


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
        # Do not dump a full traceback every time the error-event table is unavailable.
        # The original failure is already logged elsewhere; this secondary DB write failure
        # should stay visible but not flood bot logs during MariaDB/network hiccups.
        logger.warning("[%s] Failed to persist error event: %s", BOT_ENV_PREFIX, db_exc)
        logger.debug("[%s] Error-event persistence traceback", BOT_ENV_PREFIX, exc_info=True)


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
        except ValueError:
            logger.error("❌ WEBHOOK ERROR: Malformed Webhook URL.")
            return
        except discord.errors.NotFound:
            logger.warning("[%s] Error webhook no longer exists.", BOT_ENV_PREFIX)
            return
        except Exception as exc:
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)
            else:
                logger.warning("[%s] Error webhook dispatch failed: %s", BOT_ENV_PREFIX, _redact_secret_text(exc))


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
        task = loop.create_task(
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
        _bg_tasks.add(task)
        task.add_done_callback(_bg_tasks.discard)


class SwarmErrorWebhookHandler(logging.Handler):
    def emit(self, record):
        _rec_name = str(getattr(record, "name", "")).lower()
        if any(tk in _rec_name for tk in ("aiomysql", "mysql", "dbpool", "webhook")): return
        try:
            message = record.getMessage()
        except Exception as tx_error:
            message = str(record.msg)
        if not message:
            return
        lowered = message.lower()
        if "error webhook dispatch failed" in lowered or "failed to persist error event" in lowered:
            return
        traceback_text = None
        if record.exc_info:
            traceback_text = ''.join(__import__('traceback').format_exception(*record.exc_info))
        if _should_throttle_error_report(message, traceback_text):
            return
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
    root_logger = logging.getLogger()
    if not any(isinstance(_handler, SwarmErrorWebhookHandler) for _handler in root_logger.handlers):
        root_logger.addHandler(SwarmErrorWebhookHandler(level=logging.ERROR))
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
    if not value:
        return "http://127.0.0.1:2333"
    if "://" not in value:
        host_part = value.rsplit("/", 1)[0]
        # LAVALINK_HOST is commonly supplied as just "127.0.0.1" or "lavalink".
        # Without this, urlparse points the TCP preflight and Wavelink at port 80.
        if ":" not in host_part:
            value = f"{value}:2333"
        value = f"http://{value}"
    parsed = urllib.parse.urlparse(value)
    if parsed.scheme == "http" and parsed.hostname and parsed.port is None and not parsed.path.strip("/"):
        netloc = parsed.hostname
        if parsed.username or parsed.password:
            auth = parsed.username or ""
            if parsed.password:
                auth += f":{parsed.password}"
            netloc = f"{auth}@{netloc}"
        value = urllib.parse.urlunparse(parsed._replace(netloc=f"{netloc}:2333"))
    return value

LAVALINK_URI = _normalize_lavalink_uri(LAVALINK_URI)
LAVALINK_SEARCH_TIMEOUT_SECONDS = max(8.0, float(os.getenv(f"{BOT_ENV_PREFIX}_LAVALINK_SEARCH_TIMEOUT_SECONDS", os.getenv("LAVALINK_SEARCH_TIMEOUT_SECONDS", "25"))))
LAVALINK_PLAY_TIMEOUT_SECONDS = max(8.0, float(os.getenv(f"{BOT_ENV_PREFIX}_LAVALINK_PLAY_TIMEOUT_SECONDS", os.getenv("LAVALINK_PLAY_TIMEOUT_SECONDS", "20"))))
PLAYLIST_SYNC_EXTRACT_TIMEOUT_SECONDS = max(30.0, float(os.getenv(f"{BOT_ENV_PREFIX}_PLAYLIST_SYNC_EXTRACT_TIMEOUT_SECONDS", os.getenv("PLAYLIST_SYNC_EXTRACT_TIMEOUT_SECONDS", "180"))))
POSITION_UPDATER_INTERVAL = max(2.0, float(os.getenv(f"{BOT_ENV_PREFIX}_POSITION_UPDATER_INTERVAL", os.getenv("POSITION_UPDATER_INTERVAL", "5"))))
POSITION_PERSIST_INTERVAL = max(POSITION_UPDATER_INTERVAL, float(os.getenv(f"{BOT_ENV_PREFIX}_POSITION_PERSIST_INTERVAL", os.getenv("POSITION_PERSIST_INTERVAL", "5"))))
POSITION_PAUSED_PERSIST_INTERVAL = max(POSITION_PERSIST_INTERVAL, float(os.getenv(f"{BOT_ENV_PREFIX}_POSITION_PAUSED_PERSIST_INTERVAL_SECONDS", os.getenv("POSITION_PAUSED_PERSIST_INTERVAL_SECONDS", "60"))))
POSITION_STATE_FILE_INTERVAL = max(POSITION_PERSIST_INTERVAL, float(os.getenv(f"{BOT_ENV_PREFIX}_POSITION_STATE_FILE_INTERVAL", os.getenv("POSITION_STATE_FILE_INTERVAL", "15"))))
PLAYTIME_MIN_DELTA_SECONDS = max(1, int(os.getenv(f"{BOT_ENV_PREFIX}_PLAYTIME_MIN_DELTA_SECONDS", os.getenv("PLAYTIME_MIN_DELTA_SECONDS", "1"))))
PLAYER_POSITION_STALE_ZERO_SECONDS = max(3.0, float(os.getenv(f"{BOT_ENV_PREFIX}_PLAYER_POSITION_STALE_ZERO_SECONDS", os.getenv("PLAYER_POSITION_STALE_ZERO_SECONDS", "8"))))
PLAYER_POSITION_BACKSTEP_GRACE_SECONDS = max(1, int(os.getenv(f"{BOT_ENV_PREFIX}_PLAYER_POSITION_BACKSTEP_GRACE_SECONDS", os.getenv("PLAYER_POSITION_BACKSTEP_GRACE_SECONDS", "3"))))
PLAYER_POSITION_STALL_FALLBACK_SECONDS = max(10.0, float(os.getenv(f"{BOT_ENV_PREFIX}_PLAYER_POSITION_STALL_FALLBACK_SECONDS", os.getenv("PLAYER_POSITION_STALL_FALLBACK_SECONDS", "30"))))
PLAYER_POSITION_STALL_MIN_RUNTIME_AHEAD_SECONDS = max(3.0, float(os.getenv(f"{BOT_ENV_PREFIX}_PLAYER_POSITION_STALL_MIN_RUNTIME_AHEAD_SECONDS", os.getenv("PLAYER_POSITION_STALL_MIN_RUNTIME_AHEAD_SECONDS", "8"))))
RESUME_SEEK_RETRY_DELAY_SECONDS = max(0.5, float(os.getenv(f"{BOT_ENV_PREFIX}_RESUME_SEEK_RETRY_DELAY_SECONDS", os.getenv("RESUME_SEEK_RETRY_DELAY_SECONDS", "1.5"))))
RESUME_SEEK_VERIFY_GRACE_SECONDS = max(2, int(os.getenv(f"{BOT_ENV_PREFIX}_RESUME_SEEK_VERIFY_GRACE_SECONDS", os.getenv("RESUME_SEEK_VERIFY_GRACE_SECONDS", "8"))))
SHUTDOWN_POSITION_FLUSH_TIMEOUT_SECONDS = max(3.0, float(os.getenv(f"{BOT_ENV_PREFIX}_SHUTDOWN_POSITION_FLUSH_TIMEOUT_SECONDS", os.getenv("SHUTDOWN_POSITION_FLUSH_TIMEOUT_SECONDS", "8"))))
PLAYLIST_SYNC_INTERVAL = max(30.0, float(os.getenv(f"{BOT_ENV_PREFIX}_PLAYLIST_SYNC_INTERVAL", os.getenv("PLAYLIST_SYNC_INTERVAL", "30"))))
AUTO_HEAL_INTERVAL = max(15.0, float(os.getenv(f"{BOT_ENV_PREFIX}_AUTO_HEAL_INTERVAL", "20")))
AUTO_IMPORT_IDLE_SECONDS = max(45.0, float(os.getenv(f"{BOT_ENV_PREFIX}_AUTO_IMPORT_IDLE_SECONDS", "45")))
RECOVERY_RETRY_BASE_DELAY = max(2.0, float(os.getenv(f"{BOT_ENV_PREFIX}_RECOVERY_RETRY_BASE_DELAY", os.getenv("RECOVERY_RETRY_BASE_DELAY", "3"))))
RECOVERY_RETRY_MAX_DELAY = max(RECOVERY_RETRY_BASE_DELAY, float(os.getenv(f"{BOT_ENV_PREFIX}_RECOVERY_RETRY_MAX_DELAY", os.getenv("RECOVERY_RETRY_MAX_DELAY", "10"))))
MAX_RECOVERY_RETRIES = max(3, int(os.getenv(f"{BOT_ENV_PREFIX}_MAX_RECOVERY_RETRIES", "4")))
MAX_TRACK_FAILURE_REQUEUES = max(1, int(os.getenv(f"{BOT_ENV_PREFIX}_MAX_TRACK_FAILURE_REQUEUES", os.getenv("MAX_TRACK_FAILURE_REQUEUES", "3"))))
TRACK_FAILURE_WINDOW_SECONDS = max(60.0, float(os.getenv(f"{BOT_ENV_PREFIX}_TRACK_FAILURE_WINDOW_SECONDS", os.getenv("TRACK_FAILURE_WINDOW_SECONDS", "900"))))
TRACK_REQUEUE_DEDUP_SECONDS = max(15.0, float(os.getenv(f"{BOT_ENV_PREFIX}_TRACK_REQUEUE_DEDUP_SECONDS", os.getenv("TRACK_REQUEUE_DEDUP_SECONDS", "120"))))
QUEUE_PLAYBACK_CLAIM_TTL_SECONDS = max(30.0, float(os.getenv(f"{BOT_ENV_PREFIX}_QUEUE_PLAYBACK_CLAIM_TTL_SECONDS", os.getenv("QUEUE_PLAYBACK_CLAIM_TTL_SECONDS", "180"))))
TRACK_STUCK_VERIFY_DELAY_SECONDS = max(10.0, float(os.getenv(f"{BOT_ENV_PREFIX}_TRACK_STUCK_VERIFY_DELAY_SECONDS", os.getenv("TRACK_STUCK_VERIFY_DELAY_SECONDS", "45"))))
TRACK_STUCK_MIN_PROGRESS_SECONDS = max(2, int(os.getenv(f"{BOT_ENV_PREFIX}_TRACK_STUCK_MIN_PROGRESS_SECONDS", os.getenv("TRACK_STUCK_MIN_PROGRESS_SECONDS", "4"))))
TRACK_STUCK_SKIP_WHEN_POSITION_UNKNOWN = str(os.getenv(f"{BOT_ENV_PREFIX}_TRACK_STUCK_SKIP_WHEN_POSITION_UNKNOWN", os.getenv("TRACK_STUCK_SKIP_WHEN_POSITION_UNKNOWN", "true"))).strip().lower() not in {"0", "false", "off", "no"}
WATCHDOG_REVIVAL_COOLDOWN = max(10.0, float(os.getenv(f"{BOT_ENV_PREFIX}_WATCHDOG_REVIVAL_COOLDOWN", "15")))
WATCHDOG_MAX_REVIVALS = max(3, int(os.getenv(f"{BOT_ENV_PREFIX}_WATCHDOG_MAX_REVIVALS", "6")))
AUTO_RESTORE_SNOOZE_SECONDS = max(15.0, float(os.getenv(f"{BOT_ENV_PREFIX}_AUTO_RESTORE_SNOOZE_SECONDS", "45")))
RECOVERY_EXHAUSTED_COOLDOWN_SECONDS = max(10.0, float(os.getenv(f"{BOT_ENV_PREFIX}_RECOVERY_EXHAUSTED_COOLDOWN_SECONDS", "20")))
PERIODIC_RESTART_HOURS = max(0.0, float(os.getenv(f"{BOT_ENV_PREFIX}_PERIODIC_RESTART_HOURS", os.getenv("PERIODIC_RESTART_HOURS", "5"))))
PERIODIC_RESTART_JITTER_SECONDS = max(0.0, float(os.getenv(f"{BOT_ENV_PREFIX}_PERIODIC_RESTART_JITTER_SECONDS", os.getenv("PERIODIC_RESTART_JITTER_SECONDS", "900"))))
CACHE_CLEANUP_INTERVAL_HOURS = max(1.0, float(os.getenv(f"{BOT_ENV_PREFIX}_CACHE_CLEANUP_INTERVAL_HOURS", os.getenv("CACHE_CLEANUP_INTERVAL_HOURS", "10"))))
CACHE_CLEANUP_INITIAL_SPREAD_SECONDS = max(0.0, float(os.getenv(f"{BOT_ENV_PREFIX}_CACHE_CLEANUP_INITIAL_SPREAD_SECONDS", os.getenv("CACHE_CLEANUP_INITIAL_SPREAD_SECONDS", "1800"))))
CACHE_CLEANUP_LOCK_TTL_SECONDS = max(60.0, float(os.getenv(f"{BOT_ENV_PREFIX}_CACHE_CLEANUP_LOCK_TTL_SECONDS", os.getenv("CACHE_CLEANUP_LOCK_TTL_SECONDS", "1800"))))
CACHE_CLEANUP_DISK_ENABLED = str(os.getenv(f"{BOT_ENV_PREFIX}_CACHE_CLEANUP_DISK_ENABLED", os.getenv("CACHE_CLEANUP_DISK_ENABLED", "true"))).strip().lower() not in {"0", "false", "off", "no"}

intents = discord.Intents.default()
intents.message_content = False
bot = commands.Bot(command_prefix="/", intents=intents)


def _truthy_env(value, default=True):
    if value is None:
        return default
    return str(value).strip().lower() not in {"0", "false", "off", "no"}


def _parse_id_set(raw_value):
    ids = set()
    for part in str(raw_value or "").replace(";", ",").split(","):
        value = part.strip()
        if not value:
            continue
        try:
            ids.add(int(value))
        except ValueError:
            logger.warning("[%s] Ignoring non-numeric private owner id: %s", BOT_ENV_PREFIX.lower(), value)
    return ids


MUSIC_BOT_PRIVATE_MODE = _truthy_env(os.getenv(f"{BOT_ENV_PREFIX}_PRIVATE_MODE", os.getenv("MUSIC_BOT_PRIVATE_MODE", "true")))
PRIVATE_OWNER_USER_IDS = _parse_id_set(
    ",".join(
        value for value in (
            os.getenv(f"{BOT_ENV_PREFIX}_OWNER_USER_IDS", ""),
            os.getenv(f"{BOT_ENV_PREFIX}_OWNER_IDS", ""),
            os.getenv("MUSIC_BOT_OWNER_IDS", ""),
            os.getenv("DISCORD_OWNER_IDS", ""),
        )
        if value
    )
)
_application_owner_ids_cache = set()
_application_owner_ids_loaded = False


async def load_private_owner_user_ids():
    global _application_owner_ids_cache, _application_owner_ids_loaded
    if _application_owner_ids_loaded:
        return set(_application_owner_ids_cache)

    ids = set(PRIVATE_OWNER_USER_IDS)
    try:
        app_info = await bot.application_info()
        owner = getattr(app_info, "owner", None)
        owner_id = getattr(owner, "id", None)
        if owner_id:
            ids.add(int(owner_id))
        team = getattr(app_info, "team", None)
        for member in getattr(team, "members", []) or []:
            user = getattr(member, "user", member)
            user_id = getattr(user, "id", None)
            if user_id:
                ids.add(int(user_id))
    except Exception as tx_error:
        logger.exception("[%s] Failed to load Discord application owner ids for private mode.", BOT_ENV_PREFIX.lower())

    _application_owner_ids_cache = ids
    _application_owner_ids_loaded = True
    return set(ids)


async def is_private_owner_user(user):
    if not MUSIC_BOT_PRIVATE_MODE:
        return True
    user_id = getattr(user, "id", None)
    if not user_id:
        return False
    try:
        normalized = int(user_id)
    except (TypeError, ValueError):
        return False
    if normalized in PRIVATE_OWNER_USER_IDS:
        return True
    return normalized in await load_private_owner_user_ids()


# --- Guild authorization monitoring (alert-only: never auto-leaves or blocks) ---------------
# Owner-approved servers. When empty, monitoring is OFF (legacy behaviour). When set, joining
# a server NOT on the list DMs the owner + logs a warning, but the bot takes no action.
MUSIC_BOT_ALLOWED_GUILD_IDS = _parse_id_set(
    os.getenv(f"{BOT_ENV_PREFIX}_ALLOWED_GUILD_IDS", os.getenv("MUSIC_BOT_ALLOWED_GUILD_IDS", ""))
)
# Per-guild session tables cleaned when the bot leaves a server (kills phantom "mysterious
# guilds" in the panel). Never includes history (kept as a log) or Discord membership.
_GUILD_CLEANUP_TABLES = (
    "playback_state", "queue", "queue_backup",
    "active_playlists", "active_playlist_tracks", "guild_settings",
)
_guild_audit_done = False


def _guild_is_authorized(guild_id):
    if not MUSIC_BOT_ALLOWED_GUILD_IDS:
        return True
    try:
        return int(guild_id) in MUSIC_BOT_ALLOWED_GUILD_IDS
    except (TypeError, ValueError):
        return True


async def alert_owner_unauthorized_guild(guild, action_desc):
    name = getattr(guild, "name", "?")
    gid = getattr(guild, "id", "?")
    members = getattr(guild, "member_count", "?")
    logger.warning("[%s] UNAUTHORIZED guild %s (%s) — bot %s it; alert-only, no action taken.",
                   BOT_ENV_PREFIX.lower(), name, gid, action_desc)
    msg = (f"⚠️ **Security alert** — music bot `{BOT_ENV_PREFIX.lower()}` {action_desc} an "
           f"**unauthorized** server: **{name}** (`{gid}`, ~{members} members). No automatic action was "
           f"taken. Authorize it by adding the id to `{BOT_ENV_PREFIX}_ALLOWED_GUILD_IDS`, or remove the "
           f"bot from that server.")
    try:
        for oid in list(await load_private_owner_user_ids())[:3]:
            try:
                user = bot.get_user(int(oid)) or await bot.fetch_user(int(oid))
                if user:
                    await user.send(msg)
            except Exception:
                continue
    except Exception:
        logger.debug("[%s] Could not DM owner about unauthorized guild.", BOT_ENV_PREFIX.lower(), exc_info=True)


async def cleanup_guild_data(guild_id):
    """Delete this bot's persisted rows for one guild. Only touches the DB, never Discord."""
    prefix = BOT_ENV_PREFIX.lower()
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    for suffix in _GUILD_CLEANUP_TABLES:
                        try:
                            await cur.execute(f"DELETE FROM {prefix}_{suffix} WHERE guild_id = %s", (guild_id,))
                        except Exception:
                            continue
    except Exception:
        logger.debug("[%s] Guild data cleanup failed for %s.", prefix, guild_id, exc_info=True)


async def cleanup_stale_guild_rows(current_guild_ids):
    """Remove DB rows for guilds the bot is no longer a member of (phantom-guild cleanup)."""
    prefix = BOT_ENV_PREFIX.lower()
    current = [int(g) for g in current_guild_ids if g is not None]
    if not current:
        return 0  # never wipe everything if the guild cache looks empty
    placeholders = ",".join(["%s"] * len(current))
    removed = 0
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    for suffix in _GUILD_CLEANUP_TABLES:
                        try:
                            await cur.execute(
                                f"DELETE FROM {prefix}_{suffix} WHERE guild_id NOT IN ({placeholders})",
                                tuple(current),
                            )
                            removed += cur.rowcount or 0
                        except Exception:
                            continue
    except Exception:
        logger.debug("[%s] Stale guild cleanup failed.", prefix, exc_info=True)
    return removed


@bot.event
async def on_guild_join(guild):
    if not _guild_is_authorized(getattr(guild, "id", None)):
        await alert_owner_unauthorized_guild(guild, "was just ADDED to")


@bot.event
async def on_guild_remove(guild):
    gid = getattr(guild, "id", None)
    if gid is not None:
        await cleanup_guild_data(gid)
        logger.info("[%s] Left guild %s; cleaned up its session data.", BOT_ENV_PREFIX.lower(), gid)


@bot.listen("on_ready")
async def _guild_authorization_audit():
    global _guild_audit_done
    if _guild_audit_done:
        return
    _guild_audit_done = True
    try:
        await asyncio.sleep(5)  # let the guild cache populate
        if MUSIC_BOT_ALLOWED_GUILD_IDS:
            for g in list(bot.guilds):
                if g.id not in MUSIC_BOT_ALLOWED_GUILD_IDS:
                    logger.warning("[%s] Currently a member of UNAUTHORIZED guild '%s' (%s) — alert only.",
                                   BOT_ENV_PREFIX.lower(), g.name, g.id)
        removed = await cleanup_stale_guild_rows({g.id for g in bot.guilds})
        if removed:
            logger.info("[%s] Startup audit removed %s stale row(s) for guilds the bot has left.",
                        BOT_ENV_PREFIX.lower(), removed)
    except Exception:
        logger.debug("[%s] Guild authorization audit failed.", BOT_ENV_PREFIX.lower(), exc_info=True)


bot.start_time = time.monotonic()
playback_tracking = {}
guild_states = {}
STATE_FILE_WRITE_CACHE = {}
auto_heal_initialized = False
# Prefetch cache: resolve next track's Lavalink URL while current track plays,
# eliminating the gap between tracks. Keyed by guild_id.
_nexus_prefetch_cache: dict = {}  # guild_id -> {url, title, track, expires_at}
recovering_guilds = set()
process_queue_locks = {}
voice_connect_locks = {}
last_position_persist = {}
player_position_report_state = {}
player_position_stall_warning_at = {}
last_state_file_persist = {}
playlist_db_initialized = False
playlist_db_lock = asyncio.Lock()
recovery_retry_tasks = {}
recovery_retry_counts = {}
track_failure_counts = {}
track_requeue_locks = {}
recent_track_requeues = {}
queue_playback_claims = {}
recovery_exhausted_until = {}
voice_disconnect_grace_tasks = {}
interrupt_resume_tasks = {}
interrupt_resume_armed_until = {}
idle_voice_since = {}
auto_restore_snooze_until = {}
resilience_queue_retry_after = {}
voice_connect_inflight_until = {}
queue_parity_repair_state = {}
aria_authority_notice_after = {}
startup_task_registry = {}
feedback_notice_cache = {}
MAX_RUNTIME_GUILD_CACHE_ENTRIES = max(64, int(os.getenv(f"{BOT_ENV_PREFIX}_RUNTIME_GUILD_CACHE_MAX", os.getenv("RUNTIME_GUILD_CACHE_MAX", "512"))))


# --- FEATURE OPTIMIZATION CACHE LAYER ---
# These caches only optimize already-built features; they do not change feature ownership.
REQUESTER_NAME_CACHE = {}
AUTO_DJ_ENABLED_CACHE = {}
GUILD_SETTINGS_CACHE = {}
HOME_CHANNEL_CACHE = {}
SEARCH_RESULT_CACHE = {}
VOICE_STATUS_CACHE = {}
STATUS_MESSAGE_CACHE = {}
VOICE_STATE_PERSIST_CACHE = {}
AUTODJ_LAST_RUN = {}
AUTODJ_FAIL_UNTIL = {}

REQUESTER_NAME_CACHE_TTL_SECONDS = max(60.0, float(os.getenv(f"{BOT_ENV_PREFIX}_REQUESTER_NAME_CACHE_TTL_SECONDS", os.getenv("REQUESTER_NAME_CACHE_TTL_SECONDS", "900"))))
AUTO_DJ_CACHE_TTL_SECONDS = max(5.0, float(os.getenv(f"{BOT_ENV_PREFIX}_AUTO_DJ_CACHE_TTL_SECONDS", os.getenv("AUTO_DJ_CACHE_TTL_SECONDS", "30"))))
GUILD_SETTINGS_CACHE_TTL_SECONDS = max(5.0, float(os.getenv(f"{BOT_ENV_PREFIX}_GUILD_SETTINGS_CACHE_TTL_SECONDS", os.getenv("GUILD_SETTINGS_CACHE_TTL_SECONDS", "30"))))
HOME_CHANNEL_CACHE_TTL_SECONDS = max(5.0, float(os.getenv(f"{BOT_ENV_PREFIX}_HOME_CHANNEL_CACHE_TTL_SECONDS", os.getenv("HOME_CHANNEL_CACHE_TTL_SECONDS", "30"))))
SEARCH_CACHE_TTL_SECONDS = max(10.0, float(os.getenv(f"{BOT_ENV_PREFIX}_SEARCH_CACHE_TTL_SECONDS", os.getenv("SEARCH_CACHE_TTL_SECONDS", "900"))))
VOICE_STATUS_DEDUP_SECONDS = max(10.0, float(os.getenv(f"{BOT_ENV_PREFIX}_VOICE_STATUS_DEDUP_SECONDS", os.getenv("VOICE_STATUS_DEDUP_SECONDS", "60"))))
STATUS_MESSAGE_DEDUP_SECONDS = max(5.0, float(os.getenv(f"{BOT_ENV_PREFIX}_STATUS_MESSAGE_DEDUP_SECONDS", os.getenv("STATUS_MESSAGE_DEDUP_SECONDS", "20"))))
VOICE_STATE_DEDUP_SECONDS = max(5.0, float(os.getenv(f"{BOT_ENV_PREFIX}_VOICE_STATE_DEDUP_SECONDS", os.getenv("VOICE_STATE_DEDUP_SECONDS", "12"))))
AUTODJ_MIN_INTERVAL_SECONDS = max(5.0, float(os.getenv(f"{BOT_ENV_PREFIX}_AUTODJ_MIN_INTERVAL_SECONDS", os.getenv("AUTODJ_MIN_INTERVAL_SECONDS", "20"))))
AUTODJ_FAILURE_BACKOFF_SECONDS = max(15.0, float(os.getenv(f"{BOT_ENV_PREFIX}_AUTODJ_FAILURE_BACKOFF_SECONDS", os.getenv("AUTODJ_FAILURE_BACKOFF_SECONDS", "90"))))
MAX_FEATURE_CACHE_ENTRIES = max(64, int(os.getenv(f"{BOT_ENV_PREFIX}_FEATURE_CACHE_MAX", os.getenv("FEATURE_CACHE_MAX", "4096"))))
LAVALINK_TRACK_CACHE_CAPACITY = max(100, int(os.getenv(f"{BOT_ENV_PREFIX}_LAVALINK_TRACK_CACHE_CAPACITY", os.getenv("LAVALINK_TRACK_CACHE_CAPACITY", "1000"))))
YTDLP_CACHE_DIR = os.getenv(f"{BOT_ENV_PREFIX}_YTDLP_CACHE_DIR", os.getenv("YTDLP_CACHE_DIR", "/app/.cache/yt-dlp")).strip() or "/app/.cache/yt-dlp"
try:
    os.makedirs(YTDLP_CACHE_DIR, exist_ok=True)
except Exception as tx_error:
    pass
SMART_RECENT_HISTORY_LIMIT = max(12, int(os.getenv(f"{BOT_ENV_PREFIX}_SMART_RECENT_HISTORY_LIMIT", os.getenv("SMART_RECENT_HISTORY_LIMIT", "36"))))
SMART_SEED_POOL_LIMIT = max(10, int(os.getenv(f"{BOT_ENV_PREFIX}_SMART_SEED_POOL_LIMIT", os.getenv("SMART_SEED_POOL_LIMIT", "40"))))
SMART_CANDIDATE_SCAN_LIMIT = max(3, int(os.getenv(f"{BOT_ENV_PREFIX}_SMART_CANDIDATE_SCAN_LIMIT", os.getenv("SMART_CANDIDATE_SCAN_LIMIT", "12"))))
SMART_FEEDBACK_SCORE = float(os.getenv(f"{BOT_ENV_PREFIX}_SMART_FEEDBACK_SCORE", os.getenv("SMART_FEEDBACK_SCORE", "4.0")))
SMART_AUTODJ_RADIO_SUFFIXES = tuple(part.strip() for part in os.getenv(f"{BOT_ENV_PREFIX}_SMART_AUTODJ_SUFFIXES", os.getenv("SMART_AUTODJ_SUFFIXES", "radio,audio,playlist,mix")).split(",") if part.strip())
SMART_RECENCY_HALFLIFE_DAYS = max(1.0, float(os.getenv(f"{BOT_ENV_PREFIX}_SMART_RECENCY_HALFLIFE_DAYS", os.getenv("SMART_RECENCY_HALFLIFE_DAYS", "21"))))
SMART_GLOBAL_POOL_WEIGHT = float(os.getenv(f"{BOT_ENV_PREFIX}_SMART_GLOBAL_POOL_WEIGHT", os.getenv("SMART_GLOBAL_POOL_WEIGHT", "0.4")))
SMART_COOCCURRENCE_LIMIT = max(3, int(os.getenv(f"{BOT_ENV_PREFIX}_SMART_COOCCURRENCE_LIMIT", os.getenv("SMART_COOCCURRENCE_LIMIT", "8"))))
SMART_EMBEDDING_ENABLED = os.getenv(f"{BOT_ENV_PREFIX}_SMART_EMBEDDING_ENABLED", os.getenv("SMART_EMBEDDING_ENABLED", "true")).strip().lower() not in ("0", "false", "no", "off")
SMART_EMBEDDING_SIMILARITY_THRESHOLD = float(os.getenv(f"{BOT_ENV_PREFIX}_SMART_EMBEDDING_SIMILARITY_THRESHOLD", os.getenv("SMART_EMBEDDING_SIMILARITY_THRESHOLD", "0.86")))
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_EMBEDDING_MODEL = os.getenv("GEMINI_EMBEDDING_MODEL", "gemini-embedding-001").strip()

def _cache_get(cache, key, ttl_seconds):
    item = cache.get(key)
    if not item:
        return None
    value, timestamp = item
    if time.time() - timestamp > ttl_seconds:
        cache.pop(key, None)
        return None
    try:
        cache.pop(key, None)
        cache[key] = (value, timestamp)
    except Exception as tx_error:
        pass
    return value

def _cache_set(cache, key, value):
    if key in cache:
        cache.pop(key, None)
    cache[key] = (value, time.time())
    while len(cache) > MAX_FEATURE_CACHE_ENTRIES:
        try:
            cache.pop(next(iter(cache)), None)
        except StopIteration:
            break
    return value

def _cache_drop_guild(cache, guild_id):
    gid = _runtime_key(guild_id)
    for key in list(cache.keys()):
        if _runtime_key(key) == gid or (isinstance(key, tuple) and key and _runtime_key(key[0]) == gid):
            cache.pop(key, None)

def invalidate_feature_caches(guild_id=None):
    if guild_id is None:
        for cache in (REQUESTER_NAME_CACHE, AUTO_DJ_ENABLED_CACHE, GUILD_SETTINGS_CACHE, HOME_CHANNEL_CACHE, VOICE_STATUS_CACHE, STATUS_MESSAGE_CACHE, VOICE_STATE_PERSIST_CACHE, AUTODJ_LAST_RUN, AUTODJ_FAIL_UNTIL):
            cache.clear()
        return
    for cache in (REQUESTER_NAME_CACHE, AUTO_DJ_ENABLED_CACHE, GUILD_SETTINGS_CACHE, HOME_CHANNEL_CACHE, VOICE_STATUS_CACHE, STATUS_MESSAGE_CACHE, VOICE_STATE_PERSIST_CACHE, AUTODJ_LAST_RUN, AUTODJ_FAIL_UNTIL):
        _cache_drop_guild(cache, guild_id)

def _embed_fingerprint(embed):
    try:
        fields = tuple((str(field.name), str(field.value), bool(field.inline)) for field in getattr(embed, "fields", []) or [])
        return (str(getattr(embed, "title", "") or ""), str(getattr(embed, "description", "") or ""), fields)
    except Exception as tx_error:
        return (repr(embed),)

def _runtime_key(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return value


def _status_message_cache_key(guild_id):
    return (_runtime_key(guild_id), BOT_ENV_PREFIX.lower())


def aria_recovery_authority_blocks_self_heal(action="self_heal", guild_id=None):
    """Return True when Aria owns recovery and this bot should only preserve/report state."""
    blocked = bool(ARIA_RECOVERY_AUTHORITY and not BOT_SELF_HEAL_WHEN_ARIA_AUTHORITY)
    if blocked and guild_id is not None:
        now = time.monotonic()
        key = (int(guild_id), str(action))
        next_log = aria_authority_notice_after.get(key, 0.0)
        if now >= next_log:
            aria_authority_notice_after[key] = now + 120.0
            logger.info(
                "[%s] Aria recovery authority is enabled; bot preserved state and skipped automatic %s.",
                guild_id,
                action,
            )
    return blocked


def _queue_parity_signature(rows):
    digest = hashlib.sha256()
    for row in rows or []:
        try:
            digest.update(str(_track_key(_row_value(row, "video_url", _row_value(row, 1, "")), _row_value(row, "title", _row_value(row, 2, "")))).encode("utf-8", "ignore"))
            digest.update(b"\0")
            digest.update(str(_row_track_uid(row, 4, default="")).encode("utf-8", "ignore"))
            digest.update(b"\0")
        except Exception as tx_error:
            digest.update(repr(row).encode("utf-8", "ignore"))
            digest.update(b"\0")
    return digest.hexdigest()


def queue_parity_repair_allowed(guild_id, backup_rows, live_rows, *, reason="queue_integrity"):
    """Throttle duplicate parity repairs so a stuck state does not rewrite queues every tick."""
    gid = int(guild_id)
    now = time.monotonic()
    signature = f"{_queue_parity_signature(backup_rows)}:{_queue_parity_signature(live_rows)}"
    previous = queue_parity_repair_state.get(gid)
    if previous:
        prev_sig, last_repair_at, next_allowed_at = previous
        if signature == prev_sig and now - last_repair_at < QUEUE_PARITY_REPAIR_HASH_TTL_SECONDS:
            logger.info("[%s] Queue parity repair skipped; identical drift was already handled recently after %s.", gid, reason)
            return False
        if now < next_allowed_at:
            logger.info("[%s] Queue parity repair skipped for %.1fs cooldown after %s.", gid, next_allowed_at - now, reason)
            return False
    queue_parity_repair_state[gid] = (signature, now, now + QUEUE_PARITY_REPAIR_COOLDOWN_SECONDS)
    return True

def prune_runtime_state_cache():
    try:
        active_guild_ids = {_runtime_key(g.id) for g in bot.guilds}
    except Exception as tx_error:
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

    for mapping in (last_position_persist, player_position_report_state, player_position_stall_warning_at, last_state_file_persist, recovery_retry_counts, track_failure_counts, recovery_exhausted_until, idle_voice_since, auto_restore_snooze_until, resilience_queue_retry_after, voice_connect_inflight_until, interrupt_resume_armed_until, vote_skip_sessions, metrics_last_errors, STATE_FILE_WRITE_CACHE, pending_voice_channels):
        prune_mapping(mapping)




def _feature_cache_map():
    return {
        "requester_names": REQUESTER_NAME_CACHE,
        "auto_dj": AUTO_DJ_ENABLED_CACHE,
        "guild_settings": GUILD_SETTINGS_CACHE,
        "home_channels": HOME_CHANNEL_CACHE,
        "search_results": SEARCH_RESULT_CACHE,
        "voice_status": VOICE_STATUS_CACHE,
        "status_messages": STATUS_MESSAGE_CACHE,
        "voice_state_persist": VOICE_STATE_PERSIST_CACHE,
        "autodj_last_run": AUTODJ_LAST_RUN,
        "autodj_fail_until": AUTODJ_FAIL_UNTIL,
    }


def clear_feature_runtime_caches():
    counts = {}
    for name, cache in _feature_cache_map().items():
        try:
            counts[name] = len(cache)
            cache.clear()
        except Exception as tx_error:
            logger.debug("[%s] Failed to clear feature cache %s.", BOT_ENV_PREFIX.lower(), name, exc_info=True)
    try:
        prune_runtime_state_cache()
    except Exception as tx_error:
        logger.debug("[%s] Runtime prune skipped during cache cleanup.", BOT_ENV_PREFIX.lower(), exc_info=True)
    return counts


def _cache_cleanup_lock_path():
    parent = os.path.dirname(os.path.abspath(YTDLP_CACHE_DIR)) or "/tmp"
    try:
        os.makedirs(parent, exist_ok=True)
    except Exception as tx_error:
        parent = "/tmp"
    return os.path.join(parent, ".music_fleet_cache_cleanup.lock")


def _try_acquire_cache_cleanup_lock():
    if not CACHE_CLEANUP_DISK_ENABLED:
        return None
    path = _cache_cleanup_lock_path()
    now = time.monotonic()
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "w", encoding="utf-8") as lock_file:
            lock_file.write(f"{BOT_ENV_PREFIX.lower()} {now}\n")
        return path
    except FileExistsError:
        try:
            if now - os.path.getmtime(path) > CACHE_CLEANUP_LOCK_TTL_SECONDS:
                os.unlink(path)
                return _try_acquire_cache_cleanup_lock()
        except FileNotFoundError:
            return _try_acquire_cache_cleanup_lock()
        except Exception as tx_error:
            logger.debug("[%s] Cache cleanup lock inspection failed.", BOT_ENV_PREFIX.lower(), exc_info=True)
    except Exception as tx_error:
        logger.debug("[%s] Cache cleanup lock acquisition failed.", BOT_ENV_PREFIX.lower(), exc_info=True)
    return None


def _release_cache_cleanup_lock(path):
    if not path:
        return
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
    except Exception as tx_error:
        logger.debug("[%s] Cache cleanup lock release failed.", BOT_ENV_PREFIX.lower(), exc_info=True)


def _clear_directory_contents(path):
    removed = 0
    if not path:
        return removed
    os.makedirs(path, exist_ok=True)
    for entry in os.scandir(path):
        try:
            if entry.name == ".music_fleet_cache_cleanup.lock":
                continue
            if entry.is_dir(follow_symlinks=False):
                shutil.rmtree(entry.path)
            else:
                os.unlink(entry.path)
            removed += 1
        except FileNotFoundError:
            continue
        except Exception as tx_error:
            logger.debug("[%s] Failed removing cache entry %s.", BOT_ENV_PREFIX.lower(), entry.path, exc_info=True)
    return removed


def _clear_wavelink_memory_caches():
    cleared = 0
    targets = []
    try:
        nodes = getattr(wavelink.Pool, "nodes", None)
        if isinstance(nodes, dict):
            targets.extend(nodes.values())
        elif nodes:
            targets.extend(list(nodes))
    except Exception as tx_error:
        pass
    targets.append(wavelink.Pool)

    for target in targets:
        for attr in ("_cache", "cache", "_cached_tracks", "_track_cache", "_playlist_cache", "_search_cache"):
            try:
                cache = getattr(target, attr, None)
                if hasattr(cache, "clear"):
                    before = len(cache) if hasattr(cache, "__len__") else 0
                    cache.clear()
                    cleared += before
            except Exception as tx_error:
                logger.debug("[%s] Failed clearing Wavelink cache %s on %r.", BOT_ENV_PREFIX.lower(), attr, target, exc_info=True)
    return cleared


async def clear_local_cache_systems(reason="manual"):
    feature_counts = clear_feature_runtime_caches()
    disk_removed = 0
    if CACHE_CLEANUP_DISK_ENABLED:
        lock_path = _try_acquire_cache_cleanup_lock()
        if lock_path:
            try:
                disk_removed = await asyncio.to_thread(_clear_directory_contents, YTDLP_CACHE_DIR)
            finally:
                _release_cache_cleanup_lock(lock_path)
    wavelink_removed = _clear_wavelink_memory_caches()
    logger.info(
        "[%s] Cleared runtime caches reason=%s feature_entries=%s ytdlp_entries=%s wavelink_entries=%s",
        BOT_ENV_PREFIX.lower(),
        reason,
        sum(feature_counts.values()),
        disk_removed,
        wavelink_removed,
    )
    return {"features": feature_counts, "disk_entries": disk_removed, "wavelink_entries": wavelink_removed}

def schedule_named_task(name, coro, overwrite=False):
    """Prevent duplicate startup/recovery tasks across reconnecting on_ready events.

    When overwrite=True, cancel an older still-running task with the same name.
    This is used for per-track effects such as volume fades so a skipped track
    cannot keep controlling the next track's player state.
    """
    existing = startup_task_registry.get(name)
    if existing and not existing.done():
        if overwrite:
            existing.cancel()
        else:
            try:
                coro.close()
            except Exception as tx_error:
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
        except Exception as tx_error:
            logger.exception("Background task %s failed", name)

    task.add_done_callback(_cleanup)
    return task


async def flush_runtime_state_before_restart(reason: str = "restart"):
    """Persist the freshest playback position before the intentional supervisor restart path."""
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    for guild_id, data in list(playback_tracking.items()):
                        try:
                            position = current_track_position(guild_id)
                            paused = bool(data.get('paused'))
                            await persist_playback_checkpoint(
                                cur,
                                guild_id,
                                data,
                                position,
                                channel_id=data.get('channel_id'),
                                playing=not paused,
                                paused=paused,
                                connected=True,
                            )
                            await save_state(guild_id)
                        except Exception as tx_error:
                            logger.debug("[%s] Failed to flush playback position for guild %s before %s.", BOT_ENV_PREFIX.lower(), guild_id, reason, exc_info=True)
    except Exception as tx_error:
        logger.debug("[%s] Runtime flush skipped before %s.", BOT_ENV_PREFIX.lower(), reason, exc_info=True)

async def close_shared_runtime_resources():
    try:
        session = HTTPSessionManager._session
        if session and not session.closed:
            await session.close()
    except Exception as tx_error:
        logger.debug("[%s] Failed closing HTTP session.", BOT_ENV_PREFIX.lower(), exc_info=True)
    try:
        pool = DBPoolManager._pool
        if pool is not None and not getattr(pool, "closed", False):
            pool.close()
            await pool.wait_closed()
        DBPoolManager._pool = None
    except Exception as tx_error:
        logger.debug("[%s] Failed closing DB pool.", BOT_ENV_PREFIX.lower(), exc_info=True)

async def request_supervisor_restart(reason: str, *, announce: bool = True):
    logger.warning("[%s] Restart requested (%s); exiting for container supervisor restart.", BOT_ENV_PREFIX.lower(), reason)
    # Same hang risk as flush_and_close_for_shutdown: this is called from background
    # loops (periodic_restart_loop, the override listener) with no outer try/except,
    # so an unhandled error here would both skip os._exit(0) (process hangs instead of
    # restarting) and silently kill the calling @tasks.loop forever. Catch broadly and
    # always fall through to close/exit.
    try:
        await flush_runtime_state_before_restart(reason)
    except Exception:
        logger.exception("[%s] Failed to flush runtime state before restart (%s).", BOT_ENV_PREFIX.lower(), reason)
    if announce:
        try:
            await send_webhook_log(
                bot.user.name if bot.user else BOT_ENV_PREFIX.capitalize(),
                "♻️ Node Restart",
                f"This node is restarting for **{reason.replace('_', ' ')}**. Docker will bring it back online automatically.",
                discord.Color.orange(),
            )
        except Exception as tx_error:
            logger.debug("[%s] Failed to publish restart webhook.", BOT_ENV_PREFIX.lower(), exc_info=True)
    try:
        await bot.close()
    except Exception:
        pass
    try:
        await close_shared_runtime_resources()
    except Exception:
        pass
    os._exit(0)


shutdown_flush_started = False
shutdown_signal_handlers_installed = False

async def flush_and_close_for_shutdown(reason: str = "signal"):
    global shutdown_flush_started
    if shutdown_flush_started:
        return
    shutdown_flush_started = True
    import os
    logger.warning("[%s] Shutdown signal received; flushing playback checkpoints before container exits.", BOT_ENV_PREFIX.lower())
    try:
        await asyncio.wait_for(flush_runtime_state_before_restart(reason), timeout=SHUTDOWN_POSITION_FLUSH_TIMEOUT_SECONDS)
    except Exception as tx_error:
        logger.exception("[%s] Timed out or failed while flushing playback checkpoints for %s.", BOT_ENV_PREFIX.lower(), reason)
    try:
        await bot.close()
    except Exception as tx_error:
        pass
    try:
        await close_shared_runtime_resources()
    except Exception as tx_error:
        pass
    # Always exit, even if resource cleanup raised — otherwise the process can hang
    # after a shutdown signal until the supervisor force-kills it (losing the flush
    # we just performed and starving the next restart cycle).
    os._exit(0)


def install_shutdown_signal_handlers():
    global shutdown_signal_handlers_installed
    if shutdown_signal_handlers_installed:
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    for sig_name in ("SIGTERM", "SIGINT"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(
                sig,
                lambda name=sig_name: schedule_named_task(
                    f"shutdown_flush:{name}",
                    flush_and_close_for_shutdown(name),
                    overwrite=True,
                ),
            )
        except (NotImplementedError, RuntimeError, ValueError):
            continue
    shutdown_signal_handlers_installed = True



vote_skip_sessions = {}
metrics_last_errors = {}
METRICS_HEARTBEAT_INTERVAL = max(5, int(os.getenv(f"{BOT_ENV_PREFIX}_METRICS_HEARTBEAT_INTERVAL", os.getenv("METRICS_HEARTBEAT_INTERVAL", "15"))))
VOICE_REJOIN_DELAY_SECONDS = max(1, int(os.getenv(f"{BOT_ENV_PREFIX}_VOICE_REJOIN_DELAY_SECONDS", os.getenv("VOICE_REJOIN_DELAY_SECONDS", "2"))))
VOICE_CONNECT_TIMEOUT_SECONDS = max(5.0, float(os.getenv(f"{BOT_ENV_PREFIX}_VOICE_CONNECT_TIMEOUT_SECONDS", os.getenv("VOICE_CONNECT_TIMEOUT_SECONDS", "10"))))
VOICE_CONNECT_TIMEOUT_MAX_SECONDS = max(
    15.0,
    float(os.getenv(f"{BOT_ENV_PREFIX}_VOICE_CONNECT_TIMEOUT_MAX_SECONDS", os.getenv("VOICE_CONNECT_TIMEOUT_MAX_SECONDS", "30"))),
)
if VOICE_CONNECT_TIMEOUT_SECONDS > VOICE_CONNECT_TIMEOUT_MAX_SECONDS:
    logger.warning(
        "[%s] Voice connect timeout %.1fs exceeds configured max %.1fs; clamping to keep recovery responsive.",
        BOT_ENV_PREFIX.lower(),
        VOICE_CONNECT_TIMEOUT_SECONDS,
        VOICE_CONNECT_TIMEOUT_MAX_SECONDS,
    )
    VOICE_CONNECT_TIMEOUT_SECONDS = VOICE_CONNECT_TIMEOUT_MAX_SECONDS
VOICE_CONNECT_TIMEOUT_BACKOFF_SECONDS = max(
    2.0,
    float(os.getenv(f"{BOT_ENV_PREFIX}_VOICE_CONNECT_TIMEOUT_BACKOFF_SECONDS", os.getenv("VOICE_CONNECT_TIMEOUT_BACKOFF_SECONDS", "5"))),
)
VOICE_CONNECT_QUEUE_RETRY_ENABLED = os.getenv(
    f"{BOT_ENV_PREFIX}_VOICE_CONNECT_QUEUE_RETRY_ENABLED",
    os.getenv("VOICE_CONNECT_QUEUE_RETRY_ENABLED", "true"),
).strip().lower() not in {"0", "false", "off", "no"}
VOICE_CONNECT_QUEUE_RETRY_BACKOFF_SECONDS = max(
    3.0,
    float(os.getenv(
        f"{BOT_ENV_PREFIX}_VOICE_CONNECT_QUEUE_RETRY_BACKOFF_SECONDS",
        os.getenv("VOICE_CONNECT_QUEUE_RETRY_BACKOFF_SECONDS", "5"),
    )),
)
VOICE_CONNECT_AUTOMATIC_RECOVERY_SNOOZE_SECONDS = max(
    VOICE_CONNECT_QUEUE_RETRY_BACKOFF_SECONDS,
    float(os.getenv(
        f"{BOT_ENV_PREFIX}_VOICE_CONNECT_AUTOMATIC_RECOVERY_SNOOZE_SECONDS",
        os.getenv("VOICE_CONNECT_AUTOMATIC_RECOVERY_SNOOZE_SECONDS", "10"),
    )),
)
RESILIENCE_STUCK_QUEUE_RETRY_SECONDS = max(
    5.0,
    float(os.getenv(f"{BOT_ENV_PREFIX}_RESILIENCE_STUCK_QUEUE_RETRY_SECONDS", os.getenv("RESILIENCE_STUCK_QUEUE_RETRY_SECONDS", "10"))),
)
LAVALINK_HEALTH_STARTUP_GRACE_SECONDS = max(5.0, float(os.getenv(f"{BOT_ENV_PREFIX}_LAVALINK_HEALTH_STARTUP_GRACE_SECONDS", os.getenv("LAVALINK_HEALTH_STARTUP_GRACE_SECONDS", "25"))))
STARTUP_RECOVERY_JITTER_SECONDS = max(0.0, float(os.getenv(f"{BOT_ENV_PREFIX}_STARTUP_RECOVERY_JITTER_SECONDS", os.getenv("STARTUP_RECOVERY_JITTER_SECONDS", "4"))))
VOICE_REJOIN_JITTER_MIN_SECONDS = max(0.0, float(os.getenv(f"{BOT_ENV_PREFIX}_VOICE_REJOIN_JITTER_MIN_SECONDS", os.getenv("VOICE_REJOIN_JITTER_MIN_SECONDS", "0"))))
VOICE_REJOIN_JITTER_MAX_SECONDS = max(VOICE_REJOIN_JITTER_MIN_SECONDS, float(os.getenv(f"{BOT_ENV_PREFIX}_VOICE_REJOIN_JITTER_MAX_SECONDS", os.getenv("VOICE_REJOIN_JITTER_MAX_SECONDS", "3"))))
RECOVERY_RETRY_JITTER_SECONDS = max(0.0, float(os.getenv(f"{BOT_ENV_PREFIX}_RECOVERY_RETRY_JITTER_SECONDS", os.getenv("RECOVERY_RETRY_JITTER_SECONDS", "3"))))
VOICE_DISCONNECT_GRACE_SECONDS = max(1.0, float(os.getenv(f"{BOT_ENV_PREFIX}_VOICE_DISCONNECT_GRACE_SECONDS", os.getenv("VOICE_DISCONNECT_GRACE_SECONDS", "2"))))
VOICE_DISCONNECT_GRACE_JITTER_SECONDS = max(0.0, float(os.getenv(f"{BOT_ENV_PREFIX}_VOICE_DISCONNECT_GRACE_JITTER_SECONDS", os.getenv("VOICE_DISCONNECT_GRACE_JITTER_SECONDS", "0.5"))))
SOFT_VOICE_DISCONNECT_RECOVERY = os.getenv(f"{BOT_ENV_PREFIX}_SOFT_VOICE_DISCONNECT_RECOVERY", os.getenv("SOFT_VOICE_DISCONNECT_RECOVERY", "true")).strip().lower() not in {"0", "false", "off", "no"}
VOICE_DISCONNECT_REJOIN_RECOVERY = os.getenv(f"{BOT_ENV_PREFIX}_VOICE_DISCONNECT_REJOIN_RECOVERY", os.getenv("VOICE_DISCONNECT_REJOIN_RECOVERY", "true")).strip().lower() not in {"0", "false", "off", "no"}
TARGETED_TRACK_INTERRUPT_RECOVERY = os.getenv(
    f"{BOT_ENV_PREFIX}_TARGETED_TRACK_INTERRUPT_RECOVERY",
    os.getenv("TARGETED_TRACK_INTERRUPT_RECOVERY", "true"),
).strip().lower() not in {"0", "false", "off", "no"}
INTERRUPT_RESUME_GRACE_SECONDS = max(
    1.0,
    float(os.getenv(f"{BOT_ENV_PREFIX}_INTERRUPT_RESUME_GRACE_SECONDS", os.getenv("INTERRUPT_RESUME_GRACE_SECONDS", "2"))),
)
INTERRUPT_RESUME_MIN_POSITION_SECONDS = max(
    1,
    int(os.getenv(f"{BOT_ENV_PREFIX}_INTERRUPT_RESUME_MIN_POSITION_SECONDS", os.getenv("INTERRUPT_RESUME_MIN_POSITION_SECONDS", "5"))),
)
INTERRUPT_RESUME_COOLDOWN_SECONDS = max(
    INTERRUPT_RESUME_GRACE_SECONDS,
    float(os.getenv(f"{BOT_ENV_PREFIX}_INTERRUPT_RESUME_COOLDOWN_SECONDS", os.getenv("INTERRUPT_RESUME_COOLDOWN_SECONDS", "20"))),
)
PERSISTENT_VOICE_RESTORE_ON_STARTUP = os.getenv(f"{BOT_ENV_PREFIX}_PERSISTENT_VOICE_RESTORE_ON_STARTUP", os.getenv("PERSISTENT_VOICE_RESTORE_ON_STARTUP", "true")).strip().lower() not in {"0", "false", "off", "no"}
VOICE_FORCE_STALE_CLIENT_REJOIN = os.getenv(f"{BOT_ENV_PREFIX}_VOICE_FORCE_STALE_CLIENT_REJOIN", os.getenv("VOICE_FORCE_STALE_CLIENT_REJOIN", "false")).strip().lower() not in {"0", "false", "off", "no"}
VOICE_IDLE_REJOIN_RECOVERY = os.getenv(f"{BOT_ENV_PREFIX}_VOICE_IDLE_REJOIN_RECOVERY", os.getenv("VOICE_IDLE_REJOIN_RECOVERY", "false")).strip().lower() not in {"0", "false", "off", "no"}

# Aria recovery authority is optional. Direct-order controls keep RECOVER/doctoring
# idempotent and prevent duplicate workers/restarts from grabbing the same order.
ARIA_RECOVERY_AUTHORITY = os.getenv(f"{BOT_ENV_PREFIX}_ARIA_RECOVERY_AUTHORITY", os.getenv("ARIA_RECOVERY_AUTHORITY", "false")).strip().lower() not in {"0", "false", "off", "no"}
BOT_SELF_HEAL_WHEN_ARIA_AUTHORITY = os.getenv(
    f"{BOT_ENV_PREFIX}_BOT_SELF_HEAL_WHEN_ARIA_AUTHORITY",
    os.getenv("BOT_SELF_HEAL_WHEN_ARIA_AUTHORITY", "false"),
).strip().lower() not in {"0", "false", "off", "no"}
QUEUE_PARITY_REPAIR_COOLDOWN_SECONDS = max(
    30.0,
    float(os.getenv(f"{BOT_ENV_PREFIX}_QUEUE_PARITY_REPAIR_COOLDOWN_SECONDS", os.getenv("QUEUE_PARITY_REPAIR_COOLDOWN_SECONDS", "180"))),
)
QUEUE_PARITY_REPAIR_HASH_TTL_SECONDS = max(
    QUEUE_PARITY_REPAIR_COOLDOWN_SECONDS,
    float(os.getenv(f"{BOT_ENV_PREFIX}_QUEUE_PARITY_REPAIR_HASH_TTL_SECONDS", os.getenv("QUEUE_PARITY_REPAIR_HASH_TTL_SECONDS", "900"))),
)
QUEUE_PARITY_REPAIR_MAX_ROWS = max(
    1,
    int(os.getenv(f"{BOT_ENV_PREFIX}_QUEUE_PARITY_REPAIR_MAX_ROWS", os.getenv("QUEUE_PARITY_REPAIR_MAX_ROWS", "25"))),
)
QUEUE_SPREAD_EXACT_GAP = max(
    1,
    int(os.getenv(f"{BOT_ENV_PREFIX}_QUEUE_SPREAD_EXACT_GAP", os.getenv("QUEUE_SPREAD_EXACT_GAP", "8"))),
)
QUEUE_SPREAD_FAMILY_GAP = max(
    QUEUE_SPREAD_EXACT_GAP,
    int(os.getenv(f"{BOT_ENV_PREFIX}_QUEUE_SPREAD_FAMILY_GAP", os.getenv("QUEUE_SPREAD_FAMILY_GAP", "4"))),
)
QUEUE_SPREAD_HISTORY_WINDOW = max(
    QUEUE_SPREAD_FAMILY_GAP + 1,
    int(os.getenv(f"{BOT_ENV_PREFIX}_QUEUE_SPREAD_HISTORY_WINDOW", os.getenv("QUEUE_SPREAD_HISTORY_WINDOW", "8"))),
)
QUEUE_SPREAD_REORDER_LIMIT = max(
    24,
    int(os.getenv(f"{BOT_ENV_PREFIX}_QUEUE_SPREAD_REORDER_LIMIT", os.getenv("QUEUE_SPREAD_REORDER_LIMIT", "240"))),
)
FEEDBACK_NOTICE_TTL_SECONDS = max(
    15.0,
    float(os.getenv(f"{BOT_ENV_PREFIX}_FEEDBACK_NOTICE_TTL_SECONDS", os.getenv("FEEDBACK_NOTICE_TTL_SECONDS", "90"))),
)
TRACK_FEEDBACK_BROADCAST = os.getenv(
    f"{BOT_ENV_PREFIX}_TRACK_FEEDBACK_BROADCAST",
    os.getenv("TRACK_FEEDBACK_BROADCAST", "true"),
).strip().lower() not in {"0", "false", "off", "no"}
DISCORD_COMMAND_SYNC_ON_STARTUP = os.getenv(
    f"{BOT_ENV_PREFIX}_DISCORD_COMMAND_SYNC_ON_STARTUP",
    os.getenv("DISCORD_COMMAND_SYNC_ON_STARTUP", "false"),
).strip().lower() not in {"0", "false", "off", "no"}
DISCORD_COMMAND_SYNC_STAGGER_SECONDS = max(
    0.0,
    float(os.getenv(f"{BOT_ENV_PREFIX}_DISCORD_COMMAND_SYNC_STAGGER_SECONDS", os.getenv("DISCORD_COMMAND_SYNC_STAGGER_SECONDS", "300"))),
)
DIRECT_ORDER_FETCH_LIMIT = max(1, int(os.getenv(f"{BOT_ENV_PREFIX}_DIRECT_ORDER_FETCH_LIMIT", os.getenv("DIRECT_ORDER_FETCH_LIMIT", "8"))))
DIRECT_ORDER_MAX_ATTEMPTS = max(1, int(os.getenv(f"{BOT_ENV_PREFIX}_DIRECT_ORDER_MAX_ATTEMPTS", os.getenv("DIRECT_ORDER_MAX_ATTEMPTS", "3"))))
DIRECT_ORDER_CLAIM_TIMEOUT_SECONDS = max(10, int(os.getenv(f"{BOT_ENV_PREFIX}_DIRECT_ORDER_CLAIM_TIMEOUT_SECONDS", os.getenv("DIRECT_ORDER_CLAIM_TIMEOUT_SECONDS", "90"))))
DIRECT_ORDER_RETRY_DELAY_SECONDS = max(5, int(os.getenv(f"{BOT_ENV_PREFIX}_DIRECT_ORDER_RETRY_DELAY_SECONDS", os.getenv("DIRECT_ORDER_RETRY_DELAY_SECONDS", "20"))))
DIRECT_ORDER_RETRY_BACKDATE_SECONDS = max(0, DIRECT_ORDER_CLAIM_TIMEOUT_SECONDS - DIRECT_ORDER_RETRY_DELAY_SECONDS)
DIRECT_ORDER_STALE_SECONDS = max(300, int(os.getenv(f"{BOT_ENV_PREFIX}_DIRECT_ORDER_STALE_SECONDS", os.getenv("DIRECT_ORDER_STALE_SECONDS", "900"))))
DIRECT_ORDER_DB_RETRY_ATTEMPTS = max(1, int(os.getenv(f"{BOT_ENV_PREFIX}_DIRECT_ORDER_DB_RETRY_ATTEMPTS", os.getenv("DIRECT_ORDER_DB_RETRY_ATTEMPTS", "4"))))
DIRECT_ORDER_DB_RETRY_BASE_DELAY_SECONDS = max(0.05, float(os.getenv(f"{BOT_ENV_PREFIX}_DIRECT_ORDER_DB_RETRY_BASE_DELAY_SECONDS", os.getenv("DIRECT_ORDER_DB_RETRY_BASE_DELAY_SECONDS", "0.25"))))
DIRECT_ORDER_POLL_INTERVAL_SECONDS = max(5.0, float(os.getenv(f"{BOT_ENV_PREFIX}_DIRECT_ORDER_POLL_INTERVAL_SECONDS", os.getenv("DIRECT_ORDER_POLL_INTERVAL_SECONDS", "8.0"))))
SWARM_BRIDGE_DB_ERROR_LOG_INTERVAL_SECONDS = max(
    30.0,
    float(os.getenv(f"{BOT_ENV_PREFIX}_SWARM_BRIDGE_DB_ERROR_LOG_INTERVAL_SECONDS", os.getenv("SWARM_BRIDGE_DB_ERROR_LOG_INTERVAL_SECONDS", "120"))),
)
SWARM_COMMAND_TABLES_RECHECK_SECONDS = max(
    30.0,
    float(os.getenv(f"{BOT_ENV_PREFIX}_SWARM_COMMAND_TABLES_RECHECK_SECONDS", os.getenv("SWARM_COMMAND_TABLES_RECHECK_SECONDS", "60"))),
)
_last_swarm_bridge_db_error_log_at = 0.0
swarm_command_tables_ready = False
swarm_command_tables_retry_after = 0.0
swarm_command_tables_lock = asyncio.Lock()

# Discord can rate-limit /users/@me when all swarm containers login at once
# or when Docker restart:always creates a fast crash loop. These delays keep
# failures inside the process long enough to stop a login stampede.
BOT_LOGIN_STARTUP_JITTER_SECONDS = max(0.0, float(os.getenv(f"{BOT_ENV_PREFIX}_BOT_LOGIN_STARTUP_JITTER_SECONDS", os.getenv("BOT_LOGIN_STARTUP_JITTER_SECONDS", "20"))))
BOT_LOGIN_STAGGER_SLOT_SECONDS = max(0.0, float(os.getenv(f"{BOT_ENV_PREFIX}_BOT_LOGIN_STAGGER_SLOT_SECONDS", os.getenv("BOT_LOGIN_STAGGER_SLOT_SECONDS", "35"))))
BOT_LOGIN_STAGGER_MAX_SECONDS = max(0.0, float(os.getenv(f"{BOT_ENV_PREFIX}_BOT_LOGIN_STAGGER_MAX_SECONDS", os.getenv("BOT_LOGIN_STAGGER_MAX_SECONDS", "420"))))
BOT_LOGIN_FAILURE_SLEEP_SECONDS = max(60.0, float(os.getenv(f"{BOT_ENV_PREFIX}_BOT_LOGIN_FAILURE_SLEEP_SECONDS", os.getenv("BOT_LOGIN_FAILURE_SLEEP_SECONDS", "300"))))
BOT_LOGIN_FAILURE_JITTER_SECONDS = max(0.0, float(os.getenv(f"{BOT_ENV_PREFIX}_BOT_LOGIN_FAILURE_JITTER_SECONDS", os.getenv("BOT_LOGIN_FAILURE_JITTER_SECONDS", "120"))))
BOT_LOGIN_FAILURE_BACKOFF_FACTOR = max(1.0, float(os.getenv(f"{BOT_ENV_PREFIX}_BOT_LOGIN_FAILURE_BACKOFF_FACTOR", os.getenv("BOT_LOGIN_FAILURE_BACKOFF_FACTOR", "1.8"))))
BOT_LOGIN_FAILURE_MAX_SLEEP_SECONDS = max(BOT_LOGIN_FAILURE_SLEEP_SECONDS, float(os.getenv(f"{BOT_ENV_PREFIX}_BOT_LOGIN_FAILURE_MAX_SLEEP_SECONDS", os.getenv("BOT_LOGIN_FAILURE_MAX_SLEEP_SECONDS", "3600"))))
GLOBAL_DISCORD_LOGIN_GATE_ENABLED = os.getenv(f"{BOT_ENV_PREFIX}_GLOBAL_DISCORD_LOGIN_GATE_ENABLED", os.getenv("GLOBAL_DISCORD_LOGIN_GATE_ENABLED", "true")).strip().lower() not in {"0", "false", "off", "no"}
GLOBAL_DISCORD_LOGIN_MIN_INTERVAL_SECONDS = max(30.0, float(os.getenv(f"{BOT_ENV_PREFIX}_GLOBAL_DISCORD_LOGIN_MIN_INTERVAL_SECONDS", os.getenv("GLOBAL_DISCORD_LOGIN_MIN_INTERVAL_SECONDS", "150"))))
GLOBAL_DISCORD_LOGIN_JITTER_SECONDS = max(0.0, float(os.getenv(f"{BOT_ENV_PREFIX}_GLOBAL_DISCORD_LOGIN_JITTER_SECONDS", os.getenv("GLOBAL_DISCORD_LOGIN_JITTER_SECONDS", "45"))))
GLOBAL_DISCORD_LOGIN_PRESSURE_COOLDOWN_SECONDS = max(300.0, float(os.getenv(f"{BOT_ENV_PREFIX}_GLOBAL_DISCORD_LOGIN_PRESSURE_COOLDOWN_SECONDS", os.getenv("GLOBAL_DISCORD_LOGIN_PRESSURE_COOLDOWN_SECONDS", "2700"))))
GLOBAL_DISCORD_LOGIN_COOLDOWN_POLL_SECONDS = max(30.0, float(os.getenv(f"{BOT_ENV_PREFIX}_GLOBAL_DISCORD_LOGIN_COOLDOWN_POLL_SECONDS", os.getenv("GLOBAL_DISCORD_LOGIN_COOLDOWN_POLL_SECONDS", "120"))))
GLOBAL_DISCORD_LOGIN_LOCK_POLL_SECONDS = max(1.0, float(os.getenv(f"{BOT_ENV_PREFIX}_GLOBAL_DISCORD_LOGIN_LOCK_POLL_SECONDS", os.getenv("GLOBAL_DISCORD_LOGIN_LOCK_POLL_SECONDS", "2"))))
GLOBAL_DISCORD_LOGIN_LOCK_STALE_SECONDS = max(30.0, float(os.getenv(f"{BOT_ENV_PREFIX}_GLOBAL_DISCORD_LOGIN_LOCK_STALE_SECONDS", os.getenv("GLOBAL_DISCORD_LOGIN_LOCK_STALE_SECONDS", "300"))))
GLOBAL_DISCORD_LOGIN_GATE_MAX_WAIT_SECONDS = max(60.0, float(os.getenv(f"{BOT_ENV_PREFIX}_GLOBAL_DISCORD_LOGIN_GATE_MAX_WAIT_SECONDS", os.getenv("GLOBAL_DISCORD_LOGIN_GATE_MAX_WAIT_SECONDS", "900"))))
MUSIC_BOT_RUNTIME_DIR = os.getenv(f"{BOT_ENV_PREFIX}_RUNTIME_DIR", os.getenv("MUSIC_BOT_RUNTIME_DIR", "/app/.runtime"))
BOT_STAGGER_SLOTS = {
    "GWS": 0, "HARMONIC": 1, "MAESTRO": 2, "MELODIC": 3, "NEXUS": 4, "RHYTHM": 5,
    "SYMPHONY": 6, "TUNESTREAM": 7, "ALUCARD": 8, "SAPPHIRE": 9, "STRIFE": 10, "LOCKHART": 11,
    "GLITCH": 12,
}


def _runtime_path(name: str) -> str:
    try:
        os.makedirs(MUSIC_BOT_RUNTIME_DIR, exist_ok=True)
    except Exception as tx_error:
        pass
    return os.path.join(MUSIC_BOT_RUNTIME_DIR, name)



def _runtime_file_float(path: str, default: float = 0.0) -> float:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return float((handle.read() or "0").strip() or "0")
    except Exception as tx_error:
        return default


def _runtime_write_float(path: str, value: float) -> None:
    try:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(str(float(value)))
    except Exception as tx_error:
        logger.debug("[%s] Could not write runtime float file %s", BOT_ENV_PREFIX.lower(), path, exc_info=True)


def _login_gate_clock() -> float:
    """Monotonic clock that keeps advancing across host suspend/sleep.

    time.monotonic() pauses while the host is suspended, which let the
    shared login gate's max-wait timeout stall for hours after a host
    sleep/wake cycle. CLOCK_BOOTTIME includes suspended time on Linux.
    """
    try:
        return time.clock_gettime(time.CLOCK_BOOTTIME)
    except (AttributeError, OSError):
        return time.monotonic()


def _global_login_next_path() -> str:
    return _runtime_path("swarm_next_discord_login_at.txt")


def _global_login_cooldown_path() -> str:
    return _runtime_path("swarm_discord_login_cooldown_until.txt")


def _global_login_lock_dir() -> str:
    return _runtime_path("swarm_discord_login_gate.lock")


def _is_discord_login_pressure_error(exc: Exception | None) -> bool:
    text = (str(exc or "") + " " + exc.__class__.__name__).lower()
    return any(token in text for token in (
        "429", "too many requests", "40062", "rate limited",
        "503", "504", "no healthy upstream", "overflow", "cloudflare", "service unavailable",
    ))


def _set_global_discord_login_cooldown(seconds: float, reason: str = "discord_login_pressure") -> None:
    until = time.time() + max(0.0, seconds)
    current = _runtime_file_float(_global_login_cooldown_path(), 0.0)
    if until > current:
        _runtime_write_float(_global_login_cooldown_path(), until)
        logger.warning(
            "[%s] Global Discord login cooldown armed for %.0fs after %s. Shared runtime dir: %s",
            BOT_ENV_PREFIX.lower(), max(0.0, until - time.time()), reason, MUSIC_BOT_RUNTIME_DIR,
        )


def _wait_for_global_discord_login_gate() -> None:
    """Serialize Discord /users/@me login attempts across all music bot containers.

    The runtime directory is mounted from ./runtime into every bot container.  A
    tiny directory lock is enough here because we only protect the startup login
    attempt; the goal is to avoid all ten tokens hitting Discord's login route
    together after rebuilds or Docker restart loops.
    """
    if not GLOBAL_DISCORD_LOGIN_GATE_ENABLED:
        return

    warned_shared = False
    wait_started_at = _login_gate_clock()
    while True:
        elapsed = _login_gate_clock() - wait_started_at
        if elapsed >= GLOBAL_DISCORD_LOGIN_GATE_MAX_WAIT_SECONDS:
            logger.warning(
                "[%s] Shared Discord login gate max wait %.0fs exceeded; continuing so the bot does not stall forever.",
                BOT_ENV_PREFIX.lower(), GLOBAL_DISCORD_LOGIN_GATE_MAX_WAIT_SECONDS,
            )
            return

        cooldown_until = _runtime_file_float(_global_login_cooldown_path(), 0.0)
        if cooldown_until > time.time():
            wait_for = min(GLOBAL_DISCORD_LOGIN_COOLDOWN_POLL_SECONDS, cooldown_until - time.time())
            logger.warning(
                "[%s] Waiting %.0fs for shared Discord login cooldown to clear before login.",
                BOT_ENV_PREFIX.lower(), max(1.0, cooldown_until - time.time()),
            )
            time.sleep(max(1.0, min(wait_for, max(0.0, GLOBAL_DISCORD_LOGIN_GATE_MAX_WAIT_SECONDS - elapsed))))
            continue

        lock_dir = _global_login_lock_dir()
        try:
            os.mkdir(lock_dir)
        except FileExistsError:
            # A crashed container can leave the gate behind; clear it when stale.
            try:
                age = time.time() - os.path.getmtime(lock_dir)
                if age > GLOBAL_DISCORD_LOGIN_LOCK_STALE_SECONDS:
                    os.rmdir(lock_dir)
                    continue
            except Exception as tx_error:
                pass
            if not warned_shared:
                logger.info("[%s] Another music bot is reserving the Discord login gate; waiting.", BOT_ENV_PREFIX.lower())
                warned_shared = True
            time.sleep(min(GLOBAL_DISCORD_LOGIN_LOCK_POLL_SECONDS, max(1.0, GLOBAL_DISCORD_LOGIN_GATE_MAX_WAIT_SECONDS - elapsed)))
            continue
        except Exception as tx_error:
            logger.warning("[%s] Could not create shared login gate in %s; continuing with local-only stagger.", BOT_ENV_PREFIX.lower(), MUSIC_BOT_RUNTIME_DIR, exc_info=True)
            return

        try:
            next_allowed = _runtime_file_float(_global_login_next_path(), 0.0)
            now = _login_gate_clock()
            if next_allowed > now:
                wait_for = min(next_allowed - now, max(0.0, GLOBAL_DISCORD_LOGIN_GATE_MAX_WAIT_SECONDS - (_login_gate_clock() - wait_started_at)))
                if wait_for <= 0:
                    logger.warning(
                        "[%s] Shared Discord login gate spacing wait exceeded max wait; continuing without extra delay.",
                        BOT_ENV_PREFIX.lower(),
                    )
                else:
                    logger.info("[%s] Shared Discord login gate waiting %.1fs for previous bot attempt spacing.", BOT_ENV_PREFIX.lower(), wait_for)
                    time.sleep(wait_for)
                now = _login_gate_clock()
            reserve_until = now + GLOBAL_DISCORD_LOGIN_MIN_INTERVAL_SECONDS + random.uniform(0.0, GLOBAL_DISCORD_LOGIN_JITTER_SECONDS)
            _runtime_write_float(_global_login_next_path(), reserve_until)
            logger.info(
                "[%s] Shared Discord login gate reserved; next bot login allowed in %.1fs.",
                BOT_ENV_PREFIX.lower(), max(0.0, reserve_until - now),
            )
            return
        finally:
            try:
                os.rmdir(lock_dir)
            except Exception as tx_error:
                pass

def _login_failure_counter_path() -> str:
    return _runtime_path(f"{BOT_ENV_PREFIX.lower()}_login_failures.txt")

def _read_login_failure_count() -> int:
    try:
        with open(_login_failure_counter_path(), "r", encoding="utf-8") as handle:
            return max(0, int((handle.read() or "0").strip() or "0"))
    except Exception as tx_error:
        return 0

def _write_login_failure_count(count: int) -> None:
    try:
        with open(_login_failure_counter_path(), "w", encoding="utf-8") as handle:
            handle.write(str(max(0, int(count))))
    except Exception as tx_error:
        logger.debug("[%s] Could not persist login failure count.", BOT_ENV_PREFIX.lower(), exc_info=True)

def reset_login_failure_backoff() -> None:
    _write_login_failure_count(0)

def compute_login_startup_delay() -> float:
    slot = BOT_STAGGER_SLOTS.get(BOT_ENV_PREFIX.upper(), 10)
    deterministic = min(BOT_LOGIN_STAGGER_MAX_SECONDS, slot * BOT_LOGIN_STAGGER_SLOT_SECONDS)
    return deterministic + random.uniform(0.0, BOT_LOGIN_STARTUP_JITTER_SECONDS)

def compute_login_failure_delay(exc: Exception | None = None) -> float:
    failures = _read_login_failure_count() + 1
    _write_login_failure_count(failures)
    if _is_discord_login_pressure_error(exc):
        _set_global_discord_login_cooldown(GLOBAL_DISCORD_LOGIN_PRESSURE_COOLDOWN_SECONDS, reason=exc.__class__.__name__)
    exponential = BOT_LOGIN_FAILURE_SLEEP_SECONDS * (BOT_LOGIN_FAILURE_BACKOFF_FACTOR ** max(0, failures - 1))
    capped = min(BOT_LOGIN_FAILURE_MAX_SLEEP_SECONDS, exponential)
    # Pressure errors should wait at least the shared cooldown window locally too; otherwise
    # Docker restart:always can keep re-entering the login route while Discord is angry.
    if _is_discord_login_pressure_error(exc):
        capped = max(capped, min(BOT_LOGIN_FAILURE_MAX_SLEEP_SECONDS, GLOBAL_DISCORD_LOGIN_PRESSURE_COOLDOWN_SECONDS))
    return capped + random.uniform(0.0, BOT_LOGIN_FAILURE_JITTER_SECONDS)

def _parse_lavalink_endpoint():
    parsed = urllib.parse.urlparse(LAVALINK_URI)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    return host, port

async def _wait_for_lavalink_tcp(timeout: float = 1.5) -> bool:
    host, port = _parse_lavalink_endpoint()
    try:
        _reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception as tx_error:
            pass
        return True
    except Exception as tx_error:
        return False

def _has_connecting_lavalink_node() -> bool:
    for node in _get_pool_nodes():
        try:
            status = getattr(node, "status", None)
            status_text = str(status).upper()
            if status_text == "CONNECTING" or status_text.endswith(".CONNECTING"):
                return True
        except Exception as tx_error:
            continue
    return False

lavalink_connect_task = None
lavalink_connect_lock = asyncio.Lock()

def _get_pool_nodes():
    try:
        # Wavelink 3.x exposes Pool.nodes as a dictionary of node objects.
        nodes = getattr(wavelink.Pool, "nodes", None)
        if isinstance(nodes, dict):
            return list(nodes.values())
        if isinstance(nodes, (list, tuple, set)):
            return list(nodes)
    except Exception as tx_error:
        logger.debug("[%s] Unable to inspect Wavelink node pool.", BOT_ENV_PREFIX.lower(), exc_info=True)
    return []

def _has_connected_lavalink_node() -> bool:
    try:
        connected_status = getattr(getattr(wavelink, "NodeStatus", None), "CONNECTED", None)
    except Exception as tx_error:
        connected_status = None

    for node in _get_pool_nodes():
        try:
            status = getattr(node, "status", None)
            if connected_status is not None and status == connected_status:
                return True
            status_text = str(status).upper()
            if status_text == "CONNECTED" or status_text.endswith(".CONNECTED"):
                return True
            if getattr(node, "connected", False):
                return True
        except Exception as tx_error:
            continue
    return False

_LAVALINK_SESSION_FILE = os.path.join(
    os.getenv("RUNTIME_DIR", "/app/.runtime"),
    f"lavalink_session_{BOT_ENV_PREFIX.lower()}.txt",
)

def _read_saved_lavalink_session_id():
    try:
        if os.path.exists(_LAVALINK_SESSION_FILE):
            sid = open(_LAVALINK_SESSION_FILE).read().strip()
            return sid if len(sid) > 4 else None
    except Exception:
        pass
    return None

def _save_lavalink_session_id(session_id):
    try:
        os.makedirs(os.path.dirname(_LAVALINK_SESSION_FILE), exist_ok=True)
        with open(_LAVALINK_SESSION_FILE, "w") as f:
            f.write(session_id or "")
    except Exception as e:
        logger.debug("[%s] Could not save Lavalink session ID: %s", BOT_ENV_PREFIX.lower(), e)

async def _connect_lavalink_forever():
    await bot.wait_until_ready()
    async with lavalink_connect_lock:
        while not _has_connected_lavalink_node():
            if _has_connecting_lavalink_node():
                await asyncio.sleep(5)
                continue
            if not await _wait_for_lavalink_tcp():
                logger.info(f"Lavalink is not listening at {LAVALINK_URI} yet; waiting before opening a Wavelink node.")
                await asyncio.sleep(5)
                continue
            try:
                logger.info(f"Connecting to Lavalink at {LAVALINK_URI}")
                node = wavelink.Node(uri=LAVALINK_URI, password=LAVALINK_PASSWORD, identifier=f"SWARM_PRIMARY_{BOT_ENV_PREFIX}", resume_timeout=900)
                saved_session = _read_saved_lavalink_session_id()
                if saved_session:
                    # Pre-populate session ID so Wavelink sends Session-Id header,
                    # allowing Lavalink to resume the frozen player without stopping audio.
                    node._session_id = saved_session
                    logger.info("[%s] Pre-loading saved Lavalink session %s… for potential native resume.", BOT_ENV_PREFIX.lower(), saved_session[:8])
                await wavelink.Pool.connect(nodes=[node], client=bot, cache_capacity=LAVALINK_TRACK_CACHE_CAPACITY)
            except Exception as exc:
                logger.warning(f"Waiting for Lavalink to boot or authenticate... Retrying in 5s ({exc})")
                await asyncio.sleep(5)
            else:
                await asyncio.sleep(2)
                if _has_connected_lavalink_node():
                    break

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
            for guild in list(bot.guilds):
                vc = guild.voice_client
                if vc and not _player_is_active(vc):
                    state = await derive_recovery_state_from_db(guild.id)
                    channel_id = (state or {}).get("voice_channel_id") or getattr(getattr(vc, "channel", None), "id", None)
                    if channel_id:
                        position = int((state or {}).get("position", 0) or 0)
                        await remember_recovery_state(guild.id, channel_id, position, track_uid=(state or {}).get("track_uid"), url=(state or {}).get("url"), title=(state or {}).get("title"))
                        if not ARIA_RECOVERY_AUTHORITY:
                            if position > 0 and (state or {}).get("track_uid") and TARGETED_TRACK_INTERRUPT_RECOVERY:
                                schedule_targeted_interrupt_resume(guild.id, channel_id, start_position=position, reason="lavalink_health")
                            else:
                                schedule_recovery_retry(guild.id, channel_id, start_position=position, reason="lavalink_health")
                        else:
                            logger.info("[%s] Lavalink degraded; preserving state and letting Aria decide recovery.", guild.id)
    except Exception as tx_error:
        logger.exception("[nexus] Lavalink health monitor failed.")

@lavalink_health_monitor.before_loop
async def before_lavalink_health_monitor():
    await bot.wait_until_ready()
    # Avoid a false-positive Lavalink failure while the websocket is still connecting.
    await asyncio.sleep(LAVALINK_HEALTH_STARTUP_GRACE_SECONDS + random.uniform(0.0, STARTUP_RECOVERY_JITTER_SECONDS))

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
    guild_key = getattr(guild, "id", 0) or 0
    cache_key = (guild_key, int(requester_id))
    cached = _cache_get(REQUESTER_NAME_CACHE, cache_key, REQUESTER_NAME_CACHE_TTL_SECONDS)
    if cached:
        return cached
    try:
        member = guild.get_member(requester_id) if guild else None
    except Exception as tx_error:
        member = None
    if member:
        return _cache_set(REQUESTER_NAME_CACHE, cache_key, _safe_display_name(member))
    user = bot.get_user(requester_id)
    if user:
        return _cache_set(REQUESTER_NAME_CACHE, cache_key, _safe_display_name(user))
    try:
        user = await bot.fetch_user(requester_id)
        return _cache_set(REQUESTER_NAME_CACHE, cache_key, _safe_display_name(user))
    except Exception as tx_error:
        fallback = f"User {requester_id}"
        return _cache_set(REQUESTER_NAME_CACHE, cache_key, fallback)


async def get_autodj_enabled(guild_id):
    cached = _cache_get(AUTO_DJ_ENABLED_CACHE, int(guild_id), AUTO_DJ_CACHE_TTL_SECONDS)
    if cached is not None:
        return bool(cached)
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute("SELECT auto_dj FROM nexus_swarm_toggles WHERE guild_id = %s", (guild_id,))
                    row = await cur.fetchone()
                    value = bool(row and row.get('auto_dj'))
                    _cache_set(AUTO_DJ_ENABLED_CACHE, int(guild_id), value)
                    return value
    except Exception as tx_error:
        return False

async def set_autodj_enabled(guild_id, enabled):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute("INSERT INTO nexus_swarm_toggles (guild_id, auto_dj) VALUES (%s, %s) ON DUPLICATE KEY UPDATE auto_dj = VALUES(auto_dj)", (guild_id, bool(enabled)))
    invalidate_feature_caches(guild_id)
    _cache_set(AUTO_DJ_ENABLED_CACHE, int(guild_id), bool(enabled))


async def maybe_enqueue_autodj(cur, guild, channel_id):
    now = time.monotonic()
    if now < AUTODJ_FAIL_UNTIL.get(guild.id, 0):
        return False
    if now - AUTODJ_LAST_RUN.get(guild.id, 0) < AUTODJ_MIN_INTERVAL_SECONDS:
        return False
    if not await get_autodj_enabled(guild.id):
        return False
    AUTODJ_LAST_RUN[guild.id] = now
    listener_ids = _member_ids_from_voice_channel(guild, channel_id)
    try:
        chosen, recommendation = await pick_smart_recommendation_track(cur, guild.id, listener_ids=listener_ids)
        if not chosen:
            AUTODJ_FAIL_UNTIL[guild.id] = time.time() + AUTODJ_FAILURE_BACKOFF_SECONDS
            return False
        requester_id = bot.user.id if bot.user else None
        await enqueue_track(cur, guild.id, chosen.uri, chosen.title, requester_id)
        await record_smart_recommendation(cur, guild.id, requester_id, recommendation, chosen, reason="autodj")
        logger.info("[%s] Auto-DJ queued smart recommendation '%s' from %s.", guild.id, getattr(chosen, "title", "Unknown"), recommendation.get("reason"))
        schedule_named_task(f"autodj_process_queue:{guild.id}", process_queue(guild, channel_id))
        return True
    except Exception as exc:
        AUTODJ_FAIL_UNTIL[guild.id] = time.time() + AUTODJ_FAILURE_BACKOFF_SECONDS
        logger.warning(f"[{guild.id}] Auto-DJ recommendation failed: {exc}")
        return False

async def get_saved_settings_summary(guild_id):
    cached = _cache_get(GUILD_SETTINGS_CACHE, int(guild_id), GUILD_SETTINGS_CACHE_TTL_SECONDS)
    if cached is not None:
        return cached
    await ensure_guild_settings(guild_id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT home_vc_id, volume, loop_mode, filter_mode, dj_role_id, feedback_channel_id, transition_mode, custom_speed, custom_pitch, custom_modifiers_left, dj_only_mode, stay_in_vc FROM nexus_guild_settings WHERE guild_id = %s", (guild_id,))
                row = await cur.fetchone()
                _cache_set(GUILD_SETTINGS_CACHE, int(guild_id), row)
                return row

YOUTUBE_API_KEYS = [k.strip() for k in os.environ.get("YOUTUBE_API_KEY", "").split(",") if k.strip()]
YOUTUBE_API_KEY = YOUTUBE_API_KEYS[0] if YOUTUBE_API_KEYS else ""
_YOUTUBE_PLAYLIST_ID_RE = re.compile(r"[?&]list=([A-Za-z0-9_-]+)")
_YOUTUBE_PLAYLIST_MAX_ENTRIES = int(os.getenv("YOUTUBE_PLAYLIST_MAX_ENTRIES", "1000"))
_YOUTUBE_API_QUOTA_BACKOFF_SECONDS = int(os.getenv("YOUTUBE_API_QUOTA_BACKOFF_SECONDS", "21600"))
_yt_api_key_blocked_until = {}
_PLAYLIST_API_CACHE_SECONDS = float(os.getenv("PLAYLIST_API_CACHE_SECONDS", "1800"))
_PLAYLIST_API_CACHE_MAX = 256
_playlist_api_cache = {}


def _available_youtube_api_key():
    now = time.time()
    for _key in YOUTUBE_API_KEYS:
        if now >= _yt_api_key_blocked_until.get(_key, 0.0):
            return _key
    return None


def _youtube_api_available():
    return _available_youtube_api_key() is not None


def _note_youtube_api_quota_exhausted(key):
    _yt_api_key_blocked_until[key] = time.time() + _YOUTUBE_API_QUOTA_BACKOFF_SECONDS
    logger.warning("[%s] YouTube Data API key …%s quota exhausted; backing off %ss (rotating to next key / yt-dlp+cookies)", BOT_ENV_PREFIX.lower(), (key or "")[-4:], _YOUTUBE_API_QUOTA_BACKOFF_SECONDS)


def _extract_youtube_playlist_id(url):
    match = _YOUTUBE_PLAYLIST_ID_RE.search(str(url or ""))
    return match.group(1) if match else None


async def _resolve_youtube_playlist_via_api(playlist_id):
    """Enumerate a YouTube playlist via the YouTube Data API's playlistItems.list,
    which paginates via nextPageToken with no practical limit -- unlike yt-dlp's
    flat-playlist extraction, which YouTube caps at roughly 205 entries regardless
    of cookies/session for large playlists. Returns [] on any error so the caller
    falls back to yt-dlp."""
    now = time.monotonic()
    cached = _playlist_api_cache.get(playlist_id)
    if cached and cached[0] > now:
        return list(cached[1])
    key = _available_youtube_api_key()
    if not key:
        return []
    entries = []
    page_token = None
    async with aiohttp.ClientSession() as session:
        while len(entries) < _YOUTUBE_PLAYLIST_MAX_ENTRIES:
            params = {
                "part": "snippet",
                "playlistId": playlist_id,
                "maxResults": min(50, _YOUTUBE_PLAYLIST_MAX_ENTRIES - len(entries)),
                "key": key,
            }
            if page_token:
                params["pageToken"] = page_token
            async with session.get(
                "https://www.googleapis.com/youtube/v3/playlistItems",
                params=params,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 403:
                    body = await resp.text()
                    if "quotaExceeded" in body or "dailyLimitExceeded" in body:
                        _note_youtube_api_quota_exhausted(key)
                resp.raise_for_status()
                data = await resp.json()
            for item in data.get("items", []):
                snippet = item.get("snippet") or {}
                video_id = (snippet.get("resourceId") or {}).get("videoId")
                title = snippet.get("title") or ""
                if not video_id or title in ("Deleted video", "Private video"):
                    continue
                entries.append(SimpleNamespace(
                    uri=f"https://www.youtube.com/watch?v={video_id}",
                    title=title,
                ))
            page_token = data.get("nextPageToken")
            if not page_token:
                break
    if entries:
        if len(_playlist_api_cache) >= _PLAYLIST_API_CACHE_MAX:
            for _stale in [k for k, v in list(_playlist_api_cache.items()) if v[0] <= now]:
                _playlist_api_cache.pop(_stale, None)
        _playlist_api_cache[playlist_id] = (now + _PLAYLIST_API_CACHE_SECONDS, list(entries))
    return entries


_YTDLP_COOKIES_SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cookies.txt")
# The cookies file is mounted read-only and shared across all bots, but yt-dlp
# rewrites the cookie jar after each extraction. Stage a private writable copy in
# the container's own /tmp so yt-dlp can refresh it without hitting the read-only
# mount or racing the other bots that write the same shared file.
_YTDLP_COOKIES_FILE = "/tmp/yt-cookies.txt"

ytdl_format_options = {
    'format': 'bestaudio/best', 'outtmpl': '%(extractor)s-%(id)s-%(title)s.%(ext)s',
    'restrictfilenames': True, 'noplaylist': True, 'extract_flat': 'in_playlist', 'skip_download': True, 'nocheckcertificate': True,
    'ignoreerrors': True, 'logtostderr': False, 'quiet': True,
    'no_warnings': True, 'default_search': 'auto', 'source_address': '0.0.0.0',
    'cachedir': YTDLP_CACHE_DIR
}
if os.path.isfile(_YTDLP_COOKIES_SRC) and os.path.getsize(_YTDLP_COOKIES_SRC) > 0:
    # YouTube paginates flat-playlist extraction past the first ~100 entries only
    # for authenticated requests; without cookies, larger playlists silently
    # truncate at the first page.
    try:
        shutil.copyfile(_YTDLP_COOKIES_SRC, _YTDLP_COOKIES_FILE)
        ytdl_format_options['cookiefile'] = _YTDLP_COOKIES_FILE
    except OSError:
        logger.warning("[%s] Could not stage writable YouTube cookies copy", BOT_ENV_PREFIX.lower())



SCHEMA_BOOTSTRAP_EXPECTED_ERROR_CODES = {1050, 1060, 1061, 1062, 1091, 1146}
SCHEMA_BOOTSTRAP_EXPECTED_ERROR_TEXT = (
    "already exists",
    "duplicate column",
    "duplicate key",
    "duplicate entry",
    "unknown column",
    "doesn't exist",
    "does not exist",
    "check that column/key exists",
)


def _mysql_error_code(exc: BaseException) -> int | None:
    args = getattr(exc, "args", ()) or ()
    if args and isinstance(args[0], int):
        return args[0]
    return None


def _is_expected_schema_bootstrap_error(exc: BaseException) -> bool:
    code = _mysql_error_code(exc)
    if code in SCHEMA_BOOTSTRAP_EXPECTED_ERROR_CODES:
        return True
    text = str(exc).lower()
    return any(token in text for token in SCHEMA_BOOTSTRAP_EXPECTED_ERROR_TEXT)


async def safe_schema_execute(cur, sql, params=None, *, label: str = "schema bootstrap") -> bool:
    """Run an idempotent schema/bootstrap statement without hiding real failures.

    Duplicate-column/index/table errors are normal during repeated container rebuilds.
    Everything else is logged with traceback so schema drift stops being invisible.
    """
    preview = " ".join(str(sql).split())[:240]
    try:
        if params is None:
            await cur.execute(sql)
        else:
            await cur.execute(sql, params)
        return True
    except aiomysql.Error as exc:
        if _is_expected_schema_bootstrap_error(exc):
            logger.debug("[%s] %s skipped/already applied: %s | %s", BOT_ENV_PREFIX.lower(), label, exc, preview)
        else:
            logger.warning("[%s] %s failed: %s | %s", BOT_ENV_PREFIX.lower(), label, exc, preview, exc_info=True)
        return False
    except asyncio.TimeoutError:
        logger.warning("[%s] %s timed out: %s", BOT_ENV_PREFIX.lower(), label, preview, exc_info=True)
        return False
    except Exception as exc:
        logger.exception("[%s] Unexpected %s failure: %s | %s", BOT_ENV_PREFIX.lower(), label, exc, preview)
        return False

# --- DATABASE INITIALIZATION ---
async def init_db():
    global playlist_db_initialized
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SET sql_notes=0")
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_playback_state (guild_id BIGINT, bot_name VARCHAR(50), channel_id BIGINT, video_url TEXT, position_seconds INT DEFAULT 0, is_playing BOOLEAN DEFAULT FALSE, is_paused BOOLEAN DEFAULT FALSE, title TEXT, last_checkpoint_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, play_session_key VARCHAR(64) DEFAULT NULL, track_uid CHAR(32) DEFAULT NULL, PRIMARY KEY (guild_id, bot_name))")
                await safe_schema_execute(cur, "ALTER TABLE nexus_playback_state ADD COLUMN title TEXT")
                await safe_schema_execute(cur, "ALTER TABLE nexus_playback_state ADD COLUMN is_paused BOOLEAN DEFAULT FALSE")
                await safe_schema_execute(cur, "ALTER TABLE nexus_playback_state ADD COLUMN last_checkpoint_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
                await safe_schema_execute(cur, "ALTER TABLE nexus_playback_state ADD COLUMN play_session_key VARCHAR(64) DEFAULT NULL")
                await safe_schema_execute(cur, "ALTER TABLE nexus_playback_state ADD COLUMN track_uid CHAR(32) DEFAULT NULL")
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_guild_settings (guild_id BIGINT PRIMARY KEY, home_vc_id BIGINT, volume INT DEFAULT 100, loop_mode VARCHAR(10) DEFAULT 'queue', filter_mode VARCHAR(20) DEFAULT 'none', dj_role_id BIGINT DEFAULT NULL, feedback_channel_id BIGINT DEFAULT NULL, transition_mode VARCHAR(10) DEFAULT 'off', fade_seconds FLOAT DEFAULT 3.0, fade_curve VARCHAR(20) DEFAULT 'linear', custom_speed FLOAT DEFAULT 1.0, custom_pitch FLOAT DEFAULT 1.0, custom_modifiers_left INT DEFAULT 0, dj_only_mode BOOLEAN DEFAULT FALSE, stay_in_vc BOOLEAN DEFAULT FALSE)")
                await safe_schema_execute(cur, "ALTER TABLE nexus_guild_settings MODIFY loop_mode VARCHAR(10) DEFAULT 'queue'")
                await safe_schema_execute(cur, "ALTER TABLE nexus_guild_settings ADD COLUMN fade_seconds FLOAT DEFAULT 3.0")
                await safe_schema_execute(cur, "ALTER TABLE nexus_guild_settings ADD COLUMN fade_curve VARCHAR(20) DEFAULT 'linear'")
                await safe_schema_execute(cur, "UPDATE nexus_guild_settings SET loop_mode = 'queue' WHERE loop_mode IS NULL OR loop_mode NOT IN ('off', 'song', 'queue')")
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_queue (id INT AUTO_INCREMENT PRIMARY KEY, guild_id BIGINT, bot_name VARCHAR(50), video_url TEXT, title TEXT, requester_id BIGINT DEFAULT NULL, track_uid CHAR(32) DEFAULT NULL)")
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_queue_backup (id INT AUTO_INCREMENT PRIMARY KEY, guild_id BIGINT, bot_name VARCHAR(50), video_url TEXT, title TEXT, requester_id BIGINT DEFAULT NULL, track_uid CHAR(32) DEFAULT NULL)")
                await safe_schema_execute(cur, "ALTER TABLE nexus_queue ADD COLUMN bot_name VARCHAR(50)")
                await safe_schema_execute(cur, "ALTER TABLE nexus_queue_backup ADD COLUMN bot_name VARCHAR(50)")
                await safe_schema_execute(cur, "ALTER TABLE nexus_queue ADD COLUMN requester_id BIGINT DEFAULT NULL")
                await safe_schema_execute(cur, "ALTER TABLE nexus_queue_backup ADD COLUMN requester_id BIGINT DEFAULT NULL")
                await safe_schema_execute(cur, "ALTER TABLE nexus_queue ADD COLUMN track_uid CHAR(32) DEFAULT NULL")
                await safe_schema_execute(cur, "ALTER TABLE nexus_queue_backup ADD COLUMN track_uid CHAR(32) DEFAULT NULL")
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_history (id INT AUTO_INCREMENT PRIMARY KEY, guild_id BIGINT, video_url TEXT, title TEXT, played_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, requester_id BIGINT DEFAULT NULL)")
                await safe_schema_execute(cur, "ALTER TABLE nexus_history ADD COLUMN requester_id BIGINT DEFAULT NULL")
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_user_playlists (id INT AUTO_INCREMENT PRIMARY KEY, user_id BIGINT, playlist_name VARCHAR(255), video_url TEXT, title TEXT)")
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_track_intelligence (guild_id BIGINT NOT NULL, url_key VARCHAR(64) NOT NULL, video_url TEXT, title TEXT, queued_count INT NOT NULL DEFAULT 0, play_count INT NOT NULL DEFAULT 0, finish_count INT NOT NULL DEFAULT 0, skip_count INT NOT NULL DEFAULT 0, like_count INT NOT NULL DEFAULT 0, dislike_count INT NOT NULL DEFAULT 0, total_listen_seconds INT NOT NULL DEFAULT 0, last_requester_id BIGINT DEFAULT NULL, source VARCHAR(40) DEFAULT 'unknown', first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP, last_queued TIMESTAMP NULL DEFAULT NULL, last_played TIMESTAMP NULL DEFAULT NULL, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (guild_id, url_key))")
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_user_track_affinity (guild_id BIGINT NOT NULL, user_id BIGINT NOT NULL, url_key VARCHAR(64) NOT NULL, video_url TEXT, title TEXT, queued_count INT NOT NULL DEFAULT 0, play_count INT NOT NULL DEFAULT 0, finish_count INT NOT NULL DEFAULT 0, skip_count INT NOT NULL DEFAULT 0, like_count INT NOT NULL DEFAULT 0, dislike_count INT NOT NULL DEFAULT 0, score FLOAT NOT NULL DEFAULT 0, last_requested TIMESTAMP NULL DEFAULT NULL, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (guild_id, user_id, url_key))")
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_smart_recommendations (id INT AUTO_INCREMENT PRIMARY KEY, guild_id BIGINT NOT NULL, requester_id BIGINT DEFAULT NULL, seed_title TEXT, seed_url TEXT, query_text TEXT, chosen_url TEXT, chosen_title TEXT, reason VARCHAR(80), accepted BOOLEAN DEFAULT TRUE, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_track_embeddings (url_key VARCHAR(64) NOT NULL PRIMARY KEY, title TEXT, video_url TEXT, embedding MEDIUMTEXT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)")
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_track_cooccurrence (guild_id BIGINT NOT NULL, url_key_a VARCHAR(64) NOT NULL, url_key_b VARCHAR(64) NOT NULL, weight FLOAT NOT NULL DEFAULT 0, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (guild_id, url_key_a, url_key_b))")
                await safe_schema_execute(cur, "CREATE INDEX nexus_track_intelligence_recent_idx ON nexus_track_intelligence (guild_id, last_played)")
                await safe_schema_execute(cur, "CREATE INDEX nexus_track_intelligence_requester_idx ON nexus_track_intelligence (guild_id, last_requester_id, last_played)")
                await safe_schema_execute(cur, "CREATE INDEX nexus_user_affinity_recent_idx ON nexus_user_track_affinity (guild_id, user_id, last_requested)")
                await safe_schema_execute(cur, "CREATE INDEX nexus_smart_recommendations_recent_idx ON nexus_smart_recommendations (guild_id, created_at)")
                await safe_schema_execute(cur, "CREATE INDEX nexus_track_intelligence_global_idx ON nexus_track_intelligence (url_key, last_played)")
                await safe_schema_execute(cur, "CREATE INDEX nexus_track_cooccurrence_weight_idx ON nexus_track_cooccurrence (guild_id, url_key_a, weight)")
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_bot_home_channels (guild_id BIGINT, bot_name VARCHAR(50), home_vc_id BIGINT, PRIMARY KEY (guild_id, bot_name))")
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_voice_state (guild_id BIGINT, bot_name VARCHAR(50), last_channel_id BIGINT NULL, connected_channel_id BIGINT NULL, text_channel_id BIGINT NULL, disconnected_at TIMESTAMP NULL, last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, desired_connected BOOLEAN DEFAULT FALSE, reconnect_attempts INT NOT NULL DEFAULT 0, last_error TEXT NULL, PRIMARY KEY (guild_id, bot_name))")
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_metrics (guild_id BIGINT, bot_name VARCHAR(50), voice_connected BOOLEAN DEFAULT FALSE, connected_channel_id BIGINT NULL, player_connected BOOLEAN DEFAULT FALSE, player_playing BOOLEAN DEFAULT FALSE, player_paused BOOLEAN DEFAULT FALSE, queue_count INT DEFAULT 0, backup_queue_count INT DEFAULT 0, is_playing_db BOOLEAN DEFAULT FALSE, is_paused_db BOOLEAN DEFAULT FALSE, position_seconds INT DEFAULT 0, recovery_pending BOOLEAN DEFAULT FALSE, heartbeat_age_seconds INT DEFAULT 0, lavalink_ready BOOLEAN DEFAULT FALSE, last_error TEXT NULL, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (guild_id, bot_name))")
                await safe_schema_execute(cur, "ALTER TABLE nexus_voice_state ADD COLUMN reconnect_attempts INT NOT NULL DEFAULT 0")
                await safe_schema_execute(cur, "ALTER TABLE nexus_voice_state ADD COLUMN last_error TEXT NULL")
                await safe_schema_execute(cur, "CREATE INDEX nexus_voice_state_rejoin_idx ON nexus_voice_state (bot_name, desired_connected, last_seen_at)")
                await safe_schema_execute(cur, "CREATE INDEX nexus_metrics_status_idx ON nexus_metrics (bot_name, updated_at)")
                await safe_schema_execute(cur, "ALTER TABLE nexus_metrics ADD COLUMN duration_seconds INT DEFAULT 0")
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_active_playlists (guild_id BIGINT, bot_name VARCHAR(50), playlist_url TEXT, known_track_count INT DEFAULT 0, requester_id BIGINT, channel_id BIGINT DEFAULT NULL, PRIMARY KEY (guild_id, bot_name))")
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_active_playlist_tracks (guild_id BIGINT, bot_name VARCHAR(50), playlist_url TEXT, position_idx INT DEFAULT 0, track_key CHAR(40), track_uid CHAR(32) DEFAULT NULL, video_url TEXT, title TEXT, requester_id BIGINT DEFAULT NULL, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)")
                await safe_schema_execute(cur, "ALTER TABLE nexus_active_playlists ADD COLUMN channel_id BIGINT DEFAULT NULL")
                await safe_schema_execute(cur, "ALTER TABLE nexus_active_playlist_tracks ADD COLUMN track_uid CHAR(32) DEFAULT NULL")
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_swarm_toggles (guild_id BIGINT PRIMARY KEY, auto_dj BOOLEAN DEFAULT FALSE, audio_filter VARCHAR(20) DEFAULT 'normal')")
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_swarm_overrides (guild_id BIGINT, bot_name VARCHAR(50), command VARCHAR(20), PRIMARY KEY(guild_id, bot_name))")
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_swarm_direct_orders (id INT AUTO_INCREMENT PRIMARY KEY, bot_name VARCHAR(50), guild_id BIGINT, vc_id BIGINT, text_channel_id BIGINT, command VARCHAR(50), data TEXT, attempts INT NOT NULL DEFAULT 0, last_error TEXT NULL, claimed_at TIMESTAMP NULL, claim_token VARCHAR(128) NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                await cur.execute("CREATE TABLE IF NOT EXISTS swarm_health (bot_name VARCHAR(50) PRIMARY KEY, last_pulse TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, status VARCHAR(20))")
                await cur.execute(f"CREATE TABLE IF NOT EXISTS {BOT_ENV_PREFIX.lower()}_error_events (id INT AUTO_INCREMENT PRIMARY KEY, bot_name VARCHAR(50) NOT NULL, guild_id BIGINT NULL, error_level VARCHAR(20) NOT NULL DEFAULT 'error', error_type VARCHAR(50) NOT NULL DEFAULT 'runtime', title VARCHAR(255) NOT NULL, description TEXT NULL, traceback_text MEDIUMTEXT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                await safe_schema_execute(cur, "CREATE INDEX nexus_queue_lookup_idx ON nexus_queue (guild_id, bot_name, id)")
                await safe_schema_execute(cur, "CREATE INDEX nexus_queue_backup_lookup_idx ON nexus_queue_backup (guild_id, bot_name, id)")
                await safe_schema_execute(cur, "CREATE INDEX nexus_queue_track_uid_idx ON nexus_queue (guild_id, bot_name, track_uid)")
                await safe_schema_execute(cur, "CREATE INDEX nexus_queue_backup_track_uid_idx ON nexus_queue_backup (guild_id, bot_name, track_uid)")
                await safe_schema_execute(cur, "CREATE INDEX nexus_history_guild_played_idx ON nexus_history (guild_id, played_at)")
                await safe_schema_execute(cur, "CREATE INDEX nexus_history_requester_idx ON nexus_history (guild_id, requester_id, played_at)")
                await safe_schema_execute(cur, "CREATE INDEX nexus_playback_resume_idx ON nexus_playback_state (bot_name, is_playing, guild_id)")
                await safe_schema_execute(cur, "CREATE INDEX nexus_playback_track_uid_idx ON nexus_playback_state (bot_name, track_uid)")
                await safe_schema_execute(cur, "CREATE INDEX nexus_playlist_bot_idx ON nexus_active_playlists (bot_name, guild_id)")
                await cur.execute(f"CREATE TABLE IF NOT EXISTS {BOT_ENV_PREFIX.lower()}_search_cache (cache_key VARCHAR(128) PRIMARY KEY, resolved_uri TEXT NOT NULL, title VARCHAR(512), duration_ms INT DEFAULT 0, cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, expires_at TIMESTAMP NOT NULL, INDEX sc_expires_idx (expires_at))")
                await safe_schema_execute(cur, "CREATE INDEX nexus_playlist_track_uid_idx ON nexus_active_playlist_tracks (guild_id, bot_name, track_uid)")
                await safe_schema_execute(cur, "CREATE INDEX nexus_active_playlist_tracks_guild_bot_idx ON nexus_active_playlist_tracks (guild_id, bot_name, position_idx)")
                await safe_schema_execute(cur, "CREATE INDEX nexus_active_playlist_tracks_del_idx ON nexus_active_playlist_tracks (guild_id, bot_name, playlist_url(255))")
                await safe_schema_execute(cur, "CREATE INDEX nexus_user_playlist_lookup_idx ON nexus_user_playlists (user_id, playlist_name)")
                await safe_schema_execute(cur, "DROP INDEX nexus_queue_track_uid_idx ON nexus_queue")
                await safe_schema_execute(cur, "DROP INDEX nexus_queue_backup_track_uid_idx ON nexus_queue_backup")
                await safe_schema_execute(cur, "ALTER TABLE nexus_playback_state ADD COLUMN bot_name VARCHAR(50) DEFAULT 'nexus'")
                await safe_schema_execute(cur, "ALTER TABLE nexus_active_playlists ADD COLUMN bot_name VARCHAR(50) DEFAULT 'nexus'")
                await safe_schema_execute(cur, "ALTER TABLE nexus_bot_home_channels ADD COLUMN bot_name VARCHAR(50) DEFAULT 'nexus'")
                await cur.execute(
                    "UPDATE nexus_queue SET track_uid = SUBSTRING(SHA2(CONCAT(UUID(), RAND(), id, COALESCE(video_url, ''), COALESCE(title, '')), 256), 1, 32) "
                    "WHERE track_uid IS NULL OR track_uid = ''"
                )
                await cur.execute(
                    "UPDATE nexus_queue_backup SET track_uid = SUBSTRING(SHA2(CONCAT(UUID(), RAND(), id, COALESCE(video_url, ''), COALESCE(title, '')), 256), 1, 32) "
                    "WHERE track_uid IS NULL OR track_uid = ''"
                )
                await cur.execute(
                    "UPDATE nexus_active_playlist_tracks SET track_uid = SUBSTRING(SHA2(CONCAT(UUID(), RAND(), guild_id, COALESCE(playlist_url, ''), position_idx, COALESCE(video_url, ''), COALESCE(title, '')), 256), 1, 32) "
                    "WHERE track_uid IS NULL OR track_uid = ''"
                )
                await cur.execute(
                    "UPDATE nexus_playback_state SET track_uid = SUBSTRING(SHA2(CONCAT(UUID(), RAND(), guild_id, COALESCE(video_url, ''), COALESCE(title, '')), 256), 1, 32) "
                    "WHERE (video_url IS NOT NULL OR title IS NOT NULL) AND (track_uid IS NULL OR track_uid = '')"
                )
                await cur.execute(
                    "UPDATE nexus_playback_state SET play_session_key = track_uid "
                    "WHERE track_uid IS NOT NULL AND track_uid <> ''"
                )
                await dedupe_queue_table_by_track_uid(cur, "nexus_queue", bot_name="nexus")
                await dedupe_queue_table_by_track_uid(cur, "nexus_queue_backup", bot_name="nexus")
                # Cap loop/re-add URL bloat at 2 copies across ALL guilds (incl. those with
                # no active playlist), preserving small deliberate repeats. Runs every start.
                _trim_live = await trim_queue_duplicate_runs(cur, "nexus_queue", bot_name="nexus")
                _trim_backup = await trim_queue_duplicate_runs(cur, "nexus_queue_backup", bot_name="nexus")
                if _trim_live or _trim_backup:
                    logger.info("[%s] Trimmed %s live / %s backup over-duplicated queue row(s).", BOT_ENV_PREFIX.lower(), _trim_live, _trim_backup)
                # One-time legacy migration only: content-based dedupe collapses rows that
                # predate the track_uid column (those all got distinct random UIDs backfilled
                # above, so the UID-based dedupe above can't catch same-content duplicates).
                # Gate this on the unique index not existing yet — otherwise this purges
                # legitimate intentionally-repeated queue entries (same song queued twice)
                # on *every* container restart, silently shrinking guild queues over time.
                await cur.execute(
                    "SELECT COUNT(*) FROM information_schema.statistics "
                    "WHERE table_schema = DATABASE() AND table_name = 'nexus_queue' "
                    "AND index_name = 'nexus_queue_track_uid_unique_idx'"
                )
                _uid_unique_idx_row = await cur.fetchone()
                if not (_uid_unique_idx_row and _uid_unique_idx_row[0]):
                    removed_legacy_live = await dedupe_queue_table_by_track_key(cur, "nexus_queue", bot_name="nexus")
                    removed_legacy_backup = await dedupe_queue_table_by_track_key(cur, "nexus_queue_backup", bot_name="nexus")
                    if removed_legacy_live or removed_legacy_backup:
                        logger.warning(
                            "[%s] One-time legacy queue migration removed %s live / %s backup content-duplicate row(s) before adding the track_uid unique index.",
                            BOT_ENV_PREFIX.lower(), removed_legacy_live, removed_legacy_backup,
                        )
                await safe_schema_execute(cur, "CREATE UNIQUE INDEX nexus_queue_track_uid_unique_idx ON nexus_queue (guild_id, bot_name, track_uid)")
                await safe_schema_execute(cur, "CREATE UNIQUE INDEX nexus_queue_backup_track_uid_unique_idx ON nexus_queue_backup (guild_id, bot_name, track_uid)")
                await safe_schema_execute(cur, "CREATE INDEX nexus_active_playlists_lookup_idx ON nexus_active_playlists (guild_id, bot_name)")
                await safe_schema_execute(cur, "CREATE INDEX nexus_error_events_guild_time_idx ON nexus_error_events (guild_id, created_at)")
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
    state = guild_states.get(guild_id) or guild_states.get(str(guild_id))
    if not state:
        return
    cache_key = _runtime_key(guild_id)
    path = f"state_{guild_id}.json"
    tmp_path = f"{path}.tmp"
    state_json = json.dumps(state, ensure_ascii=False, separators=(",", ":"))
    if STATE_FILE_WRITE_CACHE.get(cache_key) == state_json and os.path.exists(path):
        return

    def _write_to_disk():
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                f.write(state_json)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except Exception as tx_error:
                    pass
            os.replace(tmp_path, path)
            return True
        except Exception as tx_error:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception as tx_error:
                pass
            return False

    if await asyncio.to_thread(_write_to_disk):
        STATE_FILE_WRITE_CACHE[cache_key] = state_json
    else:
        STATE_FILE_WRITE_CACHE.pop(cache_key, None)

async def delete_state(guild_id):
    for key in (guild_id, str(guild_id)):
        guild_states.pop(key, None)
    STATE_FILE_WRITE_CACHE.pop(_runtime_key(guild_id), None)
    try:
        await asyncio.to_thread(os.remove, f"state_{guild_id}.json")
    except FileNotFoundError:
        pass
    except Exception as tx_error:
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


def _coerce_text(value, default="", *, limit=512):
    if value is None:
        return default
    try:
        text = value if isinstance(value, str) else str(value)
    except Exception as tx_error:
        return default
    if not isinstance(text, str):
        return default
    if limit and len(text) > limit:
        return text[:limit]
    return text


def _normalize_track_uid(value):
    text = re.sub(r"[^0-9a-fA-F]", "", _coerce_text(value, "", limit=96)).lower()
    return text[:32] if len(text) >= 16 else ""


def _new_track_uid():
    return secrets.token_hex(16)


def _ensure_track_uid(value=None):
    return _normalize_track_uid(value) or _new_track_uid()


def _row_track_uid(row, *indices, default=None):
    candidates = [_row_value(row, "track_uid", None)]
    for index in indices:
        candidates.append(_row_value(row, index, None))
    if default is not None:
        candidates.append(default)
    for candidate in candidates:
        normalized = _normalize_track_uid(candidate)
        if normalized:
            return normalized
    return ""


def _track_instance_identity(track_uid=None, video_url=None, title=None):
    normalized_uid = _normalize_track_uid(track_uid)
    if normalized_uid:
        return f"uid:{normalized_uid}"
    return f"key:{_track_key(video_url, title)}"


def _track_uid_label(track_uid):
    normalized_uid = _normalize_track_uid(track_uid)
    return normalized_uid[:8] if normalized_uid else "unknown"


def _compact_track_title(title, limit=72):
    text = re.sub(r"\s+", " ", _coerce_text(title, "", limit=max(limit * 4, 288)).strip())
    return text if len(text) <= limit else f"{text[:limit - 1]}…"


def _queue_family_key(row):
    title = _row_value(row, "title", _row_value(row, 4, _row_value(row, 1, "")))
    url = _row_value(row, "video_url", _row_value(row, 3, _row_value(row, 0, "")))
    cleaned = _clean_smart_title(title).lower()
    cleaned = re.sub(r"\b(disk|disc|track|ost|official|audio|video|lyrics?|remix|version|ver|nightcore|speed ?up|slowed|reverb)\b", " ", cleaned)
    cleaned = re.sub(r"\b\d+\b", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -_|:")
    if " - " in cleaned:
        cleaned = cleaned.split(" - ", 1)[0].strip()
    if not cleaned and title:
        cleaned = re.sub(r"\s+", " ", _coerce_text(title, "", limit=256).lower()).strip()
    if not cleaned:
        return _track_key(url, title)
    return hashlib.sha1(cleaned.encode("utf-8", "ignore")).hexdigest()


def _queue_spacing_score(candidate, recent_exact, recent_family, remaining_exact):
    exact_identity = _queue_source_identity(candidate)
    instance_identity = _queue_track_identity(candidate)
    family_identity = _queue_family_key(candidate)
    exact_penalty = 0
    family_penalty = 0
    for distance, exact_key in enumerate(reversed(recent_exact), start=1):
        if exact_key == exact_identity:
            exact_penalty += max(0, QUEUE_SPREAD_EXACT_GAP - distance + 1) * 100
    for distance, family_key in enumerate(reversed(recent_family), start=1):
        if family_key == family_identity:
            family_penalty += max(0, QUEUE_SPREAD_FAMILY_GAP - distance + 1) * 30
    exact_pressure = int(remaining_exact.get(exact_identity, 0))
    return (-exact_penalty, -family_penalty, exact_pressure, instance_identity)


def _track_key(video_url, title=None):
    if not hasattr(_track_key, "_cache"): _track_key._cache = {}
    _ck = (video_url, title)
    if _ck in _track_key._cache: return _track_key._cache[_ck]
    raw = _coerce_text(video_url, "", limit=2048).strip()
    if not raw and title:
        raw = f"title:{_coerce_text(title, 'unknown', limit=256)}"
    try:
        parsed = urllib.parse.urlparse(raw)
        host = (parsed.netloc or "").lower().replace("www.", "")
        if parsed.scheme and host:
            query = urllib.parse.parse_qs(parsed.query)
            if "youtu.be" in host:
                video_id = parsed.path.strip("/").split("/")[0]
                if video_id:
                    raw = f"youtube:{video_id}"
            elif "youtube" in host and query.get("v"):
                raw = f"youtube:{query['v'][0]}"
            else:
                raw = urllib.parse.urlunparse((parsed.scheme.lower(), host, parsed.path.rstrip("/"), "", parsed.query, ""))
    except Exception as tx_error:
        pass
    normalized = re.sub(r"\s+", " ", raw.lower()).strip()
    if not normalized:
        normalized = re.sub(r"\s+", " ", _coerce_text(title, "unknown", limit=256).lower()).strip()
    _res = hashlib.sha1(normalized.encode("utf-8", "ignore")).hexdigest()
    _track_key._cache[_ck] = _res
    if len(_track_key._cache) > 4096:
        _track_key._cache.clear()
    return _res


def _clean_smart_title(title):
    cleaned = re.sub(r"https?://\S+", " ", _coerce_text(title, "", limit=512))
    cleaned = re.sub(r"\s*\([^)]*\)|\s*\[[^\]]*\]", " ", cleaned)
    cleaned = re.sub(r"(?i)\b(official|video|audio|lyrics?|visualizer|remaster(?:ed)?|hd|hq|mv)\b", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -_|")
    return cleaned[:120]


def _smart_query_from_title(title):
    cleaned = _clean_smart_title(title)
    suffixes = SMART_AUTODJ_RADIO_SUFFIXES or ("radio", "audio", "mix")
    suffix = random.choice(suffixes)
    return f"ytmsearch:{cleaned} {suffix}" if cleaned else f"ytmsearch:{random.choice(['lofi hip hop', 'synthwave mix', 'chill electronic', 'gaming music', 'jazz hop'])}"


def _member_ids_from_voice_channel(guild, channel_id):
    channel = guild.get_channel(channel_id) if guild and channel_id else None
    if not channel:
        return []
    try:
        return [member.id for member in channel.members if not getattr(member, "bot", False)]
    except Exception as tx_error:
        return []


def _weighted_smart_pick(rows):
    choices = []
    total = 0.0
    for row in rows or []:
        title = _row_value(row, "title", _row_value(row, 0))
        url = _row_value(row, "video_url", _row_value(row, 1))
        raw_weight = _row_value(row, "weight", _row_value(row, 2, 1.0))
        reason = _row_value(row, "reason", _row_value(row, 3, "history"))
        try:
            weight = max(0.25, float(raw_weight or 0.0))
        except Exception as tx_error:
            weight = 1.0
        if not title and not url:
            continue
        total += weight
        choices.append((total, str(title or url), str(url or ""), str(reason or "history"), weight))
    if not choices:
        return None
    needle = random.uniform(0.0, total)
    for upper, title, url, reason, weight in choices:
        if needle <= upper:
            return {"title": title, "video_url": url, "reason": reason, "weight": weight}
    upper, title, url, reason, weight = choices[-1]
    return {"title": title, "video_url": url, "reason": reason, "weight": weight}


def _cosine_similarity(vec_a, vec_b):
    if not vec_a or not vec_b or len(vec_a) != len(vec_b):
        return 0.0
    dot = sum(a * b for a, b in zip(vec_a, vec_b))
    norm_a = math.sqrt(sum(a * a for a in vec_a))
    norm_b = math.sqrt(sum(b * b for b in vec_b))
    if norm_a <= 0 or norm_b <= 0:
        return 0.0
    return dot / (norm_a * norm_b)


async def fetch_track_embedding_vector(title):
    """Call the Gemini embedding API for a cleaned track title. Returns list[float] or None."""
    if not SMART_EMBEDDING_ENABLED or not GEMINI_API_KEY:
        return None
    cleaned = _clean_smart_title(title)
    if not cleaned:
        return None
    try:
        async with HTTPSessionManager() as session:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_EMBEDDING_MODEL}:embedContent?key={GEMINI_API_KEY}"
            payload = {"model": f"models/{GEMINI_EMBEDDING_MODEL}", "content": {"parts": [{"text": cleaned}]}}
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                values = (data.get("embedding") or {}).get("values")
                if isinstance(values, list) and values:
                    return [float(v) for v in values]
    except Exception as tx_error:
        return None
    return None


async def get_or_fetch_track_embedding(cur, url_key, title, video_url=None):
    """Return a cached embedding vector for url_key, generating and storing one if missing."""
    await cur.execute("SELECT embedding FROM nexus_track_embeddings WHERE url_key = %s", (url_key,))
    row = await cur.fetchone()
    if row:
        raw = _row_value(row, "embedding", _row_value(row, 0))
        if raw:
            try:
                return json.loads(raw)
            except Exception as tx_error:
                pass
    vector = await fetch_track_embedding_vector(title)
    if vector:
        try:
            await cur.execute(
                "INSERT INTO nexus_track_embeddings (url_key, title, video_url, embedding) VALUES (%s, %s, %s, %s) "
                "ON DUPLICATE KEY UPDATE title = VALUES(title), video_url = VALUES(video_url), embedding = VALUES(embedding)",
                (url_key, title, video_url, json.dumps(vector)),
            )
        except Exception as tx_error:
            pass
    return vector


async def find_similar_tracks_global(cur, seed_vector, avoid_keys, limit=5):
    """Cross-server similarity search: scan the shared embedding table for tracks close to seed_vector."""
    if not seed_vector:
        return []
    await cur.execute("SELECT url_key, title, video_url, embedding FROM nexus_track_embeddings ORDER BY updated_at DESC LIMIT 1500")
    rows = await cur.fetchall()
    scored = []
    for row in rows:
        url_key = _row_value(row, "url_key", _row_value(row, 0))
        title = _row_value(row, "title", _row_value(row, 1))
        video_url = _row_value(row, "video_url", _row_value(row, 2))
        raw = _row_value(row, "embedding", _row_value(row, 3))
        if not raw or not video_url or url_key in avoid_keys:
            continue
        try:
            vector = json.loads(raw)
        except Exception as tx_error:
            continue
        similarity = _cosine_similarity(seed_vector, vector)
        if similarity >= SMART_EMBEDDING_SIMILARITY_THRESHOLD:
            scored.append((similarity, title, video_url, url_key))
    scored.sort(key=lambda item: item[0], reverse=True)
    return scored[:limit]


async def record_track_cooccurrence(cur, guild_id, prev_url, prev_title, next_url, next_title):
    """Track which tracks tend to play back-to-back so Auto-DJ can chain similar sessions."""
    if not (prev_url or prev_title) or not (next_url or next_title):
        return
    key_a = _track_key(prev_url, prev_title)
    key_b = _track_key(next_url, next_title)
    if key_a == key_b:
        return
    await cur.execute(
        "INSERT INTO nexus_track_cooccurrence (guild_id, url_key_a, url_key_b, weight) VALUES (%s, %s, %s, 1) "
        "ON DUPLICATE KEY UPDATE weight = weight + 1, updated_at = NOW()",
        (guild_id, key_a, key_b),
    )
    await cur.execute(
        "INSERT INTO nexus_track_cooccurrence (guild_id, url_key_a, url_key_b, weight) VALUES (%s, %s, %s, 1) "
        "ON DUPLICATE KEY UPDATE weight = weight + 1, updated_at = NOW()",
        (guild_id, key_b, key_a),
    )


async def bulk_record_tracks_queued(cur, guild_id, tracks):
    """Bulk-update queue intelligence for rows shaped as (video_url, title, requester_id)."""
    if not tracks:
        return 0
    intelligence_rows = []
    affinity_rows = []
    for video_url, title, requester_id in tracks:
        url_key = _track_key(video_url, title)
        intelligence_rows.append((guild_id, url_key, video_url, title, requester_id))
        if requester_id:
            affinity_rows.append((guild_id, requester_id, url_key, video_url, title))
    if intelligence_rows:
        await cur.executemany(
            "INSERT INTO nexus_track_intelligence (guild_id, url_key, video_url, title, queued_count, last_requester_id, last_queued, source) "
            "VALUES (%s, %s, %s, %s, 1, %s, NOW(), 'queue') "
            "ON DUPLICATE KEY UPDATE video_url = VALUES(video_url), title = VALUES(title), queued_count = queued_count + 1, last_requester_id = VALUES(last_requester_id), last_queued = NOW(), source = 'queue'",
            intelligence_rows,
        )
    if affinity_rows:
        await cur.executemany(
            "INSERT INTO nexus_user_track_affinity (guild_id, user_id, url_key, video_url, title, queued_count, last_requested, score) "
            "VALUES (%s, %s, %s, %s, %s, 1, NOW(), 0.15) "
            "ON DUPLICATE KEY UPDATE video_url = VALUES(video_url), title = VALUES(title), queued_count = queued_count + 1, last_requested = NOW(), score = score + 0.15",
            affinity_rows,
        )
    return len(tracks)


async def record_track_play_started(cur, guild_id, video_url, title, requester_id):
    url_key = _track_key(video_url, title)
    await cur.execute(
        "INSERT INTO nexus_track_intelligence (guild_id, url_key, video_url, title, play_count, last_requester_id, last_played, source) "
        "VALUES (%s, %s, %s, %s, 1, %s, NOW(), 'playback') "
        "ON DUPLICATE KEY UPDATE video_url = VALUES(video_url), title = VALUES(title), play_count = play_count + 1, last_requester_id = VALUES(last_requester_id), last_played = NOW(), source = 'playback'",
        (guild_id, url_key, video_url, title, requester_id),
    )
    if requester_id:
        await cur.execute(
            "INSERT INTO nexus_user_track_affinity (guild_id, user_id, url_key, video_url, title, play_count, last_requested, score) "
            "VALUES (%s, %s, %s, %s, %s, 1, NOW(), 1.0) "
            "ON DUPLICATE KEY UPDATE video_url = VALUES(video_url), title = VALUES(title), play_count = play_count + 1, last_requested = NOW(), score = score + 1.0",
            (guild_id, requester_id, url_key, video_url, title),
        )


async def record_track_play_resumed(cur, guild_id, video_url, title, requester_id):
    if not video_url and not title:
        return
    url_key = _track_key(video_url, title)
    await cur.execute(
        "INSERT INTO nexus_track_intelligence (guild_id, url_key, video_url, title, last_requester_id, last_played, source) "
        "VALUES (%s, %s, %s, %s, %s, NOW(), 'playback_resume') "
        "ON DUPLICATE KEY UPDATE video_url = VALUES(video_url), title = VALUES(title), last_requester_id = VALUES(last_requester_id), last_played = NOW(), source = 'playback_resume'",
        (guild_id, url_key, video_url, title, requester_id),
    )
    if requester_id:
        await cur.execute(
            "INSERT INTO nexus_user_track_affinity (guild_id, user_id, url_key, video_url, title, last_requested, score) "
            "VALUES (%s, %s, %s, %s, %s, NOW(), 0.0) "
            "ON DUPLICATE KEY UPDATE video_url = VALUES(video_url), title = VALUES(title), last_requested = NOW()",
            (guild_id, requester_id, url_key, video_url, title),
        )


async def record_track_outcome(cur, guild_id, video_url, title, requester_id, *, outcome, listen_seconds=0):
    if not video_url and not title:
        return
    url_key = _track_key(video_url, title)
    listen_seconds = max(0, int(listen_seconds or 0))
    finished = outcome == "finished"
    finish_delta = 1 if finished else 0
    skip_delta = 0 if finished else 1
    score_delta = 1.25 if finished else -1.75
    await cur.execute(
        "INSERT INTO nexus_track_intelligence (guild_id, url_key, video_url, title, finish_count, skip_count, total_listen_seconds, last_requester_id, last_played, source) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), 'playback') "
        "ON DUPLICATE KEY UPDATE video_url = VALUES(video_url), title = VALUES(title), finish_count = finish_count + VALUES(finish_count), skip_count = skip_count + VALUES(skip_count), total_listen_seconds = total_listen_seconds + VALUES(total_listen_seconds), last_requester_id = VALUES(last_requester_id), last_played = NOW(), source = 'playback'",
        (guild_id, url_key, video_url, title, finish_delta, skip_delta, listen_seconds, requester_id),
    )
    if requester_id:
        await cur.execute(
            "INSERT INTO nexus_user_track_affinity (guild_id, user_id, url_key, video_url, title, finish_count, skip_count, score, last_requested) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW()) "
            "ON DUPLICATE KEY UPDATE video_url = VALUES(video_url), title = VALUES(title), finish_count = finish_count + VALUES(finish_count), skip_count = skip_count + VALUES(skip_count), score = score + VALUES(score), last_requested = NOW()",
            (guild_id, requester_id, url_key, video_url, title, finish_delta, skip_delta, score_delta),
        )


async def record_track_feedback(cur, guild_id, user_id, video_url, title, liked=True):
    url_key = _track_key(video_url, title)
    like_delta = 1 if liked else 0
    dislike_delta = 0 if liked else 1
    score_delta = SMART_FEEDBACK_SCORE if liked else -SMART_FEEDBACK_SCORE
    await cur.execute(
        "INSERT INTO nexus_track_intelligence (guild_id, url_key, video_url, title, like_count, dislike_count, last_requester_id, source) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, 'feedback') "
        "ON DUPLICATE KEY UPDATE video_url = VALUES(video_url), title = VALUES(title), like_count = like_count + VALUES(like_count), dislike_count = dislike_count + VALUES(dislike_count), last_requester_id = VALUES(last_requester_id), source = 'feedback'",
        (guild_id, url_key, video_url, title, like_delta, dislike_delta, user_id),
    )
    await cur.execute(
        "INSERT INTO nexus_user_track_affinity (guild_id, user_id, url_key, video_url, title, like_count, dislike_count, score, last_requested) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW()) "
        "ON DUPLICATE KEY UPDATE video_url = VALUES(video_url), title = VALUES(title), like_count = like_count + VALUES(like_count), dislike_count = dislike_count + VALUES(dislike_count), score = score + VALUES(score), last_requested = NOW()",
        (guild_id, user_id, url_key, video_url, title, like_delta, dislike_delta, score_delta),
    )


async def get_current_track_snapshot(guild_id):
    data = playback_tracking.get(guild_id) or playback_tracking.get(str(guild_id))
    if data and (data.get("url") or data.get("title")):
        return {
            "url": data.get("url"),
            "title": data.get("title"),
            "requester_id": data.get("requester_id"),
            "track_uid": data.get("track_uid"),
        }
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT video_url, title, track_uid FROM nexus_playback_state WHERE guild_id = %s AND bot_name = 'nexus' AND (is_playing = TRUE OR is_paused = TRUE) ORDER BY is_playing DESC LIMIT 1",
                    (guild_id,),
                )
                row = await cur.fetchone()
    if not row:
        return None
    return {"url": _row_value(row, 0), "title": _row_value(row, 1), "requester_id": None, "track_uid": _row_track_uid(row, 2)}


async def load_smart_avoid_keys(cur, guild_id, listener_ids=None):
    avoid = set()
    await cur.execute("SELECT video_url, title FROM nexus_history WHERE guild_id = %s ORDER BY played_at DESC LIMIT %s", (guild_id, SMART_RECENT_HISTORY_LIMIT))
    for row in await cur.fetchall():
        avoid.add(_track_key(_row_value(row, "video_url", _row_value(row, 0)), _row_value(row, "title", _row_value(row, 1))))
    await cur.execute("SELECT video_url, title FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC LIMIT %s", (guild_id, SMART_RECENT_HISTORY_LIMIT))
    for row in await cur.fetchall():
        avoid.add(_track_key(_row_value(row, "video_url", _row_value(row, 0)), _row_value(row, "title", _row_value(row, 1))))
    ids = sorted({int(v) for v in (listener_ids or []) if v})
    if ids:
        placeholders = ",".join(["%s"] * len(ids))
        await cur.execute(
            f"SELECT url_key FROM nexus_user_track_affinity WHERE guild_id = %s AND user_id IN ({placeholders}) AND dislike_count > like_count",
            (guild_id, *ids),
        )
        for row in await cur.fetchall():
            key = _row_value(row, "url_key", _row_value(row, 0))
            if key:
                avoid.add(str(key))
    return avoid


async def build_smart_recommendation(cur, guild_id, listener_ids=None):
    candidates = []
    recency_decay = "POW(0.5, LEAST(GREATEST(DATEDIFF(NOW(), COALESCE(last_requested, NOW())), 0), 365) / %s)"
    server_recency_decay = "POW(0.5, LEAST(GREATEST(DATEDIFF(NOW(), COALESCE(last_played, last_queued, first_seen)), 0), 365) / %s)"
    ids = sorted({int(v) for v in (listener_ids or []) if v})
    if ids:
        placeholders = ",".join(["%s"] * len(ids))
        await cur.execute(
            f"""SELECT title, video_url,
                       (score + play_count * 1.35 + finish_count * 1.5 + like_count * 3.0 - dislike_count * 4.0
                        - (skip_count / (play_count + finish_count + 1)) * 6.0) * {recency_decay} AS weight,
                       'listener taste' AS reason
                FROM nexus_user_track_affinity
                WHERE guild_id = %s AND user_id IN ({placeholders}) AND (dislike_count <= like_count OR like_count > 0)
                ORDER BY weight DESC, last_requested DESC
                LIMIT %s""",
            (SMART_RECENCY_HALFLIFE_DAYS, guild_id, *ids, SMART_SEED_POOL_LIMIT),
        )
        candidates.extend(await cur.fetchall())

        await cur.execute(
            f"""SELECT title, video_url,
                       (COUNT(*) * 1.1) AS weight,
                       'saved playlist' AS reason
                FROM nexus_user_playlists
                WHERE user_id IN ({placeholders})
                GROUP BY title, video_url
                ORDER BY weight DESC
                LIMIT %s""",
            (*ids, max(5, SMART_SEED_POOL_LIMIT // 2)),
        )
        candidates.extend(await cur.fetchall())

    await cur.execute(
        f"""SELECT title, video_url,
                   (play_count * 1.1 + finish_count * 1.6 + like_count * 3.0 + queued_count * 0.2 - dislike_count * 4.0
                    - (skip_count / (play_count + finish_count + 1)) * 5.0) * {server_recency_decay} AS weight,
                   'server taste' AS reason
            FROM nexus_track_intelligence
            WHERE guild_id = %s AND (dislike_count <= like_count OR like_count > 0)
            ORDER BY weight DESC, COALESCE(last_played, last_queued, first_seen) DESC
            LIMIT %s""",
        (SMART_RECENCY_HALFLIFE_DAYS, guild_id, SMART_SEED_POOL_LIMIT),
    )
    candidates.extend(await cur.fetchall())

    # Cross-server learning: blend in trending tracks from every other guild this bot serves.
    await cur.execute(
        """SELECT MIN(title) AS title, MIN(video_url) AS video_url,
                   (SUM(play_count) * 0.6 + SUM(finish_count) * 0.9 + SUM(like_count) * 1.5 - SUM(dislike_count) * 2.0) * %s AS weight,
                   'cross-server trend' AS reason
            FROM nexus_track_intelligence
            WHERE guild_id != %s AND (dislike_count <= like_count OR like_count > 0)
            GROUP BY url_key
            HAVING weight > 0
            ORDER BY weight DESC
            LIMIT %s""",
        (SMART_GLOBAL_POOL_WEIGHT, guild_id, SMART_SEED_POOL_LIMIT),
    )
    candidates.extend(await cur.fetchall())

    # "Often played next": boost tracks that have historically followed the current/most recent track.
    await cur.execute("SELECT video_url, title FROM nexus_history WHERE guild_id = %s ORDER BY played_at DESC LIMIT 1", (guild_id,))
    last_row = await cur.fetchone()
    if last_row:
        last_key = _track_key(_row_value(last_row, "video_url", _row_value(last_row, 0)), _row_value(last_row, "title", _row_value(last_row, 1)))
        await cur.execute(
            """SELECT ti.title AS title, ti.video_url AS video_url,
                       (tc.weight * 2.0) AS weight,
                       'often played next' AS reason
                FROM nexus_track_cooccurrence tc
                JOIN nexus_track_intelligence ti ON ti.guild_id = tc.guild_id AND ti.url_key = tc.url_key_b
                WHERE tc.guild_id = %s AND tc.url_key_a = %s AND (ti.dislike_count <= ti.like_count OR ti.like_count > 0)
                ORDER BY tc.weight DESC
                LIMIT %s""",
            (guild_id, last_key, SMART_COOCCURRENCE_LIMIT),
        )
        candidates.extend(await cur.fetchall())

    if not candidates:
        await cur.execute("SELECT title, video_url, 1.0 AS weight, 'recent history' AS reason FROM nexus_history WHERE guild_id = %s ORDER BY played_at DESC LIMIT %s", (guild_id, SMART_SEED_POOL_LIMIT))
        candidates.extend(await cur.fetchall())

    seed = _weighted_smart_pick(candidates)
    if seed:
        query = _smart_query_from_title(seed["title"])
        return {"query": query, "seed_title": seed["title"], "seed_url": seed.get("video_url"), "reason": seed.get("reason", "history")}

    fallback_terms = ["lofi hip hop", "synthwave mix", "chill electronic", "gaming music", "jazz hop"]
    term = random.choice(fallback_terms)
    return {"query": f"ytmsearch:{term}", "seed_title": term, "seed_url": None, "reason": "fallback"}


async def pick_smart_recommendation_track(cur, guild_id, listener_ids=None):
    recommendation = await build_smart_recommendation(cur, guild_id, listener_ids=listener_ids)
    avoid_keys = await load_smart_avoid_keys(cur, guild_id, listener_ids=listener_ids)

    # Embedding-based similarity: try to find a close match to the seed track from the
    # cross-server embedding library before falling back to a keyword search.
    if SMART_EMBEDDING_ENABLED and recommendation.get("seed_title"):
        try:
            seed_key = _track_key(recommendation.get("seed_url"), recommendation.get("seed_title"))
            seed_vector = await get_or_fetch_track_embedding(cur, seed_key, recommendation["seed_title"], recommendation.get("seed_url"))
            similar = await find_similar_tracks_global(cur, seed_vector, avoid_keys | {seed_key}, limit=5)
            if similar:
                _similarity, sim_title, sim_url, _sim_key = random.choice(similar)
                chosen = SimpleNamespace(uri=sim_url, title=sim_title)
                recommendation["reason"] = "similar track"
                return chosen, recommendation
        except Exception as tx_error:
            pass

    entries, _playlist_result = await search_playables(recommendation["query"])
    chosen = None
    scanned = 0
    for entry in entries:
        scanned += 1
        title = str(getattr(entry, "title", "") or "").strip()
        uri = str(getattr(entry, "uri", "") or "").strip()
        if not title and not uri:
            continue
        if _track_key(uri, title) in avoid_keys:
            if scanned < SMART_CANDIDATE_SCAN_LIMIT:
                continue
        chosen = entry
        break
    if not chosen and entries:
        chosen = entries[0]
    return chosen, recommendation


async def record_smart_recommendation(cur, guild_id, requester_id, recommendation, chosen, *, reason):
    await cur.execute(
        "INSERT INTO nexus_smart_recommendations (guild_id, requester_id, seed_title, seed_url, query_text, chosen_url, chosen_title, reason) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (
            guild_id,
            requester_id,
            recommendation.get("seed_title"),
            recommendation.get("seed_url"),
            recommendation.get("query"),
            getattr(chosen, "uri", None),
            getattr(chosen, "title", None),
            reason or recommendation.get("reason"),
        ),
    )


async def build_user_taste_summary(guild_id, user_id):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """SELECT title, video_url, play_count, finish_count, like_count, dislike_count, skip_count, score
                       FROM nexus_user_track_affinity
                       WHERE guild_id = %s AND user_id = %s
                       ORDER BY score DESC, last_requested DESC
                       LIMIT 10""",
                    (guild_id, user_id),
                )
                top_tracks = await cur.fetchall()
                await cur.execute(
                    """SELECT COALESCE(SUM(play_count), 0), COALESCE(SUM(finish_count), 0), COALESCE(SUM(like_count), 0), COALESCE(SUM(dislike_count), 0), COALESCE(SUM(skip_count), 0)
                       FROM nexus_user_track_affinity
                       WHERE guild_id = %s AND user_id = %s""",
                    (guild_id, user_id),
                )
                totals = await cur.fetchone()
    return top_tracks, totals



def get_process_queue_lock(guild_id):
    lock = process_queue_locks.get(guild_id)
    if lock is None:
        lock = asyncio.Lock()
        process_queue_locks[guild_id] = lock
    return lock

def get_track_requeue_lock(guild_id):
    lock = track_requeue_locks.get(guild_id)
    if lock is None:
        lock = asyncio.Lock()
        track_requeue_locks[guild_id] = lock
    return lock

def get_voice_connect_lock(guild_id):
    lock = voice_connect_locks.get(guild_id)
    if lock is None:
        lock = asyncio.Lock()
        voice_connect_locks[guild_id] = lock
    return lock

def invalidate_position_persist(guild_id):
    last_position_persist.pop(guild_id, None)
    player_position_report_state.pop(guild_id, None)
    player_position_report_state.pop(str(guild_id), None)
    player_position_stall_warning_at.pop(guild_id, None)
    player_position_stall_warning_at.pop(str(guild_id), None)
    if guild_id in playback_tracking:
        playback_tracking[guild_id].pop("last_position_checkpoint", None)
        playback_tracking[guild_id].pop("last_listen_position", None)

def normalize_position_seconds(position, duration=None):
    try:
        value = max(0, int(float(position or 0)))
    except (TypeError, ValueError):
        value = 0
    try:
        duration_value = int(float(duration or 0))
        if duration_value > 0 and duration_value < 10000000:
            value = min(value, duration_value)
    except (TypeError, ValueError):
        pass
    return value

def _estimated_runtime_position_seconds(data, now=None):
    if not data:
        return 0
    try:
        duration = int(float(data.get("duration") or 0))
    except (TypeError, ValueError):
        duration = 0
    try:
        offset = int(float(data.get("offset", data.get("last_position_checkpoint", 0)) or 0))
    except (TypeError, ValueError):
        offset = 0
    offset = normalize_position_seconds(offset, duration)
    if data.get("paused"):
        return offset
    now = now or time.monotonic()
    try:
        started_at = float(data.get("start_time", now) or now)
        if started_at > time.monotonic():
            started_at = now
    except (TypeError, ValueError):
        started_at = now
    try:
        speed = float(data.get("speed", 1.0) or 1.0)
    except (TypeError, ValueError):
        speed = 1.0
    position = max(0, int((now - started_at) * speed + offset))
    if duration > 0:
        position = min(position, duration)
    return position

def update_runtime_position_baseline(guild_id, position, *, channel_id=None, reset_listen_baseline=True):
    position = normalize_position_seconds(position)
    tracked = playback_tracking.get(guild_id) or playback_tracking.get(str(guild_id))
    if tracked is not None:
        tracked["offset"] = position
        tracked["start_time"] = time.monotonic()
        tracked["last_position_checkpoint"] = position
        if reset_listen_baseline:
            tracked["last_listen_position"] = position
    existing_state = guild_states.get(guild_id) or guild_states.get(str(guild_id)) or {}
    voice_channel_id = channel_id or existing_state.get("voice_channel_id") or (tracked or {}).get("channel_id")
    if voice_channel_id:
        guild_states[guild_id] = {
            "voice_channel_id": voice_channel_id,
            "position": position,
            "track_uid": (tracked or {}).get("track_uid") or existing_state.get("track_uid"),
            "url": (tracked or {}).get("url") or existing_state.get("url"),
            "title": (tracked or {}).get("title") or existing_state.get("title"),
        }
    invalidate_position_persist(guild_id)
    return position

def consume_realtime_listen_delta(data, position, *, playing=True):
    if not data:
        return 0
    position = normalize_position_seconds(position, data.get("duration"))
    if not playing:
        data["last_listen_position"] = position
        return 0
    try:
        previous = int(data.get("last_listen_position", data.get("offset", position)) or 0)
    except (TypeError, ValueError):
        previous = position
    previous = normalize_position_seconds(previous, data.get("duration"))
    if position < previous:
        data["last_listen_position"] = position
        return 0
    delta = position - previous
    if delta > (POSITION_PERSIST_INTERVAL * 2.0): delta = 0
    if delta < PLAYTIME_MIN_DELTA_SECONDS:
        return 0
    data["last_listen_position"] = position
    data["listen_seconds_committed"] = int(data.get("listen_seconds_committed") or 0) + delta
    return delta

async def reset_runtime_position_after_seek(guild_id, position, channel_id=None):
    if guild_id in playback_tracking: playback_tracking[guild_id]["seek_lock_until"] = time.monotonic() + 2.5
    update_runtime_position_baseline(guild_id, position, channel_id=channel_id, reset_listen_baseline=True)
    await save_state(guild_id)
    last_state_file_persist[guild_id] = time.time()

def current_track_position(guild_id, now=None):
    data = playback_tracking.get(guild_id) or playback_tracking.get(str(guild_id))
    now = now or time.monotonic()
    state = guild_states.get(guild_id) or guild_states.get(str(guild_id)) or {}

    if not data:
        return normalize_position_seconds(state.get("position", 0))

    estimated_position = _estimated_runtime_position_seconds(data, now=now)
    last_checkpoint = normalize_position_seconds(
        data.get("last_position_checkpoint", data.get("offset", estimated_position)),
        data.get("duration"),
    )

    # Lavalink/Wavelink is the best source when it is reporting sane values.
    # However, on lag/reconnect windows it can freeze at *any* old value, not
    # just 0. If we keep trusting that frozen value, the DB timestamp updates
    # but position_seconds stays pinned, so a rebuild resumes from an old spot.
    try:
        guild = bot.get_guild(int(guild_id))
        vc = guild.voice_client if guild else None
        if vc and _voice_client_connected(vc) and now >= float(data.get("seek_lock_until", 0.0)):
            position_ms = getattr(vc, "position", None)
            if position_ms is not None:
                reported_position = normalize_position_seconds(float(position_ms) / 1000, data.get("duration"))
                active_playing = bool(_player_is_playing(vc)) and not bool(data.get("paused"))
                track_key = _track_key(data.get("url"), data.get("title"))

                report_state = player_position_report_state.get(guild_id) or player_position_report_state.get(str(guild_id))
                if (
                    not report_state
                    or report_state.get("track_key") != track_key
                    or int(report_state.get("reported_position", -1)) != int(reported_position)
                ):
                    report_state = {
                        "track_key": track_key,
                        "reported_position": int(reported_position),
                        "first_seen_at": now,
                        "last_seen_at": now,
                    }
                    player_position_report_state[guild_id] = report_state
                else:
                    report_state["last_seen_at"] = now

                stalled_for = max(0.0, now - float(report_state.get("first_seen_at", now) or now))
                runtime_ahead_by = int(estimated_position) - int(reported_position)

                # If Wavelink is stuck at/near zero but our runtime clock has
                # clearly moved, treat the player position as unavailable. This
                # is the exact failure mode that makes rebuilt containers resume
                # from 0 even after several minutes of playback.
                if active_playing and reported_position <= 1 and estimated_position >= PLAYER_POSITION_STALE_ZERO_SECONDS and (now - float(data.get("start_time", now))) >= PLAYER_POSITION_STALE_ZERO_SECONDS:
                    return max(estimated_position, last_checkpoint)

                # Newer failure mode: Wavelink can keep reporting the same
                # non-zero position for a long track while audio continues. The
                # watch/checker then shows fresh last_checkpoint_at values but a
                # frozen position_seconds. After the stall has lasted long enough,
                # trust our monotonic runtime clock until Wavelink starts moving
                # again. This avoids stale resume points without allowing
                # backwards jumps.
                if (
                    active_playing
                    and stalled_for >= PLAYER_POSITION_STALL_FALLBACK_SECONDS
                    and runtime_ahead_by >= PLAYER_POSITION_STALL_MIN_RUNTIME_AHEAD_SECONDS
                ):
                    warn_key = guild_id
                    last_warn = player_position_stall_warning_at.get(warn_key, 0)
                    if now - last_warn >= 120:
                        logger.warning(
                            "[%s] Player position appears frozen at %ss for %.0fs while runtime clock is at %ss; using runtime checkpoint fallback for '%s'.",
                            guild_id,
                            reported_position,
                            stalled_for,
                            estimated_position,
                            data.get("title") or data.get("url") or "active track",
                        )
                        player_position_stall_warning_at[warn_key] = now
                    return max(estimated_position, last_checkpoint)

                # Never persist a backwards jump for the same active track unless
                # the user explicitly sought/reset the baseline. Lavalink can
                # briefly report 0 around reconnects and right after play().
                if reported_position + PLAYER_POSITION_BACKSTEP_GRACE_SECONDS < last_checkpoint:
                    return last_checkpoint

                return max(reported_position, last_checkpoint)
    except Exception as tx_error:
        pass

    if data.get("paused"):
        return normalize_position_seconds(data.get("offset", last_checkpoint), data.get("duration"))
    return max(estimated_position, last_checkpoint)

async def persist_playback_checkpoint(cur, guild_id, data, position, *, channel_id=None, playing=True, paused=False, connected=True):
    if not data:
        return 0
    bot_n = BOT_ENV_PREFIX.lower()
    position = normalize_position_seconds(position, data.get("duration"))
    url = data.get("url")
    title = data.get("title")
    requester_id = data.get("requester_id")
    channel_id = channel_id or data.get("channel_id") or (guild_states.get(guild_id) or {}).get("voice_channel_id")
    track_uid = _ensure_track_uid(data.get("track_uid")) if (url or title or data.get("track_uid")) else None
    play_session_key = track_uid
    previous_checkpoint = normalize_position_seconds(data.get("last_position_checkpoint", data.get("offset", 0)), data.get("duration"))
    if play_session_key and previous_checkpoint > 0 and position + PLAYER_POSITION_BACKSTEP_GRACE_SECONDS < previous_checkpoint:
        logger.debug(
            "[%s] Refusing backwards checkpoint jump for %s from %ss to %ss; keeping %ss.",
            guild_id,
            title or url or "active track",
            previous_checkpoint,
            position,
            previous_checkpoint,
        )
        position = previous_checkpoint

    await cur.execute(
        f"REPLACE INTO {bot_n}_playback_state (guild_id, bot_name, channel_id, video_url, position_seconds, is_playing, is_paused, title, play_session_key, track_uid) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (guild_id, bot_n, channel_id, url, position, bool(playing), bool(paused), title, play_session_key, track_uid),
    )
    await cur.execute(
        f"""
        INSERT INTO {bot_n}_metrics
            (guild_id, bot_name, voice_connected, connected_channel_id, player_connected, player_playing, player_paused,
             is_playing_db, is_paused_db, position_seconds, recovery_pending)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            voice_connected = VALUES(voice_connected),
            connected_channel_id = VALUES(connected_channel_id),
            player_connected = VALUES(player_connected),
            player_playing = VALUES(player_playing),
            player_paused = VALUES(player_paused),
            is_playing_db = VALUES(is_playing_db),
            is_paused_db = VALUES(is_paused_db),
            position_seconds = VALUES(position_seconds),
            recovery_pending = VALUES(recovery_pending)
        """,
        (guild_id, bot_n, bool(connected), channel_id, bool(connected), bool(playing), bool(paused), bool(playing), bool(paused), position, guild_id in recovering_guilds),
    )

    listen_delta = consume_realtime_listen_delta(data, position, playing=bool(playing))
    if listen_delta > 0 and play_session_key:
        await cur.execute(
            f"""
            INSERT INTO {bot_n}_track_intelligence
                (guild_id, url_key, video_url, title, total_listen_seconds, last_requester_id, last_played, source)
            VALUES (%s, %s, %s, %s, %s, %s, NOW(), 'playback_checkpoint')
            ON DUPLICATE KEY UPDATE
                video_url = VALUES(video_url),
                title = VALUES(title),
                total_listen_seconds = total_listen_seconds + VALUES(total_listen_seconds),
                last_requester_id = VALUES(last_requester_id),
                last_played = NOW(),
                source = 'playback_checkpoint'
            """,
            (guild_id, play_session_key, url, title, listen_delta, requester_id),
        )

    data["last_position_checkpoint"] = position
    data["last_checkpoint_at"] = time.time()
    if track_uid:
        data["track_uid"] = track_uid
    if channel_id:
        guild_states[guild_id] = {
            "voice_channel_id": channel_id,
            "position": position,
            "track_uid": track_uid,
            "url": url,
            "title": title,
        }
    return listen_delta

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

def voice_connect_inflight_remaining(guild_id):
    remaining = voice_connect_inflight_until.get(guild_id, 0) - time.time()
    if remaining <= 0:
        voice_connect_inflight_until.pop(guild_id, None)
        return 0
    return int(max(1, remaining))

def clear_voice_connect_inflight(guild_id):
    voice_connect_inflight_until.pop(guild_id, None)

async def cleanup_failed_voice_session(guild, *, reason="voice_connect_error"):
    """Force-clear a failed Discord voice handshake without touching persisted queue state."""
    clear_voice_connect_inflight(guild.id)
    voice_client = getattr(guild, "voice_client", None)
    if voice_client:
        try:
            await asyncio.wait_for(voice_client.disconnect(force=True), timeout=10.0)
        except TypeError:
            try:
                await asyncio.wait_for(voice_client.disconnect(), timeout=10.0)
            except Exception as tx_error:
                logger.debug(f"[{guild.id}] Failed to disconnect stale voice client after {reason}.", exc_info=True)
        except Exception as tx_error:
            logger.debug(f"[{guild.id}] Failed to disconnect stale voice client after {reason}.", exc_info=True)
    try:
        await asyncio.wait_for(guild.change_voice_state(channel=None), timeout=10.0)
    except Exception as tx_error:
        logger.debug(f"[{guild.id}] Failed to clear Discord voice state after {reason}.", exc_info=True)

def clear_recovery_backoff(guild_id):
    recovery_exhausted_until.pop(guild_id, None)

def arm_recovery_backoff(guild_id, *, seconds=RECOVERY_EXHAUSTED_COOLDOWN_SECONDS, reason="recovery_exhausted"):
    cooldown = max(5.0, float(seconds))
    recovery_exhausted_until[guild_id] = time.time() + cooldown
    # Keep playback state around during network/Discord voice lag so recovery
    # can resume the same track instead of treating the guild as fully stopped.
    recovering_guilds.discard(guild_id)
    snooze_auto_restore(guild_id, cooldown)
    logger.warning(f"[{guild_id}] Recovery paused for {int(cooldown)}s after {reason}.")

def automatic_queue_recovery_paused(guild_id):
    return time.time() < auto_restore_snooze_until.get(guild_id, 0)

def should_defer_automatic_queue_recovery(guild_id):
    return (
        guild_id in recovering_guilds
        or recovery_backoff_remaining(guild_id) > 0
        or voice_connect_inflight_remaining(guild_id) > 0
        or automatic_queue_recovery_paused(guild_id)
    )

def clear_idle_restore_state(guild_id):
    process_queue_locks.pop(guild_id, None)
    track_requeue_locks.pop(guild_id, None)
    voice_connect_locks.pop(guild_id, None)
    idle_voice_since.pop(guild_id, None)

def clear_recovery_retry(guild_id):
    recovery_retry_counts.pop(guild_id, None)
    retry_task = recovery_retry_tasks.pop(guild_id, None)
    current_task = asyncio.current_task()
    if retry_task and retry_task is not current_task and not retry_task.done():
        retry_task.cancel()


def clear_interrupt_resume(guild_id):
    resume_task = interrupt_resume_tasks.pop(guild_id, None)
    interrupt_resume_armed_until.pop(guild_id, None)
    current_task = asyncio.current_task()
    if resume_task and resume_task is not current_task and not resume_task.done():
        resume_task.cancel()

def clear_voice_disconnect_grace(guild_id):
    grace_task = voice_disconnect_grace_tasks.pop(guild_id, None)
    current_task = asyncio.current_task()
    if grace_task and grace_task is not current_task and not grace_task.done():
        grace_task.cancel()

def freeze_playback_for_soft_disconnect(guild_id, position=None):
    tracked = playback_tracking.get(guild_id)
    if not tracked:
        return
    frozen_position = max(0, int(position if position is not None else current_track_position(guild_id)))
    tracked["offset"] = frozen_position
    tracked["start_time"] = time.monotonic()
    tracked["paused"] = True
    tracked["last_position_checkpoint"] = frozen_position
    tracked["last_listen_position"] = frozen_position
    tracked["voice_soft_disconnected"] = True

def unfreeze_playback_after_voice_return(guild_id):
    tracked = playback_tracking.get(guild_id)
    if not tracked or not tracked.get("voice_soft_disconnected"):
        return
    tracked["start_time"] = time.monotonic()
    tracked["paused"] = False
    tracked["last_listen_position"] = normalize_position_seconds(tracked.get("offset", 0), tracked.get("duration"))
    tracked.pop("voice_soft_disconnected", None)

async def _run_soft_voice_recovery_after_grace(guild_id, channel_id, position, reason, delay):
    try:
        await asyncio.sleep(delay)
        guild = bot.get_guild(guild_id)
        if not guild:
            return

        vc = guild.voice_client
        vc_channel_id = getattr(getattr(vc, "channel", None), "id", None) if vc else None
        if vc and _voice_client_connected(vc) and vc_channel_id == channel_id:
            await persist_voice_state(guild_id, channel_id, desired_connected=True, connected=True)
            if _player_is_active(vc):
                unfreeze_playback_after_voice_return(guild_id)
                clear_recovery_retry(guild_id)
                clear_recovery_backoff(guild_id)
                logger.info(f"[{guild_id}] Voice link recovered during soft-disconnect grace; skipping hard rejoin.")
                return

        if recovery_backoff_remaining(guild_id) > 0:
            return

        tracked = playback_tracking.get(guild_id)
        if tracked and tracked.get("voice_soft_disconnected"):
            # Let restore_guild_state rebuild the active track from persisted state.
            playback_tracking.pop(guild_id, None)

        logger.warning(f"[{guild_id}] Voice link stayed down for {int(delay)}s after {reason}; starting gentle recovery from {int(position)}s.")
        await restore_guild_state(guild_id, {"voice_channel_id": channel_id, "position": max(0, int(position))})
    except asyncio.CancelledError:
        raise
    except Exception as tx_error:
        logger.exception(f"[{guild_id}] Soft voice recovery grace task failed.")
        schedule_recovery_retry(guild_id, channel_id, start_position=position, reason=f"{reason}_grace_failure")
    finally:
        current = asyncio.current_task()
        if voice_disconnect_grace_tasks.get(guild_id) is current:
            voice_disconnect_grace_tasks.pop(guild_id, None)

def schedule_soft_voice_recovery(guild_id, channel_id, *, start_position=0, reason="voice_disconnect"):
    if not channel_id:
        return False
    if not VOICE_DISCONNECT_REJOIN_RECOVERY:
        logger.info(f"[{guild_id}] Bot-side voice disconnect rejoin recovery is disabled; preserving playback state for manual/direct recovery ({reason}).")
        return False
    if not SOFT_VOICE_DISCONNECT_RECOVERY:
        return schedule_recovery_retry(guild_id, channel_id, start_position=start_position, reason=reason)
    if recovery_backoff_remaining(guild_id) > 0:
        return False
    existing = voice_disconnect_grace_tasks.get(guild_id)
    if existing and not existing.done():
        return False
    clear_recovery_retry(guild_id)
    delay = VOICE_DISCONNECT_GRACE_SECONDS + random.uniform(0.0, VOICE_DISCONNECT_GRACE_JITTER_SECONDS)
    task = asyncio.create_task(_run_soft_voice_recovery_after_grace(guild_id, channel_id, start_position, reason, delay))
    voice_disconnect_grace_tasks[guild_id] = task
    return True


async def _run_targeted_interrupt_resume(guild_id, channel_id, position, reason, delay):
    try:
        await asyncio.sleep(delay)
        if recovery_backoff_remaining(guild_id) > 0:
            return
        if voice_connect_inflight_remaining(guild_id) > 0:
            return
        guild = bot.get_guild(int(guild_id))
        if guild:
            vc = guild.voice_client
            if vc and _player_is_active(vc):
                return
        state = await derive_recovery_state_from_db(guild_id)
        if not state:
            return
        state["voice_channel_id"] = int(state.get("voice_channel_id") or channel_id or 0)
        state["position"] = max(0, int(state.get("position", position) or position or 0))
        if state.get("position", 0) < INTERRUPT_RESUME_MIN_POSITION_SECONDS:
            return
        if guild:
            await send_feedback_notice(
                guild,
                "🔁 Track Resume",
                f"Recovering **{_compact_track_title(state.get('title') or state.get('url') or 'interrupted track')}** from **{state.get('position', 0)}s** after {reason.replace('_', ' ')}.",
                discord.Color.blurple(),
                dedupe_key=f"interrupt_resume:{reason}:{state.get('track_uid') or guild_id}:{state.get('position', 0)}",
                fields=[("Track ID", f"`{_track_uid_label(state.get('track_uid'))}`", True)],
            )
        if guild_id in playback_tracking: playback_tracking[guild_id]["paused"] = False
        await restore_guild_state(guild_id, state, override_backoff=True)
    except asyncio.CancelledError:
        raise
    except Exception as tx_error:
        logger.exception("[%s] Targeted interrupt resume failed after %s.", guild_id, reason)
    finally:
        current = asyncio.current_task()
        if interrupt_resume_tasks.get(guild_id) is current:
            interrupt_resume_tasks.pop(guild_id, None)


def schedule_targeted_interrupt_resume(guild_id, channel_id, *, start_position=0, reason="voice_interrupt"):
    if not TARGETED_TRACK_INTERRUPT_RECOVERY or not channel_id:
        return False
    guild_id = int(guild_id)
    position = max(0, int(start_position or 0))
    if position < INTERRUPT_RESUME_MIN_POSITION_SECONDS:
        return False
    if recovery_backoff_remaining(guild_id) > 0 or voice_connect_inflight_remaining(guild_id) > 0:
        return False
    existing = interrupt_resume_tasks.get(guild_id)
    if existing and not existing.done():
        return False
    now = time.monotonic()
    armed_until = interrupt_resume_armed_until.get(guild_id, 0)
    if armed_until > now:
        return False
    delay = INTERRUPT_RESUME_GRACE_SECONDS + random.uniform(0.0, min(3.0, VOICE_DISCONNECT_GRACE_JITTER_SECONDS))
    task = asyncio.create_task(_run_targeted_interrupt_resume(guild_id, int(channel_id), position, str(reason or "voice_interrupt"), delay))
    interrupt_resume_tasks[guild_id] = task
    interrupt_resume_armed_until[guild_id] = now + INTERRUPT_RESUME_COOLDOWN_SECONDS
    return True

def _track_feedback_fields(track_uid=None, position=None):
    fields = []
    normalized_uid = _normalize_track_uid(track_uid)
    if normalized_uid:
        fields.append(("Track ID", f"`{_track_uid_label(normalized_uid)}`", True))
    if position is not None:
        fields.append(("Position", f"`{max(0, int(position or 0))}s`", True))
    return fields

async def remember_recovery_state(guild_id, channel_id, position=0, *, track_uid=None, url=None, title=None):
    if not channel_id:
        return
    position = normalize_position_seconds(position)
    tracked = playback_tracking.get(guild_id) or playback_tracking.get(str(guild_id))
    track_uid = _normalize_track_uid(track_uid) or _normalize_track_uid((tracked or {}).get("track_uid"))
    url = url or (tracked or {}).get("url")
    title = title or (tracked or {}).get("title")
    guild_states[guild_id] = {
        "voice_channel_id": channel_id,
        "position": position,
        "track_uid": track_uid,
        "url": url,
        "title": title,
    }
    if tracked is not None:
        tracked["last_position_checkpoint"] = position
        tracked["last_listen_position"] = position
        if track_uid:
            tracked["track_uid"] = track_uid
    invalidate_position_persist(guild_id)
    await save_state(guild_id)
    last_state_file_persist[guild_id] = time.time()



async def persist_voice_state(guild_id, channel_id=None, *, text_channel_id=None, desired_connected=True, connected=True, last_error=None):
    """Persist desired/actual voice channel so restarts and Discord voice drops can recover safely."""
    if not guild_id:
        return
    cache_key = int(guild_id)
    fingerprint = (channel_id, text_channel_id, bool(desired_connected), bool(connected), str(last_error or ""))
    cached = VOICE_STATE_PERSIST_CACHE.get(cache_key)
    if not last_error and cached and cached[0] == fingerprint and time.time() - cached[1] < VOICE_STATE_DEDUP_SECONDS:
        return
    VOICE_STATE_PERSIST_CACHE[cache_key] = (fingerprint, time.time())
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
                            None if connected else datetime.datetime.now(datetime.timezone.utc),
                            bool(desired_connected),
                            str(last_error)[:2000] if last_error else None,
                        ),
                    )
    except Exception as tx_error:
        logger.exception("[nexus] Failed to persist voice state for guild %s", guild_id)


async def mark_voice_disconnected(guild_id, channel_id=None, *, desired_connected=True, reason="voice_disconnect", position=None):
    await persist_voice_state(guild_id, channel_id, desired_connected=desired_connected, connected=False, last_error=reason)
    try:
        if position is None and (guild_id in playback_tracking or str(guild_id) in playback_tracking):
            position = current_track_position(guild_id)
    except Exception as tx_error:
        position = None
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    if position is None:
                        await cur.execute(
                            "UPDATE nexus_playback_state SET is_playing = FALSE, is_paused = FALSE WHERE guild_id = %s AND bot_name = 'nexus'",
                            (guild_id,),
                        )
                    else:
                        await cur.execute(
                            "UPDATE nexus_playback_state SET is_playing = FALSE, is_paused = FALSE, position_seconds = %s WHERE guild_id = %s AND bot_name = 'nexus'",
                            (normalize_position_seconds(position), guild_id),
                        )
    except Exception as tx_error:
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
    except Exception as tx_error:
        logger.exception("[nexus] Failed to reconcile playback state for guild %s", guild.id)


async def collect_and_persist_metrics(guild=None):
    guilds = [guild] if guild else list(bot.guilds)
    try:
        lavalink_ready = await ensure_lavalink_ready(timeout=1.0)
    except Exception as tx_error:
        lavalink_ready = False

    # Snapshot in-memory state (no DB, no await) for all guilds upfront.
    guild_snapshots = []
    for g in guilds:
        if not g:
            continue
        vc = g.voice_client
        channel_id = getattr(getattr(vc, "channel", None), "id", None)
        voice_connected = bool(vc and _voice_client_connected(vc))
        player_playing = bool(vc and _player_is_playing(vc))
        player_paused = bool(vc and _player_is_paused(vc))
        live_position = current_track_position(g.id) if (player_playing or player_paused or g.id in playback_tracking) else 0
        live_duration = int(playback_tracking.get(g.id, {}).get('duration', 0)) if g.id in playback_tracking else 0
        guild_snapshots.append((g, channel_id, voice_connected, player_playing, player_paused, live_position, live_duration))

    async def _fetch_guild_queue_counts(pool, g):
        """Fetch per-guild queue counts on its own connection so all guilds run in parallel.

        Note: does NOT join nexus_playback_state — reconcile_runtime_playback_state()
        already wrote the authoritative is_playing/is_paused/position_seconds for every
        guild in guild_snapshot immediately before this runs (see metrics_heartbeat_loop),
        so re-reading it back here would just be re-fetching values we already hold
        in-memory (player_playing/player_paused/live_position below).
        """
        try:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute(
                        """
                        SELECT
                            (SELECT COUNT(*) FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus') AS queue_total,
                            (SELECT COUNT(*) FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus') AS backup_total
                        """,
                        (g.id, g.id),
                    )
                    return await cur.fetchone() or {}
        except Exception:
            logger.debug("[nexus] Per-guild queue-count SELECT failed for guild %s", g.id, exc_info=True)
            return {}

    try:
        async with DBPoolManager() as pool:
            # Phase 1: fan-out all per-guild queue-count SELECTs in parallel (each on its own connection).
            # nexus_playback_state is intentionally NOT re-read here — reconcile_runtime_playback_state()
            # already wrote the authoritative is_playing/is_paused/position_seconds for these exact guilds
            # moments ago (metrics_heartbeat_loop runs it first), so the in-memory snapshot below is both
            # fresher and cheaper than a round trip to read back what we just wrote.
            fetch_results = await asyncio.gather(
                *[_fetch_guild_queue_counts(pool, g) for g, *_rest in guild_snapshots],
                return_exceptions=True,
            )

            # Phase 2: bulk REPLACE on one connection (no per-guild UPDATEs — reconcile already persisted state).
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    metrics_rows = []
                    for (g, channel_id, voice_connected, player_playing, player_paused, live_position, live_duration), counts_row in zip(guild_snapshots, fetch_results):
                        if isinstance(counts_row, Exception):
                            counts_row = {}
                        metrics_rows.append((
                            g.id,
                            voice_connected,
                            channel_id,
                            voice_connected,
                            player_playing,
                            player_paused,
                            int(counts_row.get("queue_total") or 0),
                            int(counts_row.get("backup_total") or 0),
                            player_playing if voice_connected else False,
                            player_paused if voice_connected else False,
                            int(live_position or 0),
                            live_duration,
                            g.id in recovering_guilds or str(g.id) in guild_states,
                            bool(lavalink_ready),
                            metrics_last_errors.get(g.id),
                        ))
                    if metrics_rows:
                        await cur.executemany(
                            """
                            REPLACE INTO nexus_metrics
                                (guild_id, bot_name, voice_connected, connected_channel_id, player_connected, player_playing, player_paused,
                                 queue_count, backup_queue_count, is_playing_db, is_paused_db, position_seconds, duration_seconds, recovery_pending,
                                 heartbeat_age_seconds, lavalink_ready, last_error)
                            VALUES (%s, 'nexus', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, %s, %s)
                            """,
                            metrics_rows,
                        )
    except Exception as exc:
        logger.exception("[nexus] Metrics collection failed: %s", exc)


async def restore_persistent_voice_states():
    """Rejoin desired voice channels after bot restart / Discord voice reconnect edges."""
    try:
        await bot.wait_until_ready()
        if aria_recovery_authority_blocks_self_heal("persistent_voice_restore"):
            logger.info(f"[{BOT_ENV_PREFIX.lower()}] Persistent voice restore deferred because Aria owns recovery decisions.")
            return
        if not PERSISTENT_VOICE_RESTORE_ON_STARTUP:
            logger.info(f"[{BOT_ENV_PREFIX.lower()}] Persistent voice restore is disabled by configuration; not auto-joining saved voice channels on startup.")
            return
        # Spread the fleet out at boot so all bot containers do not hit Discord voice at once.
        await asyncio.sleep(random.uniform(0.0, STARTUP_RECOVERY_JITTER_SECONDS))
        if not await ensure_lavalink_ready(timeout=LAVALINK_HEALTH_STARTUP_GRACE_SECONDS):
            logger.warning("[%s] Persistent voice restore skipped because Lavalink is not ready yet.", BOT_ENV_PREFIX.lower())
            return
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
                    async with DBPoolManager() as _pool:
                        async with _pool.acquire() as _conn:
                            async with _conn.cursor() as _cur:
                                await _cur.execute("SELECT stay_in_vc FROM nexus_guild_settings WHERE guild_id = %s", (guild.id,))
                                stay_row = await _cur.fetchone()
                                stay_in_vc = bool(stay_row[0]) if stay_row else False
                except Exception as tx_error:
                    logger.debug("[nexus] Failed checking 24/7 state for persistent voice restore.", exc_info=True)
                if not state and not stay_in_vc:
                    await persist_voice_state(guild.id, channel_id, desired_connected=False, connected=False, last_error="no_recoverable_playback")
                    continue
                await asyncio.sleep(VOICE_REJOIN_DELAY_SECONDS + random.uniform(VOICE_REJOIN_JITTER_MIN_SECONDS, VOICE_REJOIN_JITTER_MAX_SECONDS))
                vc = await ensure_voice_connection(guild, channel_id, respect_recovery_backoff=True)
                if vc:
                    await persist_voice_state(guild.id, channel_id, desired_connected=bool(stay_in_vc or state), connected=True)
                    if state:
                        schedule_named_task(f"persistent_voice_restore_process_queue:{guild.id}", process_queue(guild, channel_id, start_position=state.get("position", 0), allow_recovery_restore=True))
                else:
                    try:
                        async with DBPoolManager() as _pool:
                            async with _pool.acquire() as _conn:
                                async with _conn.cursor() as _cur:
                                    await _cur.execute("UPDATE nexus_voice_state SET reconnect_attempts = reconnect_attempts + 1, last_error = %s WHERE guild_id = %s AND bot_name = 'nexus'", ("rejoin_failed", guild.id))
                    except Exception as tx_error:
                        logger.debug("[nexus] Failed to increment reconnect_attempts for guild %s.", guild.id, exc_info=True)
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
        tracked = playback_tracking.get(guild_id) or playback_tracking.get(str(guild_id))
        if tracked is not None:
            tracked["paused"] = bool(paused)
        update_runtime_position_baseline(
            guild_id,
            position,
            channel_id=(tracked or {}).get("channel_id"),
            reset_listen_baseline=True,
        )
        await save_state(guild_id)
        last_state_file_persist[guild_id] = time.time()
    except Exception as tx_error:
        logger.exception("[nexus] Failed to sync pause state for guild %s.", guild_id)

async def insert_queue_front(cur, table_name, guild_id, bot_name, video_url, title, requester_id, track_uid=None, max_attempts=5, dedupe_identity=False):
    if not re.fullmatch(r"[A-Za-z0-9_]+", table_name):
        raise ValueError(f"Unsafe table name: {table_name}")

    track_uid = _ensure_track_uid(track_uid)
    existing = await find_existing_queue_row(
        cur,
        table_name,
        guild_id,
        bot_name,
        track_uid=track_uid,
        video_url=video_url,
        title=title,
        match_identity=dedupe_identity,
    )
    if existing:
        return int(_row_value(existing, "id", _row_value(existing, 0, 0)) or 0)
    for attempt in range(max_attempts):
        await cur.execute(f"SELECT COALESCE(MIN(id), 0) AS min_id FROM {table_name}")
        min_row = await cur.fetchone()
        new_id = (_scalar_from_row(min_row, 0) or 0) - 1
        try:
            await cur.execute(
                f"INSERT INTO {table_name} (id, guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (new_id, guild_id, bot_name, video_url, title, requester_id, track_uid)
            )
            return new_id
        except aiomysql.IntegrityError as e:
            if e.args and e.args[0] == 1062 and attempt < max_attempts - 1:
                existing = await find_existing_queue_row(
                    cur,
                    table_name,
                    guild_id,
                    bot_name,
                    track_uid=track_uid,
                    video_url=video_url,
                    title=title,
                    match_identity=dedupe_identity,
                )
                if existing:
                    return int(_row_value(existing, "id", _row_value(existing, 0, 0)) or 0)
                track_uid = _ensure_track_uid()
                await asyncio.sleep(0.05 * (attempt + 1))
                continue
            raise

async def snapshot_queue_backup(cur, guild_id):
    removed_live_dupes = await dedupe_queue_table_by_track_uid(cur, "nexus_queue", guild_id=guild_id, bot_name="nexus")
    if removed_live_dupes > 0:
        logger.warning("[%s] Removed %s duplicate live queue row(s) before rebuilding backup snapshot.", guild_id, removed_live_dupes)
    await cur.execute("SELECT video_url, title, requester_id, track_uid FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC", (guild_id,))
    rows = list(await cur.fetchall() or [])
    insert_data = []
    seen_track_uids = set()
    skipped_duplicates = 0
    for row in rows:
        normalized_uid = _ensure_track_uid(_row_track_uid(row, 3))
        if normalized_uid in seen_track_uids:
            skipped_duplicates += 1
            continue
        seen_track_uids.add(normalized_uid)
        insert_data.append((
            guild_id,
            'nexus',
            _row_value(row, "video_url", _row_value(row, 0)),
            _row_value(row, "title", _row_value(row, 1)),
            _row_value(row, "requester_id", _row_value(row, 2)),
            normalized_uid,
        ))
    try:
        await cur.execute("START TRANSACTION")
        await cur.execute("DELETE FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus'", (guild_id,))
        if insert_data:
            await cur.executemany(
                """
                INSERT INTO nexus_queue_backup (guild_id, bot_name, video_url, title, requester_id, track_uid)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    video_url = VALUES(video_url),
                    title = VALUES(title),
                    requester_id = VALUES(requester_id)
                """,
                insert_data,
            )
        await cur.execute("COMMIT")
    except Exception as tx_error:
        try:
            await cur.execute("ROLLBACK")
        except Exception as tx_error:
            pass
        raise
    if skipped_duplicates > 0:
        logger.warning("[%s] Skipped %s duplicate live queue row(s) while rebuilding backup snapshot.", guild_id, skipped_duplicates)
    return len(insert_data)

async def backup_track(cur, guild_id, video_url, title, requester_id, track_uid=None):
    track_uid = _ensure_track_uid(track_uid)
    await cur.execute(
        "SELECT COUNT(*) FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus' AND track_uid = %s",
        (guild_id, track_uid),
    )
    existing = await cur.fetchone()
    if existing and int(_scalar_from_row(existing, 0) or 0) > 0:
        return 0
    try:
        await cur.execute(
            "INSERT INTO nexus_queue_backup (guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, 'nexus', %s, %s, %s, %s)",
            (guild_id, video_url, title, requester_id, track_uid),
        )
        return track_uid
    except aiomysql.IntegrityError as e:
        if e.args and e.args[0] == 1062:
            return 0
        raise


async def delete_backup_track(cur, guild_id, *, track_uid=None, video_url=None, title=None):
    normalized_uid = _normalize_track_uid(track_uid)
    if normalized_uid:
        await cur.execute(
            "DELETE FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus' AND track_uid = %s LIMIT 1",
            (guild_id, normalized_uid),
        )
        return max(0, int(getattr(cur, "rowcount", 0) or 0))
    if not video_url and not title:
        return 0
    await cur.execute(
        "DELETE FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus' AND video_url = %s AND title = %s LIMIT 1",
        (guild_id, video_url, title),
    )
    return max(0, int(getattr(cur, "rowcount", 0) or 0))


async def enqueue_track(cur, guild_id, video_url, title, requester_id, *, backup=True, track_uid=None):
    track_uid = _ensure_track_uid(track_uid)
    for attempt in range(5):
        try:
            await cur.execute(
                "INSERT INTO nexus_queue (guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, 'nexus', %s, %s, %s, %s)",
                (guild_id, video_url, title, requester_id, track_uid),
            )
            break
        except aiomysql.IntegrityError as e:
            if e.args and e.args[0] == 1062 and attempt < 4:
                track_uid = _ensure_track_uid()
                await asyncio.sleep(0.01 * (attempt + 1))
                continue
            raise
    if backup:
        try:
            await bulk_record_tracks_queued(cur, guild_id, [(video_url, title, requester_id)])
        except Exception as tx_error:
            logger.debug("[nexus] Track intelligence queue write skipped.", exc_info=True)
        await backup_track(cur, guild_id, video_url, title, requester_id, track_uid)
    return track_uid


def _queue_track_identity(row):
    url = _row_value(row, "video_url", _row_value(row, 3, _row_value(row, 0, "")))
    title = _row_value(row, "title", _row_value(row, 4, _row_value(row, 1, "")))
    return _track_instance_identity(_row_track_uid(row, 6, 5, 4, 3), url, title)

def _queue_source_identity(row):
    url = _row_value(row, "video_url", _row_value(row, 3, _row_value(row, 0, "")))
    title = _row_value(row, "title", _row_value(row, 4, _row_value(row, 1, "")))
    return _track_key(url, title)

def _spread_duplicate_tracks(rows, previous_row=None, *, reorder_limit=None):
    remaining = list(rows)
    tail = []
    if reorder_limit and len(remaining) > reorder_limit:
        tail = remaining[reorder_limit:]
        remaining = remaining[:reorder_limit]
    arranged = []
    recent_exact = deque(maxlen=QUEUE_SPREAD_HISTORY_WINDOW)
    recent_family = deque(maxlen=QUEUE_SPREAD_HISTORY_WINDOW)
    if previous_row is not None:
        recent_exact.append(_queue_source_identity(previous_row))
        recent_family.append(_queue_family_key(previous_row))
    remaining_exact = Counter(_queue_source_identity(row) for row in remaining)
    while remaining:
        best_index = 0
        best_score = None
        for idx, candidate in enumerate(remaining):
            score = _queue_spacing_score(candidate, recent_exact, recent_family, remaining_exact)
            if best_score is None or score > best_score:
                best_score = score
                best_index = idx
        row = remaining.pop(best_index)
        arranged.append(row)
        source_identity = _queue_source_identity(row)
        recent_exact.append(source_identity)
        recent_family.append(_queue_family_key(row))
        remaining_exact[source_identity] = max(0, int(remaining_exact.get(source_identity, 0)) - 1)
        if remaining_exact.get(source_identity, 0) <= 0:
            remaining_exact.pop(source_identity, None)
    return arranged + tail

def claim_live_queue_track(guild_id, video_url, title, track_uid=None):
    """Mark the dequeued row as in-flight so parity repair will not resurrect it too early."""
    queue_playback_claims[int(guild_id)] = (_track_instance_identity(track_uid, video_url, title), time.time() + QUEUE_PLAYBACK_CLAIM_TTL_SECONDS)


def clear_live_queue_claim(guild_id, video_url=None, title=None, track_uid=None):
    key = int(guild_id)
    current = queue_playback_claims.get(key)
    if not current:
        return
    current_key, _expires_at = current
    if video_url is None and title is None:
        queue_playback_claims.pop(key, None)
        return
    if current_key == _track_instance_identity(track_uid, video_url, title):
        queue_playback_claims.pop(key, None)


def current_live_queue_claim_key(guild_id):
    key = int(guild_id)
    current = queue_playback_claims.get(key)
    if not current:
        return ""
    track_key, expires_at = current
    if expires_at <= time.time():
        queue_playback_claims.pop(key, None)
        return ""
    return track_key


async def dedupe_queue_table_by_track_uid(cur, table_name, *, guild_id=None, bot_name=None):
    if not re.fullmatch(r"[A-Za-z0-9_]+", table_name):
        raise ValueError(f"Unsafe table name: {table_name}")

    where = ["track_uid IS NOT NULL", "track_uid <> ''"]
    params = []
    if guild_id is not None:
        where.append("guild_id = %s")
        params.append(int(guild_id))
    if bot_name is not None:
        where.append("bot_name = %s")
        params.append(str(bot_name))

    await cur.execute(
        f"SELECT id, guild_id, bot_name, track_uid FROM {table_name} WHERE {' AND '.join(where)} ORDER BY guild_id ASC, bot_name ASC, track_uid ASC, id ASC",
        tuple(params),
    )
    rows = list(await cur.fetchall() or [])
    if not rows:
        return 0

    seen = set()
    duplicate_ids = []
    for row in rows:
        normalized_uid = _normalize_track_uid(_row_track_uid(row, 3))
        row_id = _row_value(row, "id", _row_value(row, 0))
        row_guild_id = int(_row_value(row, "guild_id", _row_value(row, 1, 0)) or 0)
        row_bot_name = str(_row_value(row, "bot_name", _row_value(row, 2, "")) or "")
        if not normalized_uid or row_id is None:
            continue
        dedupe_key = (row_guild_id, row_bot_name, normalized_uid)
        if dedupe_key in seen:
            duplicate_ids.append(int(row_id))
            continue
        seen.add(dedupe_key)

    deleted = 0
    for row_id in duplicate_ids:
        await cur.execute(f"DELETE FROM {table_name} WHERE id = %s", (row_id,))
        deleted += max(0, int(getattr(cur, "rowcount", 0) or 0))
    return deleted



async def dedupe_queue_table_by_track_key(cur, table_name, *, guild_id=None, bot_name=None):
    """Remove duplicate rows sharing the same URL/title hash, keeping the lowest id (oldest entry)."""
    if not re.fullmatch(r"[A-Za-z0-9_]+", table_name):
        raise ValueError(f"Unsafe table name: {table_name}")

    where = []
    params = []
    if guild_id is not None:
        where.append("guild_id = %s")
        params.append(int(guild_id))
    if bot_name is not None:
        where.append("bot_name = %s")
        params.append(str(bot_name))

    where_clause = f"WHERE {' AND '.join(where)}" if where else ""
    await cur.execute(
        f"SELECT id, guild_id, bot_name, video_url, title FROM {table_name} {where_clause} ORDER BY guild_id ASC, bot_name ASC, id ASC",
        tuple(params),
    )
    rows = list(await cur.fetchall() or [])
    if not rows:
        return 0

    seen = set()
    duplicate_ids = []
    for row in rows:
        row_id = _row_value(row, "id", _row_value(row, 0))
        row_guild_id = int(_row_value(row, "guild_id", _row_value(row, 1, 0)) or 0)
        row_bot_name = str(_row_value(row, "bot_name", _row_value(row, 2, "")) or "")
        url = str(_row_value(row, "video_url", _row_value(row, 3, "")) or "")
        title = str(_row_value(row, "title", _row_value(row, 4, "")) or "")
        if row_id is None:
            continue
        dedup_key = (row_guild_id, row_bot_name, _track_key(url, title))
        if dedup_key in seen:
            duplicate_ids.append(int(row_id))
            continue
        seen.add(dedup_key)

    deleted = 0
    for row_id in duplicate_ids:
        await cur.execute(f"DELETE FROM {table_name} WHERE id = %s", (row_id,))
        deleted += max(0, int(getattr(cur, "rowcount", 0) or 0))
    return deleted


QUEUE_DUPLICATE_CAP = int(os.getenv("QUEUE_DUPLICATE_CAP", "2"))


async def trim_queue_duplicate_runs(cur, table_name, *, guild_id=None, bot_name=None, max_copies=QUEUE_DUPLICATE_CAP):
    """Keep at most max_copies rows per (guild, URL/title), lowest ids first. Collapses
    loop/re-add bloat while leaving small deliberate repeats intact. Returns rows deleted."""
    if not re.fullmatch(r"[A-Za-z0-9_]+", table_name):
        raise ValueError(f"Unsafe table name: {table_name}")
    cap = max(1, int(max_copies))
    where = []
    params = []
    if guild_id is not None:
        where.append("guild_id = %s")
        params.append(int(guild_id))
    if bot_name is not None:
        where.append("bot_name = %s")
        params.append(str(bot_name))
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""
    await cur.execute(
        f"SELECT id, guild_id, bot_name, video_url, title FROM {table_name} {where_clause} ORDER BY guild_id ASC, bot_name ASC, id ASC",
        tuple(params),
    )
    rows = list(await cur.fetchall() or [])
    if not rows:
        return 0
    counts = {}
    excess_ids = []
    for row in rows:
        row_id = _row_value(row, "id", _row_value(row, 0))
        row_guild_id = int(_row_value(row, "guild_id", _row_value(row, 1, 0)) or 0)
        row_bot_name = str(_row_value(row, "bot_name", _row_value(row, 2, "")) or "")
        url = str(_row_value(row, "video_url", _row_value(row, 3, "")) or "")
        title = str(_row_value(row, "title", _row_value(row, 4, "")) or "")
        if row_id is None or not url:
            continue
        key = (row_guild_id, row_bot_name, _track_key(url, title))
        counts[key] = counts.get(key, 0) + 1
        if counts[key] > cap:
            excess_ids.append(int(row_id))
    deleted = 0
    for row_id in excess_ids:
        await cur.execute(f"DELETE FROM {table_name} WHERE id = %s", (row_id,))
        deleted += max(0, int(getattr(cur, "rowcount", 0) or 0))
    return deleted


async def find_existing_queue_row(cur, table_name, guild_id, bot_name, *, track_uid=None, video_url=None, title=None, match_identity=False):
    if not re.fullmatch(r"[A-Za-z0-9_]+", table_name):
        raise ValueError(f"Unsafe table name: {table_name}")

    normalized_uid = _normalize_track_uid(track_uid)
    if normalized_uid:
        await cur.execute(
            f"SELECT id, video_url, title, track_uid FROM {table_name} WHERE guild_id = %s AND bot_name = %s AND track_uid = %s ORDER BY id ASC LIMIT 1",
            (guild_id, bot_name, normalized_uid),
        )
        row = await cur.fetchone()
        if row:
            return row

    if not match_identity:
        return None

    target_identity = _track_instance_identity(normalized_uid, video_url, title)
    if not target_identity:
        return None

    await cur.execute(
        f"SELECT id, video_url, title, track_uid FROM {table_name} WHERE guild_id = %s AND bot_name = %s ORDER BY id ASC",
        (guild_id, bot_name),
    )
    rows = list(await cur.fetchall() or [])
    for row in rows:
        row_identity = _track_instance_identity(
            _row_track_uid(row, 3),
            _row_value(row, "video_url", _row_value(row, 1, "")),
            _row_value(row, "title", _row_value(row, 2, "")),
        )
        if row_identity == target_identity:
            return row
    return None


async def delete_live_queue_copies(cur, guild_id, video_url, title, track_uid=None):
    """Delete live-queue copies that match the exact normalized track identity.

    The old SQL used ``video_url = X OR title = Y``. That was too broad: two
    different uploads can share a title, and YouTube URL variants can share a
    video id. Select candidate rows first, compare with _track_key(), then
    delete only true identity matches by primary key.
    """
    target_identity = _track_instance_identity(track_uid, video_url, title)
    if not target_identity:
        return 0
    normalized_uid = _normalize_track_uid(track_uid)
    if normalized_uid:
        await cur.execute(
            "DELETE FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' AND track_uid = %s",
            (guild_id, normalized_uid),
        )
        return max(0, int(getattr(cur, "rowcount", 0) or 0))
    await cur.execute(
        "SELECT id, video_url, title, track_uid FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'",
        (guild_id,),
    )
    rows = list(await cur.fetchall() or [])
    deleted = 0
    for row in rows:
        row_identity = _track_instance_identity(
            _row_track_uid(row, 3),
            _row_value(row, "video_url", _row_value(row, 1, "")),
            _row_value(row, "title", _row_value(row, 2, "")),
        )
        if row_identity != target_identity:
            continue
        row_id = _row_value(row, "id", _row_value(row, 0))
        if row_id is None:
            continue
        await cur.execute(
            "DELETE FROM nexus_queue WHERE id = %s AND guild_id = %s AND bot_name = 'nexus'",
            (row_id, guild_id),
        )
        deleted += max(0, int(getattr(cur, "rowcount", 0) or 0))
    return deleted

async def _fetch_shuffle_weights(cur, guild_id, rows):
    """Score queued tracks from this server's play/finish/skip history so the
    smart shuffle nudges crowd-favorites earlier while staying randomized."""
    url_keys = {_queue_source_identity(row) for row in rows}
    if not url_keys:
        return {}
    weights = {}
    try:
        placeholders = ",".join(["%s"] * len(url_keys))
        await cur.execute(
            f"SELECT url_key, play_count, finish_count, skip_count, like_count, dislike_count "
            f"FROM nexus_track_intelligence WHERE guild_id = %s AND url_key IN ({placeholders})",
            (guild_id, *url_keys),
        )
        for row in await cur.fetchall() or []:
            url_key = _row_value(row, "url_key", _row_value(row, 0))
            play_count = int(_row_value(row, "play_count", _row_value(row, 1)) or 0)
            finish_count = int(_row_value(row, "finish_count", _row_value(row, 2)) or 0)
            skip_count = int(_row_value(row, "skip_count", _row_value(row, 3)) or 0)
            like_count = int(_row_value(row, "like_count", _row_value(row, 4)) or 0)
            dislike_count = int(_row_value(row, "dislike_count", _row_value(row, 5)) or 0)
            plays = max(1, play_count)
            score = 1.0
            score += (finish_count / plays) * 0.4
            score -= (skip_count / plays) * 0.5
            score += like_count * 0.15
            score -= dislike_count * 0.2
            weights[url_key] = max(0.15, min(2.5, score))
    except Exception:
        return {}
    return weights

def _weighted_shuffle(rows, weights):
    """Randomize order while biasing toward higher-weighted tracks. Falls
    back to a plain shuffle when no weight data is available."""
    if not weights:
        random.shuffle(rows)
        return rows
    pool = [(row, max(0.05, weights.get(_queue_source_identity(row), 1.0))) for row in rows]
    ordered = []
    while pool:
        total = sum(w for _, w in pool)
        target = random.random() * total
        upto = 0.0
        for idx, (row, w) in enumerate(pool):
            upto += w
            if upto >= target:
                ordered.append(pool.pop(idx)[0])
                break
        else:
            ordered.append(pool.pop()[0])
    return ordered

async def shuffle_queue_rows(cur, guild_id, *, preserve_first=True):
    await cur.execute("SELECT id, guild_id, bot_name, video_url, title, requester_id, track_uid FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC", (guild_id,))
    rows = list(await cur.fetchall() or [])
    if len(rows) <= 1:
        return len(rows)
    head = []
    if preserve_first:
        head = [rows.pop(0)]
    weights = await _fetch_shuffle_weights(cur, guild_id, rows)
    rows = _weighted_shuffle(rows, weights)
    reorder_limit = max(1, QUEUE_SPREAD_REORDER_LIMIT - len(head))
    rows = head + _spread_duplicate_tracks(rows, head[-1] if head else None, reorder_limit=reorder_limit)

    # Queue leak fix: this function used to DELETE the live queue and then
    # reinsert rows while autocommit was enabled. A DB hiccup between those
    # statements could permanently drop queued tracks even though backup still
    # had them. Keep the rebuild atomic.
    try:
        await cur.execute("START TRANSACTION")
        await cur.execute("DELETE FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", (guild_id,))
        insert_data = [
            (
                _row_value(row, "guild_id", _row_value(row, 1, guild_id)),
                'nexus',
                _row_value(row, "video_url", _row_value(row, 3)),
                _row_value(row, "title", _row_value(row, 4)),
                _row_value(row, "requester_id", _row_value(row, 5)),
                _ensure_track_uid(_row_track_uid(row, 6)),
            )
            for row in rows
        ]
        if insert_data:
            await cur.executemany(
                "INSERT INTO nexus_queue (guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, %s, %s, %s, %s, %s)",
                insert_data,
            )
        await cur.execute("COMMIT")
    except Exception as tx_error:
        try:
            await cur.execute("ROLLBACK")
        except Exception as tx_error:
            pass
        logger.exception("[nexus] Queue reshuffle transaction failed; live queue was rolled back instead of leaking tracks.")
        raise
    return len(rows)

async def requeue_finished_track(cur, guild_id, video_url, title, requester_id, track_uid=None):
    original_uid = _normalize_track_uid(track_uid)
    # loop_mode='queue' re-appends the finished track; each re-append gets a fresh UID
    # (bypassing the track_uid unique index), so cap copies to stop a degenerate loop
    # (e.g. a single-track queue) from growing without bound.
    if video_url:
        await cur.execute(
            "SELECT COUNT(*) FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' AND video_url = %s",
            (guild_id, video_url),
        )
        _existing_copies = await cur.fetchone()
        if _existing_copies and int(_existing_copies[0] or 0) >= QUEUE_DUPLICATE_CAP:
            return True
    actual_uid = await enqueue_track(cur, guild_id, video_url, title, requester_id, backup=False, track_uid=track_uid)
    # If the insert had to regenerate a new UID (duplicate-key collision), the backup
    # still holds the old UID. Sync it now so parity repair never sees a phantom gap.
    if original_uid and actual_uid and actual_uid != original_uid:
        try:
            await cur.execute(
                "UPDATE nexus_queue_backup SET track_uid = %s "
                "WHERE guild_id = %s AND bot_name = 'nexus' AND track_uid = %s",
                (actual_uid, guild_id, original_uid),
            )
        except Exception:
            # actual_uid is already in backup — stale original_uid entry is now a duplicate; remove it
            try:
                await cur.execute(
                    "DELETE FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus' AND track_uid = %s LIMIT 1",
                    (guild_id, original_uid),
                )
            except Exception:
                pass
    return True

async def prime_loop_queue_defaults(cur, guild_id, *, shuffle_if_needed=True):
    await cur.execute(
        "INSERT INTO nexus_guild_settings (guild_id, loop_mode) VALUES (%s, 'queue') ON DUPLICATE KEY UPDATE loop_mode = 'queue'",
        (guild_id,),
    )
    return 0

async def restore_queue_from_backup(cur, guild_id, requester_id=None):
    await cur.execute("SELECT COUNT(*) FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", (guild_id,))
    queue_count_row = await cur.fetchone()
    queue_count = queue_count_row[0] if queue_count_row else 0
    if queue_count:
        return 0

    removed_backup_dupes = await dedupe_queue_table_by_track_uid(cur, "nexus_queue_backup", guild_id=guild_id, bot_name="nexus")
    if removed_backup_dupes > 0:
        logger.warning("[%s] Removed %s duplicate backup queue row(s) before restoring live queue.", guild_id, removed_backup_dupes)
    # NOTE: deliberately NOT running dedupe_queue_table_by_track_key here. Each queue
    # insertion gets its own random track_uid (see _new_track_uid), so two legitimate
    # plays of the same song are distinct rows with distinct UIDs but identical content
    # hashes — exactly what _spread_duplicate_tracks below is designed to keep and
    # space apart. A content-hash dedupe pass on every restore would silently delete
    # one of every intentionally-repeated track, on every recovery cycle.
    await cur.execute("SELECT video_url, title, requester_id, track_uid FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC", (guild_id,))
    rows = list(await cur.fetchall() or [])
    if not rows:
        return 0
    rows = _spread_duplicate_tracks(rows)

    insert_data = []
    for row in rows:
        url = _row_value(row, "video_url", _row_value(row, 0))
        title = _row_value(row, "title", _row_value(row, 1))
        backup_requester_id = _row_value(row, "requester_id", _row_value(row, 2))
        track_uid = _ensure_track_uid(_row_track_uid(row, 3))
        insert_data.append((guild_id, 'nexus', url, title, requester_id if requester_id is not None else backup_requester_id, track_uid))
    if insert_data:
        await cur.executemany(
            """
            INSERT INTO nexus_queue (guild_id, bot_name, video_url, title, requester_id, track_uid)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                video_url = VALUES(video_url),
                title = VALUES(title),
                requester_id = VALUES(requester_id)
            """,
            insert_data,
        )
    return len(insert_data)


async def repair_queue_backup_parity(cur, guild_id, *, reason="queue_integrity", active_player=None):
    """Reconcile live queue against backup queue, with backup as the source of truth.

    The live queue is what gets consumed by playback, so a crash/timeout can remove
    a row from live before the track actually finishes.  The backup queue is the
    durable copy.  When a backup row is missing from live, remove one stale live
    row if needed, then copy the exact backup row back into live.
    """
    removed_live_dupes = await dedupe_queue_table_by_track_uid(cur, "nexus_queue", guild_id=guild_id, bot_name="nexus")
    removed_backup_dupes = await dedupe_queue_table_by_track_uid(cur, "nexus_queue_backup", guild_id=guild_id, bot_name="nexus")
    if removed_live_dupes or removed_backup_dupes:
        logger.warning(
            "[%s] Removed duplicate queue rows before parity repair (live=%s, backup=%s) after %s.",
            guild_id,
            removed_live_dupes,
            removed_backup_dupes,
            reason,
        )
    await cur.execute("SELECT id, video_url, title, requester_id, track_uid FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC", (guild_id,))
    backup_rows = list(await cur.fetchall() or [])
    await cur.execute("SELECT id, video_url, title, requester_id, track_uid FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC", (guild_id,))
    live_rows = list(await cur.fetchall() or [])
    if not backup_rows and not live_rows:
        return 0, 0

    await cur.execute(
        "SELECT video_url, title, is_playing, is_paused, position_seconds, track_uid FROM nexus_playback_state WHERE guild_id = %s AND bot_name = 'nexus' LIMIT 1",
        (guild_id,),
    )
    playback_row = await cur.fetchone() or {}
    active_url = str(_row_value(playback_row, "video_url", _row_value(playback_row, 0, "")) or "").strip()
    active_title = str(_row_value(playback_row, "title", _row_value(playback_row, 1, "")) or "").strip()
    active_track_uid = _row_track_uid(playback_row, 5)
    db_active_playback = bool(
        (active_url or active_title)
        and (
            bool(_row_value(playback_row, "is_playing", _row_value(playback_row, 2, False)))
            or bool(_row_value(playback_row, "is_paused", _row_value(playback_row, 3, False)))
            or int(_row_value(playback_row, "position_seconds", _row_value(playback_row, 4, 0)) or 0) > 0
        )
    )
    active_playback = db_active_playback if active_player is None else bool(active_player and (active_url or active_title))
    claimed_track_key = current_live_queue_claim_key(guild_id)

    def _matches_active(row):
        row_url = str(_row_value(row, "video_url", _row_value(row, 1, "")) or "").strip()
        row_title = str(_row_value(row, "title", _row_value(row, 2, "")) or "").strip()
        row_identity = _track_instance_identity(_row_track_uid(row, 4), row_url, row_title)
        return (
            (active_playback and row_identity == _track_instance_identity(active_track_uid, active_url, active_title))
            or (claimed_track_key and row_identity == claimed_track_key)
        )

    def _parity_track_key(row):
        return _track_instance_identity(
            _row_track_uid(row, 4),
            _row_value(row, "video_url", _row_value(row, 1, "")),
            _row_value(row, "title", _row_value(row, 2, "")),
        )

    def _row_id(row):
        return _row_value(row, "id", _row_value(row, 0))

    # Count current live rows, then walk backup in order.  Any backup row that
    # cannot consume a matching live row is a lost live-queue row that must be
    # copied from backup back into live.
    live_counts = {}
    for row in live_rows:
        key = _parity_track_key(row)
        live_counts[key] = live_counts.get(key, 0) + 1

    missing_live_rows = []
    skipped_active = False
    for row in backup_rows:
        if not skipped_active and _matches_active(row):
            skipped_active = True
            continue
        key = _parity_track_key(row)
        if live_counts.get(key, 0) > 0:
            live_counts[key] -= 1
            continue
        missing_live_rows.append(row)

    if active_playback and not skipped_active and missing_live_rows:
        # Playback state may use a resolved Lavalink URI while backup still has
        # the raw request/search. If the player is truly active, reserve one
        # unexplained backup/live gap for the currently playing track.
        missing_live_rows = missing_live_rows[1:]
    if len(missing_live_rows) > QUEUE_PARITY_REPAIR_MAX_ROWS:
        logger.warning("[%s] Queue parity repair capped missing live rows from %s to %s after %s.", guild_id, len(missing_live_rows), QUEUE_PARITY_REPAIR_MAX_ROWS, reason)
        missing_live_rows = missing_live_rows[:QUEUE_PARITY_REPAIR_MAX_ROWS]

    # Secondary pass: filter out "missing" backup rows that are actually in live
    # under a different UID (UID drift from loop_mode requeue). When loop_mode='queue'
    # re-inserts a finished track and a duplicate-key forces a new UID, the backup
    # keeps the old UID. Treating it as "missing" would restore a duplicate.
    # Instead, sync the backup UID to the live UID and skip the restoration.
    if missing_live_rows:
        live_by_track_key = {}
        for row in live_rows:
            key = _track_key(
                _row_value(row, "video_url", _row_value(row, 1, "")),
                _row_value(row, "title", _row_value(row, 2, "")),
            )
            if key not in live_by_track_key:
                live_by_track_key[key] = _ensure_track_uid(_row_track_uid(row, 4))
        uid_sync_pairs = []
        genuine_missing = []
        for row in missing_live_rows:
            bk_url = _row_value(row, "video_url", _row_value(row, 1, ""))
            bk_title = _row_value(row, "title", _row_value(row, 2, ""))
            bk_uid = _ensure_track_uid(_row_track_uid(row, 4))
            live_uid = live_by_track_key.get(_track_key(bk_url, bk_title))
            if live_uid and live_uid != bk_uid:
                uid_sync_pairs.append((bk_uid, live_uid))
            else:
                genuine_missing.append(row)
        if uid_sync_pairs:
            logger.debug("[%s] Queue parity: syncing %s backup UID(s) that drifted from live requeue UIDs after %s.", guild_id, len(uid_sync_pairs), reason)
            for old_uid, new_uid in uid_sync_pairs:
                try:
                    await cur.execute(
                        "UPDATE nexus_queue_backup SET track_uid = %s "
                        "WHERE guild_id = %s AND bot_name = 'nexus' AND track_uid = %s",
                        (new_uid, guild_id, old_uid),
                    )
                except Exception:
                    # new_uid already exists in backup — stale old_uid is a true duplicate; remove it
                    try:
                        await cur.execute(
                            "DELETE FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus' AND track_uid = %s LIMIT 1",
                            (guild_id, old_uid),
                        )
                    except Exception:
                        pass
        missing_live_rows = genuine_missing

    if missing_live_rows and not queue_parity_repair_allowed(guild_id, backup_rows, live_rows, reason=reason):
        return 0, 0

    # Find live rows that do not have a matching backup row.  When live and
    # backup counts are equal but contain different tracks, the previous repair
    # code missed the issue.  We now delete one stale live row before copying the
    # missing backup row, keeping queue length stable and preventing duplicates.
    backup_counts = {}
    skipped_active_for_surplus = False
    for row in backup_rows:
        if not skipped_active_for_surplus and _matches_active(row):
            skipped_active_for_surplus = True
            continue
        key = _parity_track_key(row)
        backup_counts[key] = backup_counts.get(key, 0) + 1

    surplus_live_rows = []
    for row in live_rows:
        key = _parity_track_key(row)
        if backup_counts.get(key, 0) > 0:
            backup_counts[key] -= 1
            continue
        surplus_live_rows.append(row)

    purged_live = 0
    if surplus_live_rows:
        for row in surplus_live_rows[:len(missing_live_rows)]:
            row_id = _row_id(row)
            if row_id is None:
                continue
            await cur.execute(
                "DELETE FROM nexus_queue WHERE id = %s AND guild_id = %s AND bot_name = 'nexus'",
                (row_id, guild_id),
            )
            purged_live += max(0, int(getattr(cur, "rowcount", 0) or 0))

    restored_live = 0
    for row in reversed(missing_live_rows):
        await insert_queue_front(
            cur,
            "nexus_queue",
            guild_id,
            "nexus",
            _row_value(row, "video_url", _row_value(row, 1)),
            _row_value(row, "title", _row_value(row, 2)),
            _row_value(row, "requester_id", _row_value(row, 3)),
            _ensure_track_uid(_row_track_uid(row, 4)),
            dedupe_identity=True,
        )
        restored_live += 1

    # If a live row exists but backup is behind, heal backup too.  This path is
    # intentionally limited so normal removal/skip commands that already delete
    # from backup are not undone.
    backup_counts = {}
    for row in backup_rows:
        key = _parity_track_key(row)
        backup_counts[key] = backup_counts.get(key, 0) + 1

    missing_backup_rows = []
    for row in live_rows:
        key = _parity_track_key(row)
        if backup_counts.get(key, 0) > 0:
            backup_counts[key] -= 1
            continue
        # Rows already purged as stale should not be copied back into backup.
        if missing_live_rows and row in surplus_live_rows[:len(missing_live_rows)]:
            continue
        missing_backup_rows.append(row)

    backup_restore_budget = max(0, len(live_rows) - len(backup_rows))
    if backup_restore_budget <= 0:
        missing_backup_rows = []
    elif len(missing_backup_rows) > backup_restore_budget:
        missing_backup_rows = missing_backup_rows[:backup_restore_budget]

    if len(missing_backup_rows) > QUEUE_PARITY_REPAIR_MAX_ROWS:
        logger.warning("[%s] Queue parity repair capped missing backup rows from %s to %s after %s.", guild_id, len(missing_backup_rows), QUEUE_PARITY_REPAIR_MAX_ROWS, reason)
        missing_backup_rows = missing_backup_rows[:QUEUE_PARITY_REPAIR_MAX_ROWS]

    if missing_backup_rows and restored_live == 0 and not queue_parity_repair_allowed(guild_id, backup_rows, live_rows, reason=reason):
        return restored_live, 0

    restored_backup = 0
    for row in missing_backup_rows:
        await cur.execute(
            """
            INSERT INTO nexus_queue_backup (guild_id, bot_name, video_url, title, requester_id, track_uid)
            VALUES (%s, 'nexus', %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                video_url = VALUES(video_url),
                title = VALUES(title),
                requester_id = VALUES(requester_id)
            """,
            (
                guild_id,
                _row_value(row, "video_url", _row_value(row, 1)),
                _row_value(row, "title", _row_value(row, 2)),
                _row_value(row, "requester_id", _row_value(row, 3)),
                _ensure_track_uid(_row_track_uid(row, 4)),
            ),
        )
        restored_backup += 1

    if restored_live or restored_backup or purged_live:
        logger.warning(
            "[%s] Queue parity repair purged %s stale live row(s), restored %s live row(s) from backup, and restored %s backup row(s) from live after %s.",
            guild_id, purged_live, restored_live, restored_backup, reason,
        )
    return restored_live, restored_backup


async def restore_active_playback_entry(cur, guild_id, requester_id=None):
    await cur.execute(
        "SELECT video_url, title, track_uid FROM nexus_playback_state "
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
    track_uid = _ensure_track_uid(_row_track_uid(row, 2))
    existing = await find_existing_queue_row(
        cur,
        "nexus_queue",
        guild_id,
        "nexus",
        track_uid=track_uid,
        video_url=video_url,
        title=title,
        match_identity=True,
    )
    if existing:
        return 0

    await insert_queue_front(
        cur,
        "nexus_queue",
        guild_id,
        "nexus",
        video_url,
        title or "Resumed Track",
        requester_id if requester_id is not None else (bot.user.id if bot.user else None),
        track_uid,
        dedupe_identity=True,
    )
    return 1

def schedule_recovery_retry(guild_id, channel_id, *, start_position=0, reason="recovery"):
    if not channel_id:
        return False

    reason_text = str(reason or "").lower()
    voice_rejoin_reason = any(token in reason_text for token in (
        "voice_disconnect",
        "voice_connect",
        "restore_voice",
        "persistent_voice_restore",
        "idle_restore",
        "zombie_restore",
        "lavalink_health",
    ))
    queue_voice_retry = VOICE_CONNECT_QUEUE_RETRY_ENABLED and any(token in reason_text for token in (
        "voice_connect",
        "voice_connect_timeout",
        "restore_voice",
        "restore_exception",
    ))
    if voice_rejoin_reason and not VOICE_DISCONNECT_REJOIN_RECOVERY and not queue_voice_retry:
        logger.info(f"[{guild_id}] Skipping {reason} recovery retry because bot-side voice rejoin recovery is disabled; manual/direct recovery remains available.")
        return False

    backoff_delay = recovery_backoff_remaining(guild_id)
    if backoff_delay > 0 and not queue_voice_retry:
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
    delay = backoff_delay + min(RECOVERY_RETRY_BASE_DELAY * attempts, RECOVERY_RETRY_MAX_DELAY) + random.uniform(0.0, RECOVERY_RETRY_JITTER_SECONDS)
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

async def requeue_failed_track(cur, guild_id, channel_id, url, title, requester_id, *, position=0, reason="recovery", track_uid=None):
    # Queue backup copy fix: if the failed/current track was already stored in
    # the backup queue, copy that exact backup row back to the front of the live
    # queue instead of trusting possibly stale Wavelink payload metadata.
    #
    # Track-stuck storms can dispatch the same Lavalink event more than once, or
    # overlap with resilience/process_queue retries.  Keep this section serialized
    # per guild and rate-limit by track identity so a temporary network stall
    # cannot multiply the same track in the live queue.
    async with get_track_requeue_lock(guild_id):
        original_url = url
        original_title = title
        original_track_uid = _normalize_track_uid(track_uid)
        if not original_track_uid:
            await cur.execute("SELECT track_uid FROM nexus_playback_state WHERE guild_id = %s AND bot_name = 'nexus' LIMIT 1", (guild_id,))
            _pb_row = await cur.fetchone()
            if _pb_row: original_track_uid = _normalize_track_uid(_row_value(_pb_row, 0)); track_uid = original_track_uid
            try:
                normalized_track_uid = _normalize_track_uid(track_uid)
                if normalized_track_uid:
                    await cur.execute(
                        "SELECT video_url, title, requester_id, track_uid FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus' AND track_uid = %s ORDER BY id ASC LIMIT 1",
                        (guild_id, normalized_track_uid),
                    )
                else:
                    await cur.execute(
                        "SELECT video_url, title, requester_id, track_uid FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus' AND (video_url = %s OR title = %s) ORDER BY id ASC LIMIT 1",
                        (guild_id, url, title),
                    )
                backup_row = await cur.fetchone()
                if backup_row:
                    url = _row_value(backup_row, "video_url", _row_value(backup_row, 0)) or url
                    title = _row_value(backup_row, "title", _row_value(backup_row, 1)) or title
                    backup_requester = _row_value(backup_row, "requester_id", _row_value(backup_row, 2))
                    track_uid = _row_track_uid(backup_row, 3, default=track_uid) or _ensure_track_uid(track_uid)
                    if requester_id is None and backup_requester is not None:
                        requester_id = backup_requester
            except Exception as tx_error:
                logger.debug("[nexus] Backup row lookup skipped while requeueing failed track.", exc_info=True)

        track_uid = _ensure_track_uid(track_uid)
        track_identity = _track_instance_identity(track_uid, url, title)
        now = time.monotonic()
        recent_key = (int(guild_id), track_identity)
        recent_until = recent_track_requeues.get(recent_key, 0.0)
        if recent_until and now < recent_until:
            already_queued = bool(
                await find_existing_queue_row(
                    cur,
                    "nexus_queue",
                    guild_id,
                    "nexus",
                    track_uid=track_uid,
                    video_url=url,
                    title=title,
                    match_identity=True,
                )
            )
            if already_queued:
                logger.warning(
                    "[%s] Skipping duplicate %s requeue for '%s'; same track was already restored %.0fs ago and is still present in live queue.",
                    guild_id,
                    reason,
                    title,
                    max(0.0, TRACK_REQUEUE_DEDUP_SECONDS - (recent_until - now)),
                )
                if guild_id in playback_tracking: playback_tracking[guild_id]["track_uid"] = track_uid
                await remember_recovery_state(guild_id, channel_id, position)
                schedule_recovery_retry(guild_id, channel_id, start_position=position, reason=f"{reason}_deduped")
                return False

            logger.info(
                "[%s] %s requeue for '%s' hit the dedupe window, but the track is missing from live queue; restoring one guarded copy from backup/current payload.",
                guild_id,
                reason,
                title,
            )
            await insert_queue_front(cur, "nexus_queue", guild_id, "nexus", url, title, requester_id, track_uid)
            if guild_id in playback_tracking: playback_tracking[guild_id]["track_uid"] = track_uid
            await remember_recovery_state(guild_id, channel_id, position)
            schedule_recovery_retry(guild_id, channel_id, start_position=position, reason=f"{reason}_dedupe_missing_live")
            return True

        # Remove stale copies of this exact failed track, then insert it once at
        # the front only if a matching row is not already present.  The match
        # uses _track_key so YouTube URL variants do not slip through as dupes.
        await delete_live_queue_copies(cur, guild_id, original_url, original_title, original_track_uid)
        if (original_url, original_title) != (url, title):
            await delete_live_queue_copies(cur, guild_id, url, title, track_uid)

        already_queued = bool(
            await find_existing_queue_row(
                cur,
                "nexus_queue",
                guild_id,
                "nexus",
                track_uid=track_uid,
                video_url=url,
                title=title,
                match_identity=True,
            )
        )
        inserted = False
        if not already_queued:
            await insert_queue_front(cur, "nexus_queue", guild_id, "nexus", url, title, requester_id, track_uid)
            inserted = True
        else:
            logger.info("[%s] %s requeue for '%s' found an existing live queue copy; not inserting another.", guild_id, reason, title)

        recent_track_requeues[recent_key] = now + TRACK_REQUEUE_DEDUP_SECONDS
        if len(recent_track_requeues) > MAX_RUNTIME_GUILD_CACHE_ENTRIES * 4:
            for old_key, expires_at in list(recent_track_requeues.items()):
                if expires_at <= now:
                    recent_track_requeues.pop(old_key, None)

        # Keep backup parity, but do not spam duplicate backup entries during storms.
        await cur.execute(
            "SELECT COUNT(*) FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus' AND track_uid = %s",
            (guild_id, track_uid),
        )
        backup_count_row = await cur.fetchone()
        backup_count = _scalar_from_row(backup_count_row, 0) or 0
        if backup_count <= 0:
            await cur.execute(
                "INSERT INTO nexus_queue_backup (guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, 'nexus', %s, %s, %s, %s)",
                (guild_id, url, title, requester_id, track_uid),
            )
        await cur.execute(
            "UPDATE nexus_playback_state SET is_playing = FALSE, is_paused = FALSE, position_seconds = %s, track_uid = %s, play_session_key = %s WHERE guild_id = %s AND bot_name = 'nexus'",
            (position, track_uid, track_uid, guild_id),
        )
        if guild_id in playback_tracking: playback_tracking[guild_id]["track_uid"] = track_uid
        await remember_recovery_state(guild_id, channel_id, position)
        schedule_recovery_retry(guild_id, channel_id, start_position=position, reason=reason)
        return inserted


def _track_failure_identity(guild_id, url=None, title=None):
    return (int(guild_id), _track_key(url, title))


def clear_track_failure(guild_id, url=None, title=None):
    track_failure_counts.pop(_track_failure_identity(guild_id, url, title), None)


def _player_reported_position_seconds(player):
    if not player:
        return None
    try:
        position_ms = getattr(player, "position", None)
        if position_ms is None:
            return None
        return max(0, int(float(position_ms) / 1000))
    except Exception as tx_error:
        return None


def _current_player_track_key(player):
    track = _player_current_track(player)
    if not track:
        return ""
    return _track_key(getattr(track, "uri", None) or getattr(track, "url", None), _track_title_from_obj(track))


def _track_uid_from_obj(track):
    if not track:
        return ""
    extras = getattr(track, "extras", None)
    try:
        if extras:
            if hasattr(extras, "track_uid"):
                normalized = _normalize_track_uid(getattr(extras, "track_uid", None))
                if normalized:
                    return normalized
            normalized = _normalize_track_uid(dict(extras).get("track_uid"))
            if normalized:
                return normalized
    except Exception as tx_error:
        pass
    for attr in ("track_uid", "play_session_key"):
        try:
            normalized = _normalize_track_uid(getattr(track, attr, None))
            if normalized:
                return normalized
        except Exception as tx_error:
            continue
    return ""


def _current_player_track_identity(player):
    track = _player_current_track(player)
    if not track:
        return ""
    return _track_instance_identity(
        _track_uid_from_obj(track),
        getattr(track, "uri", None) or getattr(track, "url", None),
        _track_title_from_obj(track),
    )


def _apply_track_runtime_extras(track, *, track_uid=None, requester_id=None, queue_url=None, queue_title=None, resume_position=0):
    if not track:
        return
    extras = {}
    try:
        current_extras = getattr(track, "extras", None)
        if current_extras:
            extras.update(dict(current_extras))
    except Exception as tx_error:
        pass
    normalized_uid = _ensure_track_uid(track_uid) if track_uid else ""
    if normalized_uid:
        extras["track_uid"] = normalized_uid
        extras["play_session_key"] = normalized_uid
    if requester_id is not None:
        try:
            extras["requester_id"] = int(requester_id)
        except Exception as tx_error:
            extras["requester_id"] = requester_id
    if queue_url:
        extras["queue_url"] = queue_url
    if queue_title:
        extras["queue_title"] = queue_title
    extras["resume_position_seconds"] = max(0, int(resume_position or 0))
    try:
        track.extras = extras
    except Exception as tx_error:
        logger.debug("[%s] Failed to attach runtime extras to playable.", BOT_ENV_PREFIX.lower(), exc_info=True)


async def apply_resume_seek(voice_client, guild_id, start_position, *, track_uid=None, video_url=None, title=None):
    """Verify or repair restored playback position for the exact expected track instance."""
    start_position = normalize_position_seconds(start_position)
    if start_position <= 0:
        return 0
    target_ms = int(start_position * 1000)
    retry_delay = min(RESUME_SEEK_RETRY_DELAY_SECONDS, 2.0)
    verify_window = max(float(RESUME_SEEK_VERIFY_GRACE_SECONDS), retry_delay + 0.5)
    max_attempts = max(2, min(4, int(verify_window / max(retry_delay, 0.5)) + 1))
    last_reported = None
    expected_identity = _track_instance_identity(track_uid, video_url, title)

    # Issue an explicit seek immediately — the start= parameter to play() is unreliable
    # for deep positions on YouTube streams. This mirrors what the manual /seek command does.
    try:
        await asyncio.sleep(0.5)  # brief pause for Lavalink to accept the new player
        await voice_client.seek(target_ms)
    except Exception:
        pass

    for attempt in range(1, max_attempts + 1):
        if expected_identity and guild_id in playback_tracking and float(playback_tracking[guild_id].get("last_user_seek_at", 0.0)) > (time.monotonic() - 5.0): return start_position
        try:
            await asyncio.sleep(max(0.75, retry_delay if attempt == 1 else retry_delay))
        except Exception as tx_error:
            pass
        current_identity = _current_player_track_identity(voice_client)
        if expected_identity and current_identity and current_identity != expected_identity and _track_key(video_url, title) != _current_player_track_key(voice_client):
            logger.warning(
                "[%s] Resume verification is waiting for the expected track instance %s, but the player currently reports %s.",
                guild_id,
                expected_identity,
                current_identity,
            )
            continue
        reported = _player_reported_position_seconds(voice_client)
        if reported is not None:
            last_reported = normalize_position_seconds(reported)
            if last_reported + RESUME_SEEK_VERIFY_GRACE_SECONDS >= start_position:
                return max(start_position, last_reported)
        await voice_client.seek(target_ms)  # re-seek on every verification attempt
        if attempt == 1 and max_attempts > 1:
            logger.warning(
                "[%s] Resume seek verification saw player at %s after asking for %ss; retrying seek.",
                guild_id,
                "unknown" if reported is None else f"{reported}s",
                start_position,
            )

    if last_reported is not None and last_reported > start_position:
        return normalize_position_seconds(last_reported)

    # Detect a Lavalink deep-seek failure: player is stuck near zero despite a seek
    # target that was far into the track. This happens with long YouTube streams where
    # the DASH segment URL doesn't honour the start-time offset. Rather than returning
    # max(target, 3s) = target and lying to the position tracker (which then fires a
    # false track_stuck), return the ACTUAL player position so everything downstream
    # knows the track is playing from near the beginning.
    SEEK_FAIL_NEAR_ZERO_THRESHOLD = 15   # seconds — Lavalink reporting ≤ this counts as "stuck at start"
    SEEK_FAIL_MIN_DEEP_TARGET = 60       # seconds — only flag when we were aiming this far in
    if (
        last_reported is not None
        and last_reported <= SEEK_FAIL_NEAR_ZERO_THRESHOLD
        and start_position >= SEEK_FAIL_MIN_DEEP_TARGET
    ):
        logger.warning(
            "[%s] Deep seek to %ss failed for '%s'; Lavalink is at %ss. "
            "YouTube stream likely cannot seek to that offset. Resetting to 0 and reporting actual position.",
            guild_id,
            start_position,
            title or video_url or _track_uid_label(track_uid) or "track",
            last_reported,
        )
        # Explicitly reset Lavalink to position 0 so it stops being stuck at the
        # near-zero offset from the failed seek rather than continuing to report ~3s
        # while the runtime clock races ahead and falsely triggers track_stuck.
        try:
            await voice_client.seek(0)
        except Exception as tx_error:
            logger.debug("[%s] seek(0) reset after failed deep seek could not complete.", guild_id, exc_info=True)
        return last_reported

    final_pos = max(start_position, last_reported if last_reported is not None else 0)
    if not final_pos:
        logger.warning('[%s] Resume verify failed for %s at %ss; player=%s.', guild_id, _track_uid_label(track_uid), start_position, 'unknown' if last_reported is None else str(last_reported)+'s')
    return final_pos


async def requeue_verified_playback_failure(guild, channel_id, url, title, requester_id, position, *, reason="track_exception", track_uid=None):
    attempts = mark_track_failure(guild.id, track_uid if track_uid else url, title)
    inserted = False
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                if attempts <= MAX_TRACK_FAILURE_REQUEUES:
                    inserted = await requeue_failed_track(cur, guild.id, channel_id, url, title, requester_id, position=position, reason=reason, track_uid=track_uid)
                    if inserted:
                        logger.warning(
                            "[%s] Lavalink %s verified for '%s'; restored it to the front of the queue (attempt %s/%s).",
                            guild.id,
                            reason,
                            title,
                            attempts,
                            MAX_TRACK_FAILURE_REQUEUES,
                        )
                    else:
                        logger.warning(
                            "[%s] Lavalink %s verified for '%s', but a duplicate live-queue restore was suppressed (attempt %s/%s).",
                            guild.id,
                            reason,
                            title,
                            attempts,
                            MAX_TRACK_FAILURE_REQUEUES,
                        )
                else:
                    # Do not let one poison YouTube stream trap the bot forever.
                    # Keep it in backup so it is not lost, mark active playback idle,
                    # then let the next queued track continue.
                    await delete_live_queue_copies(cur, guild.id, url, title)
                    await backup_track(cur, guild.id, url, title, requester_id)
                    await cur.execute(
                        f"UPDATE {BOT_ENV_PREFIX.lower()}_playback_state SET is_playing = FALSE, is_paused = FALSE, position_seconds = %s WHERE guild_id = %s AND bot_name = '{BOT_ENV_PREFIX.lower()}'",
                        (position, guild.id),
                    )
                    logger.error(
                        "[%s] Lavalink %s kept failing for '%s' after %s attempts; preserved it in backup and moved on.",
                        guild.id,
                        reason,
                        title,
                        MAX_TRACK_FAILURE_REQUEUES,
                    )

    if channel_id:
        schedule_named_task(f"lavalink_failure_process_queue:{guild.id}", process_queue(guild, channel_id, allow_recovery_restore=True))
    return True


async def verify_track_stuck_before_requeue(guild_id, channel_id, url, title, requester_id, observed_position, observed_reported_position, track_uid=None, *, reason="track_stuck"):
    expected_key = _track_key(url, title)
    try:
        await asyncio.sleep(TRACK_STUCK_VERIFY_DELAY_SECONDS)
        guild = bot.get_guild(int(guild_id))
        if not guild:
            return False
        player = guild.voice_client
        current_key = _current_player_track_key(player)
        if current_key and current_key != expected_key:
            logger.info(
                "[%s] Lavalink %s for '%s' ignored; current track changed during %.0fs verification window.",
                guild_id,
                reason,
                title,
                TRACK_STUCK_VERIFY_DELAY_SECONDS,
            )
            clear_track_failure(guild_id, url, title)
            return False

        later_reported_position = _player_reported_position_seconds(player)
        if observed_reported_position is not None and later_reported_position is not None:
            progress = later_reported_position - observed_reported_position
            if progress >= TRACK_STUCK_MIN_PROGRESS_SECONDS:
                logger.info(
                    "[%s] Lavalink %s for '%s' self-recovered; player advanced %ss during verification, no queue restore needed.",
                    guild_id,
                    reason,
                    title,
                    progress,
                )
                clear_track_failure(guild_id, url, title)
                return False

        if later_reported_position is None and player and _player_is_active(player) and TRACK_STUCK_SKIP_WHEN_POSITION_UNKNOWN:
            logger.warning(
                "[%s] Lavalink %s for '%s' could not verify player position, but the player still reports active; skipping queue restore to avoid duplicates.",
                guild_id,
                reason,
                title,
            )
            return False

        verified_position = later_reported_position if later_reported_position is not None else observed_position

        # Detect a seek-induced stuck: Lavalink is near position 0 while the runtime
        # checkpoint was far into the track. This is NOT a broken/unplayable track — it
        # means Lavalink silently ignored the deep seek offset (common with long YouTube
        # streams). Don't burn a failure retry counter; just requeue at position 0 so the
        # track plays from the beginning instead of looping through the same stuck state.
        SEEK_INDUCED_STUCK_MAX_PLAYER = 15    # Lavalink reporting ≤ this seconds
        SEEK_INDUCED_STUCK_MIN_CHECKPOINT = 30  # runtime was at least this far in
        is_seek_induced = (
            later_reported_position is not None
            and later_reported_position <= SEEK_INDUCED_STUCK_MAX_PLAYER
            and observed_position >= SEEK_INDUCED_STUCK_MIN_CHECKPOINT
        )
        if is_seek_induced:
            logger.warning(
                "[%s] Lavalink %s for '%s' is seek-induced (player=%ss, checkpoint=%ss); "
                "requeuing at position 0 without counting as a failure.",
                guild_id, reason, title, later_reported_position, observed_position,
            )
            # Requeue at 0 directly — skip mark_track_failure so the track isn't penalised.
            async with DBPoolManager() as pool:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await requeue_failed_track(cur, guild_id, channel_id, url, title, requester_id, position=0, reason=f"{reason}_seek_induced", track_uid=track_uid)
            if channel_id:
                schedule_named_task(f"lavalink_failure_process_queue:{guild_id}", process_queue(guild, channel_id, start_position=0, allow_recovery_restore=True))
            return True

        logger.warning(
            "[%s] Lavalink %s for '%s' did not recover after %.0fs; restoring exactly one live queue copy.",
            guild_id,
            reason,
            title,
            TRACK_STUCK_VERIFY_DELAY_SECONDS,
        )
        return await requeue_verified_playback_failure(
            guild,
            channel_id,
            url,
            title,
            requester_id,
            max(0, int(verified_position or 0)),
            reason=f"{reason}_verified",
            track_uid=track_uid,
        )
    except asyncio.CancelledError:
        raise
    except Exception as tx_error:
        logger.exception("[%s] Failed delayed %s verification for '%s'.", guild_id, reason, title)
        return False


def schedule_track_stuck_verification(guild, channel_id, url, title, requester_id, position, player, track_uid=None, *, reason="track_stuck"):
    observed_reported_position = _player_reported_position_seconds(player)
    task_key = _track_key(url, title)[:16]
    task_name = f"track_stuck_verify:{guild.id}:{task_key}"
    existing = startup_task_registry.get(task_name)
    if existing and not existing.done():
        logger.info(
            "[%s] Coalesced duplicate Lavalink %s for '%s'; verification already pending.",
            guild.id,
            reason,
            title,
        )
        return True
    logger.warning(
        "[%s] Lavalink %s hit '%s'; waiting %.0fs to verify playback before touching the live queue.",
        guild.id,
        reason,
        title,
        TRACK_STUCK_VERIFY_DELAY_SECONDS,
    )
    schedule_named_task(
        task_name,
        verify_track_stuck_before_requeue(guild.id, channel_id, url, title, requester_id, position, observed_reported_position, track_uid, reason=reason),
    )
    return True


def mark_track_failure(guild_id, url=None, title=None):
    now = time.monotonic()
    key = _track_failure_identity(guild_id, url, title)
    count, first_seen = track_failure_counts.get(key, (0, now))
    if now - first_seen > TRACK_FAILURE_WINDOW_SECONDS:
        count = 0
        first_seen = now
    count += 1
    track_failure_counts[key] = (count, first_seen)
    if len(track_failure_counts) > MAX_RUNTIME_GUILD_CACHE_ENTRIES * 4:
        stale_before = now - TRACK_FAILURE_WINDOW_SECONDS
        for old_key, (_old_count, old_first_seen) in list(track_failure_counts.items()):
            if old_first_seen < stale_before:
                track_failure_counts.pop(old_key, None)
    return count


# --- Anti-storm circuit breaker: cap how fast one guild can churn through failed tracks. ---
GUILD_FAILURE_BURST_MAX = 8
GUILD_FAILURE_BURST_WINDOW_SECONDS = 20.0
GUILD_FAILURE_COOLDOWN_SECONDS = 60.0
_guild_failure_burst = {}
_guild_failure_cooldown_until = {}


def register_guild_failure(guild_id):
    """Return True if auto-advance should be suppressed (burst tripped or still in cooldown)."""
    now = time.monotonic()
    if now < _guild_failure_cooldown_until.get(guild_id, 0.0):
        return True
    count, start = _guild_failure_burst.get(guild_id, (0, now))
    if now - start > GUILD_FAILURE_BURST_WINDOW_SECONDS:
        count, start = 0, now
    count += 1
    _guild_failure_burst[guild_id] = (count, start)
    if count >= GUILD_FAILURE_BURST_MAX:
        _guild_failure_cooldown_until[guild_id] = now + GUILD_FAILURE_COOLDOWN_SECONDS
        _guild_failure_burst[guild_id] = (0, now)
        logger.error("[%s] Playback failure burst (%s in %.0fs); pausing auto-advance for %.0fs to stop a skip storm.",
                     guild_id, count, GUILD_FAILURE_BURST_WINDOW_SECONDS, GUILD_FAILURE_COOLDOWN_SECONDS)
        return True
    return False


async def _resume_after_failure_cooldown(guild_id, channel_id):
    try:
        await asyncio.sleep(GUILD_FAILURE_COOLDOWN_SECONDS + 1.0)
        guild = bot.get_guild(int(guild_id))
        if guild and channel_id:
            await process_queue(guild, channel_id, allow_recovery_restore=True)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.debug("[%s] Failed to resume after failure cooldown.", guild_id, exc_info=True)


async def handle_track_playback_failure(payload, *, reason="track_exception"):
    player = getattr(payload, "player", None)
    guild = getattr(player, "guild", None)
    if not guild:
        return False

    track = getattr(payload, "track", None) or _player_current_track(player)
    track_data = playback_tracking.get(guild.id, {})
    track_uri = getattr(track, "uri", None) or getattr(track, "url", None)
    # Prefer the original (YouTube) url from runtime tracking. The currently-playing Playable
    # may be a LOCAL cache file (uri = file://...), which must NEVER be requeued as-is.
    url = track_data.get("url") or track_uri
    title = track_data.get("title") or getattr(track, "title", None) or _track_title_from_obj(track) or "Recoverable Track"

    # Local-cache guardrail: a cache file that fails playback is unplayable. Delete it from
    # disk so it is never served again, and make sure we requeue the streamable YouTube url.
    if _is_local_cache_uri(track_uri) or _is_local_cache_uri(url):
        vid = _cache_id_from_uri(track_uri) or _audio_cache_id(url)
        if vid:
            await asyncio.to_thread(_audio_cache_drop, vid)
            logger.warning("[%s] Local cache track %s failed playback (%s); removed it from disk and falling back to streaming.", guild.id, vid, reason)
            if not _audio_cache_id(url):
                url = f"https://www.youtube.com/watch?v={vid}"

    channel_id = (
        getattr(getattr(player, "channel", None), "id", None)
        or track_data.get("channel_id")
        or guild_states.get(guild.id, {}).get("voice_channel_id")
    )
    requester_id = track_data.get("requester_id") or (bot.user.id if bot.user else None)
    position = current_track_position(guild.id)

    if not url:
        logger.warning("[%s] Lavalink failure had no track URL; preserving state but cannot requeue unknown track (%s).", guild.id, reason)
        return False

    # Anti-storm breaker: if failures arrive in a burst, stop auto-advancing so one bad
    # batch can't churn the whole queue, then schedule a single graceful resume.
    if register_guild_failure(guild.id):
        logger.error("[%s] Suppressing requeue/advance for '%s' due to failure-burst cooldown.", guild.id, title)
        if channel_id:
            schedule_named_task(f"failure_burst_resume:{guild.id}", _resume_after_failure_cooldown(guild.id, channel_id), overwrite=True)
        return False

    reason_name = str(reason or "").strip().lower()
    if reason_name in {"track_stuck", "track_exception"}:
        return schedule_track_stuck_verification(guild, channel_id, url, title, requester_id, position, player, track_data.get("track_uid"), reason=reason_name)

    return await requeue_verified_playback_failure(guild, channel_id, url, title, requester_id, position, reason=reason_name or "track_exception", track_uid=track_data.get("track_uid"))


@bot.event
async def on_wavelink_track_exception(payload):
    try:
        await handle_track_playback_failure(payload, reason="track_exception")
    except Exception as tx_error:
        logger.exception("[nexus] Failed to handle Wavelink track exception without leaking queue state.")


@bot.event
async def on_wavelink_track_stuck(payload):
    try:
        await handle_track_playback_failure(payload, reason="track_stuck")
    except Exception as tx_error:
        logger.exception("[nexus] Failed to handle Wavelink stuck track without leaking queue state.")


async def get_home_channel(guild):
    cached = _cache_get(HOME_CHANNEL_CACHE, int(guild.id), HOME_CHANNEL_CACHE_TTL_SECONDS)
    if cached is not None:
        return guild.get_channel(int(cached)) if cached else None
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT home_vc_id FROM nexus_bot_home_channels WHERE guild_id = %s AND bot_name = 'nexus'", (guild.id,))
                res = await cur.fetchone()
    channel_id = int(res[0]) if res and res[0] else 0
    _cache_set(HOME_CHANNEL_CACHE, int(guild.id), channel_id)
    return guild.get_channel(channel_id) if channel_id else None

def _fade_curve_progress(progress, curve='smooth'):
    progress = max(0.0, min(1.0, float(progress)))
    curve = str(curve or 'smooth').lower()
    if curve in {'smooth', 'ease'}:
        return progress
    if curve in {'ease_in', 'slow_start'}:
        return progress * progress * progress
    if curve in {'ease_out', 'soft_land'}:
        remaining = 1 - progress
        return 1 - (remaining * remaining * remaining)
    return progress

async def _fade_volume(voice_client, start_volume, end_volume, duration=3.0, steps=None, curve='smooth'):
    if not voice_client:
        return
    duration = max(0.25, min(12.0, float(duration or 3.0)))
    steps = steps or max(8, min(24, int(duration * 8)))
    step_delay = duration / steps if steps > 0 else duration
    last_volume = None
    for step in range(steps + 1):
        eased = _fade_curve_progress(step / steps if steps else 1, curve)
        volume = max(0, min(200, int(round(start_volume + (end_volume - start_volume) * eased))))
        try:
            if volume != last_volume:
                await voice_client.set_volume(volume)
                last_volume = volume
        except Exception as tx_error:
            return
        if step < steps:
            await asyncio.sleep(step_delay)
    if last_volume != max(0, min(200, int(end_volume))):
        try:
            await voice_client.set_volume(max(0, min(200, int(end_volume))))
        except Exception as tx_error:
            return

def choose_fade_duration(mode, configured_seconds, track_duration, filter_mode, title):
    if mode != 'smart':
        return max(0.25, min(12.0, float(configured_seconds or 3.0)))
    duration = float(track_duration or 0)
    filter_name = str(filter_mode or 'none').lower()
    title_text = str(title or '').lower()
    seconds = 3.0
    if duration and duration < 120:
        seconds = 1.25
    elif duration and duration > 420:
        seconds = 4.5
    if filter_name in {'nightcore', 'party', 'electronic'}:
        seconds = max(0.75, seconds - 0.75)
    if filter_name in {'vaporwave', 'lofi', 'cinema'}:
        seconds = min(6.0, seconds + 1.0)
    if any(word in title_text for word in ('ambient', 'sleep', 'rain', 'piano', 'acoustic')):
        seconds = min(7.0, seconds + 1.0)
    return max(0.25, min(12.0, seconds))


async def update_stage_topic(guild, title, requester_id):
    """Update Stage topic AND Voice/Stage channel status for the current song.

    This is intentionally best-effort. A small dedup cache prevents repeated
    Discord channel/status edits for the same track during reconnects, queue
    retries, or duplicate now-playing events.
    """
    try:
        vc = guild.voice_client
        if not vc or not getattr(vc, "channel", None):
            return
        channel = vc.channel
        requester_name = await resolve_requester_name(guild, requester_id)
        safe_title = str(title or "Unknown Track").replace("\n", " ").strip()
        stage_topic = f"🎵 {safe_title[:60]} | 👤 Req: {requester_name}"
        voice_status = f"🎵 {safe_title[:80]}"
        fingerprint = (getattr(channel, "id", None), stage_topic, voice_status)
        cached = VOICE_STATUS_CACHE.get(guild.id)
        if cached and cached[0] == fingerprint and time.time() - cached[1] < VOICE_STATUS_DEDUP_SECONDS:
            return

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
        VOICE_STATUS_CACHE[guild.id] = (fingerprint, time.time())
    except Exception as e:
        logger.error(f"[STAGE/VOICE STATUS ERROR] {e}")


async def clear_voice_channel_status(guild):
    VOICE_STATUS_CACHE.pop(getattr(guild, "id", None), None)
    try:
        vc = guild.voice_client
        channel = getattr(vc, "channel", None) if vc else None
        if not channel:
            return
        try:
            await channel.edit(status=None)
        except TypeError:
            pass
        except Exception as tx_error:
            pass
    except Exception as tx_error:
        pass


async def _resolve_feedback_channel(guild, channel_id):
    """Resolve a feedback channel id to a TextChannel or Thread.

    guild.get_channel() doesn't see threads, so check get_channel_or_thread
    first and fall back to fetch_channel for archived/uncached threads.
    """
    if not channel_id:
        return None
    channel = guild.get_channel_or_thread(int(channel_id))
    if channel:
        return channel
    try:
        channel = await bot.fetch_channel(int(channel_id))
    except Exception:
        return None
    return channel if getattr(channel, "guild", None) and channel.guild.id == guild.id else None

async def send_or_update_status_message(guild, embed):
    """Maintain one live status message per guild for this bot without duplicate edits."""
    fingerprint = _embed_fingerprint(embed)
    status_cache_key = _status_message_cache_key(guild.id)
    cached = STATUS_MESSAGE_CACHE.get(status_cache_key)
    if cached and cached.get("fingerprint") == fingerprint and time.time() - cached.get("updated_at", 0) < STATUS_MESSAGE_DEDUP_SECONDS:
        return
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("CREATE TABLE IF NOT EXISTS nexus_status_messages (guild_id BIGINT, bot_name VARCHAR(50), feedback_channel_id BIGINT, message_id BIGINT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (guild_id, bot_name))")
                await cur.execute("SELECT feedback_channel_id FROM nexus_guild_settings WHERE guild_id = %s", (guild.id,))
                res = await cur.fetchone()
                if not res or not res[0]:
                    return
                channel = await _resolve_feedback_channel(guild, res[0])
                if not channel:
                    return
                cached_message_id = cached.get("message_id") if cached and cached.get("channel_id") == channel.id else None
                if cached_message_id:
                    message_id = int(cached_message_id)
                else:
                    await cur.execute("SELECT message_id FROM nexus_status_messages WHERE guild_id = %s AND bot_name = 'nexus' LIMIT 1", (guild.id,))
                    msg_row = await cur.fetchone()
                    message_id = int(msg_row[0]) if msg_row and msg_row[0] else None
                message = None
                if message_id:
                    try:
                        message = channel.get_partial_message(message_id)
                        await message.edit(embed=embed)
                        new_message_id = message_id
                    except Exception as tx_error:
                        message = None
                if not message:
                    try:
                        message = await channel.send(embed=embed)
                        new_message_id = message.id
                    except (aiohttp.ClientConnectionResetError, ConnectionResetError):
                        return
                STATUS_MESSAGE_CACHE[status_cache_key] = {"fingerprint": fingerprint, "updated_at": time.time(), "message_id": new_message_id, "channel_id": channel.id}
                await cur.execute("REPLACE INTO nexus_status_messages (guild_id, bot_name, feedback_channel_id, message_id) VALUES (%s, 'nexus', %s, %s)", (guild.id, channel.id, new_message_id))

async def send_feedback(guild, embed):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT feedback_channel_id FROM nexus_guild_settings WHERE guild_id = %s", (guild.id,))
                res = await cur.fetchone()
                if res and res[0]:
                    channel = await _resolve_feedback_channel(guild, res[0])
                    if channel:
                        try: await channel.send(embed=embed)
                        except discord.Forbidden: pass
                        except discord.HTTPException as e:
                            if getattr(e, "code", None) != 50083:  # thread archived & locked
                                raise


async def send_feedback_notice(guild, title, description, color, *, dedupe_key=None, fields=None):
    if not guild:
        return False
    cache_key = dedupe_key or f"{title}:{description}"
    scoped_key = (int(guild.id), str(cache_key))
    now = time.monotonic()
    expires_at = feedback_notice_cache.get(scoped_key, 0)
    if expires_at > now:
        return False
    embed = discord.Embed(title=title, description=description, color=color)
    for name, value, inline in fields or []:
        embed.add_field(name=name, value=value, inline=inline)
    try:
        await send_feedback(guild, embed)
        feedback_notice_cache[scoped_key] = now + FEEDBACK_NOTICE_TTL_SECONDS
        if len(feedback_notice_cache) > MAX_RUNTIME_GUILD_CACHE_ENTRIES * 4:
            for old_key, old_expiry in list(feedback_notice_cache.items()):
                if old_expiry <= now:
                    feedback_notice_cache.pop(old_key, None)
        return True
    except Exception as tx_error:
        logger.debug("[%s] Feedback notice dispatch skipped.", BOT_ENV_PREFIX.lower(), exc_info=True)
        return False

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

async def ensure_voice_connection(guild, channel_id, *, respect_recovery_backoff=False, allow_stale_rejoin=False):
    channel = guild.get_channel(channel_id)
    if not channel: return None
    lock = get_voice_connect_lock(guild.id)
    async with lock:
        if respect_recovery_backoff and not allow_stale_rejoin and recovery_backoff_remaining(guild.id) > 0:
            return None
        if not await ensure_lavalink_ready():
            logger.warning(f"[{guild.id}] Lavalink not ready yet; deferring voice connection.")
            return None
        voice_client = guild.voice_client
        pending_voice_channels[guild.id] = channel_id
        voice_operation_started = False
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
                if not (VOICE_FORCE_STALE_CLIENT_REJOIN or allow_stale_rejoin):
                    logger.warning(f"[{guild.id}] Voice client looks unstable and cannot be recycled by this caller; requeueing instead of playing into a stale voice session.")
                    return None
                try:
                    await asyncio.wait_for(voice_client.disconnect(), timeout=10.0)
                except Exception as tx_error:
                    pass
                voice_client = None

            if not voice_client:
                voice_connect_inflight_until[guild.id] = time.time() + VOICE_CONNECT_TIMEOUT_SECONDS + 15.0
                voice_operation_started = True
                voice_client = await channel.connect(cls=wavelink.Player, timeout=VOICE_CONNECT_TIMEOUT_SECONDS)
                voice_operation_started = False
                clear_voice_connect_inflight(guild.id)
            elif voice_client.channel.id != channel_id:
                voice_connect_inflight_until[guild.id] = time.time() + VOICE_CONNECT_TIMEOUT_SECONDS + 15.0
                voice_operation_started = True
                await voice_client.move_to(channel)
                voice_operation_started = False
                clear_voice_connect_inflight(guild.id)
            await ensure_self_deaf(guild, voice_client)
            if getattr(voice_client, "channel", None):
                pending_voice_channels[guild.id] = voice_client.channel.id

            if isinstance(channel, discord.StageChannel):
                if guild.me.voice and guild.me.voice.suppress:
                    try: await guild.me.edit(suppress=False)
                    except Exception as tx_error: pass

            await persist_voice_state(guild.id, channel_id, desired_connected=True, connected=True)
            clear_recovery_backoff(guild.id)

            return voice_client
        except Exception as e:
            clear_voice_connect_inflight(guild.id)
            logger.error(f"[{guild.id}] Voice connect error: {e}")
            message = str(e).lower()
            timeout_error = isinstance(e, asyncio.TimeoutError) or "exceeded the timeout" in message or "timed out" in message
            reason = "voice_connect_timeout" if timeout_error else "voice_connect_error"
            if voice_operation_started:
                await cleanup_failed_voice_session(guild, reason=reason)
            if timeout_error:
                timeout_backoff = VOICE_CONNECT_QUEUE_RETRY_BACKOFF_SECONDS if VOICE_CONNECT_QUEUE_RETRY_ENABLED else max(VOICE_CONNECT_TIMEOUT_BACKOFF_SECONDS, VOICE_CONNECT_TIMEOUT_SECONDS * 2.0)
                if not VOICE_DISCONNECT_REJOIN_RECOVERY:
                    timeout_backoff = max(timeout_backoff, VOICE_CONNECT_AUTOMATIC_RECOVERY_SNOOZE_SECONDS)
                arm_recovery_backoff(guild.id, seconds=timeout_backoff, reason="voice_connect_timeout")
            try:
                await mark_voice_disconnected(guild.id, channel_id, desired_connected=True, reason=reason)
            except Exception as tx_error:
                logger.debug(f"[{guild.id}] Failed to persist voice failure state.", exc_info=True)
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

async def _record_track_outcome_parallel(pool, guild_id, track_uri, track_title, original_requester, outcome, listen_seconds):
    """Run record_track_outcome on its own connection so it can be gathered with the loop_mode SELECT."""
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await record_track_outcome(
                    cur,
                    guild_id,
                    track_uri,
                    track_title,
                    original_requester,
                    outcome=outcome,
                    listen_seconds=listen_seconds,
                )
    except Exception:
        logger.debug("[%s] Track intelligence outcome write skipped (parallel).", guild_id, exc_info=True)


async def _record_track_play_started_bg(guild_id, video_url, title, requester_id):
    """Fire-and-forget wrapper: write track_intelligence + user_track_affinity on its own connection."""
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await record_track_play_started(cur, guild_id, video_url, title, requester_id)
                    if SMART_EMBEDDING_ENABLED:
                        await get_or_fetch_track_embedding(cur, _track_key(video_url, title), title, video_url)
    except Exception:
        logger.debug("[%s] track_play_started intelligence write skipped (bg).", guild_id, exc_info=True)

async def _record_track_cooccurrence_bg(guild_id, prev_url, prev_title, next_url, next_title):
    """Fire-and-forget wrapper: record back-to-back track pairs on their own connection."""
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await record_track_cooccurrence(cur, guild_id, prev_url, prev_title, next_url, next_title)
    except Exception:
        logger.debug("[%s] track_cooccurrence write skipped (bg).", guild_id, exc_info=True)


async def _record_track_play_resumed_bg(guild_id, video_url, title, requester_id):
    """Fire-and-forget wrapper: write track_intelligence + user_track_affinity on its own connection."""
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await record_track_play_resumed(cur, guild_id, video_url, title, requester_id)
    except Exception:
        logger.debug("[%s] track_play_resumed intelligence write skipped (bg).", guild_id, exc_info=True)

@bot.event
async def on_wavelink_track_end(payload: wavelink.TrackEndEventPayload):
    player = getattr(payload, "player", None)
    if not player:
        return

    guild = getattr(player, "guild", None)
    if not guild:
        return

    reason = _wavelink_event_reason(getattr(payload, "reason", ""))
    if reason == "REPLACED":
        return

    track = getattr(payload, "track", None)
    try:
        if reason in {"LOAD_FAILED", "CLEANUP"}:
            handled = await handle_track_playback_failure(payload, reason=f"track_end_{reason.lower()}")
            if handled:
                return

        if track:
            # Collect all in-memory data before touching the DB.
            track_data = playback_tracking.get(guild.id, {})
            original_requester = track_data.get('requester_id', bot.user.id if bot.user else None)
            # Prefer the original queue url/title from runtime tracking. A track played from the
            # local cache reports its uri/title from the file — the title becomes "Opus" (no
            # metadata) and the uri may be a file:// path — which would otherwise pollute the
            # queue/backup/history/state with "Opus" entries on loop re-queue.
            track_uri = track_data.get('original_queue_url') or track_data.get('url') or getattr(track, 'uri', None)
            track_title = track_data.get('original_queue_title') or track_data.get('title') or getattr(track, 'title', None)
            resolved_track_uid = track_data.get("track_uid") or _track_uid_from_obj(track)
            if reason == "FINISHED":
                final_position = int(track_data.get('duration') or current_track_position(guild.id) or 0)
            else:
                final_position = int(current_track_position(guild.id) or track_data.get('last_position_checkpoint') or 0)
            listen_seconds = consume_realtime_listen_delta(track_data, final_position, playing=True) if track_data else max(0, final_position)
            outcome = "finished" if reason == "FINISHED" else "skipped"

            _track_end_db_delays = [0.05, 0.1]
            for _track_end_attempt in range(3):
                try:
                    async with DBPoolManager() as pool:
                        # SELECT loop_mode and record_track_outcome write are independent —
                        # run them in parallel on separate pool connections.
                        async with pool.acquire() as conn:
                            async with conn.cursor() as cur:
                                loop_mode_coro = cur.execute("SELECT loop_mode FROM nexus_guild_settings WHERE guild_id = %s", (guild.id,))
                                outcome_coro = _record_track_outcome_parallel(pool, guild.id, track_uri, track_title, original_requester, outcome, listen_seconds)
                                await asyncio.gather(loop_mode_coro, outcome_coro)
                                mode_row = await cur.fetchone()
                                loop_mode = mode_row[0] if mode_row and mode_row[0] else 'queue'

                                if reason == "FINISHED":
                                    clear_track_failure(guild.id, track_uri, track_title)
                                    if loop_mode == 'queue':
                                        await requeue_finished_track(cur, guild.id, track_uri, track_title, original_requester, resolved_track_uid)
                                    elif loop_mode == 'song':
                                        await insert_queue_front(cur, "nexus_queue", guild.id, "nexus", track_uri, track_title, original_requester, resolved_track_uid)
                                    else:
                                        if track_uri and track_title:
                                            await delete_backup_track(cur, guild.id, track_uid=resolved_track_uid, video_url=track_uri, title=track_title)
                                        original_url = track_data.get('original_queue_url')
                                        original_title = track_data.get('original_queue_title')
                                        if original_url and original_title and (original_url != track_uri or original_title != track_title):
                                            await delete_backup_track(cur, guild.id, video_url=original_url, title=original_title)
                    break
                except Exception as _track_end_exc:
                    if _is_retryable_mysql_error(_track_end_exc) and _track_end_attempt < 2:
                        logger.warning(
                            "[%s] Track-end DB hit retryable error %s; retrying (%s/3).",
                            guild.id, _mysql_error_code(_track_end_exc) or type(_track_end_exc).__name__,
                            _track_end_attempt + 1,
                        )
                        await asyncio.sleep(_track_end_db_delays[_track_end_attempt])
                        continue
                    raise
    except Exception as e:
        logger.error(f"[{guild.id}] Track-end queue/loop handling error: {e}")

    channel_id = (
        getattr(getattr(player, 'channel', None), 'id', None)
        or playback_tracking.get(guild.id, {}).get('channel_id')
        or guild_states.get(guild.id, {}).get('voice_channel_id')
    )
    if channel_id:
        schedule_named_task(f"track_end_process_queue:{guild.id}", process_queue(guild, channel_id), overwrite=True)


@bot.event
async def on_wavelink_websocket_closed(payload: wavelink.WebsocketClosedEventPayload):
    player = getattr(payload, "player", None)
    guild = getattr(player, "guild", None) if player else None
    if not guild:
        return

    track_data = playback_tracking.get(guild.id, {})
    channel_id = (
        getattr(getattr(player, "channel", None), "id", None)
        or track_data.get("channel_id")
        or guild_states.get(guild.id, {}).get("voice_channel_id")
    )
    if not channel_id:
        return

    position = current_track_position(guild.id)
    await remember_recovery_state(
        guild.id,
        channel_id,
        position,
        track_uid=track_data.get("track_uid"),
        url=track_data.get("url"),
        title=track_data.get("title"),
    )
    await mark_voice_disconnected(guild.id, channel_id, desired_connected=True, reason="wavelink_websocket_closed", position=position)
    if not ARIA_RECOVERY_AUTHORITY and position > 0 and track_data.get("track_uid") and TARGETED_TRACK_INTERRUPT_RECOVERY:
        if guild:
            await send_feedback_notice(
                guild,
                "📡 Playback Interrupted",
                f"Saved **{_compact_track_title(track_data.get('title') or track_data.get('url') or 'current track')}** for recovery after the voice websocket closed.",
                discord.Color.orange(),
                dedupe_key=f"ws_closed:{track_data.get('track_uid')}:{position // 15}",
                fields=_track_feedback_fields(track_data.get("track_uid"), position),
            )
        schedule_targeted_interrupt_resume(guild.id, channel_id, start_position=position, reason="wavelink_websocket_closed")
    elif VOICE_DISCONNECT_REJOIN_RECOVERY and not ARIA_RECOVERY_AUTHORITY:
        schedule_soft_voice_recovery(guild.id, channel_id, start_position=position, reason="wavelink_websocket_closed")
    else:
        logger.warning(
            "[%s] Wavelink voice websocket closed (code=%s, remote=%s); preserved playback state at %ss for manual/direct recovery.",
            guild.id,
            getattr(payload, "code", None),
            getattr(payload, "by_remote", None),
            position,
        )


async def _prefetch_next_track_bg(guild_id: int, url: str, title: str) -> None:
    """Resolve the next track's Lavalink URL in the background while the current track plays."""
    try:
        track, _ = await resolve_queue_track(url, title)
        _nexus_prefetch_cache[guild_id] = {
            'url': url, 'title': title, 'track': track,
            'expires_at': time.monotonic() + 300,
        }
        logger.debug("[nexus] Prefetched next track for guild %s: %s", guild_id, title)
    except Exception:
        _nexus_prefetch_cache.pop(guild_id, None)


async def _process_queue_inner(guild, channel_id, start_position=0, *, allow_recovery_restore=False):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT volume, loop_mode, filter_mode, transition_mode, custom_speed, custom_pitch, custom_modifiers_left, stay_in_vc, fade_seconds, fade_curve FROM nexus_guild_settings WHERE guild_id = %s", (guild.id,))
                res = await cur.fetchone()
                vol, loop_mode, filter_mode, trans_mode, c_speed, c_pitch, c_mod_left, stay_in_vc, fade_seconds, fade_curve = res if res else (100, 'queue', 'none', 'off', 1.0, 1.0, 0, False, 3.0, 'smooth')
                fade_seconds = max(0.25, min(12.0, float(fade_seconds or 3.0)))
                fade_curve = str(fade_curve or 'linear').lower()

                restored_active = 0
                if allow_recovery_restore and start_position > 0:
                    restored_active = await restore_active_playback_entry(cur, guild.id)
                await cur.execute("SELECT id, video_url, title, requester_id, track_uid FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC LIMIT 1", (guild.id,))
                next_song = await cur.fetchone()

                if not next_song:
                    restored_backup = 0
                    if allow_recovery_restore and not restored_active:
                        restored_active = await restore_active_playback_entry(cur, guild.id)
                    if loop_mode == 'queue' and not restored_active:
                        restored_backup = await restore_queue_from_backup(cur, guild.id)
                        if restored_backup:
                            await shuffle_queue_rows(cur, guild.id, preserve_first=True)
                    if restored_active or restored_backup:
                        logger.info(f"[{guild.id}] Rebuilt live queue from persisted playback/backup state (active={restored_active}, backup={restored_backup}).")
                        await cur.execute("SELECT id, video_url, title, requester_id, track_uid FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC LIMIT 1", (guild.id,))
                        next_song = await cur.fetchone()

                if not next_song:
                    await cur.execute(
                        "UPDATE nexus_playback_state SET channel_id = NULL, video_url = NULL, title = NULL, position_seconds = 0, is_playing = FALSE, is_paused = FALSE, play_session_key = NULL, track_uid = NULL WHERE guild_id = %s AND bot_name = 'nexus'",
                        (guild.id,),
                    )
                    playback_tracking.pop(guild.id, None)
                    guild_states.pop(guild.id, None)
                    invalidate_position_persist(guild.id)
                    clear_recovery_retry(guild.id)
                    clear_interrupt_resume(guild.id)
                    clear_idle_restore_state(guild.id)
                    await delete_state(guild.id)
                    try:
                        await bot.change_presence(status=discord.Status.online)
                    except Exception as tx_error:
                        pass
                    if loop_mode != 'queue' and not allow_recovery_restore:
                        await cur.execute("DELETE FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus'", (guild.id,))
                    if await maybe_enqueue_autodj(cur, guild, channel_id):
                        return

                    if _should_auto_disconnect(guild, stay_in_vc) and guild.voice_client:
                        await mark_voice_disconnected(guild.id, getattr(getattr(guild.voice_client, "channel", None), "id", channel_id), desired_connected=False, reason="queue_complete")
                        await guild.voice_client.disconnect()
                    elif guild.voice_client and getattr(guild.voice_client, "channel", None):
                        await persist_voice_state(guild.id, guild.voice_client.channel.id, desired_connected=bool(stay_in_vc), connected=True)
                    return

                song_id, url, title, requester_id, track_uid = next_song
                track_uid = _ensure_track_uid(track_uid)
                original_queue_url, original_queue_title = url, title
                claim_live_queue_track(guild.id, url, title, track_uid)
                await cur.execute("DELETE FROM nexus_queue WHERE id = %s AND guild_id = %s AND bot_name = 'nexus'", (song_id, guild.id))
                try:
                    # Use prefetch cache if available for this guild+url (eliminates Lavalink round-trip gap)
                    _cached = _nexus_prefetch_cache.get(guild.id)
                    if _cached and _cached.get('url') == url and _cached.get('expires_at', 0) > time.monotonic():
                        track = _cached['track']
                        resolved_source = url
                        _nexus_prefetch_cache.pop(guild.id, None)
                        logger.debug("[nexus] Prefetch cache hit for guild %s: %s", guild.id, title)
                    else:
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
                            "UPDATE nexus_playback_state SET video_url = NULL, title = NULL, is_playing = FALSE, is_paused = FALSE, position_seconds = 0, play_session_key = NULL, track_uid = NULL WHERE guild_id = %s AND bot_name = 'nexus'",
                            (guild.id,),
                        )
                        await remember_recovery_state(guild.id, channel_id, 0)
                        clear_live_queue_claim(guild.id, original_queue_url, original_queue_title, track_uid)
                        clear_recovery_retry(guild.id)
                        schedule_named_task(f"skip_unavailable:{guild.id}", process_queue(guild, channel_id))
                        return
                    await requeue_failed_track(cur, guild.id, channel_id, url, title, requester_id, position=start_position, reason="search_failure", track_uid=track_uid)
                    clear_live_queue_claim(guild.id, original_queue_url, original_queue_title, track_uid)
                    return

                _apply_track_runtime_extras(
                    track,
                    track_uid=track_uid,
                    requester_id=requester_id,
                    queue_url=original_queue_url,
                    queue_title=original_queue_title,
                    resume_position=start_position,
                )

                voice_client = await ensure_voice_connection(guild, channel_id, respect_recovery_backoff=False, allow_stale_rejoin=allow_recovery_restore)
                if not voice_client:
                    logger.warning(f"[{guild.id}] Voice connection unavailable. Requeueing '{title}' for recovery.")
                    await requeue_failed_track(cur, guild.id, channel_id, url, title, requester_id, position=start_position, reason="voice_connect", track_uid=track_uid)
                    clear_live_queue_claim(guild.id, original_queue_url, original_queue_title, track_uid)
                    return

                wav_filters = wavelink.Filters()
                try:
                    await voice_client.set_volume(vol)

                    if c_mod_left > 0:
                        wav_filters.timescale.set(speed=c_speed, pitch=c_pitch)
                        c_mod_left -= 1
                        await cur.execute("UPDATE nexus_guild_settings SET custom_modifiers_left = %s WHERE guild_id = %s", (c_mod_left, guild.id))
                        if c_mod_left == 0: await cur.execute("UPDATE nexus_guild_settings SET custom_speed = 1.0, custom_pitch = 1.0 WHERE guild_id = %s", (guild.id,))

                    c_speed = apply_filter_preset(wav_filters, filter_mode, c_speed)

                    await replace_audio_filters(voice_client, wav_filters)

                    if trans_mode in {'fade', 'smart'} and start_position <= 0:
                        await voice_client.set_volume(0)
                except Exception as e:
                    logger.error(f"[{guild.id}] Player preparation failed for '{title}': {e}")
                    if _is_stale_lavalink_player_error(e) and guild.voice_client:
                        if VOICE_FORCE_STALE_CLIENT_REJOIN:
                            try:
                                await guild.voice_client.disconnect()
                            except Exception as tx_error:
                                logger.debug(f"[{guild.id}] Failed to recycle stale Lavalink player cleanly.", exc_info=True)
                        else:
                            logger.warning(f"[{guild.id}] Stale Lavalink player detected, but this playback path is not allowed to recycle/rejoin automatically; waiting for a direct/manual recovery command.")
                    await requeue_failed_track(cur, guild.id, channel_id, url, title, requester_id, position=start_position, reason="player_prepare", track_uid=track_uid)
                    clear_live_queue_claim(guild.id, original_queue_url, original_queue_title, track_uid)
                    return

                try:
                    play_start_ms = int(max(0, start_position) * 1000)
                    await asyncio.wait_for(
                        voice_client.play(track, start=play_start_ms if play_start_ms > 0 else 0),
                        timeout=LAVALINK_PLAY_TIMEOUT_SECONDS,
                    )
                    # Do not clear Lavalink failure counters here. play() only means Lavalink accepted the track;
                    # the stream can still fail seconds later. Clear only after a clean FINISHED event.
                    # Seed runtime tracking immediately after Lavalink accepts play(), before resume seek verification.
                    # This protects long-track resume points if Discord voice drops or the checkpoint loop runs
                    # during the seek/verification window. The final block below overwrites this provisional state.
                    playback_tracking[guild.id] = {'start_time': time.monotonic(), 'offset': start_position, 'url': url, 'channel_id': channel_id, 'title': title, 'track_uid': track_uid, 'original_queue_url': original_queue_url, 'original_queue_title': original_queue_title, 'duration': duration, 'speed': c_speed, 'current_filter': filter_mode, 'requester_id': requester_id, 'transition_mode': trans_mode, 'volume': vol, 'paused': False, 'last_position_checkpoint': start_position, 'last_listen_position': start_position, 'listen_seconds_committed': 0}
                    guild_states[guild.id] = {"voice_channel_id": channel_id, "position": start_position, "track_uid": track_uid, "url": url, "title": title}
                    invalidate_position_persist(guild.id)
                    if start_position > 0:
                        start_position = await apply_resume_seek(
                            voice_client,
                            guild.id,
                            start_position,
                            track_uid=track_uid,
                            video_url=url,
                            title=title,
                        )
                    elif trans_mode in {'fade', 'smart'}:
                        fade_duration = choose_fade_duration(trans_mode, fade_seconds, duration, filter_mode, title)
                        schedule_named_task(f"fade_volume:{guild.id}", _fade_volume(voice_client, 0, vol, duration=fade_duration, curve=fade_curve), overwrite=True)
                except Exception as e:
                    logger.error(f"[{guild.id}] Playback start failed for '{title}': {e}")
                    playback_tracking.pop(guild.id, None)
                    invalidate_position_persist(guild.id)
                    await requeue_failed_track(cur, guild.id, channel_id, url, title, requester_id, position=start_position, reason="playback_start", track_uid=track_uid)
                    clear_live_queue_claim(guild.id, original_queue_url, original_queue_title, track_uid)
                    return

                # FIX: Execute auto-stage updater
                schedule_named_task(f"stage_topic:{guild.id}", update_stage_topic(guild, title, requester_id))

                # Prefetch the next track's Lavalink URL in the background so the
                # gap between tracks is eliminated on the next on_wavelink_track_end.
                try:
                    await cur.execute(
                        "SELECT video_url, title FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC LIMIT 1",
                        (guild.id,),
                    )
                    _next_row = await cur.fetchone()
                    if _next_row and _next_row[0]:
                        asyncio.create_task(_prefetch_next_track_bg(guild.id, _next_row[0], _next_row[1] or ""))
                except Exception:
                    pass

                if start_position <= 0:
                    await cur.execute("INSERT INTO nexus_history (guild_id, video_url, title, requester_id) VALUES (%s, %s, %s, %s)", (guild.id, url, title, requester_id))
                    # Fire-and-forget: intelligence write gets its own connection so playback resumes immediately.
                    asyncio.ensure_future(_record_track_play_started_bg(guild.id, url, title, requester_id))
                    _prev_track = playback_tracking.get(guild.id)
                    if _prev_track and (_prev_track.get("url") or _prev_track.get("title")):
                        asyncio.ensure_future(_record_track_cooccurrence_bg(guild.id, _prev_track.get("url"), _prev_track.get("title"), url, title))
                else:
                    asyncio.ensure_future(_record_track_play_resumed_bg(guild.id, url, title, requester_id))
                try:
                    await bot.change_presence(status=discord.Status.online, activity=discord.Activity(type=discord.ActivityType.listening, name=str(title).replace("\\n", " ").strip()[:120]))
                except Exception as tx_error:
                    pass
                playback_tracking[guild.id] = {'start_time': time.monotonic(), 'offset': start_position, 'url': url, 'channel_id': channel_id, 'title': title, 'track_uid': track_uid, 'original_queue_url': original_queue_url, 'original_queue_title': original_queue_title, 'duration': duration, 'speed': c_speed, 'current_filter': filter_mode, 'requester_id': requester_id, 'transition_mode': trans_mode, 'volume': vol, 'paused': False, 'last_position_checkpoint': start_position, 'last_listen_position': start_position, 'listen_seconds_committed': 0}
                clear_live_queue_claim(guild.id, original_queue_url, original_queue_title, track_uid)

                bot_n = os.path.basename(__file__).replace('.py', '')
                await cur.execute(f"REPLACE INTO {bot_n}_playback_state (guild_id, bot_name, channel_id, video_url, position_seconds, is_playing, is_paused, title, play_session_key, track_uid) VALUES (%s, '{bot_n}', %s, %s, %s, TRUE, FALSE, %s, %s, %s)", (guild.id, channel_id, url, start_position, title, track_uid, track_uid))
                # Update persistent state
                guild_states[guild.id] = {"voice_channel_id": channel_id, "position": start_position, "track_uid": track_uid, "url": url, "title": title}
                invalidate_position_persist(guild.id)
                clear_recovery_retry(guild.id)
                clear_interrupt_resume(guild.id)
                clear_voice_disconnect_grace(guild.id)
                clear_recovery_backoff(guild.id)
                clear_auto_restore_snooze(guild.id)
                clear_idle_restore_state(guild.id)
                await save_state(guild.id)

                embed = discord.Embed(title="🎵 Now Playing", description=f"**[{title}]({url})**\n*By: {uploader}*", color=discord.Color.from_rgb(88, 101, 242))
                embed.add_field(name="Track ID", value=f"`{_track_uid_label(track_uid)}`", inline=True)
                if start_position > 0:
                    embed.add_field(name="Resumed At", value=f"`{start_position}s`", inline=True)
                if requester_id:
                    requester_name = await resolve_requester_name(guild, requester_id)
                    embed.add_field(name="Requested by", value=requester_name, inline=True)
                await send_or_update_status_message(guild, embed)
                if TRACK_FEEDBACK_BROADCAST and start_position <= 0:
                    now_playing_desc = f"**[{_compact_track_title(title)}]({url})**\n*By: {uploader}*"
                    if requester_id:
                        requester_name = await resolve_requester_name(guild, requester_id)
                        now_playing_desc += f"\nRequested by **{requester_name}**"
                    await send_feedback_notice(
                        guild,
                        "🎵 Now Playing",
                        now_playing_desc,
                        discord.Color.from_rgb(88, 101, 242),
                        dedupe_key=f"now_playing:{track_uid}",
                        fields=_track_feedback_fields(track_uid),
                    )
                if TRACK_FEEDBACK_BROADCAST and start_position > 0:
                    await send_feedback_notice(
                        guild,
                        "✅ Track Resumed",
                        f"Resumed **{_compact_track_title(title)}** from **{start_position}s**.",
                        discord.Color.green(),
                        dedupe_key=f"resume_success:{track_uid}:{start_position // 15}",
                        fields=_track_feedback_fields(track_uid, start_position),
                    )

async def process_queue(guild, channel_id, start_position=0, *, allow_recovery_restore=False):
    backoff_remaining = recovery_backoff_remaining(guild.id)
    if backoff_remaining > 0 and not allow_recovery_restore:
        logger.info(f"[{guild.id}] Process queue deferred for {backoff_remaining}s because voice/recovery backoff is active.")
        return
    inflight_remaining = voice_connect_inflight_remaining(guild.id)
    if inflight_remaining > 0:
        logger.info(f"[{guild.id}] Process queue deferred for {inflight_remaining}s because a voice connection attempt is already in flight.")
        return
    lock = get_process_queue_lock(guild.id)
    was_recovering = guild.id in recovering_guilds
    async with lock:
        try:
            vc = guild.voice_client
            current_track = _player_current_track(vc) if vc else None
            if vc and current_track is not None:
                if _player_is_playing(vc):
                    return
                if _player_is_paused(vc) and start_position <= 0:
                    return
            return await _process_queue_inner(guild, channel_id, start_position=start_position, allow_recovery_restore=allow_recovery_restore)
        finally:
            if was_recovering:
                recovering_guilds.discard(guild.id)

async def stop_playback(guild):
    playback_tracking.pop(guild.id, None)
    guild_states.pop(guild.id, None)
    recovering_guilds.discard(guild.id)
    invalidate_position_persist(guild.id)
    clear_recovery_retry(guild.id)
    clear_interrupt_resume(guild.id)
    clear_voice_disconnect_grace(guild.id)
    clear_idle_restore_state(guild.id)
    process_queue_locks.pop(guild.id, None)
    track_requeue_locks.pop(guild.id, None)
    voice_connect_locks.pop(guild.id, None)
    await delete_state(guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("UPDATE nexus_playback_state SET channel_id = NULL, video_url = NULL, title = NULL, position_seconds = 0, is_playing = FALSE, is_paused = FALSE, play_session_key = NULL, track_uid = NULL WHERE guild_id = %s AND bot_name = 'nexus'", (guild.id,))
                await cur.execute("DELETE FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", (guild.id,))
                await cur.execute("DELETE FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus'", (guild.id,))
                await cur.execute("UPDATE nexus_voice_state SET desired_connected = FALSE, connected_channel_id = NULL, disconnected_at = CURRENT_TIMESTAMP WHERE guild_id = %s AND bot_name = 'nexus'", (guild.id,))
    await clear_voice_channel_status(guild)
    if guild.voice_client:
        try:
            await guild.voice_client.disconnect()
        except Exception as tx_error:
            logger.debug("[%s] Voice disconnect failed during stop_playback.", guild.id, exc_info=True)
    try:
        await bot.change_presence(status=discord.Status.online, activity=discord.Activity(type=discord.ActivityType.watching, name="the Swarm | Idle"))
    except Exception as tx_error:
        pass

async def restore_guild_state(guild_id, state, *, override_backoff=False):
    target_guild_id = int(guild_id)
    guild = bot.get_guild(target_guild_id)
    vc = guild.voice_client if guild else None
    if not override_backoff and aria_recovery_authority_blocks_self_heal("restore_guild_state", target_guild_id):
        return

    # FIX 2: Gate on recovering_guilds BEFORE the backoff check to prevent
    # simultaneous recoveries when multiple healer loops fire at once.
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
        merged_state = dict(state or {})
        db_state = await derive_recovery_state_from_db(target_guild_id)
        if db_state:
            merged_state.update({k: v for k, v in db_state.items() if v not in (None, "", [])})

        vc_id = merged_state.get("voice_channel_id")
        channel = None
        if vc_id:
            channel = guild.get_channel(vc_id)
        # Fallback to configured home channel if no saved voice channel
        if not channel:
            try:
                async with DBPoolManager() as pool:
                    async with pool.acquire() as conn:
                        async with conn.cursor() as cur:
                            await cur.execute(
                                "SELECT home_vc_id FROM nexus_bot_home_channels WHERE guild_id = %s AND bot_name = 'nexus' LIMIT 1",
                                (target_guild_id,),
                            )
                            home_row = await cur.fetchone()
                            if home_row and home_row[0]:
                                channel = guild.get_channel(home_row[0])
                                vc_id = home_row[0]
            except Exception as tx_error:
                pass
        if not channel:
            logger.warning(f"[{target_guild_id}] Could not resolve any voice channel (saved or home) for recovery.")
            return

        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    restored_active = await restore_active_playback_entry(cur, target_guild_id)
                    if not restored_active:
                        await restore_queue_from_backup(cur, target_guild_id)
                    await cur.execute(
                        "SELECT position_seconds, video_url, title, track_uid FROM nexus_playback_state WHERE guild_id = %s AND bot_name = 'nexus' AND (is_playing = TRUE OR is_paused = TRUE OR position_seconds > 0 OR video_url IS NOT NULL OR title IS NOT NULL) ORDER BY is_playing DESC, is_paused DESC, position_seconds DESC LIMIT 1",
                        (target_guild_id,),
                    )
                    pos_row = await cur.fetchone()
                    db_position = pos_row[0] if pos_row and pos_row[0] is not None else None
                    db_url = _row_value(pos_row, "video_url", _row_value(pos_row, 1)) if pos_row else None
                    db_title = _row_value(pos_row, "title", _row_value(pos_row, 2)) if pos_row else None
                    db_track_uid = _normalize_track_uid(_row_track_uid(pos_row, 3)) if pos_row else None

        # NOTE: use `is not None` rather than a truthy check — db_position can
        # legitimately be 0 (track restarted from the beginning / deep-seek reset),
        # and a bare truthy check would then fall back to merged_state's possibly
        # stale position from an earlier snapshot, resuming at the wrong offset.
        resume_position = max(0, int(db_position if db_position is not None else merged_state.get("position", 0)))
        resume_track_uid = db_track_uid or merged_state.get("track_uid")
        resume_url = db_url or merged_state.get("url")
        resume_title = db_title or merged_state.get("title")
        await remember_recovery_state(target_guild_id, vc_id, resume_position, track_uid=resume_track_uid, url=resume_url, title=resume_title)

        if not override_backoff:
            await asyncio.sleep(random.uniform(0.0, min(STARTUP_RECOVERY_JITTER_SECONDS, 8.0)))
        vc = await ensure_voice_connection(guild, vc_id, respect_recovery_backoff=not override_backoff, allow_stale_rejoin=override_backoff)
        if not vc:
            schedule_recovery_retry(target_guild_id, vc_id, start_position=resume_position, reason="restore_voice")
            return

        schedule_named_task(f"process_queue:{target_guild_id}", process_queue(guild, vc_id, start_position=resume_position, allow_recovery_restore=True))
        handoff = True
    except Exception as e:
        logger.error(f"[RESTORE ERROR] {guild_id}: {e}")
        schedule_recovery_retry(target_guild_id, (state or {}).get("voice_channel_id"), start_position=(state or {}).get("position", 0), reason="restore_exception")
    finally:
        if not handoff:
            recovering_guilds.discard(target_guild_id)

async def _drsd_fetch_playback(pool, guild_id):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT channel_id, position_seconds, is_playing, video_url, title, track_uid "
                "FROM nexus_playback_state WHERE guild_id = %s AND bot_name = 'nexus' LIMIT 1",
                (guild_id,),
            )
            return await cur.fetchone()

async def _drsd_fetch_home(pool, guild_id):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT home_vc_id FROM nexus_bot_home_channels WHERE guild_id = %s AND bot_name = 'nexus' LIMIT 1",
                (guild_id,),
            )
            return await cur.fetchone()

async def _drsd_fetch_voice(pool, guild_id):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT COALESCE(connected_channel_id, last_channel_id) FROM nexus_voice_state WHERE guild_id = %s AND bot_name = 'nexus' AND desired_connected = TRUE LIMIT 1",
                (guild_id,),
            )
            return await cur.fetchone()

async def _drsd_fetch_queue_count(pool, guild_id):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT COUNT(*) FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'",
                (guild_id,),
            )
            return await cur.fetchone()

async def _drsd_fetch_backup_count(pool, guild_id):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT COUNT(*) FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus'",
                (guild_id,),
            )
            return await cur.fetchone()

async def derive_recovery_state_from_db(guild_id):
    async with DBPoolManager() as pool:
        playback, home_row, voice_row, queue_row, backup_row = await asyncio.gather(
            _drsd_fetch_playback(pool, guild_id),
            _drsd_fetch_home(pool, guild_id),
            _drsd_fetch_voice(pool, guild_id),
            _drsd_fetch_queue_count(pool, guild_id),
            _drsd_fetch_backup_count(pool, guild_id),
        )

    playback_channel_id = playback[0] if playback and playback[0] else None
    playback_position = playback[1] if playback and playback[1] is not None else 0
    playback_is_playing = bool(playback[2]) if playback else False
    playback_url = playback[3] if playback else None
    playback_title = playback[4] if playback else None
    playback_track_uid = _normalize_track_uid(playback[5]) if playback else ""
    home_channel_id = home_row[0] if home_row and home_row[0] else None
    voice_channel_id = voice_row[0] if voice_row and voice_row[0] else None
    queue_count = queue_row[0] if queue_row else 0
    backup_count = backup_row[0] if backup_row else 0

    channel_id = playback_channel_id or voice_channel_id or home_channel_id
    has_playback_state = bool((playback_is_playing or playback_position > 0) and (playback_url or playback_title))
    has_recoverable_state = bool(channel_id and (has_playback_state or queue_count > 0 or backup_count > 0 or playback_url or playback_title))
    if not has_recoverable_state:
        return None

    start_position = int(playback_position or 0)
    if (queue_count > 0 or backup_count > 0) and not has_playback_state:
        start_position = 0

    return {
        "voice_channel_id": int(channel_id),
        "position": max(0, start_position),
        "track_uid": playback_track_uid,
        "url": playback_url,
        "title": playback_title,
        "was_playing": has_playback_state,
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
    if aria_recovery_authority_blocks_self_heal("startup_playback_restore"):
        logger.info(f"[{BOT_ENV_PREFIX.lower()}] Startup playback restore deferred because Aria owns recovery decisions.")
        return
    if not PERSISTENT_VOICE_RESTORE_ON_STARTUP:
        logger.info(f"[{BOT_ENV_PREFIX.lower()}] Startup playback restore is disabled by configuration; not restoring saved playback on ready.")
        return
    try:
        states = await bootstrap_recovery_states_from_db()
        for gid, state in states.items():
            schedule_named_task(f"restore_guild_state:{gid}", restore_guild_state(gid, state))
    except Exception as e:
        logger.error(f"Recovery bootstrap error: {e}")

@tasks.loop(seconds=AUTO_HEAL_INTERVAL)
async def auto_heal_loop():
    global auto_heal_initialized

    if not VOICE_DISCONNECT_REJOIN_RECOVERY and not TARGETED_TRACK_INTERRUPT_RECOVERY:
        if not auto_heal_initialized:
            auto_heal_initialized = True
            logger.info(f"[{BOT_ENV_PREFIX.lower()}] Bot-side automatic voice recovery is disabled; only direct/manual recovery remains available.")
        return

    if not auto_heal_initialized:
        try:
            db_states = await bootstrap_recovery_states_from_db()
            for gid, state in db_states.items():
                schedule_named_task(f"restore_guild_state:{gid}", restore_guild_state(gid, state))
            auto_heal_initialized = True
        except Exception as tx_error:
            logger.exception("[%s] Auto-heal bootstrap failed; will retry on next tick.", BOT_ENV_PREFIX.lower())

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
                    if vc and not _player_is_active(vc):
                        playback_tracking.pop(normalized_gid, None)
                    scheduled = False
                    if state.get("position", 0) > 0 and state.get("track_uid") and TARGETED_TRACK_INTERRUPT_RECOVERY:
                        scheduled = schedule_targeted_interrupt_resume(normalized_gid, state.get("voice_channel_id"), start_position=state.get("position", 0), reason="auto_heal")
                    elif VOICE_DISCONNECT_REJOIN_RECOVERY:
                        task_name = f"restore_guild_state:{gid}"
                        existing_task = startup_task_registry.get(task_name)
                        if not existing_task or existing_task.done():
                            schedule_named_task(task_name, restore_guild_state(normalized_gid, state))
                            scheduled = True
                    if scheduled:
                        logger.info(f"[HEAL] Rejoining/restarting playback for {gid}")
        except Exception as tx_error:
            pass

# --- BOT EVENTS & LAVALINK CONNECTION ---
@bot.event
async def setup_hook():
    install_shutdown_signal_handlers()
    await init_db_with_retries()

@bot.event
async def on_wavelink_node_ready(payload: wavelink.NodeReadyEventPayload):
    logger.info(f"🔥 Lavalink Bridge Officially Connected and Locked! (Node: {payload.node.identifier})")

    # Persist the session ID so the next restart can resume without audio interruption.
    if payload.node.session_id:
        _save_lavalink_session_id(payload.node.session_id)

    if getattr(payload, "resumed", False):
        logger.info(
            "✅ Lavalink session RESUMED (session=%s) — audio kept playing through restart. "
            "Skipping orphan restore; rebuilding runtime state from active Lavalink players.",
            payload.node.session_id,
        )
        # Rebuild playback_tracking for every guild the Lavalink player reports as active
        # so position_updater and position tracking stay correct without needing a seek.
        try:
            lava_players = await payload.node._fetch_players()
            for lp in lava_players or []:
                gid = int(lp.get("guildId", 0))
                state_data = lp.get("state", {})
                track_info = (lp.get("track") or {})
                info = track_info.get("info", {})
                position_ms = state_data.get("position", 0)
                position_s = max(0, int(position_ms / 1000))
                url = info.get("uri", "")
                title = info.get("title", "")
                duration_s = max(0, int(info.get("length", 0) / 1000))
                guild = bot.get_guild(gid)
                if not guild:
                    continue
                # Lavalink still has this player live — audio never stopped. Cancel any
                # pending re-play/seek recovery so the bot doesn't restart it (the YouTube
                # deep seek would fail and reset the song to 0).
                clear_interrupt_resume(gid)
                clear_recovery_retry(gid)
                if gid not in playback_tracking and (url or title):
                    playback_tracking[gid] = {
                        "start_time": __import__("time").monotonic() - position_s,
                        "offset": position_s,
                        "url": url,
                        "channel_id": guild_states.get(gid, {}).get("voice_channel_id"),
                        "title": title,
                        "track_uid": _ensure_track_uid(None),
                        "duration": duration_s,
                        "speed": 1.0,
                        "current_filter": "none",
                        "requester_id": None,
                        "transition_mode": "off",
                        "volume": lp.get("volume", 100),
                        "paused": bool(lp.get("paused", False)),
                        "last_position_checkpoint": position_s,
                        "last_listen_position": position_s,
                        "listen_seconds_committed": 0,
                    }
                    guild_states[gid] = {
                        "voice_channel_id": guild_states.get(gid, {}).get("voice_channel_id"),
                        "position": position_s,
                        "url": url,
                        "title": title,
                    }
                    invalidate_position_persist(gid)
                    logger.info("[%s] Rebuilt runtime state from resumed Lavalink player at %ss for '%s'.", gid, position_s, title)
        except Exception as _e:
            logger.warning("Failed to rebuild playback state from resumed Lavalink players: %s", _e)
        return

    if aria_recovery_authority_blocks_self_heal("orphaned_playback_auto_resume"):
        logger.info("Orphaned playback auto-resume deferred because Aria owns recovery decisions.")
        return
    if not PERSISTENT_VOICE_RESTORE_ON_STARTUP:
        logger.info("Orphaned playback auto-resume is disabled by configuration.")
        return
    logger.info("Checking for orphaned playback states to auto-resume...")
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute("SELECT guild_id, channel_id, position_seconds, video_url, title FROM nexus_playback_state WHERE bot_name = 'nexus' AND (is_playing = TRUE OR is_paused = TRUE OR position_seconds > 0)")
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
                            schedule_named_task(
                                f"orphan_restore_process_queue:{guild.id}",
                                process_queue(
                                    guild,
                                    orphan['channel_id'],
                                    start_position=orphan['position_seconds'],
                                    allow_recovery_restore=True,
                                ),
                            )
    except Exception as e:
        logger.error(f"Auto-resume error: {e}")

@bot.event
async def on_wavelink_node_closed(_node: wavelink.Node, _disconnected):
    logger.warning("⚠️ Lavalink connection lost. Reconnecting Lavalink only; bot-side voice leave/rejoin recovery stays disabled to avoid reconnect storms. Use direct/manual RECOVER if playback stalls.")
    ensure_lavalink_connection_task()
    for guild_id, state in list(guild_states.items()):
        if state.get("position", 0) > 0 and state.get("track_uid") and TARGETED_TRACK_INTERRUPT_RECOVERY:
            schedule_targeted_interrupt_resume(int(guild_id), int(state.get("voice_channel_id") or 0), start_position=state.get("position", 0), reason="node_closed")
        elif VOICE_DISCONNECT_REJOIN_RECOVERY:
            schedule_recovery_retry(int(guild_id), int(state.get("voice_channel_id") or 0), start_position=state.get("position", 0), reason="node_closed")

@bot.event
async def on_ready():
    logger.info(f'Logged in as {bot.user}')
    reset_login_failure_backoff()

    ensure_lavalink_connection_task()

    if DISCORD_COMMAND_SYNC_ON_STARTUP:
        try:
            if DISCORD_COMMAND_SYNC_STAGGER_SECONDS > 0:
                sync_delay = (sum(ord(ch) for ch in BOT_ENV_PREFIX) % int(DISCORD_COMMAND_SYNC_STAGGER_SECONDS))
                await asyncio.sleep(sync_delay)
            await bot.tree.sync()
        except Exception as e:
            logger.error(f"Slash command sync failed: {e}")
    else:
        logger.info(f"[{BOT_ENV_PREFIX.lower()}] Startup slash command sync is disabled; run manual sync only when command definitions change.")

    if not position_updater.is_running():
        position_updater.start()
    if not metrics_heartbeat_loop.is_running():
        metrics_heartbeat_loop.start()
    if not audio_cache_eviction_loop.is_running():
        audio_cache_eviction_loop.start()
    if not audio_cache_refs_reconcile_loop.is_running():
        audio_cache_refs_reconcile_loop.start()

    schedule_named_task("restore_persistent_voice_states", restore_persistent_voice_states())
    schedule_named_task("bootstrap_recovery_after_ready", bootstrap_recovery_after_ready())

@bot.event
async def _enforce_home_channel_suppression(member, before, after):
    """Keep non-DJ listeners suppressed (no mic) in the bot's Stage home channel.

    Discord lets Stage audience members request-to-speak or be invited to
    speak, which un-suppresses their voice state and lets their mic through.
    If that happens in this bot's home Stage channel, force them back to the
    audience unless they're a DJ/admin.
    """
    try:
        if after.channel is None or after.suppress or not isinstance(after.channel, discord.StageChannel):
            return
        settings = await get_saved_settings_summary(member.guild.id)
        home_vc_id = settings[0] if settings else None
        if not home_vc_id or after.channel.id != int(home_vc_id):
            return
        if member.guild_permissions.administrator:
            return
        dj_role_id = settings[4] if settings else None
        if dj_role_id and discord.utils.get(member.roles, id=dj_role_id):
            return
        await member.edit(suppress=True)
    except Exception:
        logger.debug("[%s] Failed to enforce home-channel suppression for %s.", BOT_ENV_PREFIX.lower(), getattr(member, "id", None))

async def on_voice_state_update(member, before, after):
    if member != bot.user:
        await _enforce_home_channel_suppression(member, before, after)
    if member == bot.user:
        guild_id = member.guild.id
        if after.channel is not None:
            pending_voice_channels[guild_id] = after.channel.id
            clear_voice_disconnect_grace(guild_id)
            clear_interrupt_resume(guild_id)
            unfreeze_playback_after_voice_return(guild_id)
            await persist_voice_state(guild_id, after.channel.id, desired_connected=True, connected=True)
            await asyncio.sleep(0.5)
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
            if not VOICE_DISCONNECT_REJOIN_RECOVERY:
                recovering_guilds.discard(guild_id)
                clear_recovery_retry(guild_id)
                clear_voice_disconnect_grace(guild_id)
                await remember_recovery_state(
                    guild_id,
                    remembered_channel_id,
                    position,
                    track_uid=(tracked or {}).get("track_uid"),
                    url=(tracked or {}).get("url"),
                    title=(tracked or {}).get("title"),
                )
                await mark_voice_disconnected(guild_id, remembered_channel_id, desired_connected=True, reason="voice_disconnect_rejoin_disabled", position=position)
                scheduled = schedule_targeted_interrupt_resume(guild_id, remembered_channel_id, start_position=position, reason="voice_disconnect")
                if scheduled and member.guild:
                    await send_feedback_notice(
                        member.guild,
                        "📡 Connection Interrupted",
                        f"Saved **{_compact_track_title((tracked or {}).get('title') or (tracked or {}).get('url') or 'current track')}** and queued a focused resume from **{position}s**.",
                        discord.Color.orange(),
                        dedupe_key=f"voice_interrupt:{(tracked or {}).get('track_uid') or guild_id}:{position // 15}",
                        fields=_track_feedback_fields((tracked or {}).get("track_uid"), position),
                    )
                logger.warning(f"[{guild_id}] Voice link dropped unexpectedly. Preserved playback state at {position}s and {'scheduled focused recovery' if scheduled else 'kept it ready for direct/manual recovery'}.")
                return
            freeze_playback_for_soft_disconnect(guild_id, position)
            recovering_guilds.discard(guild_id)
            await remember_recovery_state(guild_id, remembered_channel_id, position)
            await mark_voice_disconnected(guild_id, remembered_channel_id, desired_connected=True, reason="voice_disconnect_soft_grace", position=position)
            scheduled = schedule_soft_voice_recovery(guild_id, remembered_channel_id, start_position=position, reason="voice_disconnect")
            if scheduled:
                logger.warning(f"[{guild_id}] Voice link dropped unexpectedly. Holding playback state for {int(VOICE_DISCONNECT_GRACE_SECONDS)}s before recovery from {position}s.")
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
        clear_interrupt_resume(guild_id)
        clear_voice_disconnect_grace(guild_id)
        clear_idle_restore_state(guild_id)
        process_queue_locks.pop(guild_id, None)
        track_requeue_locks.pop(guild_id, None)
        voice_connect_locks.pop(guild_id, None)
        await mark_voice_disconnected(guild_id, getattr(before.channel, "id", None), desired_connected=False, reason="manual_or_idle_disconnect")
        await delete_state(guild_id)

@tasks.loop(seconds=METRICS_HEARTBEAT_INTERVAL)
async def metrics_heartbeat_loop():
    try:
        prune_runtime_state_cache()
        # reconcile_runtime_playback_state acquires its own DBPoolManager connection — safe to gather.
        guild_snapshot = list(bot.guilds)
        reconcile_results = await asyncio.gather(
            *[reconcile_runtime_playback_state(guild) for guild in guild_snapshot],
            return_exceptions=True,
        )
        for guild, result in zip(guild_snapshot, reconcile_results):
            if isinstance(result, Exception):
                logger.exception("[nexus] Metrics reconcile failed for guild %s", getattr(guild, 'id', None))
    except Exception as tx_error:
        logger.exception("[nexus] Metrics heartbeat pre-pass failed.")
    try:
        await collect_and_persist_metrics()
    except Exception as tx_error:
        logger.exception("[nexus] Metrics heartbeat failed.")

@tasks.loop(seconds=POSITION_UPDATER_INTERVAL)
async def position_updater():
    if not bot.is_ready(): return
    try:
        if not playback_tracking:
            return
        now = time.monotonic()
        bot_n = BOT_ENV_PREFIX.lower()

        # Pre-compute all per-guild values (pure in-memory, no DB) and collect
        # batch rows for the two main writes: playback_state and metrics.
        playback_state_rows = []   # for executemany REPLACE INTO {bot_n}_playback_state
        metrics_rows = []          # for executemany INSERT INTO {bot_n}_metrics ON DUPLICATE KEY
        intelligence_rows = []     # for executemany INSERT INTO {bot_n}_track_intelligence ON DUPLICATE KEY (listen delta)
        guilds_to_persist = []     # (guild_id, data) pairs that passed the interval check

        for guild_id, data in list(playback_tracking.items()):
            last_persist = last_position_persist.get(guild_id, 0.0)
            if now - last_persist < POSITION_PERSIST_INTERVAL:
                continue
            guild = bot.get_guild(int(guild_id)) if bot else None
            vc = guild.voice_client if guild else None
            connected = bool(vc and _voice_client_connected(vc))
            playing = bool(vc and _player_is_playing(vc))
            paused = bool(vc and _player_is_paused(vc)) or bool(data.get("paused"))
            if connected and (playing or paused):
                pos = current_track_position(guild_id)
            else:
                state_position = (guild_states.get(guild_id) or guild_states.get(str(guild_id)) or {}).get("position", 0)
                pos = normalize_position_seconds(data.get("last_position_checkpoint", data.get("offset", state_position)), data.get("duration"))
                playing = False
                paused = bool(data.get("paused") or data.get("voice_soft_disconnected"))
            # Skip DB write for fully idle/stopped state — position isn't moving
            if not playing and not paused:
                continue
            # Rate-limit paused-only writes to POSITION_PAUSED_PERSIST_INTERVAL (default 60s)
            if paused and not playing and now - last_persist < POSITION_PAUSED_PERSIST_INTERVAL:
                continue

            channel_id = (
                getattr(getattr(vc, "channel", None), "id", None)
                or data.get("channel_id")
                or (guild_states.get(guild_id) or guild_states.get(str(guild_id)) or {}).get("voice_channel_id")
            )

            # Replicate the per-guild logic from persist_playback_checkpoint
            # (backward-jump guard, normalise, listen-delta) without touching the DB yet.
            position = normalize_position_seconds(pos, data.get("duration"))
            url = data.get("url")
            title = data.get("title")
            requester_id = data.get("requester_id")
            track_uid = _ensure_track_uid(data.get("track_uid")) if (url or title or data.get("track_uid")) else None
            play_session_key = track_uid
            previous_checkpoint = normalize_position_seconds(data.get("last_position_checkpoint", data.get("offset", 0)), data.get("duration"))
            if play_session_key and previous_checkpoint > 0 and position + PLAYER_POSITION_BACKSTEP_GRACE_SECONDS < previous_checkpoint:
                logger.debug(
                    "[%s] Refusing backwards checkpoint jump for %s from %ss to %ss; keeping %ss.",
                    guild_id, title or url or "active track", previous_checkpoint, position, previous_checkpoint,
                )
                position = previous_checkpoint

            playback_state_rows.append(
                (guild_id, bot_n, channel_id, url, position, bool(playing), bool(paused), title, play_session_key, track_uid)
            )
            metrics_rows.append(
                (guild_id, bot_n, bool(connected), channel_id, bool(connected), bool(playing), bool(paused), bool(playing), bool(paused), position, guild_id in recovering_guilds)
            )

            listen_delta = consume_realtime_listen_delta(data, position, playing=bool(playing))
            if listen_delta > 0 and play_session_key:
                intelligence_rows.append(
                    (guild_id, play_session_key, url, title, listen_delta, requester_id)
                )

            # Update in-memory state immediately (mirrors persist_playback_checkpoint side-effects).
            data["last_position_checkpoint"] = position
            data["last_checkpoint_at"] = time.time()
            if track_uid:
                data["track_uid"] = track_uid
            if channel_id:
                guild_states[guild_id] = {
                    "voice_channel_id": channel_id,
                    "position": position,
                    "track_uid": track_uid,
                    "url": url,
                    "title": title,
                }
            guilds_to_persist.append(guild_id)

        if not guilds_to_persist:
            return

        # Single DB round-trip: batch all rows at once.
        try:
            async with DBPoolManager() as pool:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        if playback_state_rows:
                            await cur.executemany(
                                f"REPLACE INTO {bot_n}_playback_state (guild_id, bot_name, channel_id, video_url, position_seconds, is_playing, is_paused, title, play_session_key, track_uid) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                playback_state_rows,
                            )
                        if metrics_rows:
                            await cur.executemany(
                                f"""
                                INSERT INTO {bot_n}_metrics
                                    (guild_id, bot_name, voice_connected, connected_channel_id, player_connected, player_playing, player_paused,
                                     is_playing_db, is_paused_db, position_seconds, recovery_pending)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE
                                    voice_connected = VALUES(voice_connected),
                                    connected_channel_id = VALUES(connected_channel_id),
                                    player_connected = VALUES(player_connected),
                                    player_playing = VALUES(player_playing),
                                    player_paused = VALUES(player_paused),
                                    is_playing_db = VALUES(is_playing_db),
                                    is_paused_db = VALUES(is_paused_db),
                                    position_seconds = VALUES(position_seconds),
                                    recovery_pending = VALUES(recovery_pending)
                                """,
                                metrics_rows,
                            )
                        if intelligence_rows:
                            await cur.executemany(
                                f"""
                                INSERT INTO {bot_n}_track_intelligence
                                    (guild_id, url_key, video_url, title, total_listen_seconds, last_requester_id, last_played, source)
                                VALUES (%s, %s, %s, %s, %s, %s, NOW(), 'playback_checkpoint')
                                ON DUPLICATE KEY UPDATE
                                    video_url = VALUES(video_url),
                                    title = VALUES(title),
                                    total_listen_seconds = total_listen_seconds + VALUES(total_listen_seconds),
                                    last_requester_id = VALUES(last_requester_id),
                                    last_played = NOW(),
                                    source = 'playback_checkpoint'
                                """,
                                intelligence_rows,
                            )
        except Exception as tx_error:
            logger.exception("[%s] Failed to batch-persist realtime playback checkpoints.", BOT_ENV_PREFIX.lower())
            return

        # Post-persist: update timestamps and flush state files.
        for guild_id in guilds_to_persist:
            last_position_persist[guild_id] = now
            if now - last_state_file_persist.get(guild_id, 0) >= POSITION_STATE_FILE_INTERVAL:
                try:
                    await save_state(guild_id)
                except Exception as tx_error:
                    logger.exception("[%s] Failed to save state file for guild %s.", BOT_ENV_PREFIX.lower(), guild_id)
                last_state_file_persist[guild_id] = now
    except Exception as tx_error:
        logger.exception("[%s] Position updater failed.", BOT_ENV_PREFIX.lower())

# --- SETTINGS COMMANDS ---
@bot.tree.command(name="nexus_main_sethome", description="Save this bot's default voice or stage channel for join, autoplay, and recovery behavior.")
@commands.has_permissions(administrator=True)
async def sethome(interaction: discord.Interaction, channel: discord.VoiceChannel | discord.StageChannel):
    try:
        await interaction.response.defer(ephemeral=True)
    except discord.NotFound:
        logger.warning("[%s] /sethome interaction expired before it could be acknowledged.", getattr(interaction.guild, "id", "unknown"))
        return
    except discord.InteractionResponded:
        pass

    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("REPLACE INTO nexus_bot_home_channels (guild_id, bot_name, home_vc_id) VALUES (%s, %s, %s)", (interaction.guild.id, 'nexus', channel.id))
    invalidate_feature_caches(interaction.guild.id)
    await interaction.followup.send(embed=discord.Embed(title="🏠 Home Set", description=f"Home channel set to {channel.mention}.", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_setfeedback", description="Choose the text channel (or thread) for updates, queue actions, and recovery notices.")
@commands.has_permissions(administrator=True)
async def setfeedback(interaction: discord.Interaction, channel: discord.TextChannel | discord.Thread):
    await interaction.response.defer(ephemeral=True)
    await ensure_guild_settings(interaction.guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("UPDATE nexus_guild_settings SET feedback_channel_id = %s WHERE guild_id = %s", (channel.id, interaction.guild.id))
    invalidate_feature_caches(interaction.guild.id)
    await interaction.followup.send(embed=discord.Embed(title="✅ Feedback Channel Set", description=f"Updates will be sent to {channel.mention}.", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_djrole", description="Set the server DJ role that can manage restricted playback, queue, and settings commands.")
@commands.has_permissions(administrator=True)
async def djrole(interaction: discord.Interaction, role: discord.Role):
    await interaction.response.defer(ephemeral=True)
    await ensure_guild_settings(interaction.guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("UPDATE nexus_guild_settings SET dj_role_id = %s WHERE guild_id = %s", (role.id, interaction.guild.id))
    invalidate_feature_caches(interaction.guild.id)
    await interaction.followup.send(embed=discord.Embed(description=f"🎧 DJ role set to {role.mention}", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_removedj", description="Clear the configured DJ role so only admins or open-access mode can control restricted commands.")
@commands.has_permissions(administrator=True)
async def removedj(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    await ensure_guild_settings(interaction.guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("UPDATE nexus_guild_settings SET dj_role_id = NULL WHERE guild_id = %s", (interaction.guild.id,))
    invalidate_feature_caches(interaction.guild.id)
    await interaction.followup.send(embed=discord.Embed(description="DJ role requirements removed.", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_djmode", description="Enable or disable Strict DJ Mode so only admins and the DJ role can use control commands.")
@commands.has_permissions(administrator=True)
async def toggle_djmode(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    await ensure_guild_settings(interaction.guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT dj_only_mode FROM nexus_guild_settings WHERE guild_id = %s", (interaction.guild.id,))
                res = await cur.fetchone()
                new_val = not res[0] if res else True
                await cur.execute("UPDATE nexus_guild_settings SET dj_only_mode = %s WHERE guild_id = %s", (new_val, interaction.guild.id))
    invalidate_feature_caches(interaction.guild.id)
    state = "ENABLED" if new_val else "DISABLED"
    await interaction.followup.send(embed=discord.Embed(description=f"🎧 Strict DJ Mode is now **{state}**.", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_247", description="Keep the bot connected and ready in voice channels even after playback ends until you disable it.")
@commands.has_permissions(administrator=True)
async def toggle_247(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    await ensure_guild_settings(interaction.guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT stay_in_vc FROM nexus_guild_settings WHERE guild_id = %s", (interaction.guild.id,))
                res = await cur.fetchone()
                new_val = not res[0] if res else True
                await cur.execute("UPDATE nexus_guild_settings SET stay_in_vc = %s WHERE guild_id = %s", (new_val, interaction.guild.id))
    invalidate_feature_caches(interaction.guild.id)
    state = "ENABLED" if new_val else "DISABLED"
    await interaction.followup.send(embed=discord.Embed(description=f"🕰️ 24/7 Mode is now **{state}**.", color=discord.Color.green()), ephemeral=True)

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
    except Exception as tx_error: interaction_token_valid = False

    async def send_play_feedback(embed: discord.Embed):
        if interaction_token_valid:
            try: return await interaction.followup.send(embed=embed)
            except Exception as tx_error: pass
        if interaction.channel:
            try: return await interaction.channel.send(embed=embed)
            except Exception as tx_error: pass
        return None

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
    queue_rows = [(track.uri, track.title, interaction.user.id, _new_track_uid()) for track in entries_to_add]
    added_count = len(queue_rows)

    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await prime_loop_queue_defaults(cur, interaction.guild.id)
                if queue_rows:
                    await cur.executemany(
                        "INSERT INTO nexus_queue (guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, %s, %s, %s, %s, %s)",
                        [(interaction.guild.id, 'nexus', url, title, requester_id, track_uid) for url, title, requester_id, track_uid in queue_rows],
                    )
                    try:
                        await bulk_record_tracks_queued(cur, interaction.guild.id, [(url, title, requester_id) for url, title, requester_id, _track_uid in queue_rows])
                    except Exception as tx_error:
                        logger.debug("[nexus] Bulk track intelligence queue write skipped.", exc_info=True)
                if added_count > 1:
                    await shuffle_queue_rows(cur, interaction.guild.id, preserve_first=True)
                await snapshot_queue_backup(cur, interaction.guild.id)
                await cur.execute("SELECT COUNT(*) FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
                q_len = (await cur.fetchone())[0]
    if playlist_url:
        await set_active_playlist(interaction.guild.id, playlist_url, len(entries_to_add), interaction.user.id, channel.id, entries_to_add)

    vc = interaction.guild.voice_client
    if not vc or (not getattr(vc, 'playing', False) and not getattr(vc, 'paused', False)):
        await send_play_feedback(discord.Embed(title="🎶 Queued & Starting", description=f"Added **{added_count}** tracks. Starting Lavalink Engine!", color=discord.Color.green()))
        await process_queue(interaction.guild, channel.id)
    else:
        await send_play_feedback(discord.Embed(title="📥 Added to Queue", description=f"Added **{added_count}** tracks. (Queue size: {q_len})", color=discord.Color.blue()))

@bot.tree.command(name="nexus_main_playnext", description="Queue one track to play next, ahead of the existing queue, without clearing current playback.")
async def playnext(interaction: discord.Interaction, search: str):
    if not await is_dj(interaction): return
    await interaction.response.defer(ephemeral=True)

    try:
        entries, _playlist_result = await search_playables(search)
        if not entries: raise ValueError("Track could not be found.")
        track = entries[0]
    except Exception as e:
        return await interaction.followup.send(embed=discord.Embed(description=f"Could not resolve that source: {e}", color=discord.Color.red()), ephemeral=True)

    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id, video_url, title, requester_id, track_uid FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC", (interaction.guild.id,))
                existing_rows = await cur.fetchall()
                insert_data = [(interaction.guild.id, 'nexus', track.uri, track.title, interaction.user.id, _new_track_uid())]
                insert_data.extend((
                    interaction.guild.id,
                    'nexus',
                    _row_value(row, "video_url", _row_value(row, 1)),
                    _row_value(row, "title", _row_value(row, 2)),
                    _row_value(row, "requester_id", _row_value(row, 3)),
                    _ensure_track_uid(_row_track_uid(row, 4)),
                ) for row in existing_rows)
                try:
                    await cur.execute("START TRANSACTION")
                    await cur.execute("DELETE FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
                    await cur.executemany(
                        "INSERT INTO nexus_queue (guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, %s, %s, %s, %s, %s)",
                        insert_data,
                    )
                    try:
                        await bulk_record_tracks_queued(cur, interaction.guild.id, [(track.uri, track.title, interaction.user.id)])
                    except Exception as tx_error:
                        logger.debug("[nexus] Track intelligence queue write skipped.", exc_info=True)
                    await snapshot_queue_backup(cur, interaction.guild.id)
                    await cur.execute("COMMIT")
                except Exception as tx_error:
                    try:
                        await cur.execute("ROLLBACK")
                    except Exception as tx_error:
                        pass
                    logger.exception("[nexus] playnext transaction failed; queue was rolled back instead of leaking tracks.")
                    raise
    vc = interaction.guild.voice_client
    if not vc or (not _player_is_active(vc)):
        channel = vc.channel if vc and getattr(vc, 'channel', None) else await get_home_channel(interaction.guild)
        if not channel:
            channel = interaction.user.voice.channel if interaction.user.voice else None
        if channel:
            await process_queue(interaction.guild, channel.id)
    await interaction.followup.send(embed=discord.Embed(description=f"**Playing next:** {track.title}", color=discord.Color.green()), ephemeral=True)

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
    process_queue_locks.pop(interaction.guild.id, None)
    voice_connect_locks.pop(interaction.guild.id, None)
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
    process_queue_locks.pop(interaction.guild.id, None)
    voice_connect_locks.pop(interaction.guild.id, None)
    snooze_auto_restore(interaction.guild.id)
    vc = interaction.guild.voice_client
    if vc and _player_is_active(vc):
        try: await vc.stop()
        except Exception as tx_error: pass
    playback_tracking.pop(interaction.guild.id, None)
    guild_states.pop(interaction.guild.id, None)
    recovering_guilds.discard(interaction.guild.id)
    invalidate_position_persist(interaction.guild.id)
    clear_recovery_retry(interaction.guild.id)
    clear_interrupt_resume(interaction.guild.id)
    clear_voice_disconnect_grace(interaction.guild.id)
    clear_idle_restore_state(interaction.guild.id)
    process_queue_locks.pop(interaction.guild.id, None)
    track_requeue_locks.pop(interaction.guild.id, None)
    voice_connect_locks.pop(interaction.guild.id, None)
    await delete_state(interaction.guild.id)
    await clear_active_playlist(interaction.guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("DELETE FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
                await cur.execute("DELETE FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
                await cur.execute("UPDATE nexus_playback_state SET channel_id = NULL, video_url = NULL, title = NULL, position_seconds = 0, is_playing = FALSE, is_paused = FALSE, play_session_key = NULL, track_uid = NULL WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
                await cur.execute("UPDATE nexus_voice_state SET desired_connected = FALSE, connected_channel_id = NULL, disconnected_at = CURRENT_TIMESTAMP WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
    try:
        await bot.change_presence(status=discord.Status.online)
    except Exception as tx_error:
        pass
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
        await ensure_voice_connection(interaction.guild, channel.id, allow_stale_rejoin=True)
        await interaction.followup.send(embed=discord.Embed(description=f"Joined {channel.mention}.", color=discord.Color.green()))
    else:
        await interaction.followup.send("Join a channel first, or set a home channel.", ephemeral=True)

@bot.tree.command(name="nexus_main_leave", description="Disconnect the bot from voice and clear any pending recovery handoff for this server.")
async def leave(interaction: discord.Interaction):
    if not await is_dj(interaction): return
    if interaction.guild.voice_client:
        process_queue_locks.pop(interaction.guild.id, None)
        voice_connect_locks.pop(interaction.guild.id, None)
        snooze_auto_restore(interaction.guild.id)
        playback_tracking.pop(interaction.guild.id, None)
        guild_states.pop(interaction.guild.id, None)
        recovering_guilds.discard(interaction.guild.id)
        invalidate_position_persist(interaction.guild.id)
        clear_recovery_retry(interaction.guild.id)
        clear_interrupt_resume(interaction.guild.id)
        clear_voice_disconnect_grace(interaction.guild.id)
        clear_idle_restore_state(interaction.guild.id)
        await delete_state(interaction.guild.id)
        await clear_active_playlist(interaction.guild.id)
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("DELETE FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
                    await cur.execute("DELETE FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
                    await cur.execute("UPDATE nexus_playback_state SET channel_id = NULL, video_url = NULL, title = NULL, position_seconds = 0, is_playing = FALSE, is_paused = FALSE, play_session_key = NULL, track_uid = NULL WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
                    await cur.execute("UPDATE nexus_voice_state SET desired_connected = FALSE, connected_channel_id = NULL, disconnected_at = CURRENT_TIMESTAMP WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
        await interaction.guild.voice_client.disconnect()
        await interaction.response.send_message(embed=discord.Embed(description="Left the channel.", color=discord.Color.orange()), ephemeral=True)
    else:
        await interaction.response.send_message(embed=discord.Embed(description="❌ I'm not in a voice channel.", color=discord.Color.red()), ephemeral=True)

@bot.tree.command(name="nexus_main_queue", description="Show the current queue with paging, requester names, and track positions")
async def queue_cmd(interaction: discord.Interaction, page: int = 1):
    await interaction.response.defer(ephemeral=True)
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
        return await interaction.followup.send(embed=discord.Embed(description="Queue empty.", color=discord.Color.red()), ephemeral=True)
    lines = []
    for idx, row in enumerate(songs, start=offset + 1):
        title, requester_id = row
        requester_name = await resolve_requester_name(interaction.guild, requester_id)
        lines.append(f"**{idx}.** {title} — *{requester_name}*")
    pages = max(1, (total + per_page - 1) // per_page)
    embed = discord.Embed(title="📜 Queue", description="\n".join(lines), color=discord.Color.blurple())
    embed.set_footer(text=f"Page {page}/{pages} • {total} queued track(s)")
    await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name="nexus_main_shuffle", description="Smart-shuffle the upcoming queue while keeping duplicate tracks separated when possible.")
async def shuffle(interaction: discord.Interaction):
    if not await is_dj(interaction): return
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                queue_len = await shuffle_queue_rows(cur, interaction.guild.id, preserve_first=False)
                if queue_len <= 0:
                    return await interaction.response.send_message("Queue empty.", ephemeral=True)
                await snapshot_queue_backup(cur, interaction.guild.id)
    await interaction.response.send_message(embed=discord.Embed(description=f"🔀 Smart-shuffled {queue_len} queued track(s), keeping repeat tracks apart where possible.", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_remove", description="Remove a queued track by its queue number so it will not play later.")
async def remove(interaction: discord.Interaction, index: int):
    if not await is_dj(interaction): return
    if index < 1:
        return await interaction.response.send_message("Invalid index.", ephemeral=True)
                # FIX 8: Fetch url+title so we can mirror the deletion in the backup
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id, video_url, title, track_uid FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC LIMIT 1 OFFSET %s", (interaction.guild.id, index-1))
                row = await cur.fetchone()
                if row:
                    await cur.execute("DELETE FROM nexus_queue WHERE id = %s AND guild_id = %s AND bot_name = 'nexus'", (row[0], interaction.guild.id))
                    await delete_backup_track(cur, interaction.guild.id, track_uid=_row_track_uid(row, 3), video_url=row[1], title=row[2])
                    await interaction.response.send_message(embed=discord.Embed(description=f"Removed item #{index}", color=discord.Color.green()), ephemeral=True)
                else: await interaction.response.send_message("Invalid index.", ephemeral=True)

@bot.tree.command(name="nexus_main_skipto", description="Drop everything before a chosen queue position and jump playback forward to that track.")
async def skipto(interaction: discord.Interaction, index: int):
    if not await is_dj(interaction): return
    if index < 1:
        return await interaction.response.send_message("Invalid index.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                # Fetch url+title so skipped tracks can be removed from backup too.
                # Use one ordered live-queue delete instead of one DELETE per skipped row.
                skip_count = max(0, index - 1)
                await cur.execute("SELECT video_url, title, track_uid FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC LIMIT %s", (interaction.guild.id, skip_count))
                rows = list(await cur.fetchall() or [])
                if rows:
                    try:
                        await cur.execute("START TRANSACTION")
                        await cur.execute("DELETE FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC LIMIT %s", (interaction.guild.id, skip_count))
                        # Keep LIMIT 1 on backup deletes so duplicate songs are not over-deleted.
                        for r in rows:
                            await delete_backup_track(
                                cur,
                                interaction.guild.id,
                                track_uid=_row_track_uid(r, 2),
                                video_url=_row_value(r, "video_url", _row_value(r, 0)),
                                title=_row_value(r, "title", _row_value(r, 1)),
                            )
                        await cur.execute("COMMIT")
                    except Exception as tx_error:
                        try:
                            await cur.execute("ROLLBACK")
                        except Exception as tx_error:
                            pass
                        logger.exception("[nexus] skipto transaction failed; live/backup queues rolled back instead of drifting.")
                        raise
    if interaction.guild.voice_client: await interaction.guild.voice_client.stop()
    await interaction.followup.send(embed=discord.Embed(description=f"Skipped to #{index}", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_move", description="Move a queued track from one queue slot to another without rebuilding the entire session manually.")
async def move(interaction: discord.Interaction, frm: int, to: int):
    if not await is_dj(interaction): return
    await interaction.response.defer(ephemeral=True)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id, video_url, title, requester_id, track_uid FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC", (interaction.guild.id,))
                q_rows = list(await cur.fetchall())
                if frm > len(q_rows) or to > len(q_rows) or frm < 1 or to < 1:
                    return await interaction.followup.send("Invalid index", ephemeral=True)
                item = q_rows.pop(frm - 1)
                insert_at = to - 1
                q_rows.insert(max(0, min(insert_at, len(q_rows))), item)
                insert_data = [(
                    interaction.guild.id,
                    'nexus',
                    _row_value(row, "video_url", _row_value(row, 1)),
                    _row_value(row, "title", _row_value(row, 2)),
                    _row_value(row, "requester_id", _row_value(row, 3)),
                    _ensure_track_uid(_row_track_uid(row, 4)),
                ) for row in q_rows]
                try:
                    await cur.execute("START TRANSACTION")
                    await cur.execute("DELETE FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
                    if insert_data:
                        await cur.executemany(
                            "INSERT INTO nexus_queue (guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, %s, %s, %s, %s, %s)",
                            insert_data,
                        )
                    await snapshot_queue_backup(cur, interaction.guild.id)
                    await cur.execute("COMMIT")
                except Exception as tx_error:
                    try:
                        await cur.execute("ROLLBACK")
                    except Exception as tx_error:
                        pass
                    logger.exception("[nexus] move transaction failed; queue was rolled back instead of leaking tracks.")
                    raise
    await interaction.followup.send(embed=discord.Embed(description=f"Moved item from {frm} to {to}", color=discord.Color.green()), ephemeral=True)



@bot.tree.command(name="nexus_main_bump", description="Move a queued track to the front so it plays next")
async def bump(interaction: discord.Interaction, index: int):
    if not await is_dj(interaction): return
    if index < 1:
        return await interaction.response.send_message("Invalid index.", ephemeral=True)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id, video_url, title, requester_id, track_uid FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC LIMIT 1 OFFSET %s", (interaction.guild.id, index-1))
                row = await cur.fetchone()
                if not row:
                    return await interaction.response.send_message("Invalid index.", ephemeral=True)
                await cur.execute("DELETE FROM nexus_queue WHERE id = %s AND guild_id = %s AND bot_name = 'nexus'", (row[0], interaction.guild.id))
                await insert_queue_front(cur, "nexus_queue", interaction.guild.id, "nexus", row[1], row[2], row[3], _row_track_uid(row, 4))
                await snapshot_queue_backup(cur, interaction.guild.id)
    await interaction.response.send_message(embed=discord.Embed(description=f"⬆️ Moved **{row[2]}** to play next.", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_clearmine", description="Remove your own queued songs without touching other listeners' tracks")
async def clearmine(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
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
    await interaction.followup.send(embed=discord.Embed(description=f"🧹 Removed **{removed}** of your queued track(s).", color=discord.Color.green()), ephemeral=True)

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
    await interaction.response.defer(ephemeral=True)
    await set_autodj_enabled(interaction.guild.id, enabled)
    state = "enabled" if enabled else "disabled"
    await interaction.followup.send(embed=discord.Embed(description=f"📻 Auto-DJ is now **{state}**.", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_settings", description="Show the saved playback, DJ, queue, and recovery settings for this server")
async def settings_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
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
    await interaction.followup.send(embed=embed, ephemeral=True)

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
    await interaction.response.defer(ephemeral=True)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT video_url, title FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC", (interaction.guild.id,))
                rows = await cur.fetchall()
                if not rows:
                    return await interaction.followup.send("Queue is empty!", ephemeral=True)
                insert_data = [(
                    interaction.user.id,
                    name,
                    _row_value(row, "video_url", _row_value(row, 0)),
                    _row_value(row, "title", _row_value(row, 1)),
                ) for row in rows]
                await cur.executemany(
                    "INSERT INTO nexus_user_playlists (user_id, playlist_name, video_url, title) VALUES (%s, %s, %s, %s)",
                    insert_data,
                )
    await interaction.followup.send(embed=discord.Embed(description=f"💾 Saved **{len(rows)}** tracks to your personal playlist: **{name}**", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_loadqueue", description="Load one of your saved personal playlists into the active queue and start playback if needed.")
async def loadqueue(interaction: discord.Interaction, name: str):
    await interaction.response.defer(ephemeral=True)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT video_url, title FROM nexus_user_playlists WHERE user_id = %s AND playlist_name = %s", (interaction.user.id, name))
                rows = await cur.fetchall()
                if not rows:
                    return await interaction.followup.send("Playlist not found or empty.", ephemeral=True)
                insert_data = [(
                    interaction.guild.id,
                    'nexus',
                    _row_value(row, "video_url", _row_value(row, 0)),
                    _row_value(row, "title", _row_value(row, 1)),
                    interaction.user.id,
                    _new_track_uid(),
                ) for row in rows]
                if insert_data:
                    await cur.executemany(
                        "INSERT INTO nexus_queue (guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, %s, %s, %s, %s, %s)",
                        insert_data,
                    )
                    try:
                        await bulk_record_tracks_queued(cur, interaction.guild.id, [(_row_value(row, "video_url", _row_value(row, 0)), _row_value(row, "title", _row_value(row, 1)), interaction.user.id) for row in rows])
                    except Exception as tx_error:
                        logger.debug("[nexus] Bulk track intelligence queue write skipped.", exc_info=True)
                    await snapshot_queue_backup(cur, interaction.guild.id)
    await interaction.followup.send(embed=discord.Embed(description=f"📂 Loaded **{len(rows)}** tracks from **{name}** into the queue!", color=discord.Color.green()), ephemeral=True)
    vc = interaction.guild.voice_client
    if not vc or (not _player_is_active(vc)):
        channel = interaction.user.voice.channel if interaction.user.voice else None
        if channel: await process_queue(interaction.guild, channel.id)

@bot.tree.command(name="nexus_main_leaderboard", description="Show the most played tracks from this server based on stored playback history.")
async def leaderboard(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT title, COUNT(*) as plays FROM nexus_history WHERE guild_id = %s GROUP BY title ORDER BY plays DESC LIMIT 10", (interaction.guild.id,))
                songs = await cur.fetchall()
    if not songs:
        return await interaction.followup.send("No play history yet.", ephemeral=True)
    desc = "\n".join(f"**{i+1}.** {s[0]} *(Played {s[1]} times)*" for i, s in enumerate(songs))
    await interaction.followup.send(embed=discord.Embed(title="🏆 Server Top Tracks", description=desc, color=discord.Color.gold()), ephemeral=True)

@bot.tree.command(name="nexus_main_history", description="Show the most recent tracks played in this server from playback history.")
async def history(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT title FROM nexus_history WHERE guild_id = %s ORDER BY played_at DESC LIMIT 5", (interaction.guild.id,))
                songs = await cur.fetchall()
    if songs:
        await interaction.followup.send(embed=discord.Embed(title="📜 History", description="\n".join(f"- {s[0]}" for s in songs), color=discord.Color.blurple()), ephemeral=True)
    else:
        await interaction.followup.send("No history.", ephemeral=True)

@bot.tree.command(name="nexus_main_userhistory", description="Show the most recent tracks requested by a specific user in this server.")
async def userhistory(interaction: discord.Interaction, member: discord.Member):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id, title, video_url FROM nexus_history WHERE guild_id = %s AND requester_id = %s ORDER BY played_at DESC LIMIT 10", (interaction.guild.id, member.id))
                songs = await cur.fetchall()
    if not songs: return await interaction.response.send_message(embed=discord.Embed(description=f"📭 {member.display_name} hasn't queued any songs yet.", color=discord.Color.red()), ephemeral=True)
    desc = "\n".join(f"**{idx + 1}.** [{song[1]}]({song[2]})" for idx, song in enumerate(songs))
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
    await interaction.response.defer(ephemeral=True)
    if interaction.guild.id in playback_tracking:
        data = playback_tracking[interaction.guild.id]
        dm_embed = discord.Embed(
            title="🎵 Track Saved!",
            description=f"Hey **{interaction.user.display_name}**!\nHere is the track you wanted to save:\n\n**[{data.get('title', 'Unknown Title')}]({data['url']})**",
            color=discord.Color.from_rgb(88, 101, 242)
        )
        try:
            await interaction.user.send(embed=dm_embed)
            await interaction.followup.send(embed=discord.Embed(description="📬 Check your DMs!", color=discord.Color.green()), ephemeral=True)
        except discord.Forbidden:
            await interaction.followup.send(embed=discord.Embed(description="❌ I can't DM you! Please check your privacy settings.", color=discord.Color.red()), ephemeral=True)
    else:
        await interaction.followup.send(embed=discord.Embed(description="Nothing is currently playing.", color=discord.Color.red()), ephemeral=True)

@bot.tree.command(name="nexus_main_like", description="Teach Auto-DJ to play more tracks like the current one for you.")
async def like_track(interaction: discord.Interaction):
    snapshot = await get_current_track_snapshot(interaction.guild.id)
    if not snapshot:
        return await interaction.response.send_message("Nothing is playing right now.", ephemeral=True)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await record_track_feedback(cur, interaction.guild.id, interaction.user.id, snapshot.get("url"), snapshot.get("title"), liked=True)
    if TRACK_FEEDBACK_BROADCAST:
        await send_feedback_notice(
            interaction.guild,
            "👍 Auto-DJ Learned",
            f"**{interaction.user.display_name}** liked **{snapshot.get('title') or 'this track'}**.",
            discord.Color.green(),
            dedupe_key=f"like:{interaction.user.id}:{snapshot.get('track_uid') or snapshot.get('url') or snapshot.get('title')}",
            fields=[("Track ID", f"`{_track_uid_label(snapshot.get('track_uid'))}`", True)],
        )
    await interaction.response.send_message(embed=discord.Embed(description=f"Saved your like for **{snapshot.get('title') or 'this track'}**. Auto-DJ will lean toward similar picks.", color=discord.Color.green()), ephemeral=True)


@bot.tree.command(name="nexus_main_dislike", description="Teach Auto-DJ to avoid the current track for your future recommendations.")
async def dislike_track(interaction: discord.Interaction):
    snapshot = await get_current_track_snapshot(interaction.guild.id)
    if not snapshot:
        return await interaction.response.send_message("Nothing is playing right now.", ephemeral=True)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await record_track_feedback(cur, interaction.guild.id, interaction.user.id, snapshot.get("url"), snapshot.get("title"), liked=False)
    if TRACK_FEEDBACK_BROADCAST:
        await send_feedback_notice(
            interaction.guild,
            "👎 Auto-DJ Learned",
            f"**{interaction.user.display_name}** disliked **{snapshot.get('title') or 'this track'}**.",
            discord.Color.orange(),
            dedupe_key=f"dislike:{interaction.user.id}:{snapshot.get('track_uid') or snapshot.get('url') or snapshot.get('title')}",
            fields=[("Track ID", f"`{_track_uid_label(snapshot.get('track_uid'))}`", True)],
        )
    await interaction.response.send_message(embed=discord.Embed(description=f"Saved your dislike for **{snapshot.get('title') or 'this track'}**. Auto-DJ will avoid it for you.", color=discord.Color.orange()), ephemeral=True)


@bot.tree.command(name="nexus_main_recommend", description="Queue a smart recommendation learned from server taste, playlists, and listener feedback.")
async def recommend(interaction: discord.Interaction, member: discord.Member = None):
    await interaction.response.defer()
    target = member or interaction.user
    channel = None
    if interaction.guild.voice_client and getattr(interaction.guild.voice_client, "channel", None):
        channel = interaction.guild.voice_client.channel
    if not channel:
        channel = await get_home_channel(interaction.guild)
    if not channel:
        channel = interaction.user.voice.channel if interaction.user.voice else None
    if not channel:
        return await interaction.followup.send(embed=discord.Embed(title="Source Error", description="Join a channel first or set a home channel.", color=discord.Color.red()))

    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                chosen, recommendation = await pick_smart_recommendation_track(cur, interaction.guild.id, listener_ids=[target.id])
                if not chosen:
                    return await interaction.followup.send(embed=discord.Embed(description="I could not find a recommendation yet. Play a few tracks or save a playlist first.", color=discord.Color.red()))
                await enqueue_track(cur, interaction.guild.id, chosen.uri, chosen.title, interaction.user.id)
                await record_smart_recommendation(cur, interaction.guild.id, interaction.user.id, recommendation, chosen, reason=f"slash:{target.id}")
                await cur.execute("SELECT COUNT(*) FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", (interaction.guild.id,))
                q_len_row = await cur.fetchone()
                q_len = q_len_row[0] if q_len_row else 0

    vc = interaction.guild.voice_client
    if not vc or not _player_is_active(vc):
        await process_queue(interaction.guild, channel.id)
        title = "Smart Recommendation Starting"
    else:
        title = "Smart Recommendation Queued"
    embed = discord.Embed(title=title, description=f"Added **{chosen.title}** based on **{recommendation.get('reason', 'saved taste')}**. Queue size: {q_len}", color=discord.Color.green())
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="nexus_main_taste", description="Show the saved taste profile Auto-DJ has learned for you or another member.")
async def taste(interaction: discord.Interaction, member: discord.Member = None):
    target = member or interaction.user
    top_tracks, totals = await build_user_taste_summary(interaction.guild.id, target.id)
    if not top_tracks:
        return await interaction.response.send_message(f"No saved taste profile for {target.display_name} yet.", ephemeral=True)
    lines = []
    for idx, row in enumerate(top_tracks[:8], start=1):
        title = _clean_smart_title(_row_value(row, "title", _row_value(row, 0))) or "Unknown Track"
        score = _row_value(row, "score", _row_value(row, 7, 0))
        try:
            score_text = f"{float(score):.1f}"
        except Exception as tx_error:
            score_text = str(score)
        lines.append(f"**{idx}.** {title} *(score {score_text})*")
    played, finished, liked, disliked, skipped = totals if totals else (0, 0, 0, 0, 0)
    embed = discord.Embed(title=f"NEXUS Taste Profile: {target.display_name}", description="\n".join(lines), color=discord.Color.blurple())
    embed.add_field(name="Signals", value=f"plays {played} | finishes {finished} | likes {liked} | dislikes {disliked} | skips {skipped}", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)



FILTER_PRESET_CHOICES = [
    app_commands.Choice(name="None (Standard high quality audio)", value="none"),
    app_commands.Choice(name="Bassboost", value="bassboost"),
    app_commands.Choice(name="Nightcore", value="nightcore"),
    app_commands.Choice(name="Vaporwave", value="vaporwave"),
    app_commands.Choice(name="8D Rotation", value="8d"),
    app_commands.Choice(name="Karaoke", value="karaoke"),
    app_commands.Choice(name="Tremolo", value="tremolo"),
    app_commands.Choice(name="Vibrato", value="vibrato"),
    app_commands.Choice(name="Low Pass", value="lowpass"),
    app_commands.Choice(name="Lo-fi", value="lofi"),
    app_commands.Choice(name="Electronic", value="electronic"),
    app_commands.Choice(name="Party", value="party"),
    app_commands.Choice(name="Radio", value="radio"),
    app_commands.Choice(name="Cinema", value="cinema"),
    app_commands.Choice(name="Soft", value="soft"),
    app_commands.Choice(name="Pop", value="pop"),
    app_commands.Choice(name="Rock", value="rock"),
    app_commands.Choice(name="Classical", value="classical"),
    app_commands.Choice(name="Ear Rape", value="earrape"),
    app_commands.Choice(name="Double Time (2x)", value="doubletime"),
    app_commands.Choice(name="Slow Mo (0.5x)", value="slowmo"),
    app_commands.Choice(name="Pitch Up", value="pitch_up"),
    app_commands.Choice(name="Pitch Down", value="pitch_down"),
    app_commands.Choice(name="Deep", value="deep"),
    app_commands.Choice(name="Anime", value="anime"),
]
FILTER_PRESET_VALUES = {choice.value for choice in FILTER_PRESET_CHOICES}


_LOUDNORM_EQ = [
    (0, -0.05), (1, -0.03), (2, 0.0), (3, 0.0), (4, 0.0),
    (5, 0.0), (6, 0.0), (7, -0.01), (8, -0.02), (9, -0.02),
    (10, -0.03), (11, -0.02), (12, 0.01), (13, 0.01), (14, 0.0),
]


def _blend_loudnorm(preset_bands):
    base = dict(_LOUDNORM_EQ)
    for band, gain in preset_bands:
        base[band] = round(base.get(band, 0.0) + gain, 4)
    return sorted(base.items())


def _safe_filter_call(label, callback):
    try:
        callback()
        return True
    except Exception as exc:
        logger.debug("[%s] Audio filter preset %s skipped: %s", BOT_ENV_PREFIX.lower(), label, exc)
        return False


def apply_filter_preset(wav_filters, mode, current_speed=1.0):
    mode = str(mode or 'none').lower().replace(' ', '')
    speed = current_speed
    preset_uses_eq = False
    if mode == 'nightcore':
        if _safe_filter_call(mode, lambda: wav_filters.timescale.set(speed=1.25, pitch=1.3)):
            speed = 1.25
    elif mode == 'vaporwave':
        if _safe_filter_call(mode, lambda: wav_filters.timescale.set(speed=0.8, pitch=0.8)):
            speed = 0.8
    elif mode == 'bassboost':
        preset_uses_eq = True
        _safe_filter_call(mode, lambda: wav_filters.equalizer.set(bands=_blend_loudnorm([(0, 0.32), (1, 0.24), (2, 0.12)])))
    elif mode == '8d':
        _safe_filter_call(mode, lambda: wav_filters.rotation.set(rotation_hz=0.18))
        # Lavalink's rotation filter pans hard left/right (100% to one
        # channel at the extremes of its cycle), which makes the "8D" effect
        # cut out completely in one ear. Blend in a bit of the opposite
        # channel so audio stays faintly audible on both sides while it pans.
        _safe_filter_call(mode, lambda: wav_filters.channel_mix.set(
            left_to_left=0.8, left_to_right=0.2,
            right_to_left=0.2, right_to_right=0.8,
        ))
    elif mode == 'karaoke':
        _safe_filter_call(mode, lambda: wav_filters.karaoke.set(level=1.0, mono_level=1.0, filter_band=220.0, filter_width=100.0))
    elif mode == 'tremolo':
        _safe_filter_call(mode, lambda: wav_filters.tremolo.set(frequency=4.0, depth=0.45))
    elif mode == 'vibrato':
        _safe_filter_call(mode, lambda: wav_filters.vibrato.set(frequency=4.5, depth=0.35))
    elif mode in {'lowpass', 'lofi'}:
        _safe_filter_call(mode, lambda: wav_filters.low_pass.set(smoothing=20.0 if mode == "lowpass" else 35.0))
        if mode == 'lofi':
            _safe_filter_call(mode, lambda: wav_filters.timescale.set(speed=0.94, pitch=0.96))
            speed = 0.94
    elif mode == 'electronic':
        preset_uses_eq = True
        _safe_filter_call(mode, lambda: wav_filters.equalizer.set(bands=_blend_loudnorm([(0, 0.12), (1, 0.10), (4, -0.05), (8, 0.08), (10, 0.14)])))
    elif mode == 'party':
        preset_uses_eq = True
        _safe_filter_call(mode, lambda: wav_filters.equalizer.set(bands=_blend_loudnorm([(0, 0.25), (1, 0.18), (2, 0.08), (9, 0.10)])))
    elif mode == 'radio':
        preset_uses_eq = True
        _safe_filter_call(mode, lambda: wav_filters.equalizer.set(bands=_blend_loudnorm([(0, -0.18), (1, -0.10), (4, 0.12), (5, 0.12), (10, -0.12)])))
        _safe_filter_call(mode, lambda: wav_filters.low_pass.set(smoothing=18.0))
    elif mode == 'cinema':
        preset_uses_eq = True
        _safe_filter_call(mode, lambda: wav_filters.equalizer.set(bands=_blend_loudnorm([(0, 0.18), (1, 0.12), (8, 0.08), (9, 0.10)])))
    elif mode == 'soft':
        _safe_filter_call(mode, lambda: wav_filters.low_pass.set(smoothing=25.0))
    elif mode == 'pop':
        preset_uses_eq = True
        _safe_filter_call(mode, lambda: wav_filters.equalizer.set(bands=_blend_loudnorm([
            (0, -0.1), (1, 0.1), (2, 0.2), (3, 0.25), (4, 0.2),
            (5, 0.1), (6, 0.0), (7, -0.05), (8, -0.1), (9, -0.1),
            (10, -0.05), (11, 0.0), (12, 0.1), (13, 0.2), (14, 0.1)
        ])))
    elif mode == 'rock':
        preset_uses_eq = True
        _safe_filter_call(mode, lambda: wav_filters.equalizer.set(bands=_blend_loudnorm([
            (0, 0.3), (1, 0.25), (2, 0.1), (3, -0.1), (4, -0.15),
            (5, -0.1), (6, 0.0), (7, 0.1), (8, 0.3), (9, 0.35),
            (10, 0.3), (11, 0.2), (12, 0.1), (13, 0.05), (14, 0.0)
        ])))
    elif mode == 'classical':
        preset_uses_eq = True
        _safe_filter_call(mode, lambda: wav_filters.equalizer.set(bands=_blend_loudnorm([
            (0, 0.1), (1, 0.1), (2, 0.05), (3, 0.0), (4, -0.05),
            (5, -0.05), (6, -0.05), (7, -0.05), (8, -0.05), (9, -0.05),
            (10, 0.0), (11, 0.05), (12, 0.1), (13, 0.15), (14, 0.2)
        ])))
    elif mode == 'earrape':
        _safe_filter_call(mode, lambda: wav_filters.distortion.set(
            sin_offset=0.0, sin_scale=1.5, cos_offset=0.0, cos_scale=1.5,
            tan_offset=0.0, tan_scale=1.0, offset=0.0, scale=1.5
        ))
    elif mode == 'doubletime':
        if _safe_filter_call(mode, lambda: wav_filters.timescale.set(speed=2.0, pitch=1.0)):
            speed = 2.0
    elif mode == 'slowmo':
        if _safe_filter_call(mode, lambda: wav_filters.timescale.set(speed=0.5, pitch=1.0)):
            speed = 0.5
    elif mode == 'pitch_up':
        _safe_filter_call(mode, lambda: wav_filters.timescale.set(speed=1.0, pitch=1.3))
    elif mode == 'pitch_down':
        _safe_filter_call(mode, lambda: wav_filters.timescale.set(speed=1.0, pitch=0.7))
    elif mode == 'deep':
        preset_uses_eq = True
        _safe_filter_call(mode, lambda: wav_filters.equalizer.set(bands=_blend_loudnorm([(0, 0.5), (1, 0.4), (2, 0.3)])))
        _safe_filter_call(mode, lambda: wav_filters.timescale.set(speed=1.0, pitch=0.8))
    elif mode == 'anime':
        if _safe_filter_call(mode, lambda: wav_filters.timescale.set(speed=1.4, pitch=1.5)):
            speed = 1.4
    if not preset_uses_eq:
        _safe_filter_call('loudnorm_eq', lambda: wav_filters.equalizer.set(bands=_LOUDNORM_EQ))
    return speed

async def replace_audio_filters(voice_client, wav_filters):
    if not voice_client:
        return
    await voice_client.set_filters(wav_filters)

# --- MODIFIERS & FILTERS ---
@bot.tree.command(name="nexus_main_volume", description="Set the playback volume for this server from 1 to 200 percent.")
async def volume(interaction: discord.Interaction, vol: int):
    if not await is_dj(interaction): return
    vol = max(1, min(200, vol))
    fade_task = startup_task_registry.get(f"fade_volume:{interaction.guild.id}")
    if fade_task and not fade_task.done():
        fade_task.cancel()
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("INSERT INTO nexus_guild_settings (guild_id, volume) VALUES (%s, %s) ON DUPLICATE KEY UPDATE volume = %s", (interaction.guild.id, vol, vol))
    if interaction.guild.voice_client:
        try: await interaction.guild.voice_client.set_volume(vol)
        except Exception as tx_error: pass
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
@app_commands.choices(mode=FILTER_PRESET_CHOICES)
async def filter_cmd(interaction: discord.Interaction, mode: str):
    if not await is_dj(interaction): return
    if mode not in FILTER_PRESET_VALUES:
        return await interaction.response.send_message("Invalid filter.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    await ensure_guild_settings(interaction.guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("UPDATE nexus_guild_settings SET filter_mode = %s, custom_speed = 1.0, custom_pitch = 1.0, custom_modifiers_left = 0 WHERE guild_id = %s", (mode, interaction.guild.id))
    new_speed = 1.0
    if interaction.guild.voice_client:
        wav_filters = wavelink.Filters()
        new_speed = apply_filter_preset(wav_filters, mode)
        try:
            await replace_audio_filters(interaction.guild.voice_client, wav_filters)
        except Exception as tx_error:
            pass
    # Update in-memory speed so the progress tracker stays accurate during playback
    if interaction.guild.id in playback_tracking:
        playback_tracking[interaction.guild.id]['speed'] = new_speed
        playback_tracking[interaction.guild.id]['current_filter'] = mode
    label = next((c.name for c in FILTER_PRESET_CHOICES if c.value == mode), mode)
    await interaction.followup.send(embed=discord.Embed(description=f"🎛️ Filter set to: **{label}**.", color=discord.Color.blurple()), ephemeral=True)

@bot.tree.command(name="nexus_main_fade", description="Customize track fade transitions or let the bot pick smart fade timing.")
@app_commands.describe(mode="Fade mode", seconds="Fade length in seconds, from 0.5 to 20", curve="Volume curve")
@app_commands.choices(mode=[
    app_commands.Choice(name="Smart Adaptive Fades", value="smart"),
    app_commands.Choice(name="Custom Fades", value="fade"),
    app_commands.Choice(name="Disable Fades", value="off")
], curve=[
    app_commands.Choice(name="Linear", value="linear"),
    app_commands.Choice(name="Smooth", value="smooth"),
    app_commands.Choice(name="Slow Start", value="ease_in"),
    app_commands.Choice(name="Soft Land", value="ease_out")
])
async def toggle_fade(interaction: discord.Interaction, mode: str, seconds: float = 3.0, curve: str = "smooth"):
    if not await is_dj(interaction): return
    if mode not in {'off', 'fade', 'smart'}:
        return await interaction.response.send_message("Invalid fade mode.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    curve = curve if curve in {'linear', 'smooth', 'ease_in', 'ease_out'} else 'smooth'
    seconds = max(0.25, min(12.0, float(seconds or 3.0)))
    await ensure_guild_settings(interaction.guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("UPDATE nexus_guild_settings SET transition_mode = %s, fade_seconds = %s, fade_curve = %s WHERE guild_id = %s", (mode, seconds, curve, interaction.guild.id))
    if mode == "smart":
        message = "🌊 Smart fades enabled. I will use short, smooth ramps that adapt to track length and active filters."
        color = discord.Color.green()
    elif mode == "fade":
        message = f"🌊 Custom fades enabled: {seconds:g}s using {curve.replace('_', ' ')}."
        color = discord.Color.green()
    else:
        message = "⏹️ Smooth fades disabled."
        color = discord.Color.red()
    await interaction.followup.send(embed=discord.Embed(description=message, color=color), ephemeral=True)

@bot.tree.command(name="nexus_main_modify", description="Apply temporary custom speed and pitch modifiers to the next few tracks in the queue.")
@app_commands.describe(speed="Speed multiplier (0.5 to 2.0)", pitch="Pitch multiplier (0.5 to 2.0)", duration="How many tracks this lasts (default 1)")
async def modify_audio(interaction: discord.Interaction, speed: float = 1.0, pitch: float = 1.0, duration: int = 1):
    if not await is_dj(interaction): return
    await interaction.response.defer(ephemeral=True)
    await ensure_guild_settings(interaction.guild.id)
    speed = max(0.5, min(2.0, speed))
    pitch = max(0.5, min(2.0, pitch))
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT filter_mode FROM nexus_guild_settings WHERE guild_id = %s", (interaction.guild.id,))
                res = await cur.fetchone()
                if res and res[0] != 'none': return await interaction.followup.send(embed=discord.Embed(description="❌ **Conflict:** Disable standard Filters via `/nexus_main_filter none` first.", color=discord.Color.red()), ephemeral=True)
                if interaction.guild.id in playback_tracking:
                    playback_tracking[interaction.guild.id]["speed"] = speed
                    update_runtime_position_baseline(interaction.guild.id, current_track_position(interaction.guild.id))
                await cur.execute("UPDATE nexus_guild_settings SET custom_speed = %s, custom_pitch = %s, custom_modifiers_left = %s WHERE guild_id = %s", (speed, pitch, duration, interaction.guild.id))
    if interaction.guild.voice_client:
        wav_filters = wavelink.Filters()
        wav_filters.timescale.set(speed=speed, pitch=pitch)
        try: await replace_audio_filters(interaction.guild.voice_client, wav_filters)
        except Exception as tx_error: pass
    await interaction.followup.send(embed=discord.Embed(title="🎛️ Audio Modifiers Set", description=f"**Speed:** {speed}x\n**Pitch:** {pitch}x\n*Active for the next {duration} track(s).* ", color=discord.Color.gold()), ephemeral=True)

# --- SCRUBBING ---
@bot.tree.command(name="nexus_main_seek", description="Jump to an exact time in the current track using seconds from the start.")
async def seek(interaction: discord.Interaction, seconds: int):
    if not await is_dj(interaction): return
    if interaction.guild.id not in playback_tracking: return await interaction.response.send_message("Nothing playing.", ephemeral=True)
    if interaction.guild.voice_client:
        await interaction.guild.voice_client.seek(seconds * 1000)
        await reset_runtime_position_after_seek(interaction.guild.id, seconds, getattr(getattr(interaction.guild.voice_client, "channel", None), "id", None))
    await interaction.response.send_message(embed=discord.Embed(description=f"Seeked to {seconds}s", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_forward", description="Jump forward within the current track by the number of seconds you provide.")
async def forward(interaction: discord.Interaction, seconds: int):
    if not await is_dj(interaction): return
    if interaction.guild.id not in playback_tracking: return await interaction.response.send_message("Nothing playing.", ephemeral=True)
    current = current_track_position(interaction.guild.id)
    new_pos = current + seconds
    if interaction.guild.voice_client:
        await interaction.guild.voice_client.seek(new_pos * 1000)
        await reset_runtime_position_after_seek(interaction.guild.id, new_pos, getattr(getattr(interaction.guild.voice_client, "channel", None), "id", None))
    await interaction.response.send_message(embed=discord.Embed(description=f"Skipped forward {seconds}s", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_rewind", description="Jump backward within the current track by the number of seconds you provide.")
async def rewind(interaction: discord.Interaction, seconds: int):
    if not await is_dj(interaction): return
    if interaction.guild.id not in playback_tracking: return await interaction.response.send_message("Nothing playing.", ephemeral=True)
    current = current_track_position(interaction.guild.id)
    new_pos = max(0, current - seconds)
    if interaction.guild.voice_client:
        await interaction.guild.voice_client.seek(new_pos * 1000)
        await reset_runtime_position_after_seek(interaction.guild.id, new_pos, getattr(getattr(interaction.guild.voice_client, "channel", None), "id", None))
    await interaction.response.send_message(embed=discord.Embed(description=f"Rewound {seconds}s", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="nexus_main_replay", description="Restart the current track from the beginning without changing the queue.")
async def replay(interaction: discord.Interaction):
    if not await is_dj(interaction): return
    if interaction.guild.id not in playback_tracking: return await interaction.response.send_message("Nothing playing.", ephemeral=True)
    if interaction.guild.voice_client:
        await interaction.guild.voice_client.seek(0)
        await reset_runtime_position_after_seek(interaction.guild.id, 0, getattr(getattr(interaction.guild.voice_client, "channel", None), "id", None))
    await interaction.response.send_message(embed=discord.Embed(description="Replaying song.", color=discord.Color.green()), ephemeral=True)

# --- UTILITY & INFO ---
@bot.tree.command(name="nexus_main_panel", description="Post an interactive control panel with playback, queue, and transport buttons.")
async def panel(interaction: discord.Interaction):
    class AdvancedPanel(discord.ui.View):
        def __init__(self): super().__init__(timeout=None)
        @discord.ui.button(label="⏯️ Play/Pause", style=discord.ButtonStyle.primary, row=0)
        async def pr(self, i: discord.Interaction, _button: discord.ui.Button):
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
        async def st(self, i: discord.Interaction, _button: discord.ui.Button):
            if not await is_dj(i): return
            snooze_auto_restore(i.guild.id)
            await stop_playback(i.guild)
            _f_t = startup_task_registry.get(f"fade_volume:{i.guild.id}");
            if _f_t and not _f_t.done(): _f_t.cancel()
            await i.response.send_message("⏹️ Stopped and cleared state", ephemeral=True)
        @discord.ui.button(label="⏭️ Skip", style=discord.ButtonStyle.secondary, row=0)
        async def sk(self, i: discord.Interaction, _button: discord.ui.Button):
            if not await is_dj(i): return
            if i.guild.voice_client:
                await i.guild.voice_client.stop()
                _f_t = startup_task_registry.get(f"fade_volume:{i.guild.id}")
                if _f_t and not _f_t.done(): _f_t.cancel()
                await i.response.send_message("⏹️ Skipped to next track", ephemeral=True)
            else:
                await i.response.send_message("Nothing to skip.", ephemeral=True)
        @discord.ui.button(label="⏪ -10s", style=discord.ButtonStyle.secondary, row=1)
        async def rw(self, i: discord.Interaction, _button: discord.ui.Button):
            if not await is_dj(i): return
            if i.guild.id not in playback_tracking: return await i.response.send_message("Nothing playing.", ephemeral=True)
            current = current_track_position(i.guild.id)
            new_pos = max(0, current - 10)
            if i.guild.voice_client:
                await i.guild.voice_client.seek(new_pos * 1000)
                await reset_runtime_position_after_seek(i.guild.id, new_pos, getattr(getattr(i.guild.voice_client, "channel", None), "id", None))
            await i.response.send_message("Rewound 10 seconds.", ephemeral=True)
        @discord.ui.button(label="⏩ +10s", style=discord.ButtonStyle.secondary, row=1)
        async def fw(self, i: discord.Interaction, _button: discord.ui.Button):
            if not await is_dj(i): return
            if i.guild.id not in playback_tracking: return await i.response.send_message("Nothing playing.", ephemeral=True)
            current = current_track_position(i.guild.id)
            new_pos = current + 10
            if i.guild.voice_client:
                await i.guild.voice_client.seek(new_pos * 1000)
                await reset_runtime_position_after_seek(i.guild.id, new_pos, getattr(getattr(i.guild.voice_client, "channel", None), "id", None))
            await i.response.send_message("Skipped forward 10 seconds.", ephemeral=True)
        @discord.ui.button(label="🔀 Shuffle", style=discord.ButtonStyle.success, row=2)
        async def shuf(self, i: discord.Interaction, _button: discord.ui.Button):
            if not await is_dj(i): return
            await i.response.defer(ephemeral=True)
            async with DBPoolManager() as pool:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        queue_len = await shuffle_queue_rows(cur, i.guild.id, preserve_first=False)
                        if not queue_len: return await i.followup.send("Queue empty.")
                        await snapshot_queue_backup(cur, i.guild.id)
            await i.followup.send("🔀 Queue successfully shuffled!")
        @discord.ui.button(label="📜 View Queue", style=discord.ButtonStyle.secondary, row=2)
        async def vq(self, i: discord.Interaction, _button: discord.ui.Button):
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
    up = str(datetime.timedelta(seconds=int(time.monotonic() - bot.start_time)))
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
                    await cur.execute('''CREATE TABLE IF NOT EXISTS nexus_active_playlist_tracks (guild_id BIGINT, bot_name VARCHAR(50), playlist_url TEXT, position_idx INT DEFAULT 0, track_key CHAR(40), track_uid CHAR(32) DEFAULT NULL, video_url TEXT, title TEXT, requester_id BIGINT DEFAULT NULL, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)''')
                    await safe_schema_execute(cur, "ALTER TABLE nexus_active_playlists ADD COLUMN channel_id BIGINT DEFAULT NULL")
                    await safe_schema_execute(cur, "ALTER TABLE nexus_active_playlist_tracks ADD COLUMN track_uid CHAR(32) DEFAULT NULL")
                    await safe_schema_execute(cur, "CREATE INDEX nexus_playlist_bot_idx ON nexus_active_playlists (bot_name, guild_id)")
                    await safe_schema_execute(cur, "CREATE INDEX nexus_playlist_track_uid_idx ON nexus_active_playlist_tracks (guild_id, bot_name, track_uid)")
                    await safe_schema_execute(cur, "ALTER TABLE nexus_playback_state ADD COLUMN bot_name VARCHAR(50) DEFAULT 'nexus'")
                    await safe_schema_execute(cur, "ALTER TABLE nexus_active_playlists ADD COLUMN bot_name VARCHAR(50) DEFAULT 'nexus'")
                    await safe_schema_execute(cur, "ALTER TABLE nexus_bot_home_channels ADD COLUMN bot_name VARCHAR(50) DEFAULT 'nexus'")
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


def _is_playlist_source(value):
    text = str(value or "").strip()
    if not text.startswith(("http://", "https://")):
        return False
    parsed = urllib.parse.urlparse(text)
    params = urllib.parse.parse_qs(parsed.query)
    if params.get("list"):
        return True
    lowered_path = parsed.path.lower()
    return "/playlist" in lowered_path or "/sets/" in lowered_path


def _flat_playlist_entry_url(entry):
    if not isinstance(entry, dict):
        return None
    for key in ("webpage_url", "original_url", "url"):
        candidate = entry.get(key)
        if isinstance(candidate, str) and candidate.strip():
            candidate = candidate.strip()
            if candidate.startswith(("http://", "https://")):
                return candidate
            if re.fullmatch(r"[A-Za-z0-9_-]{8,}", candidate):
                return f"https://www.youtube.com/watch?v={candidate}"
            return candidate
    entry_id = entry.get("id")
    if isinstance(entry_id, str) and re.fullmatch(r"[A-Za-z0-9_-]{8,}", entry_id):
        return f"https://www.youtube.com/watch?v={entry_id}"
    return None


def _playlist_entry_to_queue_row(entry, requester_id=None):
    """Normalize one extracted playlist entry into a queue row tuple.

    Returns (url, title, requester_id, track_key, track_uid) or None.
    """
    if entry is None:
        return None
    if isinstance(entry, dict):
        t_title = str(entry.get('title') or entry.get('id') or entry.get('url') or 'Unknown Track')
        t_url = _flat_playlist_entry_url(entry)
    else:
        t_title = str(getattr(entry, 'title', None) or getattr(entry, 'identifier', None) or getattr(entry, 'uri', None) or 'Unknown Track')
        t_url = str(getattr(entry, 'uri', None) or getattr(entry, 'url', None) or '').strip()
    if not t_url:
        return None
    return (t_url, t_title, requester_id, _track_key(t_url, t_title), _new_track_uid())

def _playlist_rows_to_snapshot(entries, requester_id=None):
    rows = []
    for entry in entries or []:
        row = _playlist_entry_to_queue_row(entry, requester_id)
        if row:
            rows.append(row)
    return rows

def _decrement_count(counts, key):
    if not key or counts.get(key, 0) <= 0:
        return False
    counts[key] -= 1
    return True


async def expand_playlist_with_ytdlp(source):
    if _youtube_api_available():
        playlist_id = _extract_youtube_playlist_id(source)
        if playlist_id:
            try:
                api_entries = await _resolve_youtube_playlist_via_api(playlist_id)
            except Exception:
                logger.warning("[%s] YouTube Data API playlist resolve failed, falling back to yt-dlp", BOT_ENV_PREFIX.lower(), exc_info=True)
                api_entries = []
            if api_entries:
                playlist = SimpleNamespace(url=source, uri=source, tracks=api_entries)
                return api_entries, playlist

    opts = dict(ytdl_format_options)
    opts.update({
        "noplaylist": False,
        "extract_flat": True,
        "playlistend": None,
        "skip_download": True,
        "ignoreerrors": True,
    })

    def extract():
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(source, download=False)

    loop = asyncio.get_running_loop()
    data = await asyncio.wait_for(
        loop.run_in_executor(None, extract),
        timeout=PLAYLIST_SYNC_EXTRACT_TIMEOUT_SECONDS,
    )
    if not isinstance(data, dict):
        return [], None

    entries = []
    for entry in data.get("entries") or []:
        url = _flat_playlist_entry_url(entry)
        if not url:
            continue
        title = str((entry or {}).get("title") or (entry or {}).get("id") or url)
        entries.append(SimpleNamespace(uri=url, title=title))

    if not entries:
        return [], None

    playlist_url = data.get("webpage_url") or data.get("original_url") or source
    playlist = SimpleNamespace(url=playlist_url, uri=playlist_url, tracks=entries)
    return entries, playlist


async def _db_search_cache_get(cache_key: str):
    """Return (uri, title) from the DB search cache, or None on miss/error."""
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute(
                        f"SELECT resolved_uri, title FROM {BOT_ENV_PREFIX.lower()}_search_cache"
                        " WHERE cache_key = %s AND expires_at > NOW() LIMIT 1",
                        (cache_key[:128],),
                    )
                    row = await cur.fetchone()
                    if row:
                        return row["resolved_uri"], row.get("title")
    except Exception:
        pass
    return None

async def _db_search_cache_put(cache_key: str, uri: str, title):
    """Write a search result to the DB cache (TTL = SEARCH_CACHE_TTL_SECONDS, min 1h)."""
    if not uri:
        return
    ttl_hours = max(1, int(SEARCH_CACHE_TTL_SECONDS / 3600))
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        f"REPLACE INTO {BOT_ENV_PREFIX.lower()}_search_cache"
                        " (cache_key, resolved_uri, title, expires_at)"
                        " VALUES (%s, %s, %s, DATE_ADD(NOW(), INTERVAL %s HOUR))",
                        (cache_key[:128], uri[:2048], (title or "")[:512], ttl_hours),
                    )
    except Exception:
        pass

async def search_playables(query):
    cleaned = str(query or "").strip()
    if not cleaned:
        return [], None
    if not _is_explicit_lavalink_query(cleaned):
        cleaned = f"ytmsearch:{cleaned}"
    if _is_playlist_source(cleaned):
        try:
            playlist_entries, playlist_result = await expand_playlist_with_ytdlp(cleaned)
            if playlist_entries:
                return playlist_entries, playlist_result
        except Exception as exc:
            logger.warning("[%s] yt-dlp playlist expansion failed for %s: %s", "nexus", cleaned, exc)
    cache_key = cleaned.casefold()
    cacheable = not cleaned.startswith(("http://", "https://")) and "list=" not in cleaned
    if cacheable:
        cached = _cache_get(SEARCH_RESULT_CACHE, cache_key, SEARCH_CACHE_TTL_SECONDS)
        if cached is not None:
            entries, playlist_result = cached
            import copy
            return [copy.copy(_t) for _t in entries], copy.copy(playlist_result)
        # DB-backed cache: survives restarts — check before hitting YouTube
        _db_hit = await _db_search_cache_get(cache_key)
        if _db_hit is not None:
            _db_uri, _db_title = _db_hit
            try:
                if await ensure_lavalink_ready():
                    _db_results = await asyncio.wait_for(
                        wavelink.Playable.search(_db_uri), timeout=LAVALINK_SEARCH_TIMEOUT_SECONDS
                    )
                    _db_entries, _db_playlist = unwrap_search_results(_db_results)
                    if _db_entries:
                        _cache_set(SEARCH_RESULT_CACHE, cache_key, (tuple(_db_entries), _db_playlist))
                        return _db_entries, _db_playlist
            except Exception:
                pass  # DB cache miss/stale — fall through to live search
    if not await ensure_lavalink_ready():
        raise RuntimeError("Lavalink is still starting up. Try again in a few seconds.")
    results = await asyncio.wait_for(wavelink.Playable.search(cleaned), timeout=LAVALINK_SEARCH_TIMEOUT_SECONDS)
    entries, playlist_result = unwrap_search_results(results)
    if cacheable and entries:
        _cache_set(SEARCH_RESULT_CACHE, cache_key, (tuple(entries), playlist_result))
        asyncio.get_event_loop().create_task(
            _db_search_cache_put(cache_key, getattr(entries[0], 'uri', None) or cleaned, getattr(entries[0], 'title', None))
        )
    return entries, playlist_result

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
    if not any(host in candidate for host in ("on.soundcloud.com", "soundcloud.app.goo.gl", "youtu.be")):
        return candidate
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with HTTPSessionManager() as session:
            async with session.get(candidate, allow_redirects=True, timeout=timeout) as response:
                return str(response.url)
    except Exception as tx_error:
        return candidate

# --- Local audio cache: download best-quality audio once, play from disk on repeat ---
AUDIO_CACHE_DIR = os.getenv("AUDIO_CACHE_DIR", "").strip()
AUDIO_CACHE_ENABLED = bool(AUDIO_CACHE_DIR) and os.getenv("AUDIO_CACHE_ENABLED", "true").strip().lower() not in {"0", "false", "off", "no"}
AUDIO_CACHE_MAX_BYTES = int(float(os.getenv("AUDIO_CACHE_MAX_GB", "60")) * (1024 ** 3))
AUDIO_CACHE_CONCURRENCY = max(1, int(os.getenv("AUDIO_CACHE_CONCURRENCY", "2")))
AUDIO_CACHE_PREDOWNLOAD = os.getenv("AUDIO_CACHE_PREDOWNLOAD", "true").strip().lower() not in {"0", "false", "off", "no"}
AUDIO_CACHE_REFS_RECONCILE_SECONDS = max(60, int(os.getenv("AUDIO_CACHE_REFS_RECONCILE_SECONDS", "300")))
_AUDIO_CACHE_ID_RE = re.compile(r"(?:v=|youtu\.be/|/shorts/|/embed/|/v/)([A-Za-z0-9_-]{11})")
# Only formats Lavalink's LOCAL source plays reliably (seekable, robust decoders).
# Raw YouTube bestaudio (webm/opus, m4a/aac) is NOT served from disk — it is normalized
# to Ogg/Opus on download. Serving raw webm/m4a triggered "Suspicious exception" skip storms.
_AUDIO_CACHE_EXTS = ("opus", "ogg", "mp3")
_AUDIO_CACHE_MIN_BYTES = 8192
_audio_cache_locks = {}
_audio_cache_sem = None
_audio_cache_tasks = set()


def _safe_unlink(path):
    try:
        if path:
            os.remove(path)
    except OSError:
        pass


def _audio_cache_validate(path):
    """True only if ffprobe confirms a decodable audio stream (guards corruption/truncation)."""
    try:
        if not path or os.path.getsize(path) < _AUDIO_CACHE_MIN_BYTES:
            return False
        proc = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "a:0",
             "-show_entries", "stream=codec_type", "-of", "default=nw=1:nk=1", path],
            capture_output=True, text=True, timeout=30,
        )
        return proc.returncode == 0 and "audio" in (proc.stdout or "")
    except Exception:
        return False


def _audio_cache_drop(cache_id):
    """Remove every on-disk variant of a cache id (used when a local file is proven unplayable)."""
    if not cache_id or not AUDIO_CACHE_DIR:
        return
    for ext in _AUDIO_CACHE_EXTS:
        _safe_unlink(os.path.join(AUDIO_CACHE_DIR, f"{cache_id}.{ext}"))


def _is_local_cache_uri(uri):
    s = str(uri or "")
    return s.startswith("file://") or (bool(AUDIO_CACHE_DIR) and AUDIO_CACHE_DIR in s)


def _cache_id_from_uri(uri):
    """file:///audiocache/<id>.opus -> <id> (only when it looks like a YouTube id)."""
    base = os.path.basename(str(uri or "").split("?", 1)[0])
    vid = os.path.splitext(base)[0]
    return vid if re.fullmatch(r"[A-Za-z0-9_-]{11}", vid or "") else None


# Shared-dir refcount: the 13 bots share one cache dir, so a file may only be deleted from
# disk when NO bot's playlist references it. Each bot writes a marker file .refs/<id>/<bot>;
# a file is unreferenced iff its .refs/<id>/ dir is empty. Each bot only ever touches its own
# marker, so there is no cross-bot write contention.
AUDIO_CACHE_REFS_DIR = os.path.join(AUDIO_CACHE_DIR, ".refs") if AUDIO_CACHE_DIR else ""
_AUDIO_CACHE_REF_NAME = re.sub(r"[^A-Za-z0-9_.-]", "_", BOT_ENV_PREFIX.lower()) or "bot"


def _audio_cache_ref_add(cache_id):
    if not cache_id or not AUDIO_CACHE_REFS_DIR:
        return
    d = os.path.join(AUDIO_CACHE_REFS_DIR, cache_id)
    try:
        os.makedirs(d, exist_ok=True)
        open(os.path.join(d, _AUDIO_CACHE_REF_NAME), "a").close()
    except OSError:
        pass


def _audio_cache_ref_remove(cache_id):
    if not cache_id or not AUDIO_CACHE_REFS_DIR:
        return
    _safe_unlink(os.path.join(AUDIO_CACHE_REFS_DIR, cache_id, _AUDIO_CACHE_REF_NAME))
    try:
        os.rmdir(os.path.join(AUDIO_CACHE_REFS_DIR, cache_id))  # succeeds only when empty (no refs left)
    except OSError:
        pass


def _audio_cache_has_refs(cache_id):
    """True if ANY bot still references cache_id."""
    if not cache_id or not AUDIO_CACHE_REFS_DIR:
        return False
    try:
        with os.scandir(os.path.join(AUDIO_CACHE_REFS_DIR, cache_id)) as it:
            return any(True for _ in it)
    except OSError:
        return False


def _audio_cache_my_ref_ids():
    """Cache ids THIS bot currently holds a ref marker for."""
    ids = set()
    if not AUDIO_CACHE_REFS_DIR:
        return ids
    try:
        with os.scandir(AUDIO_CACHE_REFS_DIR) as it:
            for e in it:
                try:
                    if e.is_dir() and os.path.isfile(os.path.join(e.path, _AUDIO_CACHE_REF_NAME)):
                        ids.add(e.name)
                except OSError:
                    continue
    except OSError:
        pass
    return ids


def _audio_cache_id(url):
    m = _AUDIO_CACHE_ID_RE.search(str(url or ""))
    return m.group(1) if m else None


def _audio_cache_find(cache_id):
    if not cache_id or not AUDIO_CACHE_DIR:
        return None
    for ext in _AUDIO_CACHE_EXTS:
        p = os.path.join(AUDIO_CACHE_DIR, f"{cache_id}.{ext}")
        try:
            if os.path.getsize(p) >= _AUDIO_CACHE_MIN_BYTES:
                return p
        except OSError:
            continue
    return None


def _audio_cache_lookup_uri(url):
    if not AUDIO_CACHE_ENABLED:
        return None
    p = _audio_cache_find(_audio_cache_id(url))
    if p:
        try:
            os.utime(p, None)  # bump atime for LRU
        except OSError:
            pass
        return "file://" + p
    return None


def _audio_cache_publish_ready(final):
    try:
        return os.path.isfile(final) and os.path.getsize(final) >= _AUDIO_CACHE_MIN_BYTES
    except OSError:
        return False


def _audio_cache_download_blocking(cache_id, url):
    os.makedirs(AUDIO_CACHE_DIR, exist_ok=True)
    final = os.path.join(AUDIO_CACHE_DIR, f"{cache_id}.opus")
    # Cross-process dedup: the 13 bots share one cache dir, so only ONE process should
    # download a given id. flock is advisory + auto-released if a holder dies (no stale
    # locks). A waiter reuses the published file instead of downloading a duplicate. If the
    # locking machinery is unavailable for any reason, we fall through and download anyway.
    lock_path = os.path.join(AUDIO_CACHE_DIR, f".lock-{cache_id}")
    lock_fd = None
    try:
        try:
            lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                # Another bot is already downloading this id; wait for it to publish.
                for _ in range(180):
                    if _audio_cache_publish_ready(final):
                        return final
                    time.sleep(0.5)
                fcntl.flock(lock_fd, fcntl.LOCK_EX)  # block until the holder finishes
        except OSError:
            lock_fd = None
        # Someone may have finished while we waited / before we acquired the lock.
        if _audio_cache_publish_ready(final):
            return final
        return _audio_cache_fetch_and_normalize(cache_id, url, final)
    finally:
        if lock_fd is not None:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            except OSError:
                pass
            os.close(lock_fd)


def _audio_cache_fetch_and_normalize(cache_id, url, final):
    tmp_base = os.path.join(AUDIO_CACHE_DIR, f".dl-{cache_id}-{os.getpid()}")
    tmpl = tmp_base + ".%(ext)s"
    # Keep the account cookiefile: YouTube (2026) blocks anonymous downloads with
    # "Sign in to confirm you're not a bot". The bgutil plugin binds its GVS PoToken to the
    # cookie session, so cookies + bgutil yields downloadable formats. Normalize to Ogg/Opus
    # (lossless copy from YouTube's opus, re-encode only for aac) so Lavalink's LOCAL source
    # plays it reliably — raw webm/m4a from disk throws "Suspicious exception" skip storms.
    opts = {k: v for k, v in ytdl_format_options.items() if k != "extract_flat"}
    opts.update({"format": "bestaudio/best", "outtmpl": tmpl, "noplaylist": True,
                 "skip_download": False, "quiet": True, "no_warnings": True,
                 "overwrites": True, "ignoreerrors": False,
                 "postprocessors": [{"key": "FFmpegExtractAudio", "preferredcodec": "opus"}]})
    produced = None
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
            if not info:
                return None
            rd = (info.get("requested_downloads") or [{}])[0]
            produced = rd.get("filepath") or rd.get("_filename")
        if not produced or not os.path.isfile(produced):
            cand = tmp_base + ".opus"
            produced = cand if os.path.isfile(cand) else produced
        if not produced or not os.path.isfile(produced):
            return None
        # Guardrail: only publish files that actually decode to an audio stream.
        if not _audio_cache_validate(produced):
            logger.warning("[%s] Audio cache rejected %s: failed integrity validation.", BOT_ENV_PREFIX.lower(), cache_id)
            return None
        if _audio_cache_publish_ready(final):
            return final
        os.replace(produced, final)  # atomic — Lavalink never sees a partial file
        produced = None
        return final
    finally:
        # Never leave a partial/temp file behind for the eviction loop or a future lookup.
        for ext in ("opus", "webm", "m4a", "mp3", "ogg", "part", "ytdl"):
            _safe_unlink(tmp_base + "." + ext)


async def ensure_audio_cached(url):
    global _audio_cache_sem
    if not AUDIO_CACHE_ENABLED:
        return None
    cache_id = _audio_cache_id(url)
    if not cache_id:
        return None
    existing = _audio_cache_find(cache_id)
    if existing:
        return existing
    if _audio_cache_sem is None:
        _audio_cache_sem = asyncio.Semaphore(AUDIO_CACHE_CONCURRENCY)
    lock = _audio_cache_locks.setdefault(cache_id, asyncio.Lock())
    async with lock:
        existing = _audio_cache_find(cache_id)
        if existing:
            return existing
        async with _audio_cache_sem:
            try:
                loop = asyncio.get_running_loop()
                path = await loop.run_in_executor(None, _audio_cache_download_blocking, cache_id, url)
                if path:
                    logger.info("[%s] Audio cached: %s", BOT_ENV_PREFIX.lower(), os.path.basename(path))
                return path
            except Exception as exc:
                logger.warning("[%s] Audio cache download FAILED for %s: %s", BOT_ENV_PREFIX.lower(), cache_id, exc)
                return None
            finally:
                _audio_cache_locks.pop(cache_id, None)


def schedule_audio_cache_download(url):
    if not AUDIO_CACHE_ENABLED:
        return
    cache_id = _audio_cache_id(url)
    if not cache_id or _audio_cache_find(cache_id):
        return
    task = asyncio.create_task(ensure_audio_cached(url))
    _audio_cache_tasks.add(task)
    task.add_done_callback(_audio_cache_tasks.discard)


def _audio_cache_evict_blocking():
    try:
        entries = []
        total = 0
        with os.scandir(AUDIO_CACHE_DIR) as it:
            for e in it:
                if e.name.startswith("."):  # skips .dl- / .lock- / .part / .refs
                    continue
                try:
                    if not e.is_file():
                        continue
                    st = e.stat()
                except OSError:
                    continue
                entries.append((st.st_atime, st.st_size, e.path, os.path.splitext(e.name)[0]))
                total += st.st_size
        if total <= AUDIO_CACHE_MAX_BYTES:
            return 0
        entries.sort(key=lambda x: x[0])  # least-recently-used first
        # Evict UNREFERENCED files first; only touch playlist-referenced files if the disk
        # cap is still exceeded afterwards (so playlist tracks survive normal eviction).
        unref = [x for x in entries if not _audio_cache_has_refs(x[3])]
        ref = [x for x in entries if _audio_cache_has_refs(x[3])]
        removed = 0
        for group in (unref, ref):
            for _atime, size, path, cid in group:
                if total <= AUDIO_CACHE_MAX_BYTES:
                    break
                try:
                    os.remove(path)
                    if not _audio_cache_has_refs(cid):
                        try:
                            os.rmdir(os.path.join(AUDIO_CACHE_REFS_DIR, cid))
                        except OSError:
                            pass
                    total -= size
                    removed += 1
                except OSError:
                    pass
        return removed
    except FileNotFoundError:
        return 0


async def audio_cache_refs_reconcile():
    """Set THIS bot's cache refs to exactly its current playlists (handles add/remove/clear):
    new playlist videos get a ref + a download; videos no longer in any playlist lose this
    bot's ref, and are deleted from disk once no bot references them."""
    if not AUDIO_CACHE_ENABLED or not AUDIO_CACHE_REFS_DIR:
        return
    tbl = f"{BOT_ENV_PREFIX.lower()}_active_playlist_tracks"
    desired = set()
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"SELECT DISTINCT video_url FROM {tbl} WHERE bot_name = %s", (BOT_ENV_PREFIX.lower(),))
                for row in (await cur.fetchall() or []):
                    cid = _audio_cache_id(_row_value(row, 'video_url', _row_value(row, 0)))
                    if cid:
                        desired.add(cid)
    current = await asyncio.to_thread(_audio_cache_my_ref_ids)
    to_add = desired - current
    to_remove = current - desired

    def _apply():
        for cid in to_add:
            _audio_cache_ref_add(cid)
        for cid in to_remove:
            _audio_cache_ref_remove(cid)
        # IMPORTANT: never delete audio from here. Immediate ref-based deletion raced with
        # playback — a file could be removed while a bot was streaming it from disk (the live
        # queue is broader than this bot's playlist snapshot, and 13 bots share the dir),
        # producing wrong/"unknown" tracks + a download/delete/re-download storm. Unreferenced
        # files are reclaimed lazily by the capacity-based LRU eviction, which spares both
        # referenced files and anything recently played (atime grace).

    await asyncio.to_thread(_apply)
    for cid in to_add:
        schedule_audio_cache_download(f"https://www.youtube.com/watch?v={cid}")
    if to_add or to_remove:
        logger.info("[%s] Audio cache refs reconciled: +%s ref, -%s ref (deletion deferred to LRU eviction).",
                    BOT_ENV_PREFIX.lower(), len(to_add), len(to_remove))


@tasks.loop(seconds=AUDIO_CACHE_REFS_RECONCILE_SECONDS)
async def audio_cache_refs_reconcile_loop():
    if not AUDIO_CACHE_ENABLED or not AUDIO_CACHE_REFS_DIR:
        return
    try:
        await audio_cache_refs_reconcile()
    except Exception:
        logger.debug("[%s] Audio cache refs reconcile error.", BOT_ENV_PREFIX.lower(), exc_info=True)


@tasks.loop(seconds=1800)
async def audio_cache_eviction_loop():
    if not AUDIO_CACHE_ENABLED:
        return
    try:
        removed = await asyncio.to_thread(_audio_cache_evict_blocking)
        if removed:
            logger.info("[%s] Audio cache LRU eviction removed %s file(s) to stay under %sGB.", BOT_ENV_PREFIX.lower(), removed, int(AUDIO_CACHE_MAX_BYTES / (1024 ** 3)))
    except Exception:
        logger.debug("[%s] Audio cache eviction error.", BOT_ENV_PREFIX.lower(), exc_info=True)


# Retry the exact requested video this many times before a title-search substitution.
RESOLVE_DIRECT_RETRIES = max(1, int(os.getenv("RESOLVE_DIRECT_RETRIES", "3")))
RESOLVE_DIRECT_RETRY_DELAY = float(os.getenv("RESOLVE_DIRECT_RETRY_DELAY", "0.5"))


async def resolve_queue_track(source, fallback_title=None):
    _cached_uri = _audio_cache_lookup_uri(source)
    if _cached_uri:
        try:
            if await ensure_lavalink_ready():
                _cres = await asyncio.wait_for(wavelink.Playable.search(_cached_uri), timeout=LAVALINK_SEARCH_TIMEOUT_SECONDS)
                _cent, _ = unwrap_search_results(_cres)
                if _cent:
                    logger.info("[%s] Local cache HIT (playing from disk): %s", BOT_ENV_PREFIX.lower(), _audio_cache_id(source))
                    return _cent[0], source
        except Exception:
            logger.debug("[%s] Local audio cache play failed; using live source.", BOT_ENV_PREFIX.lower(), exc_info=True)
    candidate = await _expand_media_url(source)

    # Retry the EXACT requested source before ever substituting a title-search result. Most
    # resolution failures are transient (DNS / rate-limit / a YouTube AAC stream hiccup), and a
    # title-search fallback can return a DIFFERENT video that merely shares the title — which is
    # exactly the "playing a whole different track that's not in the playlist" symptom. Only
    # substitute as a genuine last resort, and log it loudly so it's never silent.
    is_direct = _is_direct_media_url(candidate)
    direct_tries = RESOLVE_DIRECT_RETRIES if is_direct else 1
    last_error = None
    for _try in range(direct_tries):
        try:
            entries, _playlist_result = await search_playables(candidate)
            if entries:
                schedule_audio_cache_download(source)
                return entries[0], candidate
            last_error = ValueError("No stream found.")
        except Exception as exc:
            last_error = exc
        if _try < direct_tries - 1:
            await asyncio.sleep(RESOLVE_DIRECT_RETRY_DELAY)

    title_search = _build_track_title_search(fallback_title)
    if title_search and is_direct:
        try:
            entries, _playlist_result = await search_playables(title_search)
            if entries:
                logger.warning(
                    "[%s] Exact source for '%s' unavailable after %s tries; using title-search alternate "
                    "(may be a different upload than the playlist entry).",
                    BOT_ENV_PREFIX.lower(), fallback_title or source, direct_tries,
                )
                return entries[0], title_search
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

async def set_active_playlist(guild_id, playlist_url, known_track_count, requester_id, channel_id, playlist_entries=None):
    if not playlist_url:
        return
    await init_playlist_db()
    if AUDIO_CACHE_PREDOWNLOAD:
        for _pe in (playlist_entries or []):
            _pe_url = getattr(_pe, 'uri', None) or getattr(_pe, 'url', None)
            schedule_audio_cache_download(_pe_url)
            _pe_cid = _audio_cache_id(_pe_url)
            if _pe_cid:
                _audio_cache_ref_add(_pe_cid)
    snapshot_rows = _playlist_rows_to_snapshot(playlist_entries or [], requester_id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("REPLACE INTO nexus_active_playlists (guild_id, bot_name, playlist_url, known_track_count, requester_id, channel_id) VALUES (%s, 'nexus', %s, %s, %s, %s)", (guild_id, playlist_url, known_track_count, requester_id, channel_id))
                if snapshot_rows:
                    await cur.execute("DELETE FROM nexus_active_playlist_tracks WHERE guild_id = %s AND bot_name = 'nexus'", (guild_id,))
                    await cur.executemany(
                        "INSERT INTO nexus_active_playlist_tracks (guild_id, bot_name, playlist_url, position_idx, track_key, track_uid, video_url, title, requester_id) VALUES (%s, 'nexus', %s, %s, %s, %s, %s, %s, %s)",
                        [(guild_id, playlist_url, idx, key, track_uid, url, title, req) for idx, (url, title, req, key, track_uid) in enumerate(snapshot_rows)],
                    )

async def clear_active_playlist(guild_id):
    await init_playlist_db()
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("DELETE FROM nexus_active_playlists WHERE guild_id = %s AND bot_name = 'nexus'", (guild_id,))
                await cur.execute("DELETE FROM nexus_active_playlist_tracks WHERE guild_id = %s AND bot_name = 'nexus'", (guild_id,))

@tasks.loop(seconds=PLAYLIST_SYNC_INTERVAL)
async def playlist_sync_loop():
    if getattr(playlist_sync_loop, "_sync_active", False): return
    playlist_sync_loop._sync_active = True
    try:
        await init_playlist_db()
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT guild_id, playlist_url, known_track_count, requester_id, channel_id FROM nexus_active_playlists WHERE bot_name = 'nexus'")
                    playlists = await cur.fetchall()

        if not playlists:
            return

        opts = ytdl_format_options.copy()
        opts['noplaylist'] = False
        opts['extract_flat'] = True
        opts['playlistend'] = None
        opts['ignoreerrors'] = True
        loop = asyncio.get_running_loop()
        max_concurrent = max(1, int(os.getenv("PLAYLIST_SYNC_MAX_CONCURRENT", "4")))
        semaphore = asyncio.Semaphore(max_concurrent)

        def _extract_playlist(playlist_url):
            local_opts = opts.copy()
            with yt_dlp.YoutubeDL(local_opts) as ydl:
                return ydl.extract_info(playlist_url, download=False)

        async def _extract_one(row):
            guild_id, url, known_count, req_id, channel_id = row
            if _youtube_api_available():
                playlist_id = _extract_youtube_playlist_id(url)
                if playlist_id:
                    try:
                        api_entries = await _resolve_youtube_playlist_via_api(playlist_id)
                    except Exception:
                        logger.warning("[%s] YouTube Data API playlist sync failed for guild %s, falling back to yt-dlp", BOT_ENV_PREFIX.lower(), guild_id, exc_info=True)
                        api_entries = []
                    if api_entries:
                        return {'entries': api_entries}
            async with semaphore:
                return await asyncio.wait_for(
                    loop.run_in_executor(None, lambda playlist_url=url: _extract_playlist(playlist_url)),
                    timeout=PLAYLIST_SYNC_EXTRACT_TIMEOUT_SECONDS,
                )

        results = await asyncio.gather(*(_extract_one(row) for row in playlists), return_exceptions=True)

        for (guild_id, url, known_count, req_id, channel_id), data in zip(playlists, results):
            try:
                if isinstance(data, Exception):
                    logger.error("[%s] Playlist sync extraction failed for guild %s: %s", BOT_ENV_PREFIX.lower(), guild_id, data)
                    continue
                if not data or 'entries' not in data:
                    continue

                current_rows = _playlist_rows_to_snapshot([e for e in data.get('entries') or [] if e is not None], req_id)
                current_count = len(current_rows)
                known_count = int(known_count or 0)
                if not current_rows:
                    continue

                _psync_delays = [0.05, 0.1]
                for _psync_attempt in range(3):
                    try:
                        async with DBPoolManager() as pool:
                            async with pool.acquire() as conn:
                                async with conn.cursor() as cur:
                                    await prime_loop_queue_defaults(cur, guild_id, shuffle_if_needed=False)
                                    await cur.execute("SELECT track_key, track_uid, video_url, title, requester_id FROM nexus_active_playlist_tracks WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY position_idx ASC", (guild_id,))
                                    previous_rows_raw = await cur.fetchall() or []
                                    previous_rows = []
                                    for prev in previous_rows_raw:
                                        p_key = _row_value(prev, 'track_key', _row_value(prev, 0))
                                        p_uid = _row_track_uid(prev, 1)
                                        p_url = _row_value(prev, 'video_url', _row_value(prev, 2))
                                        p_title = _row_value(prev, 'title', _row_value(prev, 3))
                                        p_req = _row_value(prev, 'requester_id', _row_value(prev, 4))
                                        if not p_key:
                                            p_key = _track_key(p_url, p_title)
                                        previous_rows.append((p_url, p_title, p_req, p_key, p_uid or _new_track_uid()))

                                    if previous_rows and len(previous_rows) >= 10 and current_count < len(previous_rows) * 0.5:
                                            logger.warning(
                                                "[%s] Playlist sync got suspiciously few tracks (%s) vs previous snapshot (%s) for guild %s; likely a transient extraction issue, skipping this cycle.",
                                                BOT_ENV_PREFIX.lower(), current_count, len(previous_rows), guild_id,
                                            )
                                            added_count = 0
                                            purged_live = 0
                                            purged_backup = 0
                                            trim_count = 0
                                            backup_content_trim = 0
                                            break

                                    added_rows = []
                                    removed_counts = {}
                                    if previous_rows:
                                        previous_counts = {}
                                        for _p_url, _p_title, _p_req, p_key, _p_uid in previous_rows:
                                            previous_counts[p_key] = previous_counts.get(p_key, 0) + 1
                                        current_counts = {}
                                        for _c_url, _c_title, _c_req, c_key, _c_uid in current_rows:
                                            current_counts[c_key] = current_counts.get(c_key, 0) + 1
                                        for p_key, p_count in previous_counts.items():
                                            missing = p_count - current_counts.get(p_key, 0)
                                            if missing > 0:
                                                removed_counts[p_key] = missing
                                        remaining_previous = previous_counts.copy()
                                        for row in current_rows:
                                            if not _decrement_count(remaining_previous, row[3]):
                                                added_rows.append(row)
                                    else:
                                        # First run after this patch: seed the identity snapshot.
                                        # If the old count says the playlist grew, queue only the tail delta.
                                        if current_count > known_count > 0:
                                            added_rows = current_rows[known_count:]

                                    purged_live = 0
                                    purged_backup = 0
                                    if removed_counts:
                                        await cur.execute("SELECT id, video_url, title, track_uid FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC", (guild_id,))
                                        live_rows = await cur.fetchall() or []
                                        live_delete_ids = []
                                        live_budget = removed_counts.copy()
                                        for live in live_rows:
                                            live_id = _row_value(live, 'id', _row_value(live, 0))
                                            live_url = _row_value(live, 'video_url', _row_value(live, 1))
                                            live_title = _row_value(live, 'title', _row_value(live, 2))
                                            live_key = _track_key(live_url, live_title)
                                            if _decrement_count(live_budget, live_key):
                                                live_delete_ids.append(live_id)
                                        for live_id in live_delete_ids:
                                            await cur.execute("DELETE FROM nexus_queue WHERE id = %s AND guild_id = %s AND bot_name = 'nexus'", (live_id, guild_id))
                                            purged_live += 1

                                        await cur.execute("SELECT id, video_url, title, track_uid FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC", (guild_id,))
                                        backup_rows = await cur.fetchall() or []
                                        backup_delete_ids = []
                                        backup_budget = removed_counts.copy()
                                        for backup in backup_rows:
                                            backup_id = _row_value(backup, 'id', _row_value(backup, 0))
                                            backup_url = _row_value(backup, 'video_url', _row_value(backup, 1))
                                            backup_title = _row_value(backup, 'title', _row_value(backup, 2))
                                            backup_key = _track_key(backup_url, backup_title)
                                            if _decrement_count(backup_budget, backup_key):
                                                backup_delete_ids.append(backup_id)
                                        for backup_id in backup_delete_ids:
                                            await cur.execute("DELETE FROM nexus_queue_backup WHERE id = %s AND guild_id = %s AND bot_name = 'nexus'", (backup_id, guild_id))
                                            purged_backup += 1

                                    added_count = len(added_rows)
                                    if added_rows:
                                        await cur.executemany(
                                            "INSERT INTO nexus_queue (guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, %s, %s, %s, %s, %s)",
                                            [(guild_id, 'nexus', t_url, t_title, t_req, track_uid) for t_url, t_title, t_req, _key, track_uid in added_rows],
                                        )
                                        try:
                                            await bulk_record_tracks_queued(cur, guild_id, [(u, t, r) for u, t, r, _k, _uid in added_rows])
                                        except Exception as tx_error:
                                            logger.debug("[nexus] Bulk playlist-sync intelligence write skipped.", exc_info=True)
                                        if added_count > 1:
                                            await shuffle_queue_rows(cur, guild_id, preserve_first=True)

                                    # Trim loop-mode duplicate copies of playlist tracks from live queue.
                                    # loop_mode='queue' re-appends played tracks with new UIDs; over time
                                    # the queue grows to N*loops. We keep at most 1 copy per track_key
                                    # that appears in the current playlist, leaving non-playlist tracks alone.
                                    trim_count = 0
                                    if current_rows:
                                        current_playlist_keys = {row[3] for row in current_rows}
                                        await cur.execute(
                                            "SELECT id, video_url, title FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus' ORDER BY id ASC",
                                            (guild_id,)
                                        )
                                        all_queue_rows = await cur.fetchall() or []
                                        seen_by_key = {}
                                        trim_ids = []
                                        for qrow in all_queue_rows:
                                            qid = _row_value(qrow, 'id', _row_value(qrow, 0))
                                            qurl = _row_value(qrow, 'video_url', _row_value(qrow, 1))
                                            qtitle = _row_value(qrow, 'title', _row_value(qrow, 2))
                                            qkey = _track_key(qurl, qtitle)
                                            if qkey in current_playlist_keys:
                                                if qkey in seen_by_key:
                                                    trim_ids.append(qid)
                                                else:
                                                    seen_by_key[qkey] = qid
                                        for trim_id in trim_ids:
                                            await cur.execute("DELETE FROM nexus_queue WHERE id = %s AND guild_id = %s AND bot_name = 'nexus'", (trim_id, guild_id))
                                        trim_count = len(trim_ids)
                                        if trim_count > 0:
                                            logger.info("[nexus] Playlist sync trimmed %s excess loop-mode queue copies for guild %s.", trim_count, guild_id)

                                    # Also purge content-duplicate entries from the backup queue that survived
                                    # UID-based dedup (same song re-added with a different track_uid over time).
                                    backup_content_trim = await dedupe_queue_table_by_track_key(cur, "nexus_queue_backup", guild_id=guild_id, bot_name="nexus")
                                    if backup_content_trim > 0:
                                        logger.info("[nexus] Playlist sync removed %s content-duplicate backup row(s) for guild %s.", backup_content_trim, guild_id)

                                    if added_rows or purged_live or purged_backup or trim_count or backup_content_trim:
                                        await snapshot_queue_backup(cur, guild_id)

                                    await cur.execute("DELETE FROM nexus_active_playlist_tracks WHERE guild_id = %s AND bot_name = 'nexus'", (guild_id,))
                                    await cur.executemany(
                                        "INSERT INTO nexus_active_playlist_tracks (guild_id, bot_name, playlist_url, position_idx, track_key, track_uid, video_url, title, requester_id) VALUES (%s, 'nexus', %s, %s, %s, %s, %s, %s, %s)",
                                        [(guild_id, url, idx, key, track_uid, t_url, t_title, t_req) for idx, (t_url, t_title, t_req, key, track_uid) in enumerate(current_rows)],
                                    )
                                    await cur.execute("UPDATE nexus_active_playlists SET known_track_count = %s WHERE guild_id = %s AND bot_name = 'nexus'", (current_count, guild_id))
                        break
                    except Exception as _psync_exc:
                        if _is_retryable_mysql_error(_psync_exc) and _psync_attempt < 2:
                            logger.warning(
                                "[%s] Playlist sync DB hit retryable error %s for guild %s; retrying (%s/3).",
                                BOT_ENV_PREFIX.lower(), _mysql_error_code(_psync_exc) or type(_psync_exc).__name__,
                                guild_id, _psync_attempt + 1,
                            )
                            await asyncio.sleep(_psync_delays[_psync_attempt])
                            continue
                        raise

                if added_count or purged_live or purged_backup or trim_count or backup_content_trim:
                    guild = bot.get_guild(int(guild_id))
                    if guild:
                        vc = guild.voice_client
                        if added_count > 0 and (not vc or (not _player_is_active(vc))):
                            target_channel = vc.channel if vc and getattr(vc, 'channel', None) else guild.get_channel(channel_id) if channel_id else await get_home_channel(guild)
                            if target_channel:
                                schedule_named_task(f"playlist_sync_process_queue:{guild.id}", process_queue(guild, target_channel.id))
                        await send_feedback_notice(
                            guild,
                            "📡 Playlist Synced",
                            f"Added **{added_count}** new, removed **{purged_live}** stale live row(s), cleaned **{purged_backup}** backup row(s), trimmed **{trim_count}** loop duplicate(s).",
                            discord.Color.green(),
                            dedupe_key=f"playlist_sync:{added_count}:{purged_live}:{purged_backup}:{trim_count}",
                        )
                        await send_webhook_log(bot.user.name if bot.user else "Unknown Node", "📡 Playlist Sync", f"Guild {guild.name}: +{added_count} playlist track(s), -{purged_live} live row(s), -{purged_backup} backup row(s), -{trim_count} loop duplicates.", discord.Color.green())
            except Exception as e:
                logger.error("[%s] Playlist sync loop failed for guild %s: %s", BOT_ENV_PREFIX.lower(), guild_id, e, exc_info=True)
    except Exception as tx_error:
        logger.exception("[%s] Playlist sync task failed before guild processing.", BOT_ENV_PREFIX.lower())
    finally:
        playlist_sync_loop._sync_active = False

@bot.event
async def on_ready_sync():
    await init_playlist_db()
    try:
        if not playlist_sync_loop.is_running(): playlist_sync_loop.start()
    except RuntimeError: pass
bot.add_listener(on_ready_sync, 'on_ready')

# --- ARIA SWARM OVERRIDE LISTENER ---
@tasks.loop(seconds=DIRECT_ORDER_POLL_INTERVAL_SECONDS)
async def aria_command_listener():
    try:
        if not await ensure_swarm_command_tables():
            return
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
                async with DBPoolManager() as _restart_pool:
                    async with _restart_pool.acquire() as _restart_conn:
                        async with _restart_conn.cursor() as _restart_cur:
                            await _restart_cur.execute("DELETE FROM nexus_swarm_overrides WHERE guild_id = %s AND bot_name = %s", (guild_id, 'nexus'))
                await request_supervisor_restart("aria_override")
                continue

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
                                        apply_filter_preset(wav_filters, f_mode)
                                        try: await replace_audio_filters(vc, wav_filters)
                                        except Exception as tx_error: pass
                    executed = True

                if executed:
                    try: await send_webhook_log(bot.user.name if bot.user else "Unknown Node", "🤖 Aria Override", f"Aria forcefully executed a **{cmd}** command in `{guild.name}`.", discord.Color.purple())
                    except Exception as tx_error:
                        logger.exception("[nexus] Failed sending Aria override webhook for guild %s.", guild_id)
                else:
                    logger.info("[nexus] Ignored Aria override %s for guild %s because the player state did not match.", cmd, guild_id)
            else:
                logger.warning("[nexus] Received Aria override %s for unknown guild %s.", cmd, guild_id)
            async with DBPoolManager() as _cleanup_pool:
                async with _cleanup_pool.acquire() as _cleanup_conn:
                    async with _cleanup_conn.cursor() as _cleanup_cur:
                        if executed or attempts + 1 >= DIRECT_ORDER_MAX_ATTEMPTS or not guild:
                            await _cleanup_cur.execute("DELETE FROM nexus_swarm_overrides WHERE guild_id = %s AND bot_name = %s", (guild_id, 'nexus'))
                        else:
                            await _cleanup_cur.execute("UPDATE nexus_swarm_overrides SET attempts = COALESCE(attempts, 0) + 1, last_error = %s WHERE guild_id = %s AND bot_name = %s", (f"state_mismatch:{cmd}", guild_id, 'nexus'))
    except Exception as tx_error:
        logger.exception("Aria override listener failed for nexus.")

@bot.event
async def on_ready_aria_listener():
    try:
        if not aria_command_listener.is_running(): aria_command_listener.start()
    except RuntimeError: pass
bot.add_listener(on_ready_aria_listener, 'on_ready')


async def ensure_swarm_command_tables():
    global _last_swarm_bridge_db_error_log_at
    global swarm_command_tables_ready
    global swarm_command_tables_retry_after

    if swarm_command_tables_ready:
        return True

    now = time.monotonic()
    if swarm_command_tables_retry_after and now < swarm_command_tables_retry_after:
        return False

    async with swarm_command_tables_lock:
        if swarm_command_tables_ready:
            return True
        try:
            async with DBPoolManager() as pool:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("CREATE TABLE IF NOT EXISTS nexus_swarm_overrides (guild_id BIGINT, bot_name VARCHAR(50), command VARCHAR(20), PRIMARY KEY(guild_id, bot_name))")
                        await cur.execute("CREATE TABLE IF NOT EXISTS nexus_swarm_direct_orders (id INT AUTO_INCREMENT PRIMARY KEY, bot_name VARCHAR(50), guild_id BIGINT, vc_id BIGINT NULL, text_channel_id BIGINT NULL, command VARCHAR(50), data TEXT NULL, attempts INT NOT NULL DEFAULT 0, last_error TEXT NULL, claimed_at TIMESTAMP NULL, claim_token VARCHAR(128) NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
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
                            "ALTER TABLE nexus_swarm_direct_orders ADD COLUMN claimed_at TIMESTAMP NULL",
                            "ALTER TABLE nexus_swarm_direct_orders ADD COLUMN claim_token VARCHAR(128) NULL",
                            "ALTER TABLE nexus_swarm_direct_orders ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                            "ALTER TABLE nexus_swarm_direct_orders ADD INDEX nexus_direct_unclaimed_idx (bot_name, guild_id, claimed_at, id)",
                            "ALTER TABLE nexus_swarm_direct_orders ADD INDEX nexus_direct_claim_token_idx (claim_token)",
                            "ALTER TABLE nexus_swarm_direct_orders ADD INDEX nexus_direct_recent_idx (bot_name, guild_id, command, created_at)",
                        ]:
                            try:
                                await cur.execute(stmt)
                            except Exception as tx_error:
                                pass
            swarm_command_tables_ready = True
            swarm_command_tables_retry_after = 0.0
            return True
        except Exception as exc:
            now = time.monotonic()
            swarm_command_tables_retry_after = now + SWARM_COMMAND_TABLES_RECHECK_SECONDS
            if now - _last_swarm_bridge_db_error_log_at >= SWARM_BRIDGE_DB_ERROR_LOG_INTERVAL_SECONDS:
                _last_swarm_bridge_db_error_log_at = now
                logger.warning("Swarm command tables unavailable for nexus; bridge listeners will retry: %s", exc)
            return False

# --- AUTOMATED BACKGROUND MAINTENANCE ---
@tasks.loop(seconds=15.0)
async def resilience_loop():
    now = time.monotonic()
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    active_guild_ids = {_runtime_key(key) for key in playback_tracking.keys()} | {_runtime_key(key) for key in guild_states.keys()}
                    if not active_guild_ids:
                        active_guild_ids = {_runtime_key(guild.id) for guild in bot.guilds if guild.voice_client}
                    for raw_guild_id in active_guild_ids:
                        _p_lock = get_process_queue_lock(_runtime_key(raw_guild_id))
                        if _p_lock.locked(): continue
                        try:
                            guild_id = int(_runtime_key(raw_guild_id))
                        except Exception as tx_error:
                            continue
                        guild = bot.get_guild(guild_id)
                        if guild is None:
                            continue
                        vc = guild.voice_client
                        is_active = bool(vc and (_player_is_active(vc)))

                        await cur.execute("SELECT COUNT(*) FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus'", (guild.id,))
                        queue_count_row = await cur.fetchone()
                        queue_count = queue_count_row[0] if queue_count_row else 0

                        if queue_count > 0:
                            clear_idle_restore_state(guild.id)

                        if is_active:
                            continue

                        # FIX: Queue items exist but the player is dead — re-trigger playback.
                        # Previously this branch was silently skipped, causing tracks to accumulate
                        # in nexus_queue forever without being consumed (the "queue leak").
                        if queue_count > 0:
                            stuck_channel_id = (
                                guild_states.get(guild.id, {}).get("voice_channel_id")
                                or (vc.channel.id if vc and getattr(vc, "channel", None) else None)
                            )
                            if stuck_channel_id and aria_recovery_authority_blocks_self_heal("resilience_stuck_queue", guild.id):
                                continue
                            if stuck_channel_id and not should_defer_automatic_queue_recovery(guild.id):
                                process_task = startup_task_registry.get(f"resilience_stuck_queue:{guild.id}") or startup_task_registry.get(f"process_queue:{guild.id}")
                                if process_task and not process_task.done():
                                    continue
                                next_allowed = resilience_queue_retry_after.get(guild.id, 0)
                                if now < next_allowed:
                                    continue
                                resilience_queue_retry_after[guild.id] = now + RESILIENCE_STUCK_QUEUE_RETRY_SECONDS
                                logger.warning(
                                    "[%s] Resilience loop: %s queued track(s) found with no active player; re-triggering process_queue.",
                                    guild.id, queue_count,
                                )
                                schedule_named_task(
                                    f"resilience_stuck_queue:{guild.id}",
                                    process_queue(guild, stuck_channel_id, allow_recovery_restore=True),
                                )
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

                        restore_position = normalize_position_seconds(playback_row[2] if playback_row else 0)
                        await remember_recovery_state(guild.id, target_channel_id, restore_position)
                        clear_idle_restore_state(guild.id)
                        scheduled = schedule_recovery_retry(guild.id, target_channel_id, start_position=restore_position, reason="idle_restore")
                        if scheduled:
                            logger.info(f"[{guild.id}] Restored {restored} backup tracks after idle/home recovery at {restore_position}s.")
                            schedule_named_task(f"idle_restore_process_queue:{guild.id}", process_queue(guild, target_channel_id, start_position=restore_position, allow_recovery_restore=True))
                        else:
                            logger.info(f"[{guild.id}] Idle/home voice rejoin recovery is disabled; preserved {restored} backup tracks without forcing a rejoin.")
    except Exception as e:
        logger.error(f"Resilience Loop Error: {e}")

@tasks.loop(minutes=5.0)
async def zombie_reaper_loop():
    if not VOICE_IDLE_REJOIN_RECOVERY:
        logger.debug(f"[{BOT_ENV_PREFIX.lower()}] Zombie reaper voice cleanup is disabled while automatic voice recovery is off.")
        return
    for guild in list(bot.guilds):
        try:
            vc = guild.voice_client
            should_inspect = bool(vc and not _player_is_active(vc))
        except Exception as tx_error:
            logger.exception("[nexus] Zombie reaper failed to inspect voice state for guild %s.", getattr(guild, "id", "?"))
            continue
        if should_inspect:
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
                                        restore_position = normalize_position_seconds(playback_row[2] if playback_row else 0)
                                        await remember_recovery_state(guild.id, recovery_channel_id, restore_position)
                                        restored = await restore_queue_from_backup(cur, guild.id)
                                        if restored > 0:
                                            clear_idle_restore_state(guild.id)
                                            scheduled = schedule_recovery_retry(guild.id, recovery_channel_id, start_position=restore_position, reason="zombie_restore")
                                            if scheduled:
                                                schedule_named_task(f"zombie_restore_process_queue:{guild.id}", process_queue(guild, recovery_channel_id, start_position=restore_position, allow_recovery_restore=True))
                                            else:
                                                logger.info(f"[{guild.id}] Zombie voice rejoin recovery is disabled; preserved backup tracks without forcing a rejoin.")
                                    else:
                                        await cur.execute("DELETE FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus'", (guild.id,))
                                    playback_tracking.pop(guild.id, None)
                                    await cur.execute("UPDATE nexus_playback_state SET channel_id = NULL, video_url = NULL, title = NULL, position_seconds = 0, is_playing = FALSE, is_paused = FALSE, play_session_key = NULL, track_uid = NULL WHERE guild_id = %s AND bot_name = 'nexus'", (guild.id,))
                                    continue
                                await cur.execute("SELECT stay_in_vc FROM nexus_guild_settings WHERE guild_id = %s", (guild.id,))
                                cfg = await cur.fetchone()
                                stay_in_vc = bool(cfg[0]) if cfg else False
                                if _should_auto_disconnect(guild, stay_in_vc):
                                    await stop_playback(guild)
                                else:
                                    playback_tracking.pop(guild.id, None)
                                    await cur.execute("UPDATE nexus_playback_state SET channel_id = NULL, video_url = NULL, title = NULL, position_seconds = 0, is_playing = FALSE, is_paused = FALSE, play_session_key = NULL, track_uid = NULL WHERE guild_id = %s AND bot_name = 'nexus'", (guild.id,))
            except Exception as tx_error:
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
                    await cur.execute(
                        "DELETE FROM nexus_smart_recommendations WHERE created_at < NOW() - INTERVAL 90 DAY"
                    )
                    await cur.execute(
                        "DELETE FROM nexus_error_events WHERE created_at < NOW() - INTERVAL 30 DAY"
                    )
                    await cur.execute(
                        "DELETE FROM nexus_track_intelligence WHERE last_played < NOW() - INTERVAL 365 DAY AND play_count <= 1 AND finish_count = 0"
                    )
                    await cur.execute(
                        "DELETE FROM nexus_user_track_affinity WHERE last_requested < NOW() - INTERVAL 365 DAY AND play_count <= 1"
                    )
                    await cur.execute(
                        "DELETE FROM nexus_track_cooccurrence WHERE updated_at < NOW() - INTERVAL 180 DAY"
                    )
                    await cur.execute(
                        "DELETE te FROM nexus_track_embeddings te LEFT JOIN nexus_track_intelligence ti ON ti.url_key = te.url_key "
                        "WHERE ti.url_key IS NULL AND te.updated_at < NOW() - INTERVAL 365 DAY"
                    )
    except Exception as e:
        logger.error(f"Janitor Error: {e}")

@tasks.loop(minutes=30.0)
async def queue_shuffle_maintenance_loop():
    if not bot.is_ready():
        return
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    for guild in list(bot.guilds):
                        vc = guild.voice_client
                        # Skip guilds with nobody around to hear it — no point
                        # rewriting the queue order for an idle/empty channel.
                        if not vc or not _voice_client_connected(vc) or not _has_human_listeners(vc):
                            continue
                        await cur.execute("SELECT loop_mode FROM nexus_guild_settings WHERE guild_id = %s", (guild.id,))
                        row = await cur.fetchone()
                        loop_mode = row[0] if row and row[0] else 'queue'
                        if loop_mode != 'queue':
                            continue
                        queue_len = await shuffle_queue_rows(cur, guild.id, preserve_first=True)
                        if queue_len > 1:
                            logger.info(f"[{guild.id}] Scheduled 30-minute queue reshuffle touched {queue_len} queued tracks.")
    except Exception as exc:
        logger.warning("[nexus] Scheduled queue reshuffle failed: %s", exc)

@queue_shuffle_maintenance_loop.before_loop
async def before_queue_shuffle_maintenance_loop():
    await bot.wait_until_ready()
    await asyncio.sleep(30 * 60)


@tasks.loop(hours=CACHE_CLEANUP_INTERVAL_HOURS)
async def cache_cleanup_loop():
    if not bot.is_ready():
        return
    # NOTE: must be wrapped — an uncaught exception here (e.g. a filesystem error
    # from _clear_directory_contents, which has no internal try/except around
    # os.makedirs/os.scandir) would silently stop this @tasks.loop forever, just
    # like the other maintenance loops guard themselves (database_janitor_loop,
    # queue_shuffle_maintenance_loop, playlist_sync_loop, etc).
    try:
        await clear_local_cache_systems("scheduled_10h")
    except Exception as tx_error:
        logger.exception("[%s] Cache cleanup loop failed.", BOT_ENV_PREFIX.lower())


@cache_cleanup_loop.before_loop
async def before_cache_cleanup_loop():
    await bot.wait_until_ready()
    if CACHE_CLEANUP_INITIAL_SPREAD_SECONDS > 0:
        spread_window = max(1, int(CACHE_CLEANUP_INITIAL_SPREAD_SECONDS))
        deterministic_spread = sum(ord(ch) for ch in BOT_ENV_PREFIX) % spread_window
        await asyncio.sleep(deterministic_spread)


@tasks.loop(hours=max(1.0, PERIODIC_RESTART_HOURS))
async def periodic_restart_loop():
    if PERIODIC_RESTART_HOURS <= 0:
        return
    await request_supervisor_restart(f"periodic_{PERIODIC_RESTART_HOURS:g}h_cycle")


@periodic_restart_loop.before_loop
async def before_periodic_restart_loop():
    await bot.wait_until_ready()
    if PERIODIC_RESTART_HOURS > 0:
        if PERIODIC_RESTART_JITTER_SECONDS > 0:
            restart_jitter = sum(ord(ch) for ch in BOT_ENV_PREFIX) % int(PERIODIC_RESTART_JITTER_SECONDS)
            await asyncio.sleep(restart_jitter)
        await asyncio.sleep(PERIODIC_RESTART_HOURS * 60 * 60)

@bot.event
async def on_ready_maintenance():
    try:
        if not resilience_loop.is_running(): resilience_loop.start()
    except RuntimeError: pass
    try:
        if not zombie_reaper_loop.is_running(): zombie_reaper_loop.start()
    except RuntimeError: pass
    try:
        if not database_janitor_loop.is_running(): database_janitor_loop.start()
    except RuntimeError: pass
    try:
        if not queue_shuffle_maintenance_loop.is_running(): queue_shuffle_maintenance_loop.start()
    except RuntimeError: pass
    try:
        if not cache_cleanup_loop.is_running(): cache_cleanup_loop.start()
    except RuntimeError: pass
    try:
        if PERIODIC_RESTART_HOURS > 0 and not periodic_restart_loop.is_running(): periodic_restart_loop.start()
    except RuntimeError: pass
bot.add_listener(on_ready_maintenance, 'on_ready')

# --- ARIA DIRECT DRONE CONTROL ---
@tasks.loop(seconds=DIRECT_ORDER_POLL_INTERVAL_SECONDS)
async def direct_order_listener():
    orders = []
    try:
        if not await ensure_swarm_command_tables():
            return
        current_claim_token = f"{BOT_ENV_PREFIX.lower()}:{os.getpid()}:{time.time_ns()}:{random.randint(100000, 999999)}"
        for claim_attempt in range(DIRECT_ORDER_DB_RETRY_ATTEMPTS):
            try:
                async with DBPoolManager() as pool:
                    async with pool.acquire() as conn:
                        async with conn.cursor(aiomysql.DictCursor) as cur:
                            # Claim with one ordered UPDATE instead of SELECT ... FOR UPDATE to reduce deadlocks with Aria inserts.
                            await cur.execute("DELETE FROM nexus_swarm_direct_orders WHERE bot_name = %s AND created_at < NOW() - INTERVAL %s SECOND", ('nexus', DIRECT_ORDER_STALE_SECONDS))
                            await cur.execute(
                                """
                                UPDATE nexus_swarm_direct_orders
                                SET claimed_at = NOW(), claim_token = %s
                                WHERE bot_name = %s
                                  AND COALESCE(attempts, 0) < %s
                                  AND (claimed_at IS NULL OR claimed_at <= DATE_SUB(NOW(), INTERVAL %s SECOND))
                                ORDER BY id ASC
                                LIMIT %s
                                """,
                                (current_claim_token, 'nexus', DIRECT_ORDER_MAX_ATTEMPTS, DIRECT_ORDER_CLAIM_TIMEOUT_SECONDS, DIRECT_ORDER_FETCH_LIMIT),
                            )
                            if int(getattr(cur, "rowcount", 0) or 0) > 0:
                                await cur.execute(
                                    """
                                    SELECT id, bot_name, guild_id, vc_id, text_channel_id, command, data, COALESCE(attempts, 0) AS attempts
                                    FROM nexus_swarm_direct_orders
                                    WHERE claim_token = %s
                                    ORDER BY id ASC
                                    LIMIT %s
                                    """,
                                    (current_claim_token, DIRECT_ORDER_FETCH_LIMIT),
                                )
                                orders = await cur.fetchall()
                break
            except Exception as exc:
                if _is_retryable_mysql_error(exc) and claim_attempt + 1 < DIRECT_ORDER_DB_RETRY_ATTEMPTS:
                    logger.warning(
                        "[%s] Direct order claim hit retryable DB error %s; retrying (%s/%s).",
                        BOT_ENV_PREFIX.lower(),
                        _mysql_error_code(exc) or type(exc).__name__,
                        claim_attempt + 1,
                        DIRECT_ORDER_DB_RETRY_ATTEMPTS,
                    )
                    await asyncio.sleep(DIRECT_ORDER_DB_RETRY_BASE_DELAY_SECONDS * (claim_attempt + 1) + random.uniform(0.0, 0.25))
                    continue
                raise

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
                        await ensure_voice_connection(guild, vc_target.id, allow_stale_rejoin=True)
                        try:
                            entries, playlist_result = await search_playables(data)
                            if entries:
                                is_playlist_request = bool(playlist_result) or (isinstance(data, str) and 'list=' in data and len(entries) > 1)
                                playlist_url = resolve_playlist_source(data, playlist_result) if is_playlist_request else None
                                added_count = 0
                                async with DBPoolManager() as pool:
                                    async with pool.acquire() as conn:
                                        async with conn.cursor() as cur:
                                            await prime_loop_queue_defaults(cur, guild.id)
                                            for track in entries:
                                                await enqueue_track(cur, guild.id, track.uri, track.title, bot.user.id)
                                                added_count += 1
                                            if added_count > 1:
                                                await shuffle_queue_rows(cur, guild.id, preserve_first=True)
                                if playlist_url:
                                    await set_active_playlist(guild.id, playlist_url, len(entries), bot.user.id if bot.user else None, vc_target.id)

                                if added_count > 0:
                                    executed = True
                                    try:
                                        await send_feedback(guild, discord.Embed(title="🎶 Direct Order Received", description=f"Aria successfully deposited **{added_count}** tracks into my matrix. Booting audio engine...", color=discord.Color.green()))
                                        await send_webhook_log(bot.user.name if bot.user else "Unknown Node", "📥 Matrix Loaded", f"Aria routed a payload of **{added_count}** tracks directly into `{guild.name}`.", discord.Color.blue())
                                    except Exception as tx_error:
                                        pass
                        except Exception as e:
                            logger.error(f"Direct Play Extractor Error: {e}")

                        current_vc = guild.voice_client
                        if executed and (not current_vc or not _player_is_active(current_vc)):
                            schedule_named_task(f"direct_play_process_queue:{guild.id}", process_queue(guild, vc_target.id, allow_recovery_restore=True))
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
                    # RECOVER is now considered an Aria/manual doctoring order.
                    # Bot-side automatic voice-disconnect recovery can stay disabled,
                    # but explicit Aria recovery orders must still be obeyed.
                    state = await derive_recovery_state_from_db(guild.id)
                    recover_channel_id = order.get('vc_id') or (state or {}).get('voice_channel_id')
                    if recover_channel_id:
                        recover_state = dict(state or {})
                        recover_state['voice_channel_id'] = recover_channel_id
                        recover_state['position'] = int((state or {}).get('position', 0) or 0)
                        clear_recovery_backoff(guild.id)
                        clear_auto_restore_snooze(guild.id)
                        await restore_guild_state(guild.id, recover_state, override_backoff=True)
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


                elif cmd == 'SEEK':
                    vc = guild.voice_client
                    if vc and (_player_is_playing(vc) or _player_is_paused(vc)):
                        try:
                            position_seconds = max(0, int(float(str(data or '0').strip() or 0)))
                            await vc.seek(position_seconds * 1000)
                            await reset_runtime_position_after_seek(guild.id, position_seconds)
                            executed = True
                        except Exception as _seek_err:
                            logger.warning("[nexus] Direct SEEK failed in guild %s at %ss: %s", guild.id, position_seconds if 'position_seconds' in dir() else '?', _seek_err)
                    else:
                        logger.info("[nexus] Direct SEEK ignored — no active player in guild %s.", guild.id)

                else:
                    logger.warning(f"[nexus] Unsupported direct order {cmd!r} for guild {guild.id}.")

                if executed:
                    try:
                        details = f" command=`{cmd}`"
                        if cmd == 'RECOVER' and data:
                            details += f" | payload={str(data)[:160]}"
                        await send_webhook_log(bot.user.name if bot.user else "Unknown Node", "🤖 Direct Drone Execution", f"Received and executed direct `{cmd}` order from Aria in `{guild.name}`.{details}", discord.Color.purple())
                    except Exception as tx_error:
                        logger.exception("[nexus] Failed sending direct order webhook for guild %s.", guild.id)
                else:
                    logger.info("[nexus] Direct order %s in guild %s completed without state changes.", cmd, guild.id)
            else:
                logger.warning("[nexus] Received direct order %s for unknown guild %s.", cmd, order['guild_id'])

            for finalize_attempt in range(DIRECT_ORDER_DB_RETRY_ATTEMPTS):
                try:
                    async with DBPoolManager() as _fin_pool:
                        async with _fin_pool.acquire() as _fin_conn:
                            async with _fin_conn.cursor() as _fin_cur:
                                try:
                                    await _fin_cur.execute("START TRANSACTION")
                                    if executed or attempts + 1 >= DIRECT_ORDER_MAX_ATTEMPTS or not guild:
                                        await _fin_cur.execute("DELETE FROM nexus_swarm_direct_orders WHERE id = %s", (oid,))
                                    else:
                                        await _fin_cur.execute("UPDATE nexus_swarm_direct_orders SET attempts = COALESCE(attempts, 0) + 1, last_error = %s, claimed_at = DATE_SUB(NOW(), INTERVAL %s SECOND), claim_token = NULL WHERE id = %s", (f"unexecuted:{cmd}", DIRECT_ORDER_RETRY_BACKDATE_SECONDS, oid))
                                    await _fin_cur.execute("COMMIT")
                                except Exception as tx_error:
                                    try:
                                        await _fin_cur.execute("ROLLBACK")
                                    except Exception:
                                        pass
                                    raise
                    break
                except Exception as exc:
                    if _is_retryable_mysql_error(exc) and finalize_attempt + 1 < DIRECT_ORDER_DB_RETRY_ATTEMPTS:
                        logger.warning(
                            "[%s] Direct order finalize hit retryable DB error %s for order %s; retrying (%s/%s).",
                            BOT_ENV_PREFIX.lower(),
                            _mysql_error_code(exc) or type(exc).__name__,
                            oid,
                            finalize_attempt + 1,
                            DIRECT_ORDER_DB_RETRY_ATTEMPTS,
                        )
                        await asyncio.sleep(DIRECT_ORDER_DB_RETRY_BASE_DELAY_SECONDS * (finalize_attempt + 1) + random.uniform(0.0, 0.25))
                        continue
                    raise

    except Exception as tx_error:
        logger.exception("Direct order listener failed for nexus.")

@bot.event
async def on_ready_direct_order():
    try:
        if not direct_order_listener.is_running(): direct_order_listener.start()
    except RuntimeError: pass
bot.add_listener(on_ready_direct_order, 'on_ready')

# --- SWARM INTELLIGENCE MODULE ---
class SwarmIntelligence(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.bot_name = 'nexus'
        self._last_presence_state = None
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
                    async with DBPoolManager() as _pool:
                        async with _pool.acquire() as _conn:
                            async with _conn.cursor(aiomysql.DictCursor) as cur:
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
                except Exception as tx_error:
                    started = 0
                if time.time() - started < 120:
                    reconnect_candidate = (str(tracked["title"]).replace("\n", " ").strip(), guild.name)
        return reconnect_candidate or (None, None)

    @tasks.loop(seconds=15)
    async def status_updater(self):
        try:
            title, _guild_name = await self._best_presence_title()
            if title:
                activity_type = discord.ActivityType.listening
                activity_name = str(title).replace("\n", " ").strip()[:120]
            else:
                activity_type = discord.ActivityType.watching
                activity_name = "the Swarm | Idle"
            presence_state = (activity_type, activity_name)
            if self._last_presence_state == presence_state:
                return
            await self.bot.change_presence(status=discord.Status.online, activity=discord.Activity(type=activity_type, name=activity_name))
            self._last_presence_state = presence_state
        except (aiohttp.ClientConnectionResetError, ConnectionResetError):
            logger.debug("[%s] Presence update skipped while the gateway transport was closing.", self.bot_name)
        except Exception as tx_error:
            logger.exception("[%s] Status updater failed.", self.bot_name)

    @tasks.loop(seconds=30)
    async def heartbeat(self):
        try:
            async with DBPoolManager() as pool:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("INSERT INTO swarm_health (bot_name, status, last_pulse) VALUES (%s, 'HEALTHY', NOW()) ON DUPLICATE KEY UPDATE status=VALUES(status), last_pulse=NOW()", (self.bot_name,))
        except Exception as tx_error:
            logger.exception("[%s] Heartbeat update failed.", self.bot_name)

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
                        now = time.monotonic()
                        if now - track_info.get('start_time', 0) > 10:
                            if now - track_info.get('last_watchdog_revival', 0) < WATCHDOG_REVIVAL_COOLDOWN:
                                continue
                            async with DBPoolManager() as pool:
                                async with pool.acquire() as conn:
                                    async with conn.cursor() as cur:
                                        revival_attempts = track_info.get('watchdog_revival_attempts', 0)
                                        if revival_attempts >= WATCHDOG_MAX_REVIVALS:
                                            playback_tracking.pop(guild.id, None)
                                            await cur.execute(f"UPDATE {self.bot_name}_playback_state SET is_playing = FALSE WHERE guild_id = %s AND bot_name = %s", (guild.id, self.bot_name))
                                            await send_webhook_log(self.bot.user.name if self.bot.user else "Unknown Node", "⚙️ Watchdog Cooldown", f"Stall persisted in `{guild.name}`; watchdog parked to prevent revival loop.", discord.Color.orange())
                                            continue
                                        current_pos = current_track_position(guild.id)
                                        for _wd_attempt in range(DIRECT_ORDER_DB_RETRY_ATTEMPTS):
                                            try:
                                                await requeue_failed_track(
                                                    cur,
                                                    guild.id,
                                                    track_info.get('channel_id'),
                                                    track_info.get('url', ''),
                                                    track_info.get('title', 'Recovered Track'),
                                                    track_info.get('requester_id', self.bot.user.id if self.bot.user else None),
                                                    position=current_pos,
                                                    reason="watchdog_stall",
                                                    track_uid=track_info.get("track_uid"),
                                                )
                                                break
                                            except Exception as _wd_exc:
                                                if _is_retryable_mysql_error(_wd_exc) and _wd_attempt + 1 < DIRECT_ORDER_DB_RETRY_ATTEMPTS:
                                                    logger.warning(
                                                        "[%s] Watchdog requeue hit retryable DB error %s; retrying (%s/%s).",
                                                        BOT_ENV_PREFIX.lower(),
                                                        _mysql_error_code(_wd_exc) or type(_wd_exc).__name__,
                                                        _wd_attempt + 1,
                                                        DIRECT_ORDER_DB_RETRY_ATTEMPTS,
                                                    )
                                                    await asyncio.sleep(DIRECT_ORDER_DB_RETRY_BASE_DELAY_SECONDS * (_wd_attempt + 1) + random.uniform(0.0, 0.25))
                                                    continue
                                                raise
                                        track_info['watchdog_revival_attempts'] = revival_attempts + 1
                                        track_info['last_watchdog_revival'] = now
                                        track_info['start_time'] = now
                                        track_info['offset'] = current_pos
                                        await send_webhook_log(self.bot.user.name if self.bot.user else "Unknown Node", "⚙️ Watchdog Revival", f"Detected playback stall in `{guild.name}`. Recovering track safely at {current_pos}s.", discord.Color.orange())
                                        schedule_named_task(f"watchdog_recovery_process_queue:{guild.id}", process_queue(guild, track_info.get('channel_id'), start_position=current_pos))
        except Exception as tx_error:
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

@tasks.loop(seconds=30.0)
async def queue_integrity_check_loop():
    """
    Background check: find tracks that were lost from the active queue — i.e. a track
    was dequeued (DELETE FROM nexus_queue) at play-start but the bot crashed or
    disconnected before it finished, so it vanished from both the live queue and the
    player, yet the backup still has a copy.

    For each guild where nexus_playback_state reports a track was playing but
    nexus_queue is now empty and the player is not active, this loop locates that
    specific track in nexus_queue_backup and re-inserts it at the front of the live
    queue so playback can resume cleanly.
    """
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    # Repair partial queue drift before the narrower empty-queue resurrection pass.
                    await cur.execute(
                        """
                        SELECT guild_id FROM nexus_queue WHERE bot_name = %s
                        UNION SELECT guild_id FROM nexus_queue_backup WHERE bot_name = %s
                        UNION SELECT guild_id FROM nexus_playback_state WHERE bot_name = %s
                        """,
                        ("nexus", "nexus", "nexus"),
                    )
                    parity_rows = await cur.fetchall()
                    for parity_row in parity_rows:
                        guild_id = int(_row_value(parity_row, "guild_id", _row_value(parity_row, 0)))
                        if get_process_queue_lock(guild_id).locked(): continue
                        guild = bot.get_guild(guild_id)
                        if not guild:
                            continue
                        vc = guild.voice_client
                        player_active = bool(vc and _player_is_active(vc))
                        if not player_active and should_defer_automatic_queue_recovery(guild_id):
                            continue
                        # Fast-path: skip the expensive per-row parity scan when queue health is nominal.
                        # "nominal" means live == backup (perfect parity) or live is exactly 1 less than
                        # backup while the player is actively playing (one track dequeued for playback).
                        # Only escalate to full repair when there is a genuine count discrepancy.
                        await cur.execute(
                            "SELECT "
                            "(SELECT COUNT(*) FROM nexus_queue WHERE guild_id = %s AND bot_name = 'nexus') AS live_cnt, "
                            "(SELECT COUNT(*) FROM nexus_queue_backup WHERE guild_id = %s AND bot_name = 'nexus') AS backup_cnt",
                            (guild_id, guild_id),
                        )
                        cnt_row = await cur.fetchone() or {}
                        live_cnt = int(cnt_row.get("live_cnt") or 0)
                        backup_cnt = int(cnt_row.get("backup_cnt") or 0)
                        if live_cnt == backup_cnt:
                            continue  # perfect parity — nothing to repair
                        if player_active and backup_cnt > 0 and backup_cnt - live_cnt == 1:
                            continue  # one track dequeued for active playback — expected
                        restored_live, _restored_backup = await repair_queue_backup_parity(cur, guild_id, active_player=player_active)
                        if restored_live <= 0:
                            continue
                        if player_active:
                            continue
                        if should_defer_automatic_queue_recovery(guild_id):
                            continue
                        await cur.execute(
                            "SELECT channel_id FROM nexus_playback_state WHERE guild_id = %s AND bot_name = %s LIMIT 1",
                            (guild_id, "nexus"),
                        )
                        channel_row = await cur.fetchone()
                        target_channel_id = _row_value(channel_row, "channel_id", _row_value(channel_row, 0))
                        if not target_channel_id:
                            await cur.execute(
                                "SELECT COALESCE(connected_channel_id, last_channel_id) AS channel_id FROM nexus_voice_state WHERE guild_id = %s AND bot_name = %s LIMIT 1",
                                (guild_id, "nexus"),
                            )
                            channel_row = await cur.fetchone()
                            target_channel_id = _row_value(channel_row, "channel_id", _row_value(channel_row, 0))
                        if target_channel_id:
                            if not aria_recovery_authority_blocks_self_heal("queue_parity_process_queue", guild_id):
                                recovering_guilds.add(guild_id)
                            schedule_named_task(
                                f"queue_parity_process_queue:{guild_id}",
                                process_queue(guild, int(target_channel_id), allow_recovery_restore=True),
                            )

                    # Guilds that had an active/paused track in playback_state but whose
                    # live queue is now completely empty.
                    await cur.execute(
                        """
                        SELECT ps.guild_id, ps.video_url, ps.title, ps.channel_id, ps.position_seconds, ps.track_uid
                        FROM nexus_playback_state ps
                        WHERE ps.bot_name = 'nexus'
                          AND ps.video_url IS NOT NULL
                          AND ps.video_url <> ''
                          AND (ps.is_playing = TRUE OR ps.is_paused = TRUE OR ps.position_seconds > 0)
                          AND NOT EXISTS (
                              SELECT 1 FROM nexus_queue q
                              WHERE q.guild_id = ps.guild_id
                                AND q.bot_name = 'nexus'
                              LIMIT 1
                          )
                        """
                    )
                    candidates = await cur.fetchall()

                    for row in candidates:
                        guild_id = int(row["guild_id"])
                        video_url = row.get("video_url") or ""
                        title = row.get("title") or ""
                        db_channel_id = row.get("channel_id")
                        position = int(row.get("position_seconds") or 0)
                        playback_track_uid = _normalize_track_uid(row.get("track_uid"))

                        guild = bot.get_guild(guild_id)
                        if not guild:
                            continue
                        vc = guild.voice_client
                        if vc and _player_is_active(vc):
                            continue  # Already playing — nothing to restore
                        if guild_id in recovering_guilds:
                            continue
                        if recovery_backoff_remaining(guild_id) > 0:
                            continue

                        # Find the specific lost track in backup (match by URL, fall back to title)
                        await cur.execute(
                            "SELECT video_url, title, requester_id, track_uid FROM nexus_queue_backup "
                            "WHERE guild_id = %s AND bot_name = 'nexus' "
                            "  AND (video_url = %s OR (title = %s AND title <> '')) "
                            "ORDER BY id ASC LIMIT 1",
                            (guild_id, video_url, title),
                        )
                        backup_row = await cur.fetchone()
                        if not backup_row:
                            # Track resurrection fallback:
                            # playback_state stores the resolved Lavalink URI, while backup can still
                            # contain the user's raw search/playlist source.  When they do not match,
                            # do not abandon the recovery; rebuild the live queue from the known
                            # playback state so a crashed/dequeued current track is not lost forever.
                            restored_url = video_url
                            restored_title = title or video_url
                            restored_requester = bot.user.id if bot.user else None
                            restored_track_uid = playback_track_uid or _new_track_uid()
                            logger.warning(
                                "[%s] Queue integrity check: '%s' was missing from live queue and "
                                "no backup row matched resolved URI/title; restoring from playback state fallback at position %ss.",
                                guild_id, restored_title, position,
                            )
                        else:
                            restored_url = backup_row.get("video_url") or video_url
                            restored_title = backup_row.get("title") or title
                            restored_requester = backup_row.get("requester_id")
                            restored_track_uid = _row_track_uid(backup_row, 3) or _new_track_uid()
                            logger.warning(
                                "[%s] Queue integrity check: '%s' was lost from active queue "
                                "(dequeued but never finished); restoring from backup at position %ss.",
                                guild_id, restored_title, position,
                            )

                        await insert_queue_front(
                            cur,
                            "nexus_queue",
                            guild_id,
                            "nexus",
                            restored_url,
                            restored_title,
                            restored_requester,
                            restored_track_uid,
                            dedupe_identity=True,
                        )

                        # Resolve the voice channel: prefer DB record, fall back to in-memory state
                        target_channel_id = (
                            db_channel_id
                            or guild_states.get(guild_id, {}).get("voice_channel_id")
                            or (vc.channel.id if vc and getattr(vc, "channel", None) else None)
                        )
                        if target_channel_id:
                            if not aria_recovery_authority_blocks_self_heal("integrity_restore_process_queue", guild_id):
                                recovering_guilds.add(guild_id)
                            schedule_named_task(
                                f"integrity_restore_process_queue:{guild_id}",
                                process_queue(
                                    guild,
                                    target_channel_id,
                                    start_position=position,
                                    allow_recovery_restore=True,
                                ),
                            )
    except Exception as tx_error:
        logger.exception("[nexus] Queue integrity check loop failed.")


@queue_integrity_check_loop.before_loop
async def before_queue_integrity_check_loop():
    await bot.wait_until_ready()


@bot.event
async def on_ready_auto_heal():
    if not auto_heal_loop.is_running():
        auto_heal_loop.start()
    if not queue_integrity_check_loop.is_running():
        queue_integrity_check_loop.start()
bot.add_listener(on_ready_auto_heal, 'on_ready')

@bot.tree.interaction_check
async def global_proximity_shield(interaction: discord.Interaction):
    admin_only = ['sethome', 'setfeedback', 'djrole', 'removedj', 'djmode', '247', 'restart']
    protected = ['play', 'stop', 'pause', 'resume', 'skip', 'join', 'leave', 'playnext', 'shuffle', 'clear', 'skipto', 'move', 'remove', 'seek', 'forward', 'rewind', 'replay']
    owner_allowed = await is_private_owner_user(interaction.user)
    if MUSIC_BOT_PRIVATE_MODE and not owner_allowed:
        raise discord.app_commands.CheckFailure("This music node is private.")
    if owner_allowed:
        return True
    if not interaction.command: return True
    if any(interaction.command.name.endswith(s) for s in admin_only) and not interaction.user.guild_permissions.administrator:
        raise discord.app_commands.CheckFailure("You need administrator permission to use this command.")
    if not any(interaction.command.name.endswith(s) for s in protected): return True
    vc = interaction.guild.voice_client if interaction.guild else None
    if not vc: return True
    if getattr(vc, 'channel', None) and (not interaction.user.voice or interaction.user.voice.channel != vc.channel):
        raise discord.app_commands.CheckFailure("You must be in the active voice channel to issue commands.")
    return True


@bot.check
async def global_private_text_command_check(ctx):
    if MUSIC_BOT_PRIVATE_MODE and not await is_private_owner_user(getattr(ctx, "author", None)):
        raise commands.CheckFailure("This music node is private.")
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
    except Exception as tx_error:
        pass
bot.add_listener(on_ready_error_reporting, 'on_ready')


def main():
    validate_runtime_config()
    install_error_reporting()
    startup_delay = compute_login_startup_delay()
    if startup_delay > 0:
        logger.info(f"[{BOT_ENV_PREFIX.lower()}] Deterministic login stagger active; waiting {startup_delay:.1f}s before Discord login.")
        time.sleep(startup_delay)
    _wait_for_global_discord_login_gate()
    try:
        bot.run(TOKEN)
    except KeyboardInterrupt:
        raise
    except Exception as exc:
        delay = compute_login_failure_delay(exc)
        logger.exception(f"[{BOT_ENV_PREFIX.lower()}] Discord client stopped during startup/runtime; sleeping {delay:.1f}s before Docker may restart it.")
        time.sleep(delay)
        raise
    finally:
        try:
            asyncio.run(close_shared_runtime_resources())
        except Exception as tx_error:
            pass

if __name__ == "__main__":
    main()
