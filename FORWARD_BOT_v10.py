#!/usr/bin/env python3
# FORWARD BOT v10.0 — FULL POWER + MULTI-RECEIVER + LIVE 1s + REST 50s + REAL COUNT
# deps: pip install pyrofork tgcrypto aiohttp
# run : python FORWARD_BOT_v10.py

import asyncio
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

import os, re, time, json, logging, uuid
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

logging.disable(logging.WARNING)

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait, UserAlreadyParticipant, UserNotParticipant
from pyrogram.raw.functions.messages import Search
from pyrogram.raw.types import InputMessagesFilterDocument

# ---------- config ----------
API_ID = int(os.environ.get("API_ID", "36548942"))
API_HASH = os.environ.get("API_HASH", "b49299b112e2dcdc227d4c929805a28f").strip()
BOT_TOKEN = os.environ.get(
    "BOT_TOKEN", "9180800490:AAFqGD_I_vqnhhDjd0Y6U3M9_ShWuL0xe4A"
).strip()
SESSION_STRING = os.environ.get(
    "SESSION_STRING",
    "AQItsU4ArM_eSnwiuGSxxqAN9MUbYvfGc5rcxHi7ExFzwprwrDQDxGlqbZ9UjhgJC387xOvqvlXMQih_HBkotMzO4GZivayXtFcKbH-TEUsHxReV-IVU-ol9VlAi-65Xg00HxjKfb2L-_4hLfb8CjaUS4ulSw6d_WrL38OXkN2m89s39b7tKH74WGMlueY14gITK5XfC50uy3VJAERgeQ8KHcKYCFO2msvlBwSiEFxy4L-TqDCjXdbczhYYtiCIS6dEkIWR4ZkDsqXA4CRcpH4ydjUCeuzZmC0-UBT1CGvxq5wmThz7vPpP0p688OkD1StVfJzdWe1EEKqfv5JpuiM0Jg74vOgAAAAHRbYXhAA",
).strip()
OWNER_ID = int(os.environ.get("OWNER_ID", "7808583137"))
DEFAULT_TARGET = int(os.environ.get("TARGET_CHANNEL", "-1004356958698"))

DATA_FILE = Path(__file__).parent / "forward_v10_data.json"
DETAILS_FILE = Path(__file__).parent / "forward_details.json"

SCAN_CHUNK = 200
FWD_TIMEOUT = 45
INTER_BATCH_MS = 0
REST_EVERY = 500          # files — rest after every 500
REST_SEC = 50             # 50 sec break to avoid flood wait
STATUS_EVERY = 1.0        # live status refresh every 1s (Telegram edit limit ~1/s)
COPY_CONCURRENCY = 20     # target ~20 files/s ordered batches
BATCH_SIZE = 20           # line-by-line order inside each batch of 20
COUNT_CACHE: Dict = {}
COUNT_CACHE_TTL = 600
BOT_USERNAME = ""   # filled in main() after bot.get_me()


_HTTP_SESSION = None
_EDIT_BLOCK_UNTIL = 0.0


async def _http():
    """One shared aiohttp session — buttons reply fast (no new TLS handshake per message)."""
    global _HTTP_SESSION
    import aiohttp
    if _HTTP_SESSION is None or _HTTP_SESSION.closed:
        _HTTP_SESSION = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=60, ttl_dns_cache=300, keepalive_timeout=60),
            timeout=aiohttp.ClientTimeout(total=20),
        )
    return _HTTP_SESSION


async def _http_edit(chat_id: int, msg_id: int, text: str) -> bool:
    """HTTP edit. True = message is fine (edited / unchanged / rate-limited). False = message is gone."""
    global _EDIT_BLOCK_UNTIL
    if not msg_id:
        return False
    if time.time() < _EDIT_BLOCK_UNTIL:
        return True  # Telegram asked us to slow down — keep same message, skip this tick
    try:
        s = await _http()
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
        payload = {
            "chat_id": chat_id,
            "message_id": msg_id,
            "text": D(text)[:4000],
            "disable_web_page_preview": True,
        }
        async with s.post(url, json=payload) as r:
            data = await r.json()
        if data.get("ok"):
            return True
        desc = str(data.get("description", "")).lower()
        if "not modified" in desc:
            return True
        if data.get("error_code") == 429:
            wait = int((data.get("parameters") or {}).get("retry_after", 2))
            _EDIT_BLOCK_UNTIL = time.time() + wait + 0.5
            return True
        return False
    except Exception:
        return True  # network hiccup — never spam new messages


async def _http_send(chat_id: int, text: str, markup=None) -> Optional[int]:
    """Send a message via HTTP. Returns message_id or None."""
    try:
        s = await _http()
        payload: dict = {
            "chat_id": chat_id,
            "text": D(text)[:4000],
            "disable_web_page_preview": True,
        }
        if markup:
            payload["reply_markup"] = _prep_markup(chat_id, markup)
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        async with s.post(url, json=payload) as r:
            data = await r.json()
        if data.get("ok"):
            return (data.get("result") or {}).get("message_id")
    except Exception:
        pass
    return None


# ---------- bold ----------
_BOLD = str.maketrans(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
    "𝐀𝐁𝐂𝐃𝐄𝐅𝐆𝐇𝐈𝐉𝐊𝐋𝐌𝐍𝐎𝐏𝐐𝐑𝐒𝐓𝐔𝐕𝐖𝐗𝐘𝐙𝐚𝐛𝐜𝐝𝐞𝐟𝐠𝐡𝐢𝐣𝐤𝐥𝐦𝐧𝐨𝐩𝐪𝐫𝐬𝐭𝐮𝐯𝐰𝐱𝐲𝐳𝟎𝟏𝟐𝟑𝟒𝟓𝟔𝟕𝟖𝟗",
)

def B(s: str) -> str:
    return (s or "").translate(_BOLD)


# ══════════════════════════════════════════════════════
# DRIFT FONT SYSTEM  —  𝐁old initial + ꜱᴍᴀʟʟ ᴄᴀᴘꜱ rest
# Example:  "Add Source"  →  "𝐀ᴅᴅ 𝐒ᴏᴜʀᴄᴇ"
# ══════════════════════════════════════════════════════
_DRIFT_BOLD = {
    'a':'𝐀','b':'𝐁','c':'𝐂','d':'𝐃','e':'𝐄','f':'𝐅','g':'𝐆','h':'𝐇',
    'i':'𝐈','j':'𝐉','k':'𝐊','l':'𝐋','m':'𝐌','n':'𝐍','o':'𝐎','p':'𝐏',
    'q':'𝐐','r':'𝐑','s':'𝐒','t':'𝐓','u':'𝐔','v':'𝐕','w':'𝐖','x':'𝐗',
    'y':'𝐘','z':'𝐙',
}
_DRIFT_SMALL = {
    'a':'ᴀ','b':'ʙ','c':'ᴄ','d':'ᴅ','e':'ᴇ','f':'ғ','g':'ɢ','h':'ʜ',
    'i':'ɪ','j':'ᴊ','k':'ᴋ','l':'ʟ','m':'ᴍ','n':'ɴ','o':'ᴏ','p':'ᴘ',
    'q':'ǫ','r':'ʀ','s':'ꜱ','t':'ᴛ','u':'ᴜ','v':'ᴠ','w':'ᴡ','x':'x',
    'y':'ʏ','z':'ᴢ',
}
_DRIFT_NORM = str.maketrans({
    **{c: c.lower() for c in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'},
    **{c: u for c, u in zip('𝐀𝐁𝐂𝐃𝐄𝐅𝐆𝐇𝐈𝐉𝐊𝐋𝐌𝐍𝐎𝐏𝐐𝐑𝐒𝐓𝐔𝐕𝐖𝐗𝐘𝐙', 'abcdefghijklmnopqrstuvwxyz')},
    **dict(zip('ᴀʙᴄᴅᴇғɢʜɪᴊᴋʟᴍɴᴏᴘǫʀꜱᴛᴜᴠᴡxʏᴢ', 'abcdefghijklmnopqrstuvwxyz')),
})
_DRIFT_PROTECT = re.compile(
    r'(https?://[^\s<>]+|www\.[^\s<>]+|t\.me/[^\s<>]+|@[A-Za-z0-9_]{4,}|/[A-Za-z][A-Za-z0-9_]*|%[sd]'
    r'|\b\d{5,}:[A-Za-z0-9_-]{20,}'
    r'|\b[\w\-]+\.(?:py|zip|txt|json|mp4|mkv|mp3|pdf|apk|jpe?g|png|gif|webp|rar|7z|ogg|m4a|flac|html?|js|csv|docx?|xlsx?|pptx?|session|bak)\b'
    r'|-?\d{6,}|\{[^}]*\})',
    re.IGNORECASE,
)


def _drift_word(text: str) -> str:
    text = text.translate(_DRIFT_NORM)
    out, ws = [], True
    for ch in text:
        if ch.isascii() and ch.isalpha():
            lw = ch.lower()
            out.append(_DRIFT_BOLD.get(lw, ch.upper()) if ws else _DRIFT_SMALL.get(lw, lw))
            ws = False
        else:
            out.append(ch)
            ws = not (ch.isascii() and (ch.isdigit() or ch == '_'))
    return ''.join(out)


def D(text: str) -> str:
    """DRIFT font: 𝐁old initial + ꜱᴍᴀʟʟ ᴄᴀᴘꜱ. Keeps URLs/IDs/handles untouched."""
    if not isinstance(text, str) or not text:
        return text
    text = text.replace("**", "").replace("`", "")
    parts = _DRIFT_PROTECT.split(text)
    return ''.join(p if i % 2 else _drift_word(p) for i, p in enumerate(parts))


def load():
    if DATA_FILE.exists():
        try:
            return json.loads(DATA_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "sources": [],
        "receivers": [],
        "admins": [],
        "users": [],
        "selected_source": None,
        "selected_receiver": None,
        "jobs": {},
        # ── Restricted source/receive lists ──
        "res_source_groups":   [],   # [{id, title, link}]
        "res_source_channels": [],   # [{id, title, link}]
        "res_receive_groups":  [],   # [{id, title, link}]
        "res_receive_channels":[],   # [{id, title, link}]
        "selected_res_source":  None, # {id, title, type}
        "selected_res_receive": None, # {id, title, type}
    }


def save(d):
    DATA_FILE.write_text(json.dumps(d, indent=2, ensure_ascii=False), encoding="utf-8")


def is_allowed(uid: int) -> bool:
    if uid == OWNER_ID:
        return True
    d = load()
    if uid in (d.get("admins") or []):
        return True
    for u in (d.get("users") or []):
        if int(u.get("owner_id") or 0) == uid:
            return True
    return False


def get_user_role(uid: int) -> str:
    if uid == OWNER_ID:
        return "owner"
    d = load()
    for u in (d.get("users") or []):
        if int(u.get("owner_id") or 0) == uid:
            return (u.get("role") or "admin").lower()
    if uid in (d.get("admins") or []):
        return "admin"
    return "none"


def panel_kb_for(uid: int):
    """Return correct panel keyboard for this user."""
    role = get_user_role(uid)
    if role == "owner" or uid == OWNER_ID:
        return owner_kb()
    return admin_kb()


def can_manage(uid: int) -> bool:
    """Owner or admin can use panel features (except pure owner-only user mgmt)."""
    return is_allowed(uid)


def btn(text: str, color: str = "primary") -> dict:
    style = color if color in ("primary", "success", "danger") else "primary"
    return {"text": D(text), "style": style}


def make_markup(rows: list) -> dict:
    return {
        "keyboard": rows,
        "resize_keyboard": True,
        "is_persistent": True,
    }


def kb(markup: dict):
    return markup


OWNER_CTX: Dict[int, bool] = {}   # chat_id -> True while user is inside an owner-panel action


def _bc_rows():
    return [[btn("🔙 BACK TO OWNER PANEL", "primary"), btn("❌ CANCEL", "danger")]]


def _prep_markup(chat_id, markup):
    """Owner-panel context: every action screen gets BACK + CANCEL at the bottom.
    Also strips private keys (like _kind) before the markup goes to Telegram."""
    if not isinstance(markup, dict):
        return markup
    kind = markup.get("_kind")
    out = {k: v for k, v in markup.items() if not str(k).startswith("_")}
    rows = out.get("keyboard")
    if not isinstance(rows, list):
        return out
    in_ctx = OWNER_CTX.get(chat_id, False)
    if kind == "owner":
        if in_ctx and STATE.get("awaiting"):
            out["keyboard"] = _bc_rows()          # a prompt is waiting → BACK / CANCEL only
        else:
            OWNER_CTX[chat_id] = False
        return out
    if kind == "main":
        OWNER_CTX[chat_id] = False
        return out
    if in_ctx:
        has_back = any(
            plain_upper(b.get("text", "")) == "BACK TO OWNER PANEL"
            for r in rows for b in r if isinstance(b, dict)
        )
        if not has_back:
            out["keyboard"] = rows + _bc_rows()
    return out


async def reply_colored(chat_id: int, text: str, markup=None):
    text = D(text) if isinstance(text, str) else text
    if isinstance(text, str) and len(text) > 4000:
        text = text[:3990] + "…"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    if markup is not None:
        payload["reply_markup"] = _prep_markup(chat_id, markup)
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        s = await _http()
        async with s.post(url, json=payload) as resp:
            data = await resp.json()
            if not data.get("ok"):
                print("reply_colored err:", data)
            return data
    except Exception as e:
        print("reply_colored exception:", e)
        try:
            return await bot.send_message(chat_id, text)
        except Exception as e2:
            print("fallback fail:", e2)
            return None


async def m_reply(m, text: str, markup=None):
    return await reply_colored(m.chat.id, text, markup)


def main_kb(uid: int = 0):
    rows = [
        [btn("➕ ADD SOURCE", "success"), btn("📋 SOURCE LIST", "primary")],
        [btn("⚡ FORWARD NOW", "primary"), btn("📊 STATUS", "success")],
        [btn("📥 MY RECEV CHAN", "success"), btn("🔄 FORWARD RUN", "primary")],
        [btn("🔒 FORWARD GROUP", "success"), btn("🔒 FORWARD CHANNEL", "primary")],
        [btn("🏠 MENU", "primary")],
    ]
    role = get_user_role(uid)
    if role == "owner" or uid == OWNER_ID:
        rows.append([btn("👑 OWNER PANEL", "danger")])
    elif role == "admin" or is_allowed(uid):
        rows.append([btn("🛡 ADMIN PANEL", "danger")])
    mk = make_markup(rows)
    mk["_kind"] = "main"
    return kb(mk)


def owner_kb():
    """Owner panel — full button names, BACK TO MAIN MENU at the very end."""
    mk = make_markup([
        [btn("📥 ADD RECEIVER CHANNEL", "success")],
        [btn("📤 RECEIVER CHANNEL LIST", "primary")],
        [btn("🗑 REMOVE RECEIVER CHANNEL", "danger")],
        [btn("🗑 REMOVE SOURCE CHANNEL", "danger")],
        [btn("👤 ADD USER", "success"), btn("👥 USER LIST", "primary")],
        [btn("🗑 REMOVE USER", "danger")],
        [btn("➕ ADD ADMIN", "success"), btn("➖ REMOVE ADMIN", "danger")],
        [btn("👥 ADMIN LIST", "primary")],
        # ── Restricted source ──
        [btn("🔒 RES SOURCE GROUP", "success")],
        [btn("📋 RES SOURCE GROUP LIST", "primary")],
        [btn("🗑 RES REMOVE SOURCE GROUP", "danger")],
        [btn("🔒 RES SOURCE CHANNEL", "success")],
        [btn("📋 RES SOURCE CHANNEL LIST", "primary")],
        [btn("🗑 RES REMOVE SOURCE CHANNEL", "danger")],
        # ── Restricted receive ──
        [btn("📥 RES RECEIVE GROUP", "success")],
        [btn("📋 RES RECEIVE GROUP LIST", "primary")],
        [btn("🗑 RES REMOVE RECEIVE GROUP", "danger")],
        [btn("📥 RES RECEIVE CHANNEL", "success")],
        [btn("📋 RES RECEIVE CHANNEL LIST", "primary")],
        [btn("🗑 RES REMOVE RECEIVE CHANNEL", "danger")],
        [btn("🔙 BACK TO MAIN MENU", "primary")],
    ])
    mk["_kind"] = "owner"
    return kb(mk)


def admin_kb():
    """Admin panel — same full names, no user management, no RES management."""
    mk = make_markup([
        [btn("📥 ADD RECEIVER CHANNEL", "success")],
        [btn("📤 RECEIVER CHANNEL LIST", "primary")],
        [btn("🗑 REMOVE RECEIVER CHANNEL", "danger")],
        [btn("🗑 REMOVE SOURCE CHANNEL", "danger")],
        [btn("➕ ADD ADMIN", "success"), btn("➖ REMOVE ADMIN", "danger")],
        [btn("👥 ADMIN LIST", "primary")],
        [btn("🔙 BACK TO MAIN MENU", "primary")],
    ])
    mk["_kind"] = "owner"
    return kb(mk)


def add_user_step_kb(step: str):
    """Only the button for current step — click first, then type."""
    if step == "wait_api_btn":
        return kb(make_markup([[btn("📤 SEND API ID", "success")], [btn("🔙 BACK", "primary")]]))
    if step == "wait_hash_btn":
        return kb(make_markup([[btn("📤 UPLOAD HASH", "success")], [btn("🔙 BACK", "primary")]]))
    if step == "wait_session_btn":
        return kb(make_markup([[btn("📤 UPLOAD SESSION STRING", "success")], [btn("🔙 BACK", "primary")]]))
    if step == "wait_uid_btn":
        return kb(make_markup([[btn("📤 SEND USER ID", "success")], [btn("🔙 BACK", "primary")]]))
    if step == "wait_role_btn":
        return kb(make_markup([
            [btn("👑 OWNER PANEL", "danger"), btn("🛡 ADMIN PANEL", "primary")],
            [btn("🔙 BACK", "primary")],
        ]))
    return owner_kb()


def forward_mode_kb():
    return kb(make_markup([
        [btn("🚀 DIRECT FORWARD", "success"), btn("📍 PATH FORWARD", "primary")],
        [btn("🔙 BACK", "primary")],
    ]))


def list_kb(items, prefix="SRC"):
    rows, row = [], []
    for it in items[:40]:
        iid = it.get("id")
        label = (it.get("title") or str(iid) or "?")[:18]
        key = f"{label} | {prefix}_{iid}"
        row.append(btn(key, "primary"))
        if len(row) == 1:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([btn("🔙 BACK", "primary")])
    return kb(make_markup(rows))


def admin_list_kb(admin_ids):
    rows = []
    for aid in admin_ids[:40]:
        rows.append([btn(f"Admin {aid} | ADM_{aid}", "danger")])
    rows.append([btn("🔙 BACK", "primary")])
    return kb(make_markup(rows))


def receiver_list_kb(recs):
    rows = []
    for r in recs[:40]:
        rid = r.get("id")
        title = (r.get("title") or str(rid))[:18]
        rows.append([btn(f"{title} | RCV_{rid}", "success")])
    rows.append([btn("🔙 BACK", "primary")])
    return kb(make_markup(rows))


def live_control_kb():
    return kb(make_markup([
        [btn("⏹ STOP", "danger"), btn("⏸ PAUSE", "primary")],
        [btn("▶️ RESUME", "success"), btn("📊 STATUS", "primary")],
        [btn("🏠 MENU", "primary")],
    ]))


def job_list_kb(jobs: Dict[str, Any]):
    """Button list — original channel name only, no JOB_ prefix."""
    rows = []
    for jid, j in list(jobs.items())[:30]:
        title = (j.get("recv_title") or j.get("src_title") or "channel")[:22]
        st = (j.get("stage") or "").lower()
        if j.get("stop") or st in ("done", "stopped"):
            color = "danger"
            tag = "⏹"
        elif j.get("paused"):
            color = "primary"
            tag = "⏸"
        else:
            color = "success"
            tag = "▶"
        label = f"{tag} {title}"
        rows.append([btn(label, color)])
    rows.append([btn("🔙 BACK TO HOME", "primary")])
    return kb(make_markup(rows))


def job_detail_kb(job_id: str):
    """Clean button names only — selected_job_id in STATE holds which job."""
    return kb(make_markup([
        [btn("▶️ START", "success"), btn("⏹ STOP", "danger")],
        [btn("🔄 RESTART", "primary"), btn("⏸ PAUSE", "primary")],
        [btn("▶️ RESUME", "success"), btn("🗑 DELETE", "danger")],
        [btn("📊 LIVE STATUS", "success"), btn("🔙 BACK TO HOME", "primary")],
    ]))


def my_recev_kb(jobs: Dict[str, Any]):
    """MY RECEV CHAN — original receiver name only."""
    rows = []
    for jid, j in list(jobs.items())[:25]:
        rtitle = (j.get("recv_title") or "recv")[:22]
        if j.get("stop"):
            color = "danger"
            tag = "⏹"
        elif j.get("paused"):
            color = "primary"
            tag = "⏸"
        else:
            color = "success"
            tag = "▶"
        label = f"{tag} {rtitle}"
        rows.append([btn(label, color)])
    rows.append([btn("🚀 START FORWARD", "success")])
    rows.append([btn("🔙 BACK TO HOME", "primary")])
    return kb(make_markup(rows))


# no_updates=True on user client reduces unknown TL constructor spam from group calls / polls
def _client_extras(n):
    """Parallel transfers (pyrogram default is 1) — used only if this pyrogram version supports it."""
    import inspect as _insp
    try:
        if "max_concurrent_transmissions" in _insp.signature(Client.__init__).parameters:
            return {"max_concurrent_transmissions": n}
    except Exception:
        pass
    return {}


RES_PARALLEL = max(1, int(os.environ.get("RES_PARALLEL", "4")))   # files downloaded at the same time
bot = Client(
    "fwd_bot_v10",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True,
    **_client_extras(RES_PARALLEL),
)
user = Client(
    "fwd_user_v10",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    no_updates=True,   # critical: stops unknown constructor crashes from live updates
    in_memory=True,
    **_client_extras(RES_PARALLEL),
)

# Global single-job STATE kept for backward + status helpers
STATE = {
    "stage": "idle",
    "status_msg_id": None,
    "chat_id": None,
    "src_title": None,
    "recv_title": None,
    "src_link": None,
    "total": 0,
    "files": 0,
    "copied": 0,
    "failed": 0,
    "errors": 0,
    "scanned": 0,
    "total_files": 0,
    "last_err": None,
    "wait_until": 0.0,
    "started": 0.0,
    "task": None,
    "stop": False,
    "paused": False,
    "awaiting": None,
    "start_from_id": 0,
    "live_names": [],
    "last_file": None,
    "api_link": f"https://api.telegram.org/bot{BOT_TOKEN[:12]}...",
    "details": [],
    "path_query": None,
    "all_mids": [],
    "rest_until": 0.0,
    "job_id": None,
    "selected_job_id": None,
}

# Multi-job store: job_id -> job dict (live in memory)
JOBS: Dict[str, Dict[str, Any]] = {}

# Per-user Pyrogram client pool  keyed by owner_id
USER_CLIENTS: Dict[int, Any] = {}


async def get_user_client(uid: int):
    """Return a started Pyrogram user client for uid, or the global user as fallback."""
    if uid == OWNER_ID or uid == 0:
        return user
    if uid in USER_CLIENTS:
        c = USER_CLIENTS[uid]
        try:
            if c.is_connected:
                return c
        except Exception:
            pass
    d = load()
    for u_entry in (d.get("users") or []):
        if int(u_entry.get("owner_id") or 0) == uid:
            sess   = u_entry.get("session") or ""
            api_id = u_entry.get("api_id")
            api_hash = u_entry.get("api_hash") or ""
            if sess and api_id and api_hash:
                name = f"fwd_u_{uid}"
                c = Client(
                    name,
                    api_id=int(api_id),
                    api_hash=api_hash,
                    session_string=sess,
                    no_updates=True,
                    in_memory=True,
                )
                try:
                    await c.start()
                    USER_CLIENTS[uid] = c
                    print(f"[user-client] started for uid={uid}")
                    return c
                except Exception as e:
                    print(f"[user-client] start fail uid={uid}: {e}")
    return user  # global fallback


def now_str():
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


async def safe_edit(chat_id, msg_id, text):
    try:
        await bot.edit_message_text(chat_id, msg_id, D(text), disable_web_page_preview=True)
    except Exception:
        pass


class Status:
    def __init__(self, chat_id, msg_id, job_id=None):
        self.chat_id = chat_id
        self.msg_id = msg_id
        self.job_id = job_id
        self._last = 0.0

    async def flush(self, force=False):
        now = time.time()
        if not force and now - self._last < STATUS_EVERY:
            return
        self._last = now

        # prefer job-local state if multi
        st = STATE
        if self.job_id and self.job_id in JOBS:
            st = JOBS[self.job_id]
            # sync message target when LIVE STATUS button creates a new message
            cur_mid = st.get("status_msg_id")
            if cur_mid and cur_mid != self.msg_id:
                self.msg_id = cur_mid
            cur_cid = st.get("chat_id")
            if cur_cid and cur_cid != self.chat_id:
                self.chat_id = cur_cid

        up = int(now - st.get("started", 0)) if st.get("started") else 0
        speed = (st.get("copied", 0) / up) if up > 0 else 0.0
        remaining = max(0, (st.get("total_files") or st.get("files") or 0) - st.get("copied", 0) - st.get("failed", 0))
        eta = int(remaining / speed) if speed > 0.1 else 0

        # ── live field builders ──
        api_link = (f"https://t.me/{BOT_USERNAME}" if BOT_USERNAME else "—")
        bot_token_short = (BOT_TOKEN[:12] + "…") if BOT_TOKEN else "—"

        fw = ""
        if st.get("wait_until", 0) > now:
            rem = max(0, int(st["wait_until"] - now))
            fw = f"\n⛔ FLOODWAIT ACTIVE: {rem}s remaining — LIVE TIMER\n"

        rest_line = ""
        if st.get("rest_until", 0) > now:
            rem = max(0, int(st["rest_until"] - now))
            rest_line = f"\n💤 REST: {rem}s left (after every 500 files)\n"

        err = ""
        if st.get("last_err"):
            err = f"\n⚠️ ERROR: {str(st['last_err'])[:140]}"

        names = st.get("live_names") or []
        name_line = " | ".join(names[-6:]) if names else "—"
        last_f   = st.get("last_file") or "—"
        path_q   = st.get("path_query") or "—"
        total_fx = st.get("total_files") or st.get("files") or 0
        copied   = st.get("copied", 0)
        failed   = st.get("failed", 0)
        scanned  = st.get("scanned", 0)
        errors   = st.get("errors", 0)
        src_t    = st.get("src_title") or "—"
        rcv_t    = st.get("recv_title") or "—"
        stage    = st.get("stage") or "idle"
        pause_line = "⏸ PAUSED — Resume dabao\n" if st.get("paused") else ""
        progress = f"{copied}/{total_fx}" if total_fx else "0/0"
        pct = (copied / total_fx * 100) if total_fx else 0.0
        bar_len = 12
        filled = int(bar_len * pct / 100) if total_fx else 0
        bar = "█" * filled + "░" * (bar_len - filled)

        text = (
            f"⚡ {B('LIVE STATUS')}  ·  v10 FULL POWER PREMIUM\n"
            f"══════════════════════════════\n"
            f"🔗 API LINK: {api_link}\n"
            f"🔑 BOT TOKEN: {bot_token_short}\n"
            f"🕐 LIVE TIME: {now_str()}\n"
            f"📍 LIVE UPDATES: {stage}\n"
            f"📥 Source: {src_t}\n"
            f"📤 Receive: {rcv_t}\n"
            f"📂 Path / Start: {path_q}\n"
            f"──────────────────────────────\n"
            f"📁 ALL FILES: {total_fx}\n"
            f"🔎 SCANNED: {scanned}\n"
            f"✅ COPIES: {copied} / {total_fx}\n"
            f"❌ FAILS: {failed}\n"
            f"⚠ ERRORS: {errors}\n"
            f"📊 PROGRESS: [{bar}] {pct:.1f}%\n"
            f"⏱ ELAPSED: {up}s\n"
            f"⏳ ETA: {eta}s\n"
            f"🚀 SPEED: {speed:.1f} files/s\n"
            f"──────────────────────────────\n"
            f"📄 LIVE FILE: {last_f}\n"
            f"🏷 LIVE NAMES: {name_line}\n"
            f"{_extra_block(st)}"
            f"📌 LIVE DETAILS: {progress} done · left {remaining}\n"
            f"──────────────────────────────\n"
            f"{pause_line}{fw}{rest_line}{err}\n"
            f"⏹ STOP | ⏸ PAUSE | ▶️ RESUME | 🏠 MENU"
        )
        # Use HTTP API edit — bypasses Pyrogram parse-mode/session issues
        ok = await _http_edit(self.chat_id, self.msg_id, text)
        if not ok:
            # edit failed (msg deleted / too old) — send a fresh one and track it
            new_mid = await _http_send(self.chat_id, text)
            if new_mid:
                self.msg_id = new_mid
                st["status_msg_id"] = new_mid


def resolve_target(raw: str):
    raw = (raw or "").strip().rstrip("/")
    m = re.match(r"https?://t\.me/c/(\d+)(?:/\d+)?$", raw)
    if m:
        return int("-100" + m.group(1))
    m = re.match(r"https?://t\.me/joinchat/([A-Za-z0-9_-]+)", raw)
    if m:
        return "+" + m.group(1)
    m = re.match(r"https?://t\.me/\+([A-Za-z0-9_-]+)", raw)
    if m:
        return "+" + m.group(1)
    m = re.match(r"(?:https?://)?t\.me/([A-Za-z0-9_]+)(?:/\d+)?$", raw)
    if m:
        return m.group(1)
    if raw.startswith("@"):
        return raw[1:]
    if raw.startswith("+"):
        return raw
    if re.fullmatch(r"-?\d+", raw):
        return int(raw)
    if re.fullmatch(r"[A-Za-z0-9_]{4,}", raw):
        return raw
    return raw


def plain_upper(text: str) -> str:
    t = text or ""
    # mathematical bold + sans-serif bold + fullwidth digits/letters → ASCII
    bold_from = (
        "𝐀𝐁𝐂𝐃𝐄𝐅𝐆𝐇𝐈𝐉𝐊𝐋𝐌𝐍𝐎𝐏𝐐𝐑𝐒𝐓𝐔𝐕𝐖𝐗𝐘𝐙𝐚𝐛𝐜𝐝𝐞𝐟𝐠𝐡𝐢𝐣𝐤𝐥𝐦𝐧𝐨𝐩𝐪𝐫𝐬𝐭𝐮𝐯𝐰𝐱𝐲𝐳𝟎𝟏𝟐𝟑𝟒𝟓𝟔𝟕𝟖𝟗"
        "𝗔𝗕𝗖𝗗𝗘𝗙𝗚𝗛𝗜𝗝𝗞𝗟𝗠𝗡𝗢𝗣𝗤𝗥𝗦𝗧𝗨𝗩𝗪𝗫𝗬𝗭𝗮𝗯𝗰𝗱𝗲𝗳𝗴𝗵𝗶𝗷𝗸𝗹𝗺𝗻𝗼𝗽𝗾𝗿𝘀𝘁𝘂𝘃𝘄𝘅𝘆𝘇𝟬𝟭𝟮𝟯𝟰𝟱𝟲𝟳𝟴𝟵"
        "𝐴𝐵𝐶𝐷𝐸𝐹𝐺𝐻𝐼𝐽𝐾𝐿𝑀𝑁𝑂𝑃𝑄𝑅𝑆𝑇𝑈𝑉𝑊𝑋𝑌𝑍𝑎𝑏𝑐𝑑𝑒𝑓𝑔ℎ𝑖𝑗𝑘𝑙𝑚𝑛𝑜𝑝𝑞𝑟𝑠𝑡𝑢𝑣𝑤𝑥𝑦𝑧"
    )
    bold_to = (
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    )
    t = t.translate(_DRIFT_NORM)   # ꜱᴍᴀʟʟ ᴄᴀᴘꜱ / bold caps -> plain letters
    t = t.translate(str.maketrans(bold_from, bold_to))
    for e in ("🔵", "🟢", "🔴", "⚪", "🟠", "🟣", "🟡", "➕", "📋", "⚡", "📊", "⏹", "🏠", "📌",
              "👑", "🛡", "📥", "📤", "➖", "👥", "🔙", "🚀", "📍", "👤", "✅", "•", "⬇️", "📁",
              "🔄", "⏸", "▶️", "🗑", "💤", "←", "·", "|"):
        t = t.replace(e, " ")
    t = re.sub(r"[^\x00-\x7f]", " ", t)   # any other emoji / symbol
    # collapse spaces
    t = re.sub(r"\s+", " ", t)
    return t.strip().upper()


def norm_key(text: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", plain_upper(text or ""))


async def ensure_member(identifier, u_client=None):
    uc = u_client or user
    if isinstance(identifier, str) and identifier.startswith("+"):
        chat0 = await _chat_from_invite(uc, identifier)      # already a member → never join
        if chat0 is not None:
            return chat0, False
        try:
            chat = await uc.join_chat(identifier)
            return chat, True
        except UserAlreadyParticipant:
            return await uc.get_chat(identifier), False
        except FloodWait as e:
            await asyncio.sleep(e.value + 2)
            return await ensure_member(identifier, u_client=uc)
    if isinstance(identifier, int):
        try:
            return await uc.get_chat(identifier), False
        except FloodWait:
            raise
        except Exception:
            await warm_peers(uc, force=True)                 # private chat of the account → load dialogs, retry
            return await uc.get_chat(identifier), False
    if isinstance(identifier, str):
        try:
            chat = await uc.get_chat(identifier)
            try:
                await uc.get_chat_member(chat.id, "me")
                return chat, False
            except UserNotParticipant:
                try:
                    await uc.join_chat(identifier)
                    return chat, True
                except UserAlreadyParticipant:
                    return chat, False
        except FloodWait as e:
            await asyncio.sleep(e.value + 2)
            return await ensure_member(identifier, u_client=uc)
        except Exception:
            chat = await uc.join_chat(identifier)
            return chat, True
    raise ValueError(f"cannot resolve: {identifier!r}")


async def resolve_peer_with_retry(src_id, tries=5, u_client=None):
    uc = u_client or user
    for _ in range(tries):
        try:
            return await uc.resolve_peer(src_id)
        except FloodWait as e:
            await asyncio.sleep(e.value + 2)
        except Exception:
            await asyncio.sleep(1)
            try:
                await uc.get_chat(src_id)
            except Exception:
                pass
    raise RuntimeError(f"cannot resolve peer {src_id}")


async def flood_sleep(seconds, status, why="", job=None):
    target = job if job is not None else STATE
    target["wait_until"] = time.time() + seconds
    target["stage"] = f"FLOODWAIT {int(seconds)}s LIVE {why}".strip()
    target["last_err"] = f"FLOOD WAIT {int(seconds)}s — live timer"
    STATE["wait_until"] = target["wait_until"]
    STATE["stage"] = target["stage"]
    STATE["last_err"] = target["last_err"]
    await status.flush(force=True)
    rem = float(seconds)
    while rem > 0 and not target.get("stop"):
        chunk = min(0.1, rem)   # 0.1s ticks — live status never silent
        await asyncio.sleep(chunk)
        rem -= chunk
        target["wait_until"] = time.time() + rem
        target["stage"] = f"FLOODWAIT {int(rem)}s LIVE"
        STATE["wait_until"] = target["wait_until"]
        STATE["stage"] = target["stage"]
        await status.flush(force=True)
    target["wait_until"] = 0
    STATE["wait_until"] = 0
    target["last_err"] = None
    STATE["last_err"] = None
    target["stage"] = "copying (after floodwait)"
    STATE["stage"] = target["stage"]
    await status.flush(force=True)


async def rest_sleep(seconds, status, job=None):
    """50s rest after every 500 files — live timer + BOT ON REST in status."""
    target = job if job is not None else STATE
    target["rest_until"] = time.time() + seconds
    target["stage"] = f"REST {int(seconds)}s (after 500 files)"
    STATE["rest_until"] = target["rest_until"]
    STATE["stage"] = target["stage"]
    await status.flush(force=True)
    rem = float(seconds)
    while rem > 0 and not target.get("stop"):
        chunk = min(0.1, rem)
        await asyncio.sleep(chunk)
        rem -= chunk
        target["rest_until"] = time.time() + rem
        target["stage"] = f"REST {int(rem)}s left (after every 500 files)"
        STATE["rest_until"] = target["rest_until"]
        STATE["stage"] = target["stage"]
        await status.flush(force=True)
    target["rest_until"] = 0
    STATE["rest_until"] = 0
    target["stage"] = "copying"
    STATE["stage"] = "copying"
    await status.flush(force=True)


async def copy_one(src_id, tgt_id, mid, u_client=None):
    """
    Ultra fast copy — NO channel mark (pure copy only, never forward):
    1) user_client.copy_message  (per-user or global session)
    2) bot.copy_message          (bot must be admin in receiver)
    """
    uc = u_client or user
    last_err = None
    try:
        return await asyncio.wait_for(
            uc.copy_message(chat_id=tgt_id, from_chat_id=src_id, message_id=mid),
            timeout=FWD_TIMEOUT,
        )
    except FloodWait:
        raise
    except Exception as e:
        last_err = e
    try:
        return await asyncio.wait_for(
            bot.copy_message(chat_id=tgt_id, from_chat_id=src_id, message_id=mid),
            timeout=FWD_TIMEOUT,
        )
    except FloodWait:
        raise
    except Exception as e:
        last_err = e
    raise last_err if last_err else RuntimeError("copy fail")


async def copy_batch_ordered(src_id, tgt_id, mids, u_client=None):
    """
    Copy a batch of message IDs in STRICT increasing order (line-by-line).
    Tries bulk copy_messages first (preserves order), falls back to sequential copy_one.
    Returns list of (ok: bool, mid, err).
    """
    uc = u_client or user
    mids = sorted(int(x) for x in mids)  # must be strictly increasing
    results = []
    # 1) try bulk copy_messages (user) — order preserved by API
    try:
        await asyncio.wait_for(
            uc.copy_messages(
                chat_id=tgt_id,
                from_chat_id=src_id,
                message_ids=mids,
            ),
            timeout=FWD_TIMEOUT * 2,
        )
        for mid in mids:
            results.append((True, mid, None))
        return results
    except FloodWait:
        raise
    except Exception:
        pass
    # 2) try bulk bot copy_messages
    try:
        await asyncio.wait_for(
            bot.copy_messages(
                chat_id=tgt_id,
                from_chat_id=src_id,
                message_ids=mids,
            ),
            timeout=FWD_TIMEOUT * 2,
        )
        for mid in mids:
            results.append((True, mid, None))
        return results
    except FloodWait:
        raise
    except Exception:
        pass
    # 3) sequential fallback — true line-by-line, zero skip
    for mid in mids:
        try:
            await copy_one(src_id, tgt_id, mid, u_client=u_client)
            results.append((True, mid, None))
        except FloodWait:
            raise
        except Exception as e:
            results.append((False, mid, f"{type(e).__name__}: {e}"))
    return results


# ---------- deep extract helpers ----------
TOKEN_RE = re.compile(r"\b(\d{8,12}:[A-Za-z0-9_-]{30,})\b")
BOT_ID_RE = re.compile(r"(?:bot[_\s]?id|api[_\s]?id)\s*[=:]\s*[\"']?(\d{6,})", re.I)
OWNER_ID_RE = re.compile(r"(?:owner[_\s]?id|admin[_\s]?id)\s*[=:]\s*[\"']?(-?\d+)", re.I)
USERNAME_RE = re.compile(r"@([A-Za-z0-9_]{4,32})")
LINK_RE = re.compile(r"(https?://t\.me/[^\s\"']+)")
PATH_RE = re.compile(r"(?:file[_\s]?path|path)\s*[=:]\s*[\"']([^\"']+)[\"']", re.I)
SESSION_RE = re.compile(r"(?:SESSION|STRING_SESSION|session_string)\s*[=:]\s*[\"']([^\"']{20,})[\"']", re.I)


def extract_from_text(text: str) -> dict:
    if not text:
        return {
            "bot_tokens": [], "bot_ids": [], "bot_usernames": [],
            "file_links": [], "file_paths": [], "file_ids": [],
            "owner_ids": [], "owner_names": [], "owner_usernames": [],
            "sessions": [], "raw_found": [],
        }
    out = {
        "bot_tokens": list(set(TOKEN_RE.findall(text))),
        "bot_ids": list(set(BOT_ID_RE.findall(text))),
        "bot_usernames": [],
        "file_links": list(set(LINK_RE.findall(text))),
        "file_paths": list(set(PATH_RE.findall(text))),
        "file_ids": [],
        "owner_ids": list(set(OWNER_ID_RE.findall(text))),
        "owner_names": [],
        "owner_usernames": list(set(USERNAME_RE.findall(text))),
        "sessions": list(set(SESSION_RE.findall(text))),
        "raw_found": [],
    }
    for m in re.finditer(r"(?:bot[_\s]?username|username)\s*[=:]\s*[\"']?@?([A-Za-z0-9_]{4,})", text, re.I):
        out["bot_usernames"].append(m.group(1))
    out["bot_usernames"] = list(set(out["bot_usernames"]))
    for m in re.finditer(r"(?:file[_\s]?id|message[_\s]?id)\s*[=:]\s*[\"']?(\d+)", text, re.I):
        out["file_ids"].append(m.group(1))
    out["file_ids"] = list(set(out["file_ids"]))
    for k, v in out.items():
        if k == "raw_found":
            continue
        if v:
            out["raw_found"].append(k)
    return out


def empty_detail(fname: str, mid: int, reason: str = "Not Available Detail") -> dict:
    return {
        "file_name": fname or "unknown",
        "message_id": mid,
        "bot_tokens": [], "bot_ids": [], "bot_usernames": [],
        "file_links": [], "file_paths": [], "file_ids": [str(mid)],
        "owner_ids": [], "owner_names": [], "owner_usernames": [],
        "sessions": [],
        "found": [],
        "not_found": [
            "bot_tokens", "bot_ids", "bot_usernames", "file_links",
            "file_paths", "owner_ids", "owner_names", "owner_usernames", "sessions"
        ],
        "note": reason,
    }


async def deep_scan_py_message(src_id: int, mid: int, fname: str, u_client=None) -> dict:
    uc = u_client or user
    detail = empty_detail(fname, mid)
    try:
        msg = await uc.get_messages(src_id, mid)
        if not msg:
            detail["note"] = "Not Available Detail — message missing"
            return detail
        cap = (msg.caption or "") + "\n" + (msg.text or "")
        extracted = extract_from_text(cap)
        is_py = (fname or "").lower().endswith(".py")
        body = ""
        if is_py and msg.document and (msg.document.file_size or 0) < 2_000_000:
            try:
                path = await uc.download_media(msg, file_name=f"/tmp/scan_{mid}.py")
                if path and os.path.exists(path):
                    with open(path, "r", encoding="utf-8", errors="ignore") as f:
                        body = f.read(500_000)
                    try:
                        os.remove(path)
                    except Exception:
                        pass
            except Exception as e:
                detail["note"] = f"download skip: {type(e).__name__}"
        if body:
            extracted2 = extract_from_text(body)
            for k in extracted:
                if k == "raw_found":
                    continue
                if isinstance(extracted[k], list):
                    extracted[k] = list(set(extracted[k] + extracted2.get(k, [])))
        for k in ("bot_tokens", "bot_ids", "bot_usernames", "file_links", "file_paths",
                  "file_ids", "owner_ids", "owner_names", "owner_usernames", "sessions"):
            detail[k] = extracted.get(k) or []
        if mid and str(mid) not in detail["file_ids"]:
            detail["file_ids"].append(str(mid))
        found, not_found = [], []
        for k in ("bot_tokens", "bot_ids", "bot_usernames", "file_links", "file_paths",
                  "file_ids", "owner_ids", "owner_names", "owner_usernames", "sessions"):
            if detail[k]:
                found.append(k)
            else:
                not_found.append(k)
        detail["found"] = found
        detail["not_found"] = not_found
        if not found:
            detail["note"] = "Not Available Detail"
        else:
            detail["note"] = f"Found: {', '.join(found)}"
        return detail
    except Exception as e:
        detail["note"] = f"Not Available Detail — {type(e).__name__}: {e}"
        return detail


def get_filename_from_raw(media) -> str:
    try:
        n = type(media).__name__
        if n == "MessageMediaDocument":
            doc = getattr(media, "document", None)
            for attr in getattr(doc, "attributes", []) or []:
                if type(attr).__name__ == "DocumentAttributeFilename":
                    return getattr(attr, "file_name", None) or "unknown"
    except Exception:
        pass
    return "unknown"


async def collect_all_doc_ids(src_id, status, start_from_id=0, job=None, u_client=None):
    """Fast REAL document count. Primary: get_chat_history filter. Fallback: Search."""
    global COUNT_CACHE
    uc = u_client or user
    target = job if job is not None else STATE
    cache_key = (int(src_id), int(start_from_id or 0))
    now = time.time()
    if cache_key in COUNT_CACHE:
        entry = COUNT_CACHE[cache_key]
        total, mids, ts = entry[0], entry[1], entry[2]
        fname_cache = entry[3] if len(entry) > 3 else {}
        if now - ts < COUNT_CACHE_TTL and mids:
            target["total_files"] = total
            target["files"] = total
            target["scanned"] = total
            target["all_mids"] = list(mids)
            target["mid_to_fname"] = dict(fname_cache)
            target["stage"] = f"cached total {total}"
            await status.flush(force=True)
            return list(mids)

    seen = set()
    ordered_newest_first = []
    target["stage"] = "counting docs (fast)"
    await status.flush(force=True)

    # ── Method A: get_chat_history — much faster for large channels ──
    try:
        consecutive_below = 0
        async for msg in uc.get_chat_history(src_id, limit=100000):
            if target.get("stop"):
                break
            if not msg.document:
                continue
            mid = msg.id
            if mid in seen:
                continue
            if start_from_id and mid < start_from_id:
                consecutive_below += 1
                # history newest-first: once past start_from, stop after a few
                if consecutive_below > 30:
                    break
                continue
            consecutive_below = 0
            seen.add(mid)
            ordered_newest_first.append(mid)
            fname_raw = getattr(msg.document, "file_name", None) or f"id:{mid}"
            target.setdefault("mid_to_fname", {})[mid] = fname_raw
            n = len(seen)
            target["scanned"] = n
            target["files"] = n
            if n % 200 == 0:
                target["stage"] = f"counting docs… {n} found"
                await status.flush(force=True)
        if ordered_newest_first:
            all_mids = list(reversed(ordered_newest_first))
            total = len(all_mids)
            target["total_files"] = total
            target["files"] = total
            target["all_mids"] = all_mids
            target["scanned"] = total
            fname_map = target.get("mid_to_fname") or {}
            COUNT_CACHE[cache_key] = (total, tuple(all_mids), time.time(), dict(fname_map))
            target["stage"] = f"count done {total}"
            await status.flush(force=True)
            return all_mids
    except FloodWait as e:
        wait = int(getattr(e, "value", 5)) + 1
        target["last_err"] = f"FLOOD WAIT {wait}s during COUNT — live timer"
        target["stage"] = f"floodwait {wait}s (count)"
        await flood_sleep(wait, status, "(count)", job=target)
    except Exception as e:
        target["last_err"] = f"history count: {type(e).__name__}: {e}"
        # fall through to Search method

    # ── Method B: Search filter (fallback) ──
    peer = await resolve_peer_with_retry(src_id, u_client=uc)
    offset_id = 0
    seen = set()
    ordered_newest_first = []
    rounds = 0
    target["stage"] = "counting docs (search)"
    await status.flush(force=True)

    while not target.get("stop"):
        rounds += 1
        try:
            r = await uc.invoke(
                Search(
                    peer=peer, q="",
                    filter=InputMessagesFilterDocument(),
                    min_date=0, max_date=0,
                    offset_id=offset_id, add_offset=0,
                    limit=100, max_id=0, min_id=0, hash=0,
                )
            )
        except FloodWait as e:
            wait = int(getattr(e, "value", 5)) + 1
            target["last_err"] = f"FLOOD WAIT {wait}s during COUNT — live timer"
            target["stage"] = f"floodwait {wait}s (count)"
            await flood_sleep(wait, status, "(count)", job=target)
            continue
        except Exception as e:
            target["last_err"] = f"count: {type(e).__name__}: {e}"
            target["stage"] = f"count error: {type(e).__name__}"
            await status.flush(force=True)
            break

        msgs = r.messages or []
        if not msgs:
            break

        for m in msgs:
            if type(m).__name__ != "Message":
                continue
            media = getattr(m, "media", None)
            if not media or type(media).__name__ != "MessageMediaDocument":
                continue
            doc = getattr(media, "document", None)
            if not doc:
                continue
            mid = m.id
            if mid in seen:
                continue
            if start_from_id and mid < start_from_id:
                continue
            seen.add(mid)
            ordered_newest_first.append(mid)
            fname_raw = get_filename_from_raw(media) or f"id:{mid}"
            target.setdefault("mid_to_fname", {})[mid] = fname_raw

        n = len(seen)
        target["scanned"] = n
        target["files"] = n
        target["stage"] = f"counting docs… {n} found"
        await status.flush(force=True)

        offset_id = msgs[-1].id
        if len(msgs) < 100:
            break

    all_mids = list(reversed(ordered_newest_first))
    total = len(all_mids)
    target["total_files"] = total
    target["files"] = total
    target["all_mids"] = all_mids
    target["scanned"] = total
    fname_map = target.get("mid_to_fname") or {}
    COUNT_CACHE[cache_key] = (total, tuple(all_mids), time.time(), dict(fname_map))
    target["stage"] = f"count done {total}"
    await status.flush(force=True)
    return all_mids


async def _copy_one_safe(src_id, tgt_id, mid, sem, job, u_client=None):
    async with sem:
        if job.get("stop"):
            return False, mid, None
        while job.get("paused") and not job.get("stop"):
            job["stage"] = "PAUSED"
            STATE["stage"] = "PAUSED"
            STATE["paused"] = True
            await asyncio.sleep(0.1)
        if job.get("stop"):
            return False, mid, None
        # 10 strong retries — zero skip policy
        for attempt in range(10):
            try:
                await copy_one(src_id, tgt_id, mid, u_client=u_client)
                return True, mid, None
            except FloodWait as e:
                wait = int(getattr(e, "value", 5)) + 3
                job["wait_until"] = time.time() + wait
                job["stage"] = f"FLOODWAIT {wait}s LIVE"
                job["last_err"] = f"FLOOD WAIT {wait}s — waiting live"
                STATE["wait_until"] = job["wait_until"]
                STATE["stage"] = job["stage"]
                STATE["last_err"] = job["last_err"]
                # 0.1s ticks — LIVE STATUS never silent during floodwait
                rem = float(wait)
                while rem > 0 and not job.get("stop"):
                    chunk = min(0.1, rem)
                    await asyncio.sleep(chunk)
                    rem -= chunk
                    job["wait_until"] = time.time() + rem
                    STATE["wait_until"] = job["wait_until"]
                    job["stage"] = f"FLOODWAIT {int(rem)}s LIVE"
                    STATE["stage"] = job["stage"]
                job["wait_until"] = 0
                STATE["wait_until"] = 0
                job["last_err"] = None
                STATE["last_err"] = None
                job["stage"] = "copying (after floodwait)"
                STATE["stage"] = job["stage"]
                continue
            except Exception as e:
                err = f"{type(e).__name__}: {e}"
                # surface first real error in live status
                if not job.get("last_err"):
                    job["last_err"] = err[:160]
                low = err.lower()
                # ONLY truly unrecoverable Telegram API errors exit immediately.
                # Everything else gets full 10 retries so no file is missed.
                if any(x in low for x in (
                    "chat_write_forbidden", "channel_private",
                    "user_banned_in_channel", "chat_admin_required",
                    "msgid_invalid",
                )):
                    return False, mid, err
                if attempt == 9:
                    return False, mid, err
                # longer backoff on later attempts
                await asyncio.sleep(0.15 * (attempt + 1))
        return False, mid, "fail"


async def scan_and_copy(src_id, tgt_id, status, start_from_id=0, job=None, u_client=None):
    """Count once (real) → concurrent TOP→BOTTOM copy → rest every 500 → deep details."""
    target = job if job is not None else STATE
    all_mids = await collect_all_doc_ids(src_id, status, start_from_id=start_from_id, job=target, u_client=u_client)
    total = len(all_mids)
    target["total_files"] = total
    target["files"] = total
    target["stage"] = f"copying 0/{total}"
    await status.flush(force=True)

    copied = 0
    failed = 0
    since_rest = 0
    details_meta = []
    # ORDERED BATCHES of BATCH_SIZE (20): IDs sorted ascending = top→bottom line-by-line
    # bulk copy_messages keeps order; fallback sequential still ordered
    total = len(all_mids)
    idx = 0
    last_status = 0.0

    while idx < total and not target.get("stop"):
        while target.get("paused") and not target.get("stop"):
            target["stage"] = "PAUSED"
            STATE["stage"] = "PAUSED"
            STATE["paused"] = True
            await status.flush(force=True)
            await asyncio.sleep(0.1)
        if target.get("stop"):
            break

        batch = all_mids[idx: idx + BATCH_SIZE]
        idx += len(batch)

        # retry whole batch on FloodWait (live timer), never skip
        attempt = 0
        batch_results = None
        while attempt < 8 and batch_results is None and not target.get("stop"):
            try:
                batch_results = await copy_batch_ordered(src_id, tgt_id, batch, u_client=u_client)
            except FloodWait as e:
                wait = int(getattr(e, "value", 5)) + 2
                target["wait_until"] = time.time() + wait
                target["stage"] = f"FLOODWAIT {wait}s LIVE"
                target["last_err"] = f"FLOOD WAIT {wait}s — live timer"
                STATE["wait_until"] = target["wait_until"]
                STATE["stage"] = target["stage"]
                STATE["last_err"] = target["last_err"]
                rem = float(wait)
                while rem > 0 and not target.get("stop"):
                    chunk = min(0.1, rem)
                    await asyncio.sleep(chunk)
                    rem -= chunk
                    target["wait_until"] = time.time() + rem
                    target["stage"] = f"FLOODWAIT {int(rem)}s LIVE"
                    STATE["wait_until"] = target["wait_until"]
                    STATE["stage"] = target["stage"]
                    await status.flush(force=True)
                target["wait_until"] = 0
                STATE["wait_until"] = 0
                target["last_err"] = None
                STATE["last_err"] = None
                target["stage"] = "copying (after floodwait)"
                STATE["stage"] = target["stage"]
                attempt += 1
            except Exception as e:
                # hard fail → sequential one-by-one for this batch
                batch_results = []
                for mid in sorted(batch):
                    try:
                        await copy_one(src_id, tgt_id, mid, u_client=u_client)
                        batch_results.append((True, mid, None))
                    except FloodWait as fe:
                        wait = int(getattr(fe, "value", 5)) + 2
                        await flood_sleep(wait, status, "", job=target)
                        try:
                            await copy_one(src_id, tgt_id, mid, u_client=u_client)
                            batch_results.append((True, mid, None))
                        except Exception as e2:
                            batch_results.append((False, mid, f"{type(e2).__name__}: {e2}"))
                    except Exception as e2:
                        batch_results.append((False, mid, f"{type(e2).__name__}: {e2}"))
                break

        if not batch_results:
            batch_results = [(False, mid, "batch fail") for mid in batch]

        fname_map = target.get("mid_to_fname") or {}
        for ok, mid, err in batch_results:
            fname = fname_map.get(mid, f"id:{mid}")
            if ok:
                copied += 1
                since_rest += 1
                live_n = target.get("live_names") or []
                live_n = (live_n + [fname])[-6:]
                target["live_names"] = live_n
                STATE["live_names"] = live_n
            else:
                failed += 1
                target["errors"] = target.get("errors", 0) + 1
                if err and not target.get("last_err"):
                    target["last_err"] = str(err)[:160]
            details_meta.append((mid, fname))
            target["copied"] = copied
            target["failed"] = failed
            target["stage"] = f"copying {copied + failed}/{total}"
            target["last_file"] = fname
            STATE["copied"] = copied
            STATE["failed"] = failed
            STATE["stage"] = target["stage"]
            STATE["last_err"] = target.get("last_err")
            STATE["last_file"] = fname
            STATE["total_files"] = total
            STATE["scanned"] = total

        await status.flush(force=True)

        if since_rest >= REST_EVERY:
            await rest_sleep(REST_SEC, status, job=target)
            since_rest = 0
            target["stage"] = f"copying {copied}/{total}"
            await status.flush(force=True)

    # deep details — scan every file for tokens / bot IDs / paths
    n_det = len(details_meta)
    target["stage"] = f"deep scanning 0/{n_det}…"
    await status.flush(force=True)
    details = []
    for i, (mid, pre_fname) in enumerate(details_meta):
        if target.get("stop"):
            break
        # use the filename we already captured during copy (no extra API call needed)
        fname = pre_fname or "unknown"
        # for .py files we still download to extract body
        target["last_file"] = f"detail {i+1}/{n_det}  {fname}"
        target["stage"]     = f"deep scanning {i+1}/{n_det}"
        det = await deep_scan_py_message(src_id, mid, fname, u_client=u_client)
        details.append(det)
        if i % 5 == 0:
            await status.flush(force=True)
        await asyncio.sleep(0.01)

    target["details"] = details
    try:
        payload = {
            "generated_at": now_str(),
            "source": target.get("src_title"),
            "receiver": target.get("recv_title"),
            "path_query": target.get("path_query"),
            "total_files": total,
            "copied": copied,
            "failed": failed,
            "files": details,
        }
        DETAILS_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        target["last_err"] = f"json write: {e}"

    return copied, failed, details


async def find_message_by_path(src_ident, query: str, u_client=None, status_chat_id=None, status_msg_id=None):
    """
    Ultra-fast path finder.
    Accepts:
      - pure message id  (digits)
      - t.me/c/xxx/ID or t.me/username/ID  → extract message id, direct get
      - exact filename
      - filename substring / caption text match
    Returns (message_id, filename). Live status + flood timer while scanning.
    """
    uc = u_client or user
    raw_q = (query or "").strip()
    if not raw_q:
        return None, None

    async def _live(txt: str):
        if status_chat_id and status_msg_id:
            await _http_edit(status_chat_id, status_msg_id, txt)
        elif status_chat_id:
            await _http_send(status_chat_id, txt)

    # ── 1) pure message id ──
    if re.fullmatch(r"\d+", raw_q):
        mid = int(raw_q)
        try:
            chat, _ = await ensure_member(src_ident, u_client=uc)
            msg = await uc.get_messages(chat.id, mid)
            if msg and msg.document:
                fname = getattr(msg.document, "file_name", None) or f"id:{mid}"
                return mid, fname
        except Exception:
            pass
        return mid, f"id:{mid}"

    # ── 2) t.me link → extract message id ──
    m_link = re.search(
        r"(?:https?://)?t\.me/(?:c/(\d+)|([A-Za-z0-9_]+))/(\d+)",
        raw_q,
    )
    if m_link:
        mid = int(m_link.group(3))
        try:
            chat, _ = await ensure_member(src_ident, u_client=uc)
            msg = await uc.get_messages(chat.id, mid)
            if msg:
                fname = "unknown"
                if msg.document:
                    fname = getattr(msg.document, "file_name", None) or f"id:{mid}"
                elif msg.caption or msg.text:
                    fname = (msg.caption or msg.text or "")[:80]
                await _live(f"✅ LINK → message id {mid}\n📄 {fname}")
                return mid, fname
        except Exception as e:
            await _live(f"⚠ link id {mid} get fail: {type(e).__name__} — still using id")
            return mid, f"id:{mid}"

    # ── 3) filename / text deep scan (TOP→BOTTOM via history, then Search) ──
    q = raw_q.lower()
    q = re.sub(r"^\s*\[\d+/\d+\]\s*", "", q)
    q = re.sub(r"\s+\d+(?:\.\d+)?\s*[kmg]b\s*$", "", q, flags=re.I)
    q = q.strip()
    # strip [n/n] prefix style from channel captions
    q = re.sub(r"^\[\d+/\d+\]\s*", "", q).strip()
    q_name = q.replace("\\", "/").split("/")[-1].strip()
    q_full = q.replace("\\", "/")
    # also accept "filename.ext  size" patterns
    q_name2 = re.sub(r"\s+\d+(?:\.\d+)?\s*[kmg]?b\s*$", "", q_name, flags=re.I).strip()

    try:
        chat, _ = await ensure_member(src_ident, u_client=uc)
        peer = await resolve_peer_with_retry(chat.id, u_client=uc)
    except Exception as e:
        print("find path resolve:", e)
        return None, None

    scanned = 0
    best_mid = None
    best_fname = None
    best_score = 0

    def _score(fname: str, caption: str) -> int:
        fl = (fname or "").lower()
        tl = (caption or "").lower()
        if not fl and not tl:
            return 0
        if fl and (fl == q_name or fl == q_name2 or fl == q or fl == q_full or fname == raw_q):
            return 100
        if fl and q_name2 and fl == q_name2:
            return 100
        if fl and q_name2 and q_name2 in fl:
            return 90
        if fl and q_name and q_name in fl:
            return 85
        if tl and (q in tl or q_name in tl or q_name2 in tl):
            return 80
        if fl and q_stem_match(fl, q_name2 or q_name):
            return 70
        return 0

    # Method A: get_chat_history TOP of channel (oldest first by reversing collect)
    try:
        await _live(f"🔎 PATH SCAN TOP→ (history)…\nquery: {q_name2 or q_name}")
        # history = newest first; keep scanning so we can pick OLDEST (smallest id) 100% match
        async for msg in uc.get_chat_history(chat.id, limit=60000):
            if STATE.get("stop"):
                break
            if not msg.document:
                continue
            fname = getattr(msg.document, "file_name", None) or ""
            cap = (msg.caption or "") + " " + (msg.text or "")
            scanned += 1
            sc = _score(fname, cap)
            if sc > best_score or (sc >= 100 and best_mid and msg.id < best_mid):
                best_score = sc
                best_mid = msg.id
                best_fname = fname or f"id:{msg.id}"
            if scanned % 400 == 0:
                await _live(
                    f"🔎 PATH SCAN TOP→… {scanned} checked\n"
                    f"best: {best_fname or '—'} id={best_mid} ({best_score})"
                )
        if best_score >= 100:
            await _live(f"✅ 100% MATCH (TOP)\n📄 {best_fname}\n🆔 {best_mid}\n🔎 {scanned}")
            return best_mid, best_fname
        if best_score >= 70:
            await _live(f"✅ MATCH score={best_score}\n📄 {best_fname}\n🆔 {best_mid}")
            return best_mid, best_fname
    except FloodWait as e:
        wait = int(getattr(e, "value", 5)) + 1
        rem = wait
        while rem > 0 and not STATE.get("stop"):
            await _live(f"⏸ FLOOD WAIT {rem}s PATH SCAN — LIVE\nscanned: {scanned}")
            await asyncio.sleep(1)
            rem -= 1
    except Exception as e:
        print("path history:", e)

    # Method B: Search documents
    offset_id = 0
    for round_n in range(800):
        if STATE.get("stop"):
            break
        try:
            r = await uc.invoke(
                Search(
                    peer=peer, q="",
                    filter=InputMessagesFilterDocument(),
                    min_date=0, max_date=0,
                    offset_id=offset_id, add_offset=0,
                    limit=100, max_id=0, min_id=0, hash=0,
                )
            )
        except FloodWait as e:
            wait = int(getattr(e, "value", 5)) + 1
            rem = wait
            while rem > 0 and not STATE.get("stop"):
                await _live(
                    f"⏸ FLOOD WAIT {rem}s PATH SCAN — LIVE TIMER\n"
                    f"🔎 scanned: {scanned}"
                )
                await asyncio.sleep(1)
                rem -= 1
            continue
        except Exception as e:
            print("find scan:", e)
            break

        msgs = r.messages or []
        if not msgs:
            break

        for m in msgs:
            if type(m).__name__ != "Message":
                continue
            media = getattr(m, "media", None)
            if not media or type(media).__name__ != "MessageMediaDocument":
                continue
            fname = get_filename_from_raw(media) or ""
            msg_text = (getattr(m, "message", None) or "") or ""
            if isinstance(msg_text, bytes):
                try:
                    msg_text = msg_text.decode("utf-8", errors="ignore")
                except Exception:
                    msg_text = ""
            scanned += 1
            sc = _score(fname, msg_text)
            if sc > best_score or (sc >= 100 and best_mid and m.id < best_mid):
                best_score = sc
                best_mid = m.id
                best_fname = fname or f"id:{m.id}"

        if round_n % 2 == 0:
            await _live(
                f"🔎 PATH SCAN TOP→… {scanned} files\n"
                f"best: {best_fname or '—'} id={best_mid} ({best_score})"
            )
        offset_id = msgs[-1].id
        if len(msgs) < 100:
            break

    if best_score >= 100:
        await _live(f"✅ 100% MATCH (TOP)\n📄 {best_fname}\n🆔 {best_mid}")
        return best_mid, best_fname
    if best_score >= 70:
        await _live(f"✅ MATCH score={best_score}\n📄 {best_fname}\n🆔 {best_mid}")
        return best_mid, best_fname

    await _live(f"❌ no match after {scanned} files\nquery: {raw_q[:80]}")
    return None, None


def q_stem_match(fl: str, q_name: str) -> bool:
    if not fl or not q_name:
        return False
    stem = q_name.rsplit(".", 1)[0] if "." in q_name else q_name
    return bool(stem) and stem in fl


async def clone_job(status, src_ident, tgt_id, start_from_id=0, job=None, uid=0):
    target = job if job is not None else STATE
    # get per-user client (uses added-user session if available, else global)
    u_client = await get_user_client(uid)

    target["stage"] = "resolving source"
    await status.flush(force=True)
    try:
        chat, _ = await ensure_member(src_ident, u_client=u_client)
    except Exception as e:
        target["last_err"] = f"join src: {type(e).__name__}: {e}"
        target["stage"] = "join failed"
        await status.flush(force=True)
        target["task"] = None
        return

    target["src_title"] = chat.title or str(chat.id)

    # ── Receiver resolution: try per-user client first, then global user, then bot ──
    real_tgt = tgt_id
    recv_ok = False
    try:
        tgt_chat, _ = await ensure_member(tgt_id, u_client=u_client)
        real_tgt = tgt_chat.id
        target["recv_title"] = tgt_chat.title or str(real_tgt)
        recv_ok = True
    except Exception as e1:
        # per-user client failed — try global user if it's different
        if u_client is not user:
            try:
                tgt_chat, _ = await ensure_member(tgt_id, u_client=user)
                real_tgt = tgt_chat.id
                target["recv_title"] = tgt_chat.title or str(real_tgt)
                u_client = user          # switch to global for this job
                recv_ok = True
            except Exception:
                pass
        if not recv_ok:
            # last resort: try bot (works if bot is admin in receiver)
            try:
                tgt_chat = await bot.get_chat(tgt_id)
                real_tgt = tgt_chat.id
                target["recv_title"] = tgt_chat.title or str(real_tgt)
                recv_ok = True
                target["last_err"] = (
                    "⚠️ User session can't access receiver — running bot-only copy. "
                    "Add the user account to the receiver channel as admin for faster copy."
                )
            except Exception as e2:
                # nothing can see the receiver — STOP cleanly
                target["last_err"] = (
                    f"❌ Cannot access receiver channel. "
                    f"Add bot as admin AND ensure the user account has joined the receiver. "
                    f"Error: {type(e2).__name__}: {e2}"
                )
                target["stage"] = "error: receiver inaccessible"
                await status.flush(force=True)
                target["task"] = None
                return

    tgt_id = real_tgt

    # ── Quick probe — one file test before mass copy ──
    target["stage"] = "probe"
    await status.flush(force=True)
    try:
        peer_src = await resolve_peer_with_retry(chat.id, u_client=u_client)
        r = await u_client.invoke(
            Search(
                peer=peer_src, q="",
                filter=InputMessagesFilterDocument(),
                min_date=0, max_date=0,
                offset_id=0, add_offset=0,
                limit=1, max_id=0, min_id=0, hash=0,
            )
        )
        msgs = r.messages or []
        if msgs:
            probe_mid = msgs[0].id
            try:
                await copy_one(chat.id, tgt_id, probe_mid, u_client=u_client)
                target["last_err"] = None   # probe passed — clear any stale error
                target["stage"] = "probe OK — full power"
            except Exception as pe:
                target["last_err"] = f"PROBE FAIL: {type(pe).__name__}: {pe}"
                target["stage"] = "probe fail — check bot+user admin on receiver"
    except Exception as e:
        if not target.get("last_err"):
            target["last_err"] = f"probe setup: {type(e).__name__}: {e}"

    target["stage"] = "copying (full power)"
    await status.flush(force=True)

    details = []
    try:
        copied, failed, details = await scan_and_copy(
            chat.id, tgt_id, status, start_from_id=start_from_id, job=target, u_client=u_client
        )
    except Exception as e:
        target["last_err"] = f"job: {type(e).__name__}: {e}"
        target["errors"] = target.get("errors", 0) + 1
        copied, failed = target.get("copied", 0), target.get("failed", 0)

    target["stage"] = "done — sending details"
    await status.flush(force=True)

    chat_id = status.chat_id
    try:
        lines = [
            f"{B('DEEP SCAN COMPLETE')}",
            f"══════════════════════════════",
            f"✅ Copied: {copied}",
            f"❌ Failed: {failed}",
            f"📁 Total scanned: {len(details)}",
            f"🕐 {now_str()}",
            f"──────────────────────────────",
        ]
        for d in details[:40]:
            fn = d.get("file_name") or "?"
            note = d.get("note") or "Not Available Detail"
            found = d.get("found") or []
            if found:
                lines.append(f"• {fn}\n  → {note}")
            else:
                lines.append(f"• {fn}\n  → Not Available Detail")
        if len(details) > 40:
            lines.append(f"… +{len(details)-40} more in JSON")
        text = "\n".join(lines)
        await bot.send_message(chat_id, text[:4000])
        if DETAILS_FILE.exists():
            await bot.send_document(
                chat_id,
                str(DETAILS_FILE),
                caption=D("ALL DETAILS JSON\nPy files · tokens · ids · paths · owners"),
            )
    except Exception as e:
        target["last_err"] = f"report: {e}"

    target["stage"] = "done"
    await status.flush(force=True)
    target["task"] = None
    target["stop"] = False


# ═════════════════════════════════════════════════════════════════
#  v11 · RESTRICTED SOURCE → RECEIVE
#  Download via user session, re-upload to receive. Forum topics
#  (folders) of the source group are mirrored BY NAME in the receive
#  group, files go top → bottom, nothing skipped, live status.
# ═════════════════════════════════════════════════════════════════
import shutil
from pyrogram import raw as _raw, enums as _enums

import inspect
try:
    TMP_RES = Path(os.environ.get("RES_TMP_DIR") or os.path.expanduser("~/srd_tmp"))
    TMP_RES.mkdir(parents=True, exist_ok=True)
except Exception:
    TMP_RES = Path(__file__).parent / "srd_tmp"
    TMP_RES.mkdir(parents=True, exist_ok=True)
for _old in TMP_RES.iterdir():            # leftovers of an earlier crash
    shutil.rmtree(_old, ignore_errors=True) if _old.is_dir() else None


import collections
LEDGER_FILE = DATA_FILE.parent / "res_ledger.jsonl"


def _ledger_load(key):
    done = set()
    try:
        with open(LEDGER_FILE, encoding="utf-8") as f:
            for line in f:
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                if r.get("k") == key:
                    done.add(int(r.get("m", 0)))
    except FileNotFoundError:
        pass
    except Exception as e:
        print("ledger:", e)
    return done


def _ledger_add(key, mid):
    try:
        with open(LEDGER_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps({"k": key, "m": int(mid)}) + "\n")
    except Exception:
        pass


def _ledger_clear(key):
    try:
        if LEDGER_FILE.exists():
            keep = [ln for ln in open(LEDGER_FILE, encoding="utf-8") if f'"k": "{key}"' not in ln]
            open(LEDGER_FILE, "w", encoding="utf-8").writelines(keep)
    except Exception:
        pass


def _topic_kw(client, topic_id):
    """Send-into-topic kwargs that work on pyrogram 2.0.x AND pyrofork."""
    if not topic_id or int(topic_id) == 1:      # 1 = General → plain message
        return {}
    kw = {"reply_to_message_id": int(topic_id)}
    try:
        if "message_thread_id" in inspect.signature(client.send_document).parameters:
            kw["message_thread_id"] = int(topic_id)
    except Exception:
        pass
    return kw

# kind -> (data key, chat type, side, human name)
_RES_KIND = {
    "SRC_GROUP": ("res_source_groups",    "group",   "source",  "Source Group"),
    "SRC_CHAN":  ("res_source_channels",  "channel", "source",  "Source Channel"),
    "RCV_GROUP": ("res_receive_groups",   "group",   "receive", "Receive Group"),
    "RCV_CHAN":  ("res_receive_channels", "channel", "receive", "Receive Channel"),
}
# normalized button label -> (action, kind)
_RES_BTN = {
    "RES SOURCE GROUP":            ("add",  "SRC_GROUP"),
    "RES SOURCE GROUP LIST":       ("list", "SRC_GROUP"),
    "RES REMOVE SOURCE GROUP":     ("rm",   "SRC_GROUP"),
    "RES SOURCE CHANNEL":          ("add",  "SRC_CHAN"),
    "RES SOURCE CHANNEL LIST":     ("list", "SRC_CHAN"),
    "RES REMOVE SOURCE CHANNEL":   ("rm",   "SRC_CHAN"),
    "RES RECEIVE GROUP":           ("add",  "RCV_GROUP"),
    "RES RECEIVE GROUP LIST":      ("list", "RCV_GROUP"),
    "RES REMOVE RECEIVE GROUP":    ("rm",   "RCV_GROUP"),
    "RES RECEIVE CHANNEL":         ("add",  "RCV_CHAN"),
    "RES RECEIVE CHANNEL LIST":    ("list", "RCV_CHAN"),
    "RES REMOVE RECEIVE CHANNEL":  ("rm",   "RCV_CHAN"),
}
# full-name owner buttons whose old handlers still match the old short labels
OWNER_ALIAS = {
    "ADD RECEIVER CHANNEL":     "📥 ADD RECEIVER",
    "RECEIVER CHANNEL LIST":    "📤 RECEIVER LIST",
    "REMOVE RECEIVER CHANNEL":  "🗑 RM RECEV CHAN",
    "REMOVE SOURCE CHANNEL":    "🗑 RM SOURCE CHAN",
    "REMOVE USER":              "🗑 RM USER",
}
OWNER_ACTION_LABELS = set(_RES_BTN) | set(OWNER_ALIAS) | {
    "ADD USER", "USER LIST", "ADD ADMIN", "REMOVE ADMIN", "ADMIN LIST",
}
_NAV_WORDS = ("BACKTOHOME", "SOURCELIST", "ADDSOURCE", "FORWARDNOW", "MYRECEVCHAN",
              "FORWARDRUN", "OWNERPANEL", "ADMINPANEL", "LIVESTATUS")


# ═════════ account-first access: the ACCOUNT's own membership is used, nobody auto-joins ═════════
AUTO_JOIN = os.environ.get("RES_AUTO_JOIN", "0") == "1"   # restricted forward never joins unless you switch this on
_WARM_AT: Dict[int, float] = {}


async def warm_peers(client, force=False):
    """Load ALL dialogs of the account → every private channel/group it already joined is found by ID."""
    key = id(client)
    if not force and time.time() - _WARM_AT.get(key, 0) < 600:
        return 0
    n = 0
    try:
        async for _ in client.get_dialogs():
            n += 1
    except FloodWait as e:
        await asyncio.sleep(e.value + 1)
    except Exception as e:
        print("warm_peers:", type(e).__name__, e)
    _WARM_AT[key] = time.time()
    return n


async def _chat_from_invite(client, invite):
    """Invite link → chat, ONLY when the account is already a member (nothing is joined)."""
    try:
        r = await client.invoke(_raw.functions.messages.CheckChatInvite(hash=str(invite).lstrip("+")))
    except Exception:
        return None
    if type(r).__name__ != "ChatInviteAlready":
        return None                      # not a member → we do not join
    ch = getattr(r, "chat", None)
    if ch is None:
        return None
    cid = int("-100" + str(ch.id)) if type(ch).__name__ == "Channel" else -int(ch.id)
    await warm_peers(client, force=True)
    try:
        return await client.get_chat(cid)
    except Exception:
        return None


def _idents_for(*raws):
    out = []
    for raw in raws:
        if raw in (None, ""):
            continue
        if isinstance(raw, int):
            out.append(raw)
            continue
        txt = str(raw).strip()
        mm = re.search(r"t\.me/c/(\d+)", txt)
        out.append(int("-100" + mm.group(1)) if mm else resolve_target(txt))
    return out


async def res_get_chat(client, ident, link=None):
    """Find a chat through the account's own dialogs. Joins ONLY if AUTO_JOIN is switched on."""
    idents = _idents_for(ident, link)
    last = None
    for attempt in (0, 1):
        for idt in idents:
            if isinstance(idt, str) and idt.startswith("+"):
                ch = await _chat_from_invite(client, idt)
                if ch is not None:
                    return ch
                continue
            try:
                return await client.get_chat(idt)
            except FloodWait as e:
                await asyncio.sleep(e.value + 1)
            except Exception as e:
                last = e
        if attempt == 0:
            await warm_peers(client, force=True)
    if AUTO_JOIN:
        for idt in idents:
            if isinstance(idt, str):
                try:
                    chat, _ = await ensure_member(idt, u_client=client)
                    return chat
                except Exception as e:
                    last = e
    raise RuntimeError("tera account is chat ka member nahi hai (ya mil nahi raha) — account se join/open karo, "
                       "phir 📂 ACCOUNT CHATS se add karo" + (f" [{type(last).__name__}]" if last else ""))


async def _account_chats(client, ctype, flt="", cap=40):
    """Chats the account is in, filtered by type (group/channel) and optional name part."""
    out = []
    async for dlg in client.get_dialogs():
        c = dlg.chat
        tname = str(getattr(getattr(c, "type", None), "name", getattr(c, "type", ""))).upper()
        if ctype == "channel" and not tname.endswith("CHANNEL"):
            continue
        if ctype == "group" and not (tname.endswith("GROUP") or tname.endswith("SUPERGROUP")):
            continue
        title = getattr(c, "title", None) or ""
        if flt and flt.lower() not in title.lower():
            continue
        flags = ("🔒" if getattr(c, "has_protected_content", False) else "") + ("👑" if getattr(c, "is_creator", False) else "")
        out.append({"id": c.id, "title": title or str(c.id), "flags": flags})
        if len(out) >= cap:
            break
    return out


def _clear_flow():
    STATE["awaiting"] = None
    STATE["new_user"] = {}


def _is_nav_label(pu: str, pu_ns: str) -> bool:
    if pu in ("BACK", "MENU", "STATUS", "CANCEL", "BACK TO HOME", "BACK TO OWNER PANEL",
              "BACK TO MAIN MENU", "FORWARD GROUP", "FORWARD CHANNEL",
              "RES DIRECT FORWARD", "RES PATH FORWARD", "RES RESUME FORWARD", "RES FRESH FORWARD"):
        return True
    if pu in OWNER_ACTION_LABELS or pu in _RES_BTN:
        return True
    return len(pu) < 28 and any(w in pu_ns for w in _NAV_WORDS)


def _fmt_size(n) -> str:
    n = float(n or 0)
    for u in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f}{u}"
        n /= 1024
    return f"{n:.1f}TB"


def _res_label(it: dict, mark: str = "") -> str:
    title = (it.get("title") or "?").replace("|", "/")[:24]
    fl = it.get("flags") or ""
    return f"{mark}{title}{(' ' + fl) if fl else ''} | {it.get('id')}"


def _res_items_kb(items, selected_id=None, bottom=None, mark_default="📁 "):
    """One tappable button per saved group/channel. Label carries the id → exact match."""
    rows = []
    for it in items[:40]:
        sel = selected_id is not None and str(it.get("id")) == str(selected_id)
        rows.append([btn(_res_label(it, "✅ " if sel else mark_default), "success" if sel else "primary")])
    if bottom:
        rows.extend(bottom)
    return kb(make_markup(rows))


def _res_pick(items, text: str):
    for s in re.findall(r"-?\d{6,}", text or ""):
        for it in items:
            if str(it.get("id")) == s:
                return it
    return None


def _res_list_text(title: str, lst, sel) -> str:
    sid = str((sel or {}).get("id", ""))
    lines = [f"{'✅' if str(it.get('id')) == sid else '•'} {it.get('title')}\n   {it.get('id')}" for it in lst]
    return (f"📋 {title.upper()} LIST\n══════════════════\n" + "\n".join(lines) +
            "\n══════════════════\nButton tap karo → select")


async def res_ui(m, uid: int, text: str, pu: str, pu_ns: str) -> bool:
    """Owner-panel navigation + restricted source/receive UI. True = handled."""
    awaiting = STATE.get("awaiting") or ""
    bc = lambda: kb(make_markup(_bc_rows()))
    back_main = lambda: [[btn("🔙 BACK TO MAIN MENU", "primary")]]

    # ── navigation ──────────────────────────────────────────────
    if pu == "BACK TO OWNER PANEL":
        _clear_flow()
        await m_reply(m, "👑 OWNER PANEL", panel_kb_for(uid))
        return True
    if pu == "CANCEL":
        _clear_flow()
        await m_reply(m, "❌ Process cancel ho gaya\nOwner Panel pe wapas aa gaye.", panel_kb_for(uid))
        return True
    if pu == "BACK TO MAIN MENU":
        _clear_flow()
        await m_reply(m, "🏠 MENU", main_kb(uid))
        return True

    # a different menu button while a res prompt is waiting → drop the prompt
    if awaiting.startswith("res_") and _is_nav_label(pu, pu_ns):
        if not (awaiting.split(":")[0] in ("res_pick", "res_rm", "res_fwdpick", "res_acc") and re.search(r"-?\d{6,}", text)):
            STATE["awaiting"] = None
            awaiting = ""

    # ── owner panel: add / list / remove ────────────────────────
    act = _RES_BTN.get(pu)
    if act:
        action, kind = act
        key, ctype, side, human = _RES_KIND[kind]
        if action in ("add", "rm") and uid != OWNER_ID:
            await m_reply(m, "❌ Owner only", main_kb(uid))
            return True
        d = load()
        lst = d.get(key) or []
        if action == "add":
            STATE["awaiting"] = f"res_add:{kind}"
            await m_reply(
                m,
                f"{'🔒' if side == 'source' else '📥'} RES {human.upper()}\n══════════════════\n"
                f"📂 ACCOUNT CHATS dabao → tere account ke chats me se tap karke chuno (private bhi).\n"
                f"Ya link / @username / channel ID (-100…) bhejo.\n"
                f"🔐 Bot kahin join nahi karega — tera account jisme member hai wahi use hoga.",
                kb(make_markup([[btn("📂 ACCOUNT CHATS", "success")]] + _bc_rows())),
            )
            return True
        if not lst:
            STATE["awaiting"] = None
            await m_reply(m, f"📭 Koi {human} add nahi hua.\nPehle 'RES {human.upper()}' button se add karo.",
                          panel_kb_for(uid))
            return True
        if action == "list":
            sel = d.get("selected_res_source" if side == "source" else "selected_res_receive") or {}
            STATE["awaiting"] = f"res_pick:{kind}"
            await m_reply(m, _res_list_text(human, lst, sel),
                          _res_items_kb(lst, sel.get("id"), bottom=_bc_rows()))
            return True
        STATE["awaiting"] = f"res_rm:{kind}"          # action == "rm"
        await m_reply(m, f"🗑 RES REMOVE {human.upper()}\n══════════════════\nJisko remove karna hai uss button pe tap karo",
                      _res_items_kb(lst, None, bottom=_bc_rows(), mark_default="🗑 "))
        return True

    # ── input states ────────────────────────────────────────────
    if awaiting.startswith("res_add:"):
        kind = awaiting.split(":", 1)[1]
        key, ctype, side, human = _RES_KIND[kind]
        link = text.strip()
        acc_kb = lambda: kb(make_markup([[btn("📂 ACCOUNT CHATS", "success")]] + _bc_rows()))
        if pu == "ACCOUNT CHATS":
            await warm_peers(user)
            items = await _account_chats(user, ctype)
            if not items:
                await m_reply(m, "📭 Account me is type ka koi chat nahi mila.\nAccount se pehle join karo ya link/ID bhejo.", acc_kb())
                return True
            STATE["awaiting"] = f"res_acc:{kind}"
            STATE["res_acc_items"] = items
            await m_reply(m, f"📂 ACCOUNT CHATS ({'channels' if ctype == 'channel' else 'groups'})\n══════════════════\n"
                             f"Tap karke add karo.  🔒 = forward band (restricted) · 👑 = tu owner\nNaam ka kuch hissa type karo to filter ho jayega.",
                          _res_items_kb(items, None, bottom=_bc_rows(), mark_default="📂 "))
            return True
        if not re.search(r"t\.me/|^@|^\+|^-?\d{5,}$|^[A-Za-z][A-Za-z0-9_]{4,}$", link):
            await m_reply(m, "❌ Link / @username / -100id bhejo (ya BACK TO OWNER PANEL / CANCEL dabao)", bc())
            return True
        await m_reply(m, "⏳ Account se access check ho raha hai…", bc())
        try:
            chat = await res_get_chat(user, link)
        except Exception as e:
            await m_reply(m, f"❌ {e}\n\n📂 ACCOUNT CHATS dabao ya sahi link/ID bhejo, ya CANCEL.", acc_kb())
            return True
        d = load()
        lst = [x for x in (d.get(key) or []) if str(x.get("id")) != str(chat.id)]
        entry = {"id": chat.id, "title": chat.title or str(chat.id), "link": link, "type": ctype}
        lst.append(entry)
        d[key] = lst
        save(d)
        STATE["awaiting"] = None
        await m_reply(m, f"✅ {human} saved\n{entry['title']}\n{chat.id}\n\nAb uski LIST button se select karo.",
                      panel_kb_for(uid))
        return True

    if awaiting.startswith("res_acc:"):
        kind = awaiting.split(":", 1)[1]
        key, ctype, side, human = _RES_KIND[kind]
        items = STATE.get("res_acc_items") or []
        it = _res_pick(items, text)
        if it is None:                                   # typed text = name filter
            sub = await _account_chats(user, ctype, text.strip())
            if not sub:
                await m_reply(m, "❌ Is naam ka chat nahi mila — dusra naam type karo ya button tap karo",
                              _res_items_kb(items, None, bottom=_bc_rows(), mark_default="📂 "))
                return True
            STATE["res_acc_items"] = sub
            await m_reply(m, f"🔎 {len(sub)} chat mile — tap karo",
                          _res_items_kb(sub, None, bottom=_bc_rows(), mark_default="📂 "))
            return True
        d = load()
        lst = [x for x in (d.get(key) or []) if str(x.get("id")) != str(it["id"])]
        lst.append({"id": it["id"], "title": it["title"], "link": None, "type": ctype, "flags": it.get("flags", "")})
        d[key] = lst
        save(d)
        STATE["awaiting"] = None
        await m_reply(m, f"✅ {human} saved (account se)\n{it['title']}\n{it['id']}\n\nAb uski LIST button se select karo.",
                      panel_kb_for(uid))
        return True

    if awaiting.startswith("res_pick:"):
        kind = awaiting.split(":", 1)[1]
        key, ctype, side, human = _RES_KIND[kind]
        d = load()
        lst = d.get(key) or []
        it = _res_pick(lst, text)
        if not it:
            await m_reply(m, "❌ Match nahi hua — list ka button tap karo",
                          _res_items_kb(lst, None, bottom=_bc_rows()))
            return True
        d["selected_res_source" if side == "source" else "selected_res_receive"] = dict(it, type=ctype)
        save(d)
        hint = ("Ab BACK TO OWNER PANEL → BACK TO MAIN MENU → FORWARD GROUP / FORWARD CHANNEL dabao."
                if side == "source" else "Receive select ho gaya.")
        await m_reply(m, f"✅ {side.upper()} SELECTED\n{it.get('title')}\n{it.get('id')}\n\n{hint}",
                      _res_items_kb(lst, it.get("id"), bottom=_bc_rows()))
        return True

    if awaiting.startswith("res_rm:"):
        kind = awaiting.split(":", 1)[1]
        key, ctype, side, human = _RES_KIND[kind]
        d = load()
        lst = d.get(key) or []
        it = _res_pick(lst, text)
        if not it:
            await m_reply(m, "❌ Match nahi hua — remove ke liye button tap karo",
                          _res_items_kb(lst, None, bottom=_bc_rows(), mark_default="🗑 "))
            return True
        lst2 = [x for x in lst if str(x.get("id")) != str(it.get("id"))]
        d[key] = lst2
        sk = "selected_res_source" if side == "source" else "selected_res_receive"
        if str((d.get(sk) or {}).get("id")) == str(it.get("id")):
            d[sk] = None
        save(d)
        if lst2:
            await m_reply(m, f"✅ Removed: {it.get('title')}\nAur remove karna ho to tap karo.",
                          _res_items_kb(lst2, None, bottom=_bc_rows(), mark_default="🗑 "))
        else:
            STATE["awaiting"] = None
            await m_reply(m, f"✅ Removed: {it.get('title')}\nAb list khaali hai.", panel_kb_for(uid))
        return True

    # ── main menu: FORWARD GROUP / FORWARD CHANNEL ──────────────
    if pu in ("FORWARD GROUP", "FORWARD CHANNEL"):
        kind = "RCV_GROUP" if pu.endswith("GROUP") else "RCV_CHAN"
        key, ctype, side, human = _RES_KIND[kind]
        d = load()
        src = d.get("selected_res_source")
        if not src:
            await m_reply(m, "❌ Pehle source select karo\n\nOwner Panel → RES SOURCE GROUP LIST "
                             "(ya RES SOURCE CHANNEL LIST) → button tap karo.", main_kb(uid))
            return True
        lst = d.get(key) or []
        if not lst:
            await m_reply(m, f"📭 Koi {human} add nahi hai\n\nOwner Panel → RES {human.upper()} se add karo.",
                          main_kb(uid))
            return True
        STATE["awaiting"] = f"res_fwdpick:{kind}"
        rsel = (d.get("selected_res_receive") or {}).get("id")
        await m_reply(m, f"🔒 {pu}\n══════════════════\n📥 Source: {src.get('title')}\n\n"
                         f"Ab Receive select karo (button tap):",
                      _res_items_kb(lst, rsel, bottom=back_main(), mark_default="📤 "))
        return True

    if awaiting.startswith("res_fwdpick:"):
        kind = awaiting.split(":", 1)[1]
        key, ctype, side, human = _RES_KIND[kind]
        d = load()
        lst = d.get(key) or []
        it = _res_pick(lst, text)
        if not it:
            await m_reply(m, "❌ Receive ka button tap karo",
                          _res_items_kb(lst, None, bottom=back_main(), mark_default="📤 "))
            return True
        d["selected_res_receive"] = dict(it, type=ctype)
        save(d)
        STATE["awaiting"] = None
        src = d.get("selected_res_source") or {}
        await m_reply(
            m,
            f"✅ Receive: {it.get('title')}\n📥 Source: {src.get('title')}\n\nMode select karo:",
            kb(make_markup([
                [btn("🚀 RES DIRECT FORWARD", "success"), btn("📍 RES PATH FORWARD", "primary")],
                [btn("🔙 BACK TO MAIN MENU", "primary")],
            ])),
        )
        return True

    if pu in ("RES RESUME FORWARD", "RES FRESH FORWARD"):
        d = load()
        src, rcv = d.get("selected_res_source"), d.get("selected_res_receive")
        if not src or not rcv:
            await m_reply(m, "❌ Source ya Receive select nahi hai", main_kb(uid))
            return True
        if pu == "RES FRESH FORWARD":
            _ledger_clear(f"{src['id']}>{rcv['id']}")
        STATE["awaiting"] = None
        await start_res_forward(m, uid, src, rcv, 0, resume=True)
        return True

    if pu in ("RES DIRECT FORWARD", "RES PATH FORWARD"):
        d = load()
        src, rcv = d.get("selected_res_source"), d.get("selected_res_receive")
        if not src or not rcv:
            await m_reply(m, "❌ Source ya Receive select nahi hai", main_kb(uid))
            return True
        if pu == "RES DIRECT FORWARD":
            STATE["awaiting"] = None
            done_n = len(_ledger_load(f"{src['id']}>{rcv['id']}"))
            if done_n:
                await m_reply(
                    m,
                    f"♻️ Is source → receive me {done_n} files pehle forward ho chuki hain.\n\n"
                    f"▶️ RES RESUME FORWARD — sirf bachi hui files bhejega (duplicate nahi)\n"
                    f"🔁 RES FRESH FORWARD — sab files dubara bhejega",
                    kb(make_markup([
                        [btn("▶️ RES RESUME FORWARD", "success"), btn("🔁 RES FRESH FORWARD", "danger")],
                        [btn("🔙 BACK TO MAIN MENU", "primary")],
                    ])),
                )
                return True
            await start_res_forward(m, uid, src, rcv, 0, resume=True)
            return True
        STATE["awaiting"] = "res_path"
        await m_reply(m, "📍 RES PATH FORWARD\n══════════════════\n"
                         "File ka message link (t.me/c/…/123) ya message ID ya file ka naam bhejo.\n"
                         "Uss file se START hoga — uske upar wali files skip.",
                      kb(make_markup(back_main())))
        return True

    if awaiting == "res_path":
        d = load()
        src, rcv = d.get("selected_res_source"), d.get("selected_res_receive")
        if not src or not rcv:
            STATE["awaiting"] = None
            await m_reply(m, "❌ Source ya Receive select nahi hai", main_kb(uid))
            return True
        q = text.strip()
        mid = 0
        mm = re.search(r"t\.me/[^\s]*/(\d+)(?:\?[^\s]*)?$", q)
        if mm:
            mid = int(mm.group(1))
        elif re.fullmatch(r"\d{1,12}", q):
            mid = int(q)
        else:
            await m_reply(m, "🔎 File dhundh raha hoon…", kb(make_markup(back_main())))
            await warm_peers(user)
            try:
                mid, _fn = await find_message_by_path(int(src["id"]), q, u_client=user)
            except Exception:
                mid = None
            if not mid:
                await m_reply(m, "❌ File nahi mili — message link / ID / sahi file ka naam bhejo",
                              kb(make_markup(back_main())))
                return True
        STATE["awaiting"] = None
        await start_res_forward(m, uid, src, rcv, int(mid), path_query=q)
        return True

    return False


# ───────────────────────── engine ─────────────────────────
_RES_MEDIA_ATTRS = ("document", "video", "audio", "voice", "video_note", "animation", "sticker", "photo")


def _res_mtype(msg) -> str:
    for t in _RES_MEDIA_ATTRS:
        if getattr(msg, t, None):
            return t
    return "text"


def _safe_name(name: str) -> str:
    name = re.sub(r'[\\/:*?"<>|\x00-\x1f]', "_", name or "").strip(" .") or "file"
    if len(name) > 150:
        base, ext = os.path.splitext(name)
        name = base[:150 - len(ext)] + ext
    return name


def _res_fname(msg) -> str:
    d = getattr(msg, "document", None)
    if d and getattr(d, "file_name", None):
        return d.file_name
    v = getattr(msg, "video", None)
    if v and getattr(v, "file_name", None):
        return v.file_name
    a = getattr(msg, "audio", None)
    if a:
        return getattr(a, "file_name", None) or f"audio_{msg.id}.mp3"
    an = getattr(msg, "animation", None)
    if an:
        return getattr(an, "file_name", None) or f"gif_{msg.id}.mp4"
    if getattr(msg, "photo", None):
        return f"photo_{msg.id}.jpg"
    if getattr(msg, "voice", None):
        return f"voice_{msg.id}.ogg"
    if getattr(msg, "video_note", None):
        return f"videonote_{msg.id}.mp4"
    if getattr(msg, "sticker", None):
        return f"sticker_{msg.id}.webp"
    return f"file_{msg.id}"


def _set_extra(job):
    lines = []
    if job.get("resumed_skip"):
        lines.append(f"♻️ RESUME: {job['resumed_skip']} files pehle ho chuki (skip)")
    if job.get("account"):
        lines.append(f"👤 ACCOUNT: {job['account']}  (auto-join OFF)")
    if job.get("cur_folder"):
        lines.append(f"📁 FOLDER: {job['cur_folder']}  ({job.get('folder_idx', 0)}/{job.get('folders_total', 0)})")
    pr = job.get("prog") or {}
    if pr.get("dl"):
        lines.append(pr["dl"])
    if pr.get("ul"):
        lines.append(pr["ul"])
    job["extra"] = lines


def _extra_block(d) -> str:
    ex = (d or {}).get("extra") or []
    return ("\n".join(ex) + "\n") if ex else ""


def _mk_prog(job, status, tag, fname):
    last = [0.0]
    icon = "⬇ DOWNLOAD" if tag == "dl" else "⬆ UPLOAD"

    async def _p(cur, tot):
        now = time.time()
        if now - last[0] < 1.0 and cur < tot:
            return
        last[0] = now
        pct = int(cur * 100 / tot) if tot else 0
        job.setdefault("prog", {})[tag] = f"{icon}: {fname[:30]} {pct}% ({_fmt_size(cur)}/{_fmt_size(tot)})"
        _set_extra(job)
        await status.flush()
    return _p


async def _res_retry(job, status, fn, what, tries=3):
    attempt = 0
    while True:
        if job.get("stop"):
            raise RuntimeError("stopped")
        try:
            return await fn()
        except FloodWait as e:
            wait = int(getattr(e, "value", 5)) + 1
            job["wait_until"] = time.time() + wait
            job["stage"] = f"floodwait {wait}s ({what})"
            await status.flush(force=True)
            await asyncio.sleep(wait)
            job["wait_until"] = 0
        except asyncio.CancelledError:
            raise
        except Exception as e:
            attempt += 1
            job["last_err"] = f"{what}: {type(e).__name__}: {e}"[:160]
            if attempt >= tries:
                raise
            await asyncio.sleep(1.5 * attempt)


async def _list_topics(client, chat_id):
    """[(topic_id, title)] oldest→newest. None = chat is not a forum."""
    peer = await client.resolve_peer(chat_id)
    if not isinstance(peer, _raw.types.InputPeerChannel):
        return None
    ch = _raw.types.InputChannel(channel_id=peer.channel_id, access_hash=peer.access_hash)
    out, offset_date, offset_id, offset_topic = {}, 0, 0, 0
    for _ in range(60):
        try:
            r = await client.invoke(_raw.functions.channels.GetForumTopics(
                channel=ch, offset_date=offset_date, offset_id=offset_id,
                offset_topic=offset_topic, limit=100))
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
            continue
        except Exception:
            if not out:
                return None
            break
        topics = [t for t in (r.topics or []) if isinstance(t, _raw.types.ForumTopic)]
        if not topics:
            break
        for t in topics:
            out[t.id] = t.title
        if len(r.topics) < 100:
            break
        last = r.topics[-1]
        offset_topic = getattr(last, "id", 0)
        offset_id = getattr(last, "top_message", 0)
        msgs = {mm.id: mm for mm in (r.messages or [])}
        offset_date = getattr(msgs.get(offset_id), "date", 0) or 0
    return sorted(out.items())


async def _topic_mids(client, chat_id, topic_id, start_from_id, job):
    """All media messages of ONE forum topic (folder): [(message_id, size)] ascending."""
    peer = await client.resolve_peer(chat_id)
    found, offset_id = {}, 0
    for _ in range(3000):
        if job.get("stop"):
            break
        try:
            r = await client.invoke(_raw.functions.messages.GetReplies(
                peer=peer, msg_id=int(topic_id), offset_id=offset_id, offset_date=0,
                add_offset=0, limit=100, max_id=0, min_id=0, hash=0))
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
            continue
        msgs = list(getattr(r, "messages", None) or [])
        if not msgs:
            break
        for mm in msgs:
            media = getattr(mm, "media", None)
            if (isinstance(mm, _raw.types.Message)
                    and isinstance(media, (_raw.types.MessageMediaDocument, _raw.types.MessageMediaPhoto))
                    and mm.id >= (start_from_id or 0)):
                found[mm.id] = int(getattr(getattr(media, "document", None), "size", 0) or 0)
        offset_id = msgs[-1].id
        if len(msgs) < 100 or (start_from_id and offset_id < start_from_id):
            break
    return sorted(found.items())


async def _create_topic(client, chat_id, title):
    peer = await client.resolve_peer(chat_id)
    ch = _raw.types.InputChannel(channel_id=peer.channel_id, access_hash=peer.access_hash)
    r = await client.invoke(_raw.functions.channels.CreateForumTopic(
        channel=ch, title=(title or "Folder")[:128], random_id=int.from_bytes(os.urandom(7), "big")))
    for upd in (getattr(r, "updates", None) or []):
        msg = getattr(upd, "message", None)
        if msg is not None and isinstance(getattr(msg, "action", None), _raw.types.MessageActionTopicCreate):
            return msg.id
    # fallback: find it by title
    ex = await _list_topics(client, chat_id) or []
    for tid, t in ex:
        if (t or "").strip().lower() == (title or "").strip().lower()[:128]:
            return tid
    return None


def _res_msg_size(msg) -> int:
    for t in _RES_MEDIA_ATTRS:
        obj = getattr(msg, t, None)
        if obj:
            return int(getattr(obj, "file_size", 0) or 0)
    return 0


async def _res_build_plan(job, status, src_id, start_from_id):
    """Ordered plan [{folder, tid, mid, size}] — folders by creation order, files top → bottom."""
    job["stage"] = "scanning source…"
    await status.flush(force=True)
    job["scan_failed"] = []
    topics = None
    try:
        topics = await _list_topics(user, src_id)
    except Exception as e:
        job["last_err"] = f"topics: {type(e).__name__}: {e}"[:160]
    plan = []
    if topics:
        job["is_forum"] = True
        job["folders_total"] = len(topics)
        for n, (tid, title) in enumerate(topics, 1):
            if job.get("stop"):
                break
            job["folder_idx"], job["cur_folder"] = n, title
            job["stage"] = f"scanning folder {n}/{len(topics)}: {title}"
            _set_extra(job)
            await status.flush(force=True)
            mids, err = [], None
            for attempt in (1, 2, 3):                    # a folder must never be skipped silently
                try:
                    mids = await _topic_mids(user, src_id, tid, start_from_id, job)
                    err = None
                    break
                except Exception as e:
                    err = e
                    await asyncio.sleep(1.5 * attempt)
            if err is not None and tid != 1:
                job["scan_failed"].append(title)
                job["last_err"] = f"folder '{title[:30]}': {type(err).__name__}: {err}"[:160]
            for mid, sz in mids:
                plan.append({"folder": title, "tid": tid, "mid": mid, "size": sz})
            job["total_files"] = job["files"] = job["scanned"] = len(plan)
            if mids:
                job["folder_stats"][title] = [0, 0, len(mids)]
        job["folders_total"] = len({it["folder"] for it in plan}) or len(topics)
        return plan
    found, scanned = {}, 0
    async for msg in user.get_chat_history(src_id):
        if job.get("stop"):
            break
        if start_from_id and msg.id < start_from_id:
            break
        scanned += 1
        if _res_mtype(msg) != "text":
            found[msg.id] = _res_msg_size(msg)
        if scanned % 200 == 0:
            job["scanned"] = scanned
            job["total_files"] = job["files"] = len(found)
            job["stage"] = f"scanning… {scanned} messages · {len(found)} files"
            await status.flush()
    for mid in sorted(found):
        plan.append({"folder": None, "tid": None, "mid": mid, "size": found[mid]})
    job["total_files"] = job["files"] = job["scanned"] = len(plan)
    return plan


def _tkey(t):
    return re.sub(r"\s+", " ", (t or "").strip()).casefold()


def _tcore(t):
    """letters/digits only — emoji or punctuation differences between the two groups do not matter"""
    return re.sub(r"[\W_]+", "", (t or "").casefold())


async def _res_prepare_recv(job, status, rcv_id, folders):
    """Map source folder names → topic ids in the receive group. Same name → reuse it; else create."""
    mapping, forum, creator, ex_list = {}, False, None, []
    for cl in (user, bot):
        try:
            ex = await _list_topics(cl, rcv_id)
        except Exception:
            continue
        if ex is None:
            continue
        forum, creator, ex_list = True, cl, ex
        break
    fmap = job["folder_map"] = []
    if not forum:
        job["last_err"] = "ℹ️ Receive group me topics (forum) off hain — har folder ka header message bhej kar files aayengi"
        return mapping, False
    exact = {_tkey(t): (tid, t) for tid, t in ex_list}
    core = {}
    for tid, t in ex_list:
        core.setdefault(_tcore(t), (tid, t))
    order = [creator] + [c for c in (user, bot) if c is not creator]
    for name in folders:
        hit, how = exact.get(_tkey(name)), "same"
        if hit is None and _tcore(name):
            hit, how = core.get(_tcore(name)), "similar"
        if hit is not None:
            mapping[name] = hit[0]
            fmap.append((name, hit[1], how))
            continue
        tid = None
        for cl in order:
            try:
                tid = await _create_topic(cl, rcv_id, name)
            except FloodWait as e:
                await asyncio.sleep(e.value + 1)
                try:
                    tid = await _create_topic(cl, rcv_id, name)
                except Exception:
                    tid = None
            except Exception as e:
                job["last_err"] = f"topic '{name[:30]}': {type(e).__name__}: {e}"[:160]
            if tid:
                break
        mapping[name] = tid
        if tid:
            exact[_tkey(name)] = (tid, name)
            core.setdefault(_tcore(name), (tid, name))
        fmap.append((name, name if tid else None, "new" if tid else "failed"))
        await status.flush()
    return mapping, True


async def _res_fetch(job, status, src_id, it):
    msg = await user.get_messages(src_id, int(it["mid"]))
    if not msg or getattr(msg, "empty", False):
        raise RuntimeError("message deleted / not found")
    if _res_mtype(msg) == "text":
        raise RuntimeError("no media in message")
    fname = _safe_name(_res_fname(msg))
    tdir = TMP_RES / uuid.uuid4().hex[:10]
    tdir.mkdir(parents=True, exist_ok=True)
    try:
        path = await user.download_media(
            msg, file_name=str(tdir / fname), progress=_mk_prog(job, status, "dl", fname))
    except BaseException:
        shutil.rmtree(tdir, ignore_errors=True)
        raise
    if not path:
        shutil.rmtree(tdir, ignore_errors=True)
        raise RuntimeError("download returned nothing")
    return msg, str(path), tdir, fname


async def _res_send_one(client, chat_id, msg, path, topic_id, prog):
    kw = {"chat_id": chat_id, "progress": prog}
    kw.update(_topic_kw(client, topic_id))
    mt = _res_mtype(msg)
    capkw = {}
    cap = getattr(msg, "caption", None) or ""
    if cap and mt not in ("video_note", "sticker"):
        capkw = {"caption": cap[:1024], "parse_mode": _enums.ParseMode.DISABLED}
        ents = getattr(msg, "caption_entities", None)
        if ents:
            capkw["caption_entities"] = ents
    if mt == "video":
        v = msg.video
        return await client.send_video(video=path, duration=v.duration or 0, width=v.width or 0,
                                       height=v.height or 0, supports_streaming=True, **capkw, **kw)
    if mt == "audio":
        a = msg.audio
        return await client.send_audio(audio=path, duration=a.duration or 0, performer=a.performer,
                                       title=a.title, **capkw, **kw)
    if mt == "photo":
        return await client.send_photo(photo=path, **capkw, **kw)
    if mt == "voice":
        return await client.send_voice(voice=path, duration=msg.voice.duration or 0, **capkw, **kw)
    if mt == "video_note":
        vn = msg.video_note
        return await client.send_video_note(video_note=path, duration=vn.duration or 0,
                                            length=vn.length or 1, **kw)
    if mt == "animation":
        an = msg.animation
        return await client.send_animation(animation=path, duration=an.duration or 0, width=an.width or 0,
                                           height=an.height or 0, **capkw, **kw)
    if mt == "sticker":
        return await client.send_sticker(sticker=path, **kw)
    return await client.send_document(document=path, force_document=True, **capkw, **kw)


async def _res_upload(job, msg, path, rcv_id, topic_id, prog):
    last = None
    for client in list(job["uploaders"]):
        try:
            r = await _res_send_one(client, rcv_id, msg, path, topic_id, prog)
            if client is not job["uploaders"][0]:
                job["uploaders"].remove(client)
                job["uploaders"].insert(0, client)
            return r
        except FloodWait:
            raise
        except Exception as e:
            last = e
    raise last or RuntimeError("upload failed")


async def _res_text(rcv_id, text, topic_id=None):
    for cl in (user, bot):
        try:
            return await cl.send_message(rcv_id, text, **_topic_kw(cl, topic_id))
        except Exception:
            continue
    return None


async def _res_job(job, status, src, rcv, start_from_id, chat_id, uid):
    src_id, rcv_id = int(src["id"]), int(rcv["id"])
    job["uploaders"] = [user, bot]
    job["folder_stats"] = {}
    job["stage"] = "checking access"
    await status.flush(force=True)

    # ---- the ACCOUNT's own access (nothing is joined) ----
    job["stage"] = "loading account chats"
    await status.flush(force=True)
    await warm_peers(user)
    try:
        me = await user.get_me()
        job["account"] = ("@" + me.username) if getattr(me, "username", None) else (getattr(me, "first_name", None) or str(me.id))
    except Exception:
        job["account"] = "account"
    _set_extra(job)
    try:
        c = await res_get_chat(user, src_id, src.get("link"))
        src_id = int(c.id)
    except Exception as e:
        job["last_err"] = f"❌ Source: {e}"[:160]
        job["stage"] = "error: source inaccessible (account member nahi)"
        job["errors"] += 1
        return
    job["src_title"] = getattr(c, "title", None) or job.get("src_title")

    rc = None
    try:
        rc = await res_get_chat(user, rcv_id, rcv.get("link"))
        rcv_id = int(rc.id)
    except Exception as e_user:
        try:
            rc = await bot.get_chat(rcv_id)        # bot can post only if it is already inside the receive chat
        except Exception:
            job["last_err"] = f"❌ Receive: {e_user}"[:160]
            job["stage"] = "error: receive inaccessible"
            job["errors"] += 1
            return
    job["recv_title"] = getattr(rc, "title", None) or job.get("recv_title")

    # ---- scan (real count) ----
    plan = await _res_build_plan(job, status, src_id, start_from_id)
    ledger_key = f"{src_id}>{rcv_id}"
    if job.get("resume"):
        done_ids = _ledger_load(ledger_key)
        if done_ids:
            n0 = len(plan)
            plan = [it for it in plan if it["mid"] not in done_ids]
            job["resumed_skip"] = n0 - len(plan)
            cnt = collections.Counter(it["folder"] or "—" for it in plan)
            job["folder_stats"] = {k: [0, 0, v] for k, v in cnt.items()}
            _set_extra(job)
    total = len(plan)
    job["total_files"] = job["files"] = job["scanned"] = total
    if job.get("stop"):
        job["stage"] = "stopped"
        return
    if not total:
        job["stage"] = "done — source me koi file nahi mili"
        await status.flush(force=True)
        await bot.send_message(chat_id, "ℹ️ Source me forward karne layak file nahi mili.", reply_markup=main_kb(uid))
        return

    folders = []
    for it in plan:
        if it["folder"] and it["folder"] not in folders:
            folders.append(it["folder"])
    topic_map, recv_forum = {}, False
    if folders:
        job["stage"] = f"preparing {len(folders)} folders in receive"
        await status.flush(force=True)
        topic_map, recv_forum = await _res_prepare_recv(job, status, rcv_id, folders)
        fm = job.get("folder_map") or []
        if fm:
            ico = {"same": "✅", "similar": "🟰", "new": "🆕", "failed": "⚠️"}
            ml = ["📁 FOLDER MAP", "══════════════════"]
            for sn, rn, how in fm[:40]:
                ml.append(f"{ico.get(how, '•')} {sn[:34]} → " + (rn[:34] if rn else "topic nahi ban paya (General me jayegi)"))
            ml += ["══════════════════", "✅ mila · 🟰 milta-julta mila · 🆕 naya bana · ⚠️ fail"]
            await bot.send_message(chat_id, "\n".join(ml)[:3900])

    fstat = job["folder_stats"]
    sent_header = set()
    failed_items = []
    copied = failed = 0
    started_names = []

    async def _prep(it):
        return await _res_retry(job, status, lambda: _res_fetch(job, status, src_id, it),
                                f"download id:{it['mid']}")

    async def _process(it, fetch):
        pack = None
        label = f"id:{it['mid']}"
        try:
            pack = await fetch
            msg, path, tdir, fname = pack
            label = fname
            folder = it["folder"]
            tid = topic_map.get(folder) if folder else None
            if folder and (not recv_forum or not tid) and folder not in sent_header:
                sent_header.add(folder)
                await _res_text(rcv_id, f"📁 FOLDER: {folder}")
            await _res_retry(
                job, status,
                lambda: _res_upload(job, msg, path, rcv_id, tid, _mk_prog(job, status, "ul", fname)),
                f"upload {fname}")
            return True, None, label
        except asyncio.CancelledError:
            raise
        except Exception as e:
            return False, f"{type(e).__name__}: {e}", label
        finally:
            if pack:
                shutil.rmtree(pack[2], ignore_errors=True)
            job.setdefault("prog", {}).pop("dl", None) if False else None

    def _count(it, ok, label, retry=False):
        nonlocal copied, failed
        folder = it["folder"]
        st = fstat.setdefault(folder or "—", [0, 0, 0])
        if ok:
            copied += 1
            st[0] += 1
            _ledger_add(ledger_key, it["mid"])
            if retry:
                failed -= 1
                st[1] -= 1
            names = (job.get("live_names") or [])[-4:] + [f"{folder}/{label}" if folder else label]
            job["live_names"] = names
            STATE["live_names"] = names
        elif not retry:
            failed += 1
            st[1] += 1
        job["copied"], job["failed"] = copied, failed
        job["last_file"] = f"[{folder}] {label}" if folder else label
        STATE.update(copied=copied, failed=failed, last_file=job["last_file"],
                     total_files=total, scanned=total, stage=job.get("stage"))

    # files are DOWNLOADED a few steps ahead (several at once), but UPLOADED strictly one by one, in order
    WINDOW = max(1, int(os.environ.get("RES_PREFETCH", "3")))
    BUDGET = int(float(os.environ.get("RES_BUFFER_GB", "1.5")) * 1024 ** 3)
    pending = collections.deque()
    next_i, inflight = 0, 0

    def _launch():
        nonlocal next_i, inflight
        while next_i < total and len(pending) < WINDOW:
            sz = int(plan[next_i].get("size") or 0)
            if pending and inflight + sz > BUDGET:
                break
            pending.append((asyncio.create_task(_prep(plan[next_i])), sz))
            inflight += sz
            next_i += 1

    _launch()
    try:
        for i, it in enumerate(plan):
            if job.get("stop"):
                break
            while job.get("paused") and not job.get("stop"):
                job["stage"] = "⏸ PAUSED"
                await status.flush()
                await asyncio.sleep(0.5)
            if job.get("stop"):
                break
            cur, sz_cur = pending.popleft()
            _launch()
            folder = it["folder"]
            job["cur_folder"] = folder
            job["folder_idx"] = (folders.index(folder) + 1) if folder in folders else 0
            job["stage"] = f"copying {i + 1}/{total}" + (f" · folder {job['folder_idx']}/{len(folders)}" if folder else "")
            _set_extra(job)
            await status.flush()
            ok, err, label = await _process(it, cur)
            inflight -= sz_cur
            _launch()
            job["prog"] = {}
            if not ok and job.get("stop"):
                break
            if not ok:
                failed_items.append(it)
                job["errors"] = job.get("errors", 0) + 1
                job["last_err"] = f"{label}: {err}"[:160]
            _count(it, ok, label)
            await status.flush()
            if ok and copied % REST_EVERY == 0:
                await rest_sleep(REST_SEC, status, job=job)

        # ---- retry pass: nothing should stay missing ----
        if failed_items and not job.get("stop"):
            for it in list(failed_items):
                if job.get("stop"):
                    break
                job["stage"] = f"retry missing files ({len(failed_items)} left)"
                await status.flush(force=True)
                ok, err, label = await _process(it, _prep(it))
                job["prog"] = {}
                if ok:
                    failed_items.remove(it)
                    _count(it, True, label, retry=True)
                else:
                    job["last_err"] = f"{label}: {err}"[:160]
                await status.flush()
    finally:
        for t_, _sz in list(pending):
            if not t_.done():
                t_.cancel()
            try:
                res = await t_
                shutil.rmtree(res[2], ignore_errors=True)
            except BaseException:
                pass

    job["cur_folder"] = None
    job["prog"] = {}
    _set_extra(job)
    job["stage"] = "stopped" if job.get("stop") else "✅ DONE"
    await status.flush(force=True)

    # ---- final report ----
    lines = [
        "✅ RES FORWARD " + ("STOPPED" if job.get("stop") else "DONE"),
        "══════════════════",
        f"📥 Source : {job.get('src_title')}",
        f"📤 Receive: {job.get('recv_title')}",
        f"✅ Copied : {copied} / {total}",
        f"❌ Failed : {len(failed_items)}",
    ]
    if job.get("resumed_skip"):
        lines.append(f"♻️ Resume : {job['resumed_skip']} files pehle ho chuki thi (skip)")
    if job.get("scan_failed"):
        lines.append("⚠️ Scan fail (dobara chalao): " + ", ".join(x[:25] for x in job["scan_failed"][:8]))
    if folders:
        lines.append(f"📁 Folders: {len(folders)}")
        for fn_, (ok_, bad_, tot_) in list(fstat.items())[:40]:
            lines.append(f"  • {fn_[:40]} — {ok_}/{tot_}" + (f"  ({bad_} failed)" if bad_ else ""))
    if failed_items:
        lines.append("──────────────────")
        lines.append("❌ MISSING FILES:")
        for it in failed_items[:25]:
            lines.append(f"  • [{it['folder'] or '—'}] id:{it['mid']}")
    await bot.send_message(chat_id, "\n".join(lines)[:3900], reply_markup=main_kb(uid))


async def start_res_forward(m, uid, src, rcv, start_from_id=0, path_query=None, resume=False):
    """Launch a restricted forward job (download via user session → upload to receive)."""
    job_id = new_job_id()
    mode = f"from id:{start_from_id}" if start_from_id else "TOP → BOTTOM (sab files)"
    job = {
        "job_id": job_id, "stage": "starting", "status_msg_id": None, "chat_id": m.chat.id,
        "src_title": src.get("title"), "recv_title": rcv.get("title"),
        "total": 0, "files": 0, "copied": 0, "failed": 0, "errors": 0, "scanned": 0, "total_files": 0,
        "last_err": None, "wait_until": 0.0, "rest_until": 0.0, "started": time.time(),
        "task": None, "stop": False, "paused": False, "start_from_id": start_from_id,
        "live_names": [], "last_file": None, "details": [],
        "path_query": path_query or mode, "all_mids": [],
        "src_ident": int(src["id"]), "tgt_id": int(rcv["id"]),
        "res": True, "owner_uid": uid, "extra": [], "prog": {}, "folder_stats": {},
        "src_link": src.get("link"), "rcv_link": rcv.get("link"), "resume": resume,
    }
    JOBS[job_id] = job
    STATE["selected_job_id"] = job_id
    STATE.update(total=0, files=0, copied=0, failed=0, errors=0, scanned=0, total_files=0,
                 last_err=None, wait_until=0, rest_until=0, stop=False, paused=False,
                 started=time.time(), recv_title=rcv.get("title"), live_names=[],
                 last_file=None, path_query=job["path_query"], job_id=job_id)

    raw_msg = await bot.send_message(m.chat.id, "⏳ Res forward start ho raha hai…",
                                     reply_markup=job_detail_kb(job_id))
    mid = (raw_msg.get("result") or {}).get("message_id") if isinstance(raw_msg, dict) else getattr(raw_msg, "id", None)
    job["status_msg_id"] = mid
    status = Status(m.chat.id, mid, job_id=job_id)

    async def _ticker():
        while True:
            await asyncio.sleep(1.0)
            try:
                await status.flush()
            except Exception:
                pass

    async def _run():
        tick = asyncio.create_task(_ticker())
        try:
            await _res_job(job, status, src, rcv, start_from_id, m.chat.id, uid)
        except asyncio.CancelledError:
            job["stage"] = "stopped"
        except Exception as e:
            job["last_err"] = f"{type(e).__name__}: {e}"[:160]
            job["stage"] = "error"
            job["errors"] = job.get("errors", 0) + 1
        finally:
            tick.cancel()
            job["task"] = None
            try:
                await status.flush(force=True)
            except Exception:
                pass

    job["task"] = asyncio.create_task(_run())
    await bot.send_message(
        m.chat.id,
        "🚀 Res forward start!\n"
        "1) Real count — folders + files\n"
        "2) Folder = Topic: receive me usi naam ka folder\n"
        "3) Top → bottom, line by line, koi missing nahi (retry pass)\n"
        "4) Live status har 1s refresh\n"
        f"5) Har {REST_EVERY} files ke baad {REST_SEC}s rest\n"
        "Controls niche buttons pe",
        reply_markup=job_detail_kb(job_id),
    )




def get_receiver_id():
    d = load()
    sel = d.get("selected_receiver")
    if sel and sel.get("id"):
        return int(sel["id"]), sel.get("title") or str(sel["id"])
    return DEFAULT_TARGET, str(DEFAULT_TARGET)


def get_source_ident():
    d = load()
    sel = d.get("selected_source")
    if not sel:
        return None
    return sel.get("resolved") or sel.get("id") or resolve_target(sel.get("link") or "")


def new_job_id() -> str:
    return uuid.uuid4().hex


def find_job_by_short(short: str) -> Optional[str]:
    if not short:
        return None
    short = short.lower().strip()
    for jid in JOBS:
        jid_l = jid.lower()
        if jid_l.startswith(short) or jid_l[:8] == short[:8] or jid_l[:len(short)] == short:
            return jid
    for jid in JOBS:
        if short in jid.lower():
            return jid
    return None


# ---------- handlers ----------
@bot.on_message(filters.command("start") & filters.private)
async def cmd_start(_, m: Message):
    if not is_allowed(m.from_user.id):
        await m.reply_text("unauthorized")
        return
    STATE["chat_id"] = m.chat.id
    await m.reply_text(
        f"{B('FORWARD BOT v10 FULL POWER')}\n"
        f"══════════════════════════════\n"
        f"🟢 ADD SOURCE — channel link\n"
        f"🔵 SOURCE LIST — select source\n"
        f"🔴 FORWARD NOW — direct / path\n"
        f"🟢 MY RECEV CHAN — multi receive control\n"
        f"🔵 FORWARD RUN — all running jobs\n"
        f"🔒 FORWARD GROUP / CHANNEL — restricted source → receive (folders kept)\n"
        f"🔴 OWNER PANEL — receivers + admins + restricted lists\n\n"
        f"Live 1s refresh · FloodWait timer · Rest 50s / 500 files\n"
        f"Real document count · Multi job · Deep scan JSON",
        reply_markup=main_kb(m.from_user.id),
    )


@bot.on_message(filters.private & filters.text & ~filters.command(["start"]))
async def on_text(_, m: Message):
    if not is_allowed(m.from_user.id):
        return
    uid = m.from_user.id
    text = (m.text or "").strip()
    pu = plain_upper(text)
    # compact form for reliable matching (no spaces)
    pu_ns = re.sub(r"[^A-Z0-9_]", "", pu)

    awaiting = STATE.get("awaiting")

    # ── v11 layer: owner-panel BACK/CANCEL, full-name buttons, restricted source/receive ──
    if pu in ("ADD SOURCE", "SOURCE LIST", "FORWARD NOW", "STATUS", "MY RECEV CHAN", "FORWARD RUN",
              "MENU", "OWNER PANEL", "ADMIN PANEL", "FORWARD GROUP", "FORWARD CHANNEL"):
        OWNER_CTX[m.chat.id] = False
    if pu in OWNER_ACTION_LABELS:
        OWNER_CTX[m.chat.id] = True
    _alias = OWNER_ALIAS.get(pu)
    if _alias:
        text = _alias
        pu = plain_upper(text)
        pu_ns = re.sub(r"[^A-Z0-9_]", "", pu)
    if await res_ui(m, uid, text, pu, pu_ns):
        return
    awaiting = STATE.get("awaiting")

    # ----- ALWAYS intercept main menu / control buttons first (clear any stuck awaiting) -----
    MENU_KEYS = (
        "BACKTOHOME", "SOURCELIST", "ADDDSOURCE", "ADDSOURCE", "FORWARDNOW",
        "STATUS", "MYRECEVCHAN", "FORWARDRUN", "OWNERPANEL", "ADMINPANEL",
        "MENU", "DIRECTFORWARD", "PATHFORWARD", "START", "STOP", "PAUSE",
        "RESUME", "RESTART", "DELETE", "LIVESTATUS",
    )
    is_menu_btn = any(k in pu_ns for k in MENU_KEYS) or pu.strip() in (
        "BACK", "🔙 BACK", "BACK TO HOME", "MENU", "🏠 MENU",
        "SOURCE LIST", "📋 SOURCE LIST", "FORWARD NOW", "⚡ FORWARD NOW",
        "STATUS", "📊 STATUS", "MY RECEV CHAN", "FORWARD RUN",
        "OWNER PANEL", "ADMIN PANEL", "PATH FORWARD", "DIRECT FORWARD",
        "START", "STOP", "PAUSE", "RESUME", "RESTART", "DELETE", "LIVE STATUS",
    )
    if is_menu_btn and awaiting in ("path_id", "path_confirm", "pick_source", "pick_receiver_fwd", "source_link"):
        # clear stuck path/awaiting so button actually works
        STATE["awaiting"] = None
        awaiting = None

    # ----- BACK TO HOME always first -----
    if "BACKTOHOME" in pu_ns or ("BACK" in pu and "HOME" in pu) or pu.strip() in ("BACK", "🔙 BACK", "BACK TO HOME"):
        STATE["awaiting"] = None
        await m.reply_text(B("MENU"), reply_markup=main_kb(uid))
        return

    # ----- LIVE STATUS — always fresh full page + keyboard -----
    if "LIVESTATUS" in pu_ns or (pu.strip() in ("LIVE STATUS", "📊 LIVE STATUS") or ("LIVE" in pu and "STATUS" in pu)):
        jid = STATE.get("selected_job_id")
        if not jid or jid not in JOBS:
            for k, j in JOBS.items():
                if j.get("task") and not (hasattr(j["task"], "done") and j["task"].done()):
                    jid = k
                    break
            if not jid and JOBS:
                jid = list(JOBS.keys())[-1]
        if jid and jid in JOBS:
            STATE["selected_job_id"] = jid
            j = JOBS[jid]
            now_t = time.time()
            up = int(now_t - j.get("started", now_t)) if j.get("started") else 0
            speed = (j.get("copied", 0) / up) if up > 0 else 0.0
            total_fixed = j.get("total_files") or j.get("files") or 0
            remaining = max(0, total_fixed - j.get("copied", 0) - j.get("failed", 0))
            eta = int(remaining / speed) if speed > 0.1 else 0
            names = j.get("live_names") or []
            name_line = " | ".join(names[-6:]) if names else "—"
            last_f = j.get("last_file") or "—"
            path_q = j.get("path_query") or "—"
            api_link = f"https://t.me/{BOT_USERNAME}" if BOT_USERNAME else "—"
            bot_token_short = (BOT_TOKEN[:12] + "…") if BOT_TOKEN else "—"
            fw = ""
            if j.get("wait_until", 0) > now_t:
                rem = int(j["wait_until"] - now_t)
                fw = f"\n⏸ FLOOD WAIT: {rem}s LIVE"
            rest_line = ""
            if j.get("rest_until", 0) > now_t:
                rem = int(j["rest_until"] - now_t)
                rest_line = f"\n💤 REST: {rem}s left (after every 500 files)"
            err_line = ""
            if j.get("last_err"):
                err_line = f"\n⚠️ ERROR: {str(j['last_err'])[:120]}"
            pause_line = "⏸ PAUSED — Resume dabao\n" if j.get("paused") else ""
            pct = (j.get("copied", 0) / total_fixed * 100) if total_fixed else 0.0
            bar_len = 12
            filled = int(bar_len * pct / 100) if total_fixed else 0
            bar = "█" * filled + "░" * (bar_len - filled)
            progress = f"{j.get('copied', 0)}/{total_fixed}"
            # full FULL POWER PREMIUM format — same as Status.flush()
            txt = (
                f"⚡ {B('LIVE STATUS')}  ·  v10 FULL POWER PREMIUM\n"
                f"══════════════════════════════\n"
                f"🔗 API LINK: {api_link}\n"
                f"🔑 BOT TOKEN: {bot_token_short}\n"
                f"🕐 LIVE TIME: {now_str()}\n"
                f"📍 LIVE UPDATES: {j.get('stage') or 'idle'}\n"
                f"📥 Source: {j.get('src_title') or '—'}\n"
                f"📤 Receive: {j.get('recv_title') or '—'}\n"
                f"📂 Path / Start: {path_q}\n"
                f"──────────────────────────────\n"
                f"📁 ALL FILES: {total_fixed}\n"
                f"🔎 SCANNED: {j.get('scanned', 0)}\n"
                f"✅ COPIES: {j.get('copied', 0)} / {total_fixed}\n"
                f"❌ FAILS: {j.get('failed', 0)}\n"
                f"⚠ ERRORS: {j.get('errors', 0)}\n"
                f"📊 PROGRESS: [{bar}] {pct:.1f}%\n"
                f"⏱ ELAPSED: {up}s\n"
                f"⏳ ETA: {eta}s\n"
                f"🚀 SPEED: {speed:.1f} files/s\n"
                f"──────────────────────────────\n"
                f"📄 LIVE FILE: {last_f}\n"
                f"🏷 LIVE NAMES: {name_line}\n"
                f"{_extra_block(j)}"
                f"📌 LIVE DETAILS: {progress} done · left {remaining}\n"
                f"──────────────────────────────\n"
                f"{pause_line}{fw}{rest_line}{err_line}\n"
                f"⏹ STOP | ⏸ PAUSE | ▶️ RESUME | 🏠 MENU"
            )
            try:
                # send NEW message so user always sees fresh data
                sent = await bot.send_message(m.chat.id, txt)
                new_mid = getattr(sent, "id", None)
                if new_mid is None and isinstance(sent, dict):
                    new_mid = (sent.get("result") or {}).get("message_id")
                if new_mid:
                    # update job so Status.flush() auto-refresh syncs to this message
                    j["status_msg_id"] = new_mid
                    j["chat_id"] = m.chat.id
                    # one immediate flush to confirm it's live
                    status_obj = Status(m.chat.id, new_mid, job_id=jid)
                    await status_obj.flush(force=True)
            except Exception as e:
                await m_reply(m, f"{txt}\n\n(refresh note: {e})", job_detail_kb(jid))
                return
            await m_reply(m, "✅ Live status upar — auto 1s refresh PREMIUM", job_detail_kb(jid))
        else:
            await m_reply(m, "Koi job nahi — FORWARD NOW se start karo", main_kb(uid))
        return

    # ----- job control: pure START/STOP/PAUSE/RESUME/DELETE/RESTART (uses selected_job_id) -----
    pure = pu.strip().replace("▶️", "").replace("⏹", "").replace("🔄", "").replace("⏸", "").replace("🗑", "").strip()
    pure = re.sub(r"\s+", " ", pure).upper()
    action_map = {
        "START": "start", "STOP": "stop", "RESTART": "restart",
        "PAUSE": "pause", "RESUME": "resume", "DELETE": "delete",
    }
    action = action_map.get(pure)
    # also old formats with #id or JSTART_
    short = None
    if not action:
        m_act = re.search(
            r"(?:^|\s)(START|STOP|RESTART|PAUSE|RESUME|DELETE)\s*#?\s*([A-Fa-f0-9]{6,12})",
            pu, re.I,
        )
        if m_act:
            action = action_map.get(m_act.group(1).upper())
            short = m_act.group(2).lower()
        else:
            m_act = re.search(
                r"(JSTART_|JSTOP_|JRESTART_|JPAUSE_|JRESUME_|JDEL_)([A-Fa-f0-9]{6,12})",
                pu_ns, re.I,
            )
            if m_act:
                action = {
                    "JSTART_": "start", "JSTOP_": "stop", "JRESTART_": "restart",
                    "JPAUSE_": "pause", "JRESUME_": "resume", "JDEL_": "delete",
                }.get(m_act.group(1).upper())
                short = m_act.group(2).lower()

    if action:
        jid = None
        if short:
            jid = find_job_by_short(short)
        if not jid:
            jid = STATE.get("selected_job_id")
        if not jid or jid not in JOBS:
            await m_reply(m, "❌ Job not found\nFORWARD RUN / MY RECEV CHAN se list open karo", main_kb(uid))
            return
        STATE["selected_job_id"] = jid
        job = JOBS[jid]
        if action == "stop":
            job["stop"] = True
            job["paused"] = False
            job["stage"] = "stopped"
            STATE["stop"] = True
            STATE["paused"] = False
            await m_reply(m, f"⏹ STOP\nReceive: {job.get('recv_title')}", job_detail_kb(jid))
        elif action == "pause":
            job["paused"] = True
            job["stage"] = "PAUSED"
            STATE["paused"] = True
            await m_reply(m, f"⏸ PAUSED — locked\nReceive: {job.get('recv_title')}\nResume dabao tabhi chalega", job_detail_kb(jid))
        elif action == "resume":
            job["paused"] = False
            job["stop"] = False
            job["stage"] = "copying (resumed)"
            STATE["paused"] = False
            STATE["stop"] = False
            await m_reply(m, f"▶️ RESUME\nReceive: {job.get('recv_title')}", job_detail_kb(jid))
        elif action == "delete":
            job["stop"] = True
            JOBS.pop(jid, None)
            STATE["selected_job_id"] = None
            await m_reply(m, "🗑 DELETED", main_kb(uid))
        elif action in ("restart", "start"):
            # if task still running just unpause; if done/stopped relaunch same src→recv
            t = job.get("task")
            running = t and not (hasattr(t, "done") and t.done())
            if running and action == "start":
                job["stop"] = False
                job["paused"] = False
                job["stage"] = "copying (started)"
                await m_reply(m, f"▶️ START\nReceive: {job.get('recv_title')}", job_detail_kb(jid))
            else:
                job["stop"] = True
                await asyncio.sleep(0.3)
                if job.get("res"):
                    await start_res_forward(
                        m, uid,
                        {"id": job["src_ident"], "title": job.get("src_title"), "link": job.get("src_link")},
                        {"id": job["tgt_id"], "title": job.get("recv_title"), "link": job.get("rcv_link")},
                        start_from_id=job.get("start_from_id") or 0, path_query=job.get("path_query"), resume=True)
                    return
                src = job.get("src_ident") or get_source_ident()
                tgt = job.get("tgt_id")
                if not src or not tgt:
                    await m_reply(m, "❌ Source/Receiver missing — FORWARD NOW se naya start", main_kb(uid))
                    return
                # relaunch as new job keeping titles
                await start_forward(m, uid, start_from_id=job.get("start_from_id") or 0, path_query=job.get("path_query"))
            return
        return

    # open job detail from list button — match by original channel/receiver name
    if JOBS and not action:
        # strip emoji/tags from button text for match
        clean_btn = re.sub(r"^[▶⏸⏹]\s*", "", text.strip())
        clean_btn = re.sub(r"\s+", " ", clean_btn).strip()
        matched_jid = None
        for jid, j in JOBS.items():
            rt = (j.get("recv_title") or "").strip()
            st = (j.get("src_title") or "").strip()
            if not rt and not st:
                continue
            if clean_btn and (
                clean_btn == rt
                or clean_btn == st
                or clean_btn[:22] == rt[:22]
                or clean_btn[:22] == st[:22]
                or rt.lower() in clean_btn.lower()
                or clean_btn.lower() in rt.lower()
            ):
                matched_jid = jid
                break
        # also old JOB_xxxx still works
        if not matched_jid:
            m_short = re.search(r"JOB_([A-Fa-f0-9]{6,12})", pu_ns, re.I)
            if m_short:
                short = m_short.group(1).lower()
                matched_jid = find_job_by_short(short) if "find_job_by_short" in dir() else None
                if not matched_jid:
                    for k in list(JOBS.keys()):
                        if k.startswith(short) or k[:8] == short[:8]:
                            matched_jid = k
                            break
        if matched_jid and matched_jid in JOBS:
            STATE["selected_job_id"] = matched_jid
            j = JOBS[matched_jid]
            up = int(time.time() - j.get("started", time.time())) if j.get("started") else 0
            try:
                st_msg = await m.reply_text("⚡ LIVE STATUS loading…")
                mid = getattr(st_msg, "id", None)
                if mid is None and isinstance(st_msg, dict):
                    mid = (st_msg.get("result") or {}).get("message_id")
                if mid:
                    j["status_msg_id"] = mid
                    j["chat_id"] = m.chat.id
                    status = Status(m.chat.id, mid, job_id=matched_jid)
                    await status.flush(force=True)
            except Exception:
                pass
            info = (
                f"{B('FORWARD RUNNING')}\n"
                f"══════════════════\n"
                f"📥 Source: {j.get('src_title') or '—'}\n"
                f"📤 Receive: {j.get('recv_title') or '—'}\n"
                f"📍 Stage: {j.get('stage')}\n"
                f"✅ Copied: {j.get('copied', 0)} / {j.get('total_files', 0)}\n"
                f"❌ Failed: {j.get('failed', 0)}\n"
                f"⏱ Elapsed: {up}s\n"
                f"══════════════════\n"
                f"Upar LIVE STATUS auto 1s\n"
                f"Niche: START STOP RESTART PAUSE RESUME DELETE"
            )
            await m_reply(m, info, job_detail_kb(matched_jid))
            return

    if awaiting == "admin_rm_btn":
        STATE["awaiting"] = None
        if not can_manage(uid):
            await m_reply(m, "❌ Not allowed", main_kb(uid))
            return

    if awaiting == "source_link":
        STATE["awaiting"] = None
        link = text.strip()
        try:
            ident = resolve_target(link)
            chat, _ = await ensure_member(ident)
            d = load()
            entry = {
                "id": chat.id,
                "title": chat.title or str(chat.id),
                "link": link,
                "resolved": chat.id,
            }
            srcs = d.get("sources") or []
            srcs = [s for s in srcs if s.get("id") != chat.id]
            srcs.append(entry)
            d["sources"] = srcs
            d["selected_source"] = entry
            save(d)
            await m_reply(m, f"✅ Source saved\n{entry['title']}\n`{chat.id}`", main_kb(uid))
        except Exception as e:
            await m_reply(m, f"❌ Source fail: {type(e).__name__}: {e}", main_kb(uid))
        return

    if awaiting == "receiver_id":
        STATE["awaiting"] = None
        if not can_manage(uid):
            await m_reply(m, "❌ Not allowed", main_kb(uid))
            return
        try:
            raw = text.strip()
            # support t.me links + @username + -100 ids
            link_m = re.search(r"(?:t\.me/|telegram\.me/)([A-Za-z0-9_]+)", raw)
            if link_m:
                chat = await user.get_chat(link_m.group(1))
            else:
                rid_s = re.sub(r"[^\d-]", "", raw) or raw
                chat = await user.get_chat(int(rid_s) if rid_s.lstrip("-").isdigit() else rid_s)
            real_title = chat.title or getattr(chat, "first_name", None) or str(chat.id)
            d = load()
            entry = {"id": int(chat.id), "title": real_title}
            recs = d.get("receivers") or []
            recs = [r for r in recs if int(r.get("id") or 0) != int(chat.id)]
            recs.append(entry)
            d["receivers"] = recs
            d["selected_receiver"] = entry
            save(d)
            await m_reply(m, f"✅ Receiver added\n{real_title}\n`{chat.id}`", panel_kb_for(uid))
        except Exception as e:
            await m_reply(m, f"❌ {type(e).__name__}: {e}", panel_kb_for(uid))
        return

    if awaiting == "admin_add":
        STATE["awaiting"] = None
        if not can_manage(uid):
            await m_reply(m, "❌ Not allowed", main_kb(uid))
            return
        try:
            aid = int(text.strip())
            d = load()
            ads = d.get("admins") or []
            if aid not in ads:
                ads.append(aid)
            d["admins"] = ads
            save(d)
            await m_reply(m, f"✅ Admin added: `{aid}`", panel_kb_for(uid))
        except Exception:
            await m_reply(m, "❌ User id number", panel_kb_for(uid))
        return

    if awaiting == "admin_rm":
        STATE["awaiting"] = None
        if not can_manage(uid):
            await m_reply(m, "❌ Not allowed", main_kb(uid))
            return
        try:
            aid = int(text.strip())
            d = load()
            ads = [x for x in (d.get("admins") or []) if x != aid]
            d["admins"] = ads
            save(d)
            await m_reply(m, f"✅ Removed: `{aid}`", panel_kb_for(uid))
        except Exception:
            await m_reply(m, "❌ User id number", panel_kb_for(uid))
        return

    if awaiting == "rm_receiver":
        STATE["awaiting"] = None
        if not can_manage(uid):
            await m_reply(m, "❌ Not allowed", panel_kb_for(uid))
            return
        m_id = re.search(r"DELRCV_(-?\d+)", plain_upper(text).replace(" ", "")) or re.search(r"(-100\d{8,}|\d{8,})", text)
        if not m_id:
            await m_reply(m, "❌ Not matched", panel_kb_for(uid))
            return
        want = int(m_id.group(1))
        d = load()
        recs = [r for r in (d.get("receivers") or []) if int(r.get("id") or 0) != want]
        d["receivers"] = recs
        if d.get("selected_receiver") and int(d["selected_receiver"].get("id") or 0) == want:
            d["selected_receiver"] = recs[0] if recs else None
        save(d)
        await m_reply(m, f"✅ Receiver deleted\n`{want}`", panel_kb_for(uid))
        return

    if awaiting == "rm_source":
        STATE["awaiting"] = None
        if uid != OWNER_ID and get_user_role(uid) not in ("owner", "admin"):
            await m_reply(m, "❌ Owner/Admin only", owner_kb())
            return
        m_id = re.search(r"DELSRC_(-?\d+)", plain_upper(text).replace(" ", "")) or re.search(r"(-100\d{8,}|\d{8,})", text)
        if not m_id:
            await m_reply(m, "❌ Not matched", owner_kb() if uid == OWNER_ID else admin_kb())
            return
        want = int(m_id.group(1))
        d = load()
        srcs = [s for s in (d.get("sources") or []) if int(s.get("id") or 0) != want]
        d["sources"] = srcs
        if d.get("selected_source") and int(d["selected_source"].get("id") or 0) == want:
            d["selected_source"] = srcs[0] if srcs else None
        save(d)
        await m_reply(m, f"✅ Source deleted\n`{want}`", owner_kb() if uid == OWNER_ID else admin_kb())
        return

    if awaiting == "rm_user":
        STATE["awaiting"] = None
        if uid != OWNER_ID:
            await m_reply(m, "❌ Owner only", main_kb(uid))
            return
        m_id = re.search(r"DELUSR_(\d+)", plain_upper(text).replace(" ", "")) or re.search(r"(\d{6,})", text)
        if not m_id:
            await m_reply(m, "❌ Not matched", owner_kb())
            return
        want = int(m_id.group(1))
        d = load()
        users = [u for u in (d.get("users") or []) if int(u.get("owner_id") or 0) != want]
        d["users"] = users
        ads = [x for x in (d.get("admins") or []) if int(x) != want]
        d["admins"] = ads
        save(d)
        await m_reply(m, f"✅ User removed completely\n`{want}`", owner_kb())
        return

    # strict: only accept value AFTER step button was pressed (awaiting = user_input_*)
    if awaiting == "user_input_api_id":
        if uid != OWNER_ID:
            STATE["awaiting"] = None
            return
        try:
            api_id = int(re.sub(r"[^\d]", "", text) or "0")
            if api_id < 1000:
                raise ValueError("bad")
            STATE.setdefault("new_user", {})["api_id"] = api_id
            STATE["awaiting"] = "user_gate_hash"
            await m_reply(
                m,
                f"✅ API ID saved: `{api_id}`\nAb niche **UPLOAD HASH** dabao",
                add_user_step_kb("wait_hash_btn"),
            )
        except Exception:
            await m_reply(m, "❌ Valid API ID number bhejo", add_user_step_kb("wait_api_btn"))
            STATE["awaiting"] = "user_input_api_id"
        return

    if awaiting == "user_input_hash":
        if uid != OWNER_ID:
            STATE["awaiting"] = None
            return
        h = text.strip()
        if len(h) < 8:
            await m_reply(m, "❌ API HASH short", add_user_step_kb("wait_hash_btn"))
            STATE["awaiting"] = "user_input_hash"
            return
        STATE.setdefault("new_user", {})["api_hash"] = h
        STATE["awaiting"] = "user_gate_session"
        await m_reply(
            m,
            f"✅ HASH saved\nAb niche **UPLOAD SESSION STRING** dabao",
            add_user_step_kb("wait_session_btn"),
        )
        return

    if awaiting == "user_input_session":
        if uid != OWNER_ID:
            STATE["awaiting"] = None
            return
        s = text.strip()
        if len(s) < 20:
            await m_reply(m, "❌ Session short", add_user_step_kb("wait_session_btn"))
            STATE["awaiting"] = "user_input_session"
            return
        STATE.setdefault("new_user", {})["session"] = s
        STATE["awaiting"] = "user_gate_uid"
        await m_reply(
            m,
            f"✅ SESSION saved\nAb niche **SEND USER ID** dabao",
            add_user_step_kb("wait_uid_btn"),
        )
        return

    if awaiting == "user_input_uid":
        if uid != OWNER_ID:
            STATE["awaiting"] = None
            return
        try:
            oid = int(re.sub(r"[^\d]", "", text) or "0")
            if oid < 1000:
                raise ValueError("bad")
            STATE.setdefault("new_user", {})["owner_id"] = oid
            STATE["awaiting"] = "user_gate_role"
            nu = STATE["new_user"]
            await m_reply(
                m,
                f"{B('CHOOSE PANEL')}\n══════════════════\n"
                f"API ID: `{nu.get('api_id')}`\n"
                f"USER ID: `{oid}`\n"
                f"══════════════════\n"
                f"Is user ko kaunsa panel?\n"
                f"**OWNER PANEL** ya **ADMIN PANEL** dabao",
                add_user_step_kb("wait_role_btn"),
            )
        except Exception:
            await m_reply(m, "❌ Valid user id number", add_user_step_kb("wait_uid_btn"))
            STATE["awaiting"] = "user_input_uid"
        return

    if awaiting == "path_id":
        STATE["awaiting"] = None
        text_in = text.strip()
        # pure message id → direct start
        if re.fullmatch(r"\d+", text_in):
            start_id = int(text_in)
            await start_forward(m, uid, start_from_id=start_id, path_query=f"id:{start_id}")
            return
        src_ident = get_source_ident()
        if not src_ident:
            await m_reply(m, "❌ Source not selected", main_kb(uid))
            return
        # live status message for path scan (flood timer + progress)
        scan_mid = await _http_send(
            m.chat.id,
            f"🔎 DEEP PATH SCAN LIVE\nquery: {text_in[:100]}\nstarting…",
        )
        u_cli = await get_user_client(uid)
        found_id, found_fname = await find_message_by_path(
            src_ident,
            text_in,
            u_client=u_cli,
            status_chat_id=m.chat.id,
            status_msg_id=scan_mid,
        )
        if not found_id:
            await m_reply(
                m,
                f"❌ match nahi mila\n`{text_in}`\n"
                f"Try:\n• exact filename (Keys_20260909_132825.txt)\n"
                f"• message id (number only)\n"
                f"• t.me link ending with /ID",
                main_kb(uid),
            )
            STATE["awaiting"] = "path_id"
            return
        # send matched file to user for confirm
        try:
            chat_src, _ = await ensure_member(src_ident, u_client=u_cli)
            await u_cli.copy_message(
                chat_id=m.chat.id,
                from_chat_id=chat_src.id,
                message_id=found_id,
            )
            await m_reply(
                m,
                f"✅ MATCH FOUND — CONFIRM FILE UPAR\n"
                f"📄 `{found_fname}`\n"
                f"🆔 `{found_id}`\n"
                f"Isi se TOP skip → niche se ULTRA FAST forward START",
                main_kb(uid),
            )
        except Exception as e:
            await m_reply(
                m,
                f"✅ Match: `{found_fname}` id={found_id}\n"
                f"(preview fail: {type(e).__name__}) — forward START",
                main_kb(uid),
            )
        # immediate forward start + live status page
        await start_forward(m, uid, start_from_id=found_id, path_query=text_in)
        return

    if awaiting == "pick_source":
        d = load()
        srcs = d.get("sources") or []
        chosen = None
        raw = plain_upper(text).replace(" ", "")
        m_id = re.search(r"SRC_(-?\d+)", raw) or re.search(r"SRC_(-?\d+)", text.replace(" ", ""))
        if not m_id:
            m_id = re.search(r"(-100\d{8,}|\d{8,})", text)
        if m_id:
            want = int(m_id.group(1))
            for s in srcs:
                if int(s.get("id") or 0) == want:
                    chosen = s
                    break
        if not chosen:
            digits = re.sub(r"\D", "", text)
            for s in srcs:
                sid = str(s.get("id") or "")
                if digits and (digits[-8:] in sid or sid[-8:] in digits):
                    chosen = s
                    break
        STATE["awaiting"] = None
        if chosen:
            d["selected_source"] = chosen
            save(d)
            await m_reply(
                m,
                f"✅ Source selected\n{chosen.get('title')}\n`{chosen.get('id')}`",
                main_kb(uid),
            )
        else:
            await m_reply(m, "❌ Not matched — SOURCE LIST phir dabao", main_kb(uid))
        return

    if awaiting == "pick_receiver_fwd":
        d = load()
        recs = d.get("receivers") or []
        chosen = None
        raw = plain_upper(text).replace(" ", "")
        m_id = re.search(r"RCV_(-?\d+)", raw) or re.search(r"RCV_(-?\d+)", text.replace(" ", ""))
        if not m_id:
            m_id = re.search(r"(-100\d{8,}|\d{8,})", text)
        if m_id:
            want = int(m_id.group(1))
            for r in recs:
                if int(r.get("id") or 0) == want:
                    chosen = r
                    break
        if not chosen:
            digits = re.sub(r"\D", "", text)
            for r in recs:
                rid = str(r.get("id") or "")
                if digits and (digits[-8:] in rid or rid[-8:] in digits):
                    chosen = r
                    break
        STATE["awaiting"] = None
        if chosen:
            d["selected_receiver"] = chosen
            save(d)
            title = chosen.get("title") or str(chosen.get("id"))
            await m_reply(
                m,
                f"✅ Receiver selected\n{title}\n`{chosen.get('id')}`\n\nMode choose karo:",
                forward_mode_kb(),
            )
        else:
            await m_reply(m, "❌ Receiver not matched — list phir dabao", main_kb(uid))
        return

    if awaiting == "pick_receiver_list":
        d = load()
        recs = d.get("receivers") or []
        chosen = None
        m_id = re.search(r"RCV_(-?\d+)", plain_upper(text).replace(" ", "")) or re.search(r"(-100\d{8,})", text)
        if m_id:
            want = int(m_id.group(1))
            for r in recs:
                if int(r.get("id") or 0) == want:
                    chosen = r
                    break
        STATE["awaiting"] = None
        if chosen:
            d["selected_receiver"] = chosen
            save(d)
            await m_reply(m, f"✅ Selected\n{chosen.get('title')}\n`{chosen.get('id')}`", owner_kb() if uid == OWNER_ID else main_kb(uid))
        else:
            await m_reply(m, "❌ Not matched", owner_kb() if uid == OWNER_ID else main_kb(uid))
        return

    if awaiting == "pick_admin":
        m_id = re.search(r"ADM_(\d+)", plain_upper(text).replace(" ", "")) or re.search(r"(\d{6,})", text)
        STATE["awaiting"] = None
        if m_id:
            aid = m_id.group(1)
            await m_reply(
                m,
                f"👤 Admin `{aid}`\nProfile: tg://user?id={aid}\nhttps://t.me/user?id={aid}",
                owner_kb() if uid == OWNER_ID else admin_kb(),
            )
        else:
            await m_reply(m, "❌ Admin not matched", owner_kb() if uid == OWNER_ID else admin_kb())
        return

    # ----- buttons -----
    if "ADD SOURCE" in pu:
        STATE["awaiting"] = "source_link"
        await m_reply(
            m,
            f"{B('ADD SOURCE')}\n"
            f"══════════════════\n"
            f"Channel link / @user / -100id / invite+\n"
            f"bhejo:",
            main_kb(uid),
        )
        return

    if "SOURCE LIST" in pu:
        d = load()
        srcs = d.get("sources") or []
        if not srcs:
            await m.reply_text("📭 Empty — ADD SOURCE", reply_markup=main_kb(uid))
            return
        lines = []
        sel = d.get("selected_source")
        for c in srcs:
            mark = "✅" if sel and c.get("id") == sel.get("id") else "•"
            lines.append(f"{mark} {c.get('title')}\n   `{c.get('id')}`")
        await m_reply(
            m,
            B("SOURCE LIST") + "\n══════════════════\n"
            + "\n".join(lines)
            + "\n══════════════════\n"
            + "Niche button dabao → select",
            list_kb(srcs, prefix="SRC"),
        )
        STATE["awaiting"] = "pick_source"
        return

    if "FORWARD NOW" in pu:
        if not get_source_ident():
            await m_reply(
                m,
                f"{B('NOTICE')}\n"
                f"══════════════════\n"
                f"❌ Source channel not selected\n"
                f"SOURCE LIST se select karo pehle\n"
                f"══════════════════",
                main_kb(uid),
            )
            return
        d = load()
        recs = d.get("receivers") or []
        if not recs:
            try:
                ch = await user.get_chat(DEFAULT_TARGET)
                title = ch.title or f"DEFAULT {DEFAULT_TARGET}"
            except Exception:
                title = f"DEFAULT {DEFAULT_TARGET}"
            recs = [{"id": DEFAULT_TARGET, "title": title}]
            d["receivers"] = recs
            save(d)
        for r in recs:
            if str(r.get("title") or "").startswith("DEFAULT") or not r.get("title"):
                try:
                    ch = await user.get_chat(int(r["id"]))
                    r["title"] = ch.title or str(r["id"])
                except Exception:
                    pass
        save(d)
        lines = []
        sel = d.get("selected_receiver")
        for r in recs:
            mark = "✅" if sel and r.get("id") == sel.get("id") else "•"
            lines.append(f"{mark} {r.get('title')}\n   `{r.get('id')}`")
        await m_reply(
            m,
            B("SELECT RECEIVER") + "\n══════════════════\n"
            + "\n".join(lines)
            + "\n══════════════════\n"
            + "Niche button → select receiver",
            receiver_list_kb(recs),
        )
        STATE["awaiting"] = "pick_receiver_fwd"
        return

    if "DIRECT FORWARD" in pu:
        await start_forward(m, uid, start_from_id=0)
        return

    if "PATH FORWARD" in pu:
        STATE["awaiting"] = "path_id"
        await m_reply(
            m,
            f"{B('PATH FORWARD')}\n"
            f"══════════════════\n"
            f"Message ID number bhejo\n"
            f"Ya sirf filename bhejo\n"
            f"Example: prepared_inline_message.py\n"
            f"Ya: report.pdf\n\n"
            f"Full power scan — 20k+ files tak dhundega\n"
            f"Milte hi us file se TOP side skip, neeche se forward",
            main_kb(uid),
        )
        return

    # MY RECEV CHAN — multi receiver control (hosting style)
    if "MYRECEV" in pu_ns or "MY RECEV" in pu or "MY RECEIVE" in pu:
        if not JOBS:
            await m_reply(
                m,
                f"{B('MY RECEV CHAN')}\n"
                f"══════════════════\n"
                f"Abhi koi active / past job nahi\n"
                f"FORWARD NOW se naya forward start karo\n"
                f"Phir yahan multi-control dikhega\n"
                f"══════════════════",
                my_recev_kb(JOBS),
            )
            return
        lines = []
        for jid, j in list(JOBS.items())[:20]:
            flag = "🟢 RUN"
            if j.get("stop"):
                flag = "🔴 STOP"
            elif j.get("paused"):
                flag = "⏸ PAUSE"
            elif (j.get("stage") or "").startswith("rest"):
                flag = "💤 REST"
            lines.append(
                f"{flag} | {j.get('recv_title') or '?'}\n"
                f"   src: {j.get('src_title') or '?'}\n"
                f"   {j.get('copied', 0)}/{j.get('total_files', 0)} · {j.get('stage')}"
            )
        await m_reply(
            m,
            B("MY RECEV CHAN") + "\n══════════════════\n"
            + "\n".join(lines)
            + "\n══════════════════\n"
            + "Niche button dabao → job open\n"
            + "Start / Stop / Pause / Resume / Delete / Restart\n"
            + "BACK TO HOME se menu",
            my_recev_kb(JOBS),
        )
        return

    if "START FORWARD" in pu or "STARTFORWARD" in pu_ns:
        # from MY RECEV CHAN — jump to FORWARD NOW flow
        if not get_source_ident():
            await m_reply(m, "❌ Source select karo pehle (SOURCE LIST)", main_kb(uid))
            return
        d = load()
        recs = d.get("receivers") or []
        if not recs:
            await m_reply(m, "❌ Receiver add karo pehle (OWNER PANEL)", main_kb(uid))
            return
        lines = []
        for r in recs:
            lines.append(f"• {r.get('title')}\n   `{r.get('id')}`")
        await m_reply(
            m,
            B("SELECT RECEIVER") + "\n══════════════════\n"
            + "\n".join(lines)
            + "\n══════════════════\n"
            + "Niche button → select → Direct / Path",
            receiver_list_kb(recs),
        )
        STATE["awaiting"] = "pick_receiver_fwd"
        return

    # FORWARD RUN / FORWARD RUNNING — all running processes
    if "FORWARDRUN" in pu_ns or "FORWARD RUN" in pu or "FORWARD RUNNING" in pu:
        if not JOBS:
            await m_reply(
                m,
                f"{B('FORWARD RUNNING')}\n"
                f"══════════════════\n"
                f"Koi running / saved job nahi\n"
                f"FORWARD NOW se start karo",
                main_kb(uid),
            )
            return
        lines = []
        for jid, j in list(JOBS.items())[:25]:
            flag = "🟢 RUN" if (not j.get("stop") and not j.get("paused") and j.get("task")) else "🔴 STOP"
            if j.get("paused"):
                flag = "⏸ PAUSE"
            if (j.get("stage") or "").startswith("rest"):
                flag = "💤 REST"
            if (j.get("stage") or "") == "done":
                flag = "✅ DONE"
            lines.append(
                f"{flag} | {j.get('recv_title') or '?'}\n"
                f"   {j.get('copied', 0)}/{j.get('total_files', 0)} · {j.get('stage')}"
            )
        await m_reply(
            m,
            B("FORWARD RUNNING") + "\n══════════════════\n"
            + "\n".join(lines)
            + "\n══════════════════\n"
            + "Niche button dabao → job detail\n"
            + "Start / Stop / Restart / Pause / Resume / Delete\n"
            + "BACK TO HOME se menu",
            job_list_kb(JOBS),
        )
        return

    if "STATUS" in pu:
        # prefer active job status
        active = None
        for jid, j in JOBS.items():
            if j.get("task") and not (hasattr(j["task"], "done") and j["task"].done()):
                active = j
                break
        if active and active.get("status_msg_id"):
            st = Status(m.chat.id, active["status_msg_id"], job_id=active.get("job_id"))
            await st.flush(force=True)
            await m.reply_text(
                "Live status upar message me 1s refresh ho raha hai",
                reply_markup=live_control_kb(),
            )
        elif STATE.get("task") and not STATE["task"].done():
            st = Status(m.chat.id, STATE.get("status_msg_id") or m.id)
            await st.flush(force=True)
            await m.reply_text(
                "Live status upar message me 1s refresh ho raha hai",
                reply_markup=live_control_kb(),
            )
        else:
            total = STATE.get("total_files") or STATE.get("files") or 0
            await m.reply_text(
                f"Stage: {STATE.get('stage')}\n"
                f"TOTAL: {total}\n"
                f"Copied: {STATE.get('copied')}  Failed: {STATE.get('failed')}\n"
                f"No active job",
                reply_markup=main_kb(uid),
            )
        return

    if "STOP" in pu and "BACK" not in pu:
        # prefer selected job, else all
        jid = STATE.get("selected_job_id")
        if jid and jid in JOBS:
            job = JOBS[jid]
            job["stop"] = True
            job["paused"] = False
            job["stage"] = "stopped"
            STATE["stop"] = True
            await m.reply_text(f"⏹ STOP\nReceive: {job.get('recv_title')}", reply_markup=job_detail_kb(jid))
        else:
            STATE["stop"] = True
            STATE["paused"] = False
            for j in JOBS.values():
                j["stop"] = True
                j["paused"] = False
                j["stage"] = "stopped"
            await m.reply_text("⏹ STOP — all jobs band", reply_markup=live_control_kb())
        return

    if "RESUME" in pu:
        jid = STATE.get("selected_job_id")
        resumed = False
        if jid and jid in JOBS:
            job = JOBS[jid]
            job["paused"] = False
            job["stop"] = False
            job["stage"] = "copying (resumed)"
            STATE["paused"] = False
            STATE["stop"] = False
            resumed = True
            await m.reply_text(f"▶️ RESUME\nReceive: {job.get('recv_title')}", reply_markup=job_detail_kb(jid))
        else:
            if STATE.get("task") and not STATE["task"].done():
                STATE["paused"] = False
                STATE["stop"] = False
                STATE["stage"] = "copying (resumed)"
                resumed = True
            for j in JOBS.values():
                if j.get("task") and not (hasattr(j["task"], "done") and j["task"].done()):
                    j["paused"] = False
                    j["stop"] = False
                    j["stage"] = "copying (resumed)"
                    resumed = True
            if resumed:
                await m.reply_text("▶️ RESUME — continue", reply_markup=live_control_kb())
            else:
                await m.reply_text("Koi paused job nahi", reply_markup=main_kb(uid))
        return

    if "PAUSE" in pu:
        jid = STATE.get("selected_job_id")
        if jid and jid in JOBS:
            job = JOBS[jid]
            job["paused"] = True
            job["stage"] = "PAUSED"
            STATE["paused"] = True
            await m.reply_text(f"⏸ PAUSED\nReceive: {job.get('recv_title')}", reply_markup=job_detail_kb(jid))
        else:
            paused = False
            if STATE.get("task") and not STATE["task"].done():
                STATE["paused"] = True
                paused = True
            for j in JOBS.values():
                if j.get("task") and not (hasattr(j["task"], "done") and j["task"].done()):
                    j["paused"] = True
                    j["stage"] = "PAUSED"
                    paused = True
                    if not STATE.get("selected_job_id"):
                        STATE["selected_job_id"] = j.get("job_id")
            if paused:
                jid2 = STATE.get("selected_job_id")
                kb_out = job_detail_kb(jid2) if jid2 and jid2 in JOBS else live_control_kb()
                await m.reply_text("⏸ PAUSED — Resume se continue", reply_markup=kb_out)
        return

    if "MENU" in pu or "BACK" in pu:
        STATE["awaiting"] = None
        await m.reply_text(B("MENU"), reply_markup=main_kb(uid))
        return

    if "OWNER PANEL" in pu:
        # during ADD USER role-pick, OWNER PANEL button means assign owner role
        if STATE.get("awaiting") == "user_gate_role" and uid == OWNER_ID:
            nu = STATE.get("new_user") or {}
            if not all(k in nu for k in ("api_id", "api_hash", "session", "owner_id")):
                STATE["awaiting"] = None
                await m_reply(m, "❌ Incomplete data", owner_kb())
                return
            d = load()
            users = [u for u in (d.get("users") or []) if int(u.get("owner_id") or 0) != int(nu["owner_id"])]
            users.append({
                "api_id": int(nu["api_id"]),
                "api_hash": str(nu["api_hash"]),
                "session": str(nu["session"]),
                "owner_id": int(nu["owner_id"]),
                "role": "owner",
                "added_at": now_str(),
            })
            d["users"] = users
            ads = d.get("admins") or []
            oid = int(nu["owner_id"])
            if oid not in ads and oid != OWNER_ID:
                ads.append(oid)
            d["admins"] = ads
            save(d)
            STATE["awaiting"] = None
            STATE["new_user"] = {}
            await m_reply(
                m,
                f"✅ USER ADDED — role: OWNER PANEL\n"
                f"User ID: `{oid}`\nAPI ID: `{nu['api_id']}`",
                owner_kb(),
            )
            return
        if get_user_role(uid) != "owner" and uid != OWNER_ID:
            await m.reply_text("❌ Owner only", reply_markup=main_kb(uid))
            return
        await m.reply_text(B("OWNER PANEL"), reply_markup=owner_kb())
        return

    # ----- ADD USER: button-gated only -----
    if "ADD USER" in pu and "ADMIN" not in pu:
        if uid != OWNER_ID:
            return
        STATE["awaiting"] = "user_gate_api"
        STATE["new_user"] = {}
        await m_reply(
            m,
            f"{B('ADD USER')}\n══════════════════\n"
            f"Pehle niche **SEND API ID** button dabao\n"
            f"Phir API ID number bhejna",
            add_user_step_kb("wait_api_btn"),
        )
        return

    if "SEND API ID" in pu or "SENDAPIID" in pu_ns:
        if uid != OWNER_ID:
            return
        STATE["awaiting"] = "user_input_api_id"
        await m_reply(m, "Ab API ID number bhejo:", add_user_step_kb("wait_api_btn"))
        return

    if "UPLOAD HASH" in pu or "UPLOADHASH" in pu_ns:
        if uid != OWNER_ID:
            return
        if not (STATE.get("new_user") or {}).get("api_id"):
            await m_reply(m, "❌ Pehle SEND API ID complete karo", add_user_step_kb("wait_api_btn"))
            return
        STATE["awaiting"] = "user_input_hash"
        await m_reply(m, "Ab API HASH bhejo:", add_user_step_kb("wait_hash_btn"))
        return

    if "UPLOAD SESSION" in pu or "UPLOADSESSION" in pu_ns:
        if uid != OWNER_ID:
            return
        if not (STATE.get("new_user") or {}).get("api_hash"):
            await m_reply(m, "❌ Pehle UPLOAD HASH complete karo", add_user_step_kb("wait_hash_btn"))
            return
        STATE["awaiting"] = "user_input_session"
        await m_reply(m, "Ab SESSION STRING bhejo:", add_user_step_kb("wait_session_btn"))
        return

    if "SEND USER ID" in pu or "SENDUSERID" in pu_ns:
        if uid != OWNER_ID:
            return
        if not (STATE.get("new_user") or {}).get("session"):
            await m_reply(m, "❌ Pehle SESSION complete karo", add_user_step_kb("wait_session_btn"))
            return
        STATE["awaiting"] = "user_input_uid"
        await m_reply(m, "Ab USER ID number bhejo:", add_user_step_kb("wait_uid_btn"))
        return

    # role pick ADMIN PANEL during add-user
    if "ADMIN PANEL" in pu and STATE.get("awaiting") == "user_gate_role" and uid == OWNER_ID:
        nu = STATE.get("new_user") or {}
        if not all(k in nu for k in ("api_id", "api_hash", "session", "owner_id")):
            STATE["awaiting"] = None
            await m_reply(m, "❌ Incomplete", owner_kb())
            return
        d = load()
        users = [u for u in (d.get("users") or []) if int(u.get("owner_id") or 0) != int(nu["owner_id"])]
        users.append({
            "api_id": int(nu["api_id"]),
            "api_hash": str(nu["api_hash"]),
            "session": str(nu["session"]),
            "owner_id": int(nu["owner_id"]),
            "role": "admin",
            "added_at": now_str(),
        })
        d["users"] = users
        ads = d.get("admins") or []
        oid = int(nu["owner_id"])
        if oid not in ads and oid != OWNER_ID:
            ads.append(oid)
        d["admins"] = ads
        save(d)
        STATE["awaiting"] = None
        STATE["new_user"] = {}
        await m_reply(
            m,
            f"✅ USER ADDED — role: ADMIN PANEL\n"
            f"User ID: `{oid}`\nAPI ID: `{nu['api_id']}`",
            owner_kb(),
        )
        return

    if "RM USER" in pu or "RMUSER" in pu_ns:
        if uid != OWNER_ID:
            return
        d = load()
        users = d.get("users") or []
        if not users:
            await m_reply(m, "No users", owner_kb())
            return
        rows = []
        for u in users[:40]:
            oid = u.get("owner_id")
            role = u.get("role") or "?"
            rows.append([btn(f"🗑 {oid} ({role}) | DELUSR_{oid}", "danger")])
        rows.append([btn("🔙 BACK", "primary")])
        await m_reply(
            m,
            B("RM USER") + "\n══════════════════\n"
            + "Button dabao → user completely remove",
            kb(make_markup(rows)),
        )
        STATE["awaiting"] = "rm_user"
        return

    if "USER LIST" in pu:
        if uid != OWNER_ID:
            return
        d = load()
        users = d.get("users") or []
        if not users:
            await m_reply(m, "No extra users", owner_kb())
            return
        lines = []
        for u in users:
            lines.append(f"• owner `{u.get('owner_id')}` · api `{u.get('api_id')}`")
        await m_reply(m, B("USER LIST") + "\n══════════════════\n" + "\n".join(lines), owner_kb())
        return

    if "ADMIN PANEL" in pu:
        if not is_allowed(uid):
            return
        await m.reply_text(B("ADMIN PANEL"), reply_markup=admin_kb())
        return

    if "ADD RECEIVER" in pu:
        if not is_allowed(uid):
            return
        STATE["awaiting"] = "receiver_id"
        panel = owner_kb() if uid == OWNER_ID or get_user_role(uid) == "owner" else admin_kb()
        await m.reply_text("📥 Receiver channel id (-100...) bhejo:", reply_markup=panel)
        return

    # RM RECEV CHAN — list receivers as delete buttons
    if "RM RECEV" in pu or "RMRECEV" in pu_ns:
        if not can_manage(uid):
            return
        d = load()
        recs = d.get("receivers") or []
        if not recs:
            await m.reply_text("Empty receivers", reply_markup=panel_kb_for(uid))
            return
        rows = []
        for r in recs[:40]:
            rid = r.get("id")
            title = (r.get("title") or str(rid))[:18]
            rows.append([btn(f"🗑 {title} | DELRCV_{rid}", "danger")])
        rows.append([btn("🔙 BACK", "primary")])
        await m_reply(
            m,
            B("RM RECEV CHAN") + "\n══════════════════\n"
            + "Niche button dabao → woh receiver DELETE\n"
            + "══════════════════",
            kb(make_markup(rows)),
        )
        STATE["awaiting"] = "rm_receiver"
        return

    # RM SOURCE CHAN — list sources as delete buttons
    if "RM SOURCE" in pu or "RMSOURCE" in pu_ns:
        if not can_manage(uid):
            return
        d = load()
        srcs = d.get("sources") or []
        if not srcs:
            await m.reply_text("Empty sources", reply_markup=panel_kb_for(uid))
            return
        rows = []
        for s in srcs[:40]:
            sid = s.get("id")
            title = (s.get("title") or str(sid))[:18]
            rows.append([btn(f"🗑 {title} | DELSRC_{sid}", "danger")])
        rows.append([btn("🔙 BACK", "primary")])
        await m_reply(
            m,
            B("RM SOURCE CHAN") + "\n══════════════════\n"
            + "Niche button dabao → woh source DELETE\n"
            + "══════════════════",
            kb(make_markup(rows)),
        )
        STATE["awaiting"] = "rm_source"
        return

    if "RECEIVER LIST" in pu:
        if not can_manage(uid):
            return
        d = load()
        recs = d.get("receivers") or []
        if not recs:
            await m.reply_text("Empty — ADD RECEIVER", reply_markup=panel_kb_for(uid))
            return
        for r in recs:
            try:
                ch = await user.get_chat(int(r["id"]))
                r["title"] = ch.title or str(r["id"])
            except Exception:
                pass
        save(d)
        lines = []
        sel = d.get("selected_receiver")
        for r in recs:
            mark = "✅" if sel and r.get("id") == sel.get("id") else "•"
            lines.append(f"{mark} {r.get('title')}\n   `{r.get('id')}`")
        await m_reply(
            m,
            B("RECEIVERS") + "\n══════════════════\n"
            + "\n".join(lines)
            + "\n══════════════════\n"
            + "Niche button → select",
            receiver_list_kb(recs),
        )
        STATE["awaiting"] = "pick_receiver_list"
        return

    if "ADD ADMIN" in pu:
        if not can_manage(uid):
            return
        STATE["awaiting"] = "admin_add"
        await m.reply_text("➕ Admin user id bhejo:", reply_markup=panel_kb_for(uid))
        return

    if "REMOVE ADMIN" in pu:
        if not can_manage(uid):
            return
        STATE["awaiting"] = "admin_rm"
        await m.reply_text("➖ Admin user id to remove:", reply_markup=panel_kb_for(uid))
        return

    if "ADMIN LIST" in pu:
        d = load()
        ads = d.get("admins") or []
        panel = owner_kb() if uid == OWNER_ID else admin_kb()
        if not ads:
            await m_reply(m, "No admins", panel)
            return
        lines = [f"• `{a}`" for a in ads]
        await m_reply(
            m,
            B("ADMINS") + "\n══════════════════\n"
            + "\n".join(lines)
            + "\n══════════════════\n"
            + "Niche button → admin profile open",
            admin_list_kb(ads),
        )
        STATE["awaiting"] = "pick_admin"
        return


async def start_forward(m: Message, uid: int, start_from_id: int = 0, path_query=None):
    src = get_source_ident()
    if not src:
        await m_reply(
            m,
            f"{B('NOTICE')}\n══════════════════\n"
            f"❌ Source channel not selected\n"
            f"Select Source Channel And Retry\n══════════════════",
            main_kb(uid),
        )
        return
    tgt, tgt_title = get_receiver_id()

    job_id = new_job_id()
    job = {
        "job_id": job_id,
        "stage": "starting",
        "status_msg_id": None,
        "chat_id": m.chat.id,
        "src_title": None,
        "recv_title": tgt_title,
        "total": 0,
        "files": 0,
        "copied": 0,
        "failed": 0,
        "errors": 0,
        "scanned": 0,
        "total_files": 0,
        "last_err": None,
        "wait_until": 0.0,
        "rest_until": 0.0,
        "started": time.time(),
        "task": None,
        "stop": False,
        "paused": False,
        "start_from_id": start_from_id,
        "live_names": [],
        "last_file": None,
        "details": [],
        "path_query": path_query or (f"id:{start_from_id}" if start_from_id else None),
        "all_mids": [],
        "src_ident": src,
        "tgt_id": tgt,
    }
    JOBS[job_id] = job
    STATE["selected_job_id"] = job_id

    # also mirror into global STATE for compatibility
    STATE.update(
        total=0, files=0, copied=0, failed=0, errors=0, scanned=0, total_files=0,
        last_err=None, wait_until=0, rest_until=0, stop=False, paused=False,
        started=time.time(), recv_title=tgt_title, start_from_id=start_from_id,
        live_names=[], last_file=None, details=[], all_mids=[],
        path_query=job["path_query"], job_id=job_id, selected_job_id=job_id,
    )

    mode = f"path from id:{start_from_id}" if start_from_id else "direct TOP→BOTTOM"
    api_link = f"https://t.me/{BOT_USERNAME}" if BOT_USERNAME else "—"
    bot_token_short = (BOT_TOKEN[:12] + "…") if BOT_TOKEN else "—"

    # FULL POWER PREMIUM initial status — sent via bot.send_message (monkeypatched → HTTP API)
    # so message_id is always captured reliably
    live_txt = (
        f"⚡ {B('LIVE STATUS')}  ·  v10 FULL POWER PREMIUM\n"
        f"══════════════════════════════\n"
        f"🔗 API LINK: {api_link}\n"
        f"🔑 BOT TOKEN: {bot_token_short}\n"
        f"🕐 LIVE TIME: {now_str()}\n"
        f"📍 LIVE UPDATES: starting ({mode})\n"
        f"📥 Source: resolving…\n"
        f"📤 Receive: {tgt_title or '—'}\n"
        f"📂 Path / Start: {job.get('path_query') or '—'}\n"
        f"──────────────────────────────\n"
        f"📁 ALL FILES: —\n"
        f"🔎 SCANNED: 0\n"
        f"✅ COPIES: 0 / —\n"
        f"❌ FAILS: 0\n"
        f"⚠ ERRORS: 0\n"
        f"📊 PROGRESS: [░░░░░░░░░░░░] 0.0%\n"
        f"⏱ ELAPSED: 0s\n"
        f"⏳ ETA: —\n"
        f"🚀 SPEED: 0.0 files/s\n"
        f"──────────────────────────────\n"
        f"📄 LIVE FILE: —\n"
        f"🏷 LIVE NAMES: —\n"
        f"📌 LIVE DETAILS: 0/0 done · left —\n"
        f"──────────────────────────────\n"
        f"⏹ STOP | ⏸ PAUSE | ▶️ RESUME | 🏠 MENU"
    )
    # bot.send_message is monkeypatched → reply_colored → HTTP API → returns dict
    status_raw = await bot.send_message(m.chat.id, live_txt, reply_markup=job_detail_kb(job_id))
    mid = None
    if isinstance(status_raw, dict):
        mid = (status_raw.get("result") or {}).get("message_id")
    else:
        mid = getattr(status_raw, "id", None)

    job["status_msg_id"] = mid
    job["chat_id"] = m.chat.id
    STATE["status_msg_id"] = mid
    STATE["chat_id"] = m.chat.id
    status = Status(m.chat.id, mid, job_id=job_id)

    async def _run():
        await clone_job(status, src, tgt, start_from_id=start_from_id, job=job, uid=uid)
        job["task"] = None

    job["task"] = asyncio.create_task(_run())
    STATE["task"] = job["task"]

    await bot.send_message(
        m.chat.id,
        "🚀 Job started — LIVE STATUS upar\n"
        "1) Deep REAL count — no fake numbers\n"
        "2) TOP → BOTTOM\n"
        "3) Auto 1s live refresh\n"
        "4) FloodWait live timer\n"
        "5) Every 500 files → REST 25s\n"
        "6) End me deep file scan + JSON\n"
        "Controls niche buttons pe",
        reply_markup=job_detail_kb(job_id),
    )


async def main():
    await bot.start()
    await user.start()

    _orig_send = bot.send_message

    async def _send_message_colored(chat_id, text, **kwargs):
        rm = kwargs.get("reply_markup")
        if isinstance(rm, dict) or (rm is not None and type(rm).__name__ == "dict"):
            return await reply_colored(chat_id, text, rm)
        if hasattr(rm, "_d"):
            return await reply_colored(chat_id, text, rm._d)
        return await _orig_send(chat_id, D(text) if isinstance(text, str) else text, **kwargs)

    bot.send_message = _send_message_colored

    me_b = await bot.get_me()
    me_u = await user.get_me()
    global BOT_USERNAME
    BOT_USERNAME = me_b.username or ""
    print(f"bot @{BOT_USERNAME} | user {me_u.id} | target {DEFAULT_TARGET}")
    d = load()
    if not d.get("receivers"):
        d["receivers"] = [{"id": DEFAULT_TARGET, "title": f"DEFAULT {DEFAULT_TARGET}"}]
        d["selected_receiver"] = d["receivers"][0]
        save(d)
    try:
        n_ = await warm_peers(user, force=True)
        print(f"account chats loaded: {n_}")
    except Exception as e:
        print("warm:", e)
    await asyncio.Event().wait()


if __name__ == "__main__":
    bot.run(main())
