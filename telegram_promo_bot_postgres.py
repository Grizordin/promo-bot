import os
import asyncio
import html
from datetime import datetime, timedelta
from typing import List, Optional, Dict

import pytz
from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.enums import AllowedUpdates
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ---------------- CONFIG ----------------
# Token: keep fallback to original value so local usage doesn't break; you can set BOT_TOKEN in env on Render
BOT_TOKEN = os.getenv("BOT_TOKEN")
admin_ids_str = os.getenv("ADMIN_IDS", "")
try:
    ADMIN_IDS = [int(x.strip()) for x in admin_ids_str.split(",") if x.strip()]
except ValueError:
    ADMIN_IDS = []
MOSCOW_TZ = pytz.timezone("Europe/Moscow")
RESERVE_DEFAULT = 0  # default reserve if not set

# ---------------- DB SETUP (Postgres if DATABASE_URL present, otherwise fallback to SQLite) ----------------
USE_POSTGRES = False
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    USE_POSTGRES = True
    # psycopg2 imported lazily to avoid import errors when only using sqlite locally
    import psycopg2
    import psycopg2.extras

    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

    conn = psycopg2.connect(DATABASE_URL)
    # Keep raw cursor factory for creating RealDictCursor when needed
    raw_pg_cursor = conn.cursor

    def get_cursor():
        real_cur = raw_pg_cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        class CursorWrapper:
            def __init__(self, rc):
                self._rc = rc
            def execute(self, query, params=None):
                if params is not None and "?" in query:
                    q = query.replace("?", "%s")
                    return self._rc.execute(q, params)
                return self._rc.execute(query, params) if params is not None else self._rc.execute(query)
            def executemany(self, query, seq_of_params):
                if "?" in query:
                    q = query.replace("?", "%s")
                    return self._rc.executemany(q, seq_of_params)
                return self._rc.executemany(query, seq_of_params)
            def fetchone(self): return self._rc.fetchone()
            def fetchall(self): return self._rc.fetchall()
            def __getattr__(self, name): return getattr(self._rc, name)
        return CursorWrapper(real_cur)

    # override connection.cursor to return our wrapper (so existing code calling conn.cursor() works)

    # create tables for Postgres (SERIAL, BIGINT, etc.)
    c = get_cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        tg_id BIGINT UNIQUE,
        tg_username TEXT,
        site_username TEXT,
        role TEXT DEFAULT 'user',
        status TEXT DEFAULT 'pending',
        rejected_at TIMESTAMP,
        registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS promocodes (
        id SERIAL PRIMARY KEY,
        code TEXT UNIQUE,
        total_uses INTEGER,
        used INTEGER DEFAULT 0,
        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    );
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS distribution (
        id SERIAL PRIMARY KEY,
        user_id BIGINT,
        promo_id INTEGER,
        code TEXT,
        count INTEGER,
        source TEXT,
        given_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS weekly_users (
        id SERIAL PRIMARY KEY,
        week_start DATE,
        position INTEGER,
        site_username TEXT,
        user_id BIGINT
    );
    """)
    conn.commit()

    # default settings initialization (Postgres style)
    c.execute("INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO NOTHING", ("reserve", str(RESERVE_DEFAULT)))
    c.execute("INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO NOTHING", ("weekly_confirmed", "0"))
    c.execute("INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO NOTHING", ("last_distribution_date", ""))
    conn.commit()

else:
    # fallback to sqlite for local/testing use (preserves your existing DB file)
    import sqlite3
    DB_FILE = "telegram_promo_bot.db"
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # create tables (sqlite dialect)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        tg_id BIGINT UNIQUE,
        tg_username TEXT,
        site_username TEXT,
        role TEXT DEFAULT 'user',
        status TEXT DEFAULT 'pending',
        rejected_at TIMESTAMP,
        registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS promocodes (
        id SERIAL PRIMARY KEY,
        code TEXT UNIQUE,
        total_uses INTEGER,
        used INTEGER DEFAULT 0,
        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS distribution (
        id SERIAL PRIMARY KEY,
        user_id BIGINT,
        promo_id INTEGER,
        code TEXT,
        count INTEGER,
        source TEXT,
        given_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS weekly_users (
        id SERIAL PRIMARY KEY,
        week_start DATE,
        position INTEGER,
        site_username TEXT,
        user_id BIGINT
    );
    """)

    conn.commit()

    # default settings initialization (sqlite style)
    cur.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", ("reserve", str(RESERVE_DEFAULT)))
    cur.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", ("weekly_confirmed", "0"))
    cur.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", ("last_distribution_date", ""))
    conn.commit()

# ---------------- BOT / DISPATCHER / SCHEDULER ----------------
storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=storage)
scheduler = AsyncIOScheduler(timezone=MOSCOW_TZ)

# ---------------- HELPERS ----------------
def esc(s: Optional[str]) -> str:
    if s is None:
        return "-"
    return html.escape(str(s))

def db_get_setting(key: str) -> str:
    c = get_cursor()
    if USE_POSTGRES:
        c.execute("SELECT value FROM settings WHERE key = %s", (key,))
    else:
        c.execute("SELECT value FROM settings WHERE key = ?", (key,))
    r = c.fetchone()
    return r["value"] if r else ""

def db_set_setting(key: str, value: str):
    c = get_cursor()
    if USE_POSTGRES:
        c.execute("INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", (key, value))
    else:
        # sqlite: REPLACE INTO will insert or replace existing row
        c.execute("REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
    conn.commit()

def get_reserve() -> int:
    try:
        return int(db_get_setting("reserve"))
    except:
        return RESERVE_DEFAULT

def set_reserve(val: int):
    db_set_setting("reserve", str(val))

def now_msk() -> datetime:
    return datetime.now(MOSCOW_TZ)

def get_week_start() -> str:
    # determine anchor Sunday 21:08 MSK as week marker as earlier
    now = now_msk()
    # find sunday date for this week
    days_back = (now.weekday() + 1) % 7
    sunday = now - timedelta(days=days_back)
    anchor = sunday.replace(hour=21, minute=8, second=0, microsecond=0)
    if now < anchor:
        anchor -= timedelta(days=7)
    return anchor.date().isoformat()

def find_user_by_site(site_username: str):
    c = get_cursor()
    if USE_POSTGRES:
        c.execute("SELECT * FROM users WHERE site_username = %s", (site_username,))
    else:
        c.execute("SELECT * FROM users WHERE site_username = ?", (site_username,))
    return c.fetchone()

def find_user_by_tgid(tg_id: int):
    c = get_cursor()
    if USE_POSTGRES:
        c.execute("SELECT * FROM users WHERE tg_id = %s", (tg_id,))
    else:
        c.execute("SELECT * FROM users WHERE tg_id = ?", (tg_id,))
    return c.fetchone()

def user_already_has_code(tg_id: int, code: str) -> bool:
    c = get_cursor()
    if USE_POSTGRES:
        c.execute("SELECT 1 FROM distribution WHERE user_id = %s AND code = %s", (tg_id, code))
    else:
        c.execute("SELECT 1 FROM distribution WHERE user_id = ? AND code = ?", (tg_id, code))
    return c.fetchone() is not None

def add_promocodes(codes: List[str], total_uses: int):
    c = get_cursor()
    now = now_msk().strftime("%Y-%m-%d %H:%M:%S")
    for code in codes:
        if USE_POSTGRES:
            c.execute("INSERT INTO promocodes (code, total_uses, used, added_at) VALUES (%s, %s, 0, %s) ON CONFLICT (code) DO NOTHING", (code, total_uses, now))
        else:
            c.execute("INSERT OR IGNORE INTO promocodes (code, total_uses, used, added_at) VALUES (?, ?, 0, ?)", (code, total_uses, now))
    conn.commit()

# ---------------- FSM STATES ----------------
class RegisterState(StatesGroup):
    waiting_for_site_nick = State()

class AddPromoState(StatesGroup):
    waiting_for_code1 = State()
    waiting_for_code2 = State()
    waiting_for_code3 = State()
    waiting_for_uses = State()
    waiting_for_reserve = State()

class SetUsersState(StatesGroup):
    waiting_for_file = State()

class AssignState(StatesGroup):
    waiting_for_position = State()
    waiting_for_choose_user = State()

class GivePromoState(StatesGroup):
    waiting_for_site = State()
    waiting_for_choice = State()
    waiting_for_qty = State()
    waiting_for_codes = State()

class FindUserState(StatesGroup):
    waiting_for_input = State()

# ---------------- COMMANDS: /start (registration flow) ----------------
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    tg_id = message.from_user.id
    c = get_cursor()
    if USE_POSTGRES:
        c.execute("SELECT * FROM users WHERE tg_id = %s", (tg_id,))
    else:
        c.execute("SELECT * FROM users WHERE tg_id = ?", (tg_id,))
    u = c.fetchone()
    if u:
        status = u["status"]
        site = u["site_username"] or "-"
        if status == "approved":
            await message.answer(f"✅ Вы уже зарегистрированы как <code>{esc(site)}</code>.")
            return
        elif status == "pending":
            await message.answer("⏳ Ваша заявка уже отправлена и ожидает подтверждения администратора.")
            return
        elif status == "rejected":
            # check cooldown: rejected_at + 1 hour
            ra = u["rejected_at"]
            if ra:
                try:
                    ra_dt = datetime.fromisoformat(ra)
                except:
                    ra_dt = datetime.utcnow() - timedelta(hours=2)
                if now_msk() < (ra_dt + timedelta(hours=1)):
                    remaining = (ra_dt + timedelta(hours=1)) - now_msk()
                    mins = int(remaining.total_seconds() // 60) + 1
                    await message.answer(f"❌ Ваша предыдущая заявка была отклонена. Повторная подача возможна через {mins} минут.")
                    return
            # else allow reapply
    # ask for site nick (do NOT create pending until nick provided)
    await message.answer("Добро пожаловать! Пожалуйста, введите ваш ник с сайта (пример: user123). После отправки заявка будет направлена администраторам.")
    await state.set_state(RegisterState.waiting_for_site_nick)

@dp.message(RegisterState.waiting_for_site_nick)
async def process_registration_nick(message: Message, state: FSMContext):
    site_nick = message.text.strip()
    tg_id = message.from_user.id
    tg_username = message.from_user.username or message.from_user.full_name or ""
    c = get_cursor()
    if USE_POSTGRES:
        c.execute("SELECT * FROM users WHERE site_username = %s AND tg_id != %s", (site_nick, tg_id))
    else:
        c.execute("SELECT * FROM users WHERE site_username = ? AND tg_id != ?", (site_nick, tg_id))
    conflict = c.fetchone()
    if conflict and conflict["status"] == "approved":
        await message.answer("Этот ник уже зарегистрирован другим пользователем. Если вы считаете это ошибкой, свяжитесь с администратором.")
        await state.clear()
        return
    # upsert user row: create or update
    c = get_cursor()
    if USE_POSTGRES:
        c.execute("SELECT * FROM users WHERE tg_id = %s", (tg_id,))
    else:
        c.execute("SELECT * FROM users WHERE tg_id = ?", (tg_id,))
    existing = c.fetchone()
    if existing:
        # update site_username and set status pending (unless approved)
        if USE_POSTGRES:
            c.execute("UPDATE users SET site_username = %s, tg_username = %s, status = 'pending', rejected_at = NULL WHERE tg_id = %s", (site_nick, tg_username, tg_id))
        else:
            c.execute("UPDATE users SET site_username = ?, tg_username = ?, status = 'pending', rejected_at = NULL WHERE tg_id = ?", (site_nick, tg_username, tg_id))
    else:
        if USE_POSTGRES:
            c.execute("INSERT INTO users (tg_id, tg_username, site_username, status) VALUES (%s, %s, %s, 'pending')", (tg_id, tg_username, site_nick))
        else:
            c.execute("INSERT INTO users (tg_id, tg_username, site_username, status) VALUES (?, ?, ?, 'pending')", (tg_id, tg_username, site_nick))
    conn.commit()
    # notify admins with approve/reject buttons
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve:{tg_id}")],
        [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject:{tg_id}")]
    ])
    admin_text = f"[Новая регистрация]\nsite: <code>{esc(site_nick)}</code>\nTG: <a href=\"tg://user?id={esc(tg_id)}\">{esc(tg_id)}</a>"
    for admin in ADMIN_IDS:
        try:
            await bot.send_message(admin, admin_text, reply_markup=kb)
        except Exception:
            pass
    await message.answer(f"Ваша заявка отправлена администраторам. Ник: <code>{esc(site_nick)}</code>")
    await state.clear()

# ---------------- USER: /promo ----------------
@dp.message(Command("promo"))
async def cmd_promo(message: Message):
    tg_id = message.from_user.id
    user = find_user_by_tgid(tg_id)
    if not user or user["status"] != "approved":
        await message.answer("❌ Вы не зарегистрированы или заявка ещё не одобрена.")
        return

    week = get_week_start()
    c = get_cursor()
    if USE_POSTGRES:
        c.execute("""
            SELECT code
            FROM distribution
            WHERE user_id = %s AND given_at >= %s
            ORDER BY given_at
        """, (tg_id, week + " 00:00"))
    else:
        c.execute("""
            SELECT code
            FROM distribution
            WHERE user_id = ? AND given_at >= ?
            ORDER BY given_at
        """, (tg_id, week + " 00:00"))
    rows = c.fetchall()

    if not rows:
        await message.answer("❌ На этой неделе вы не были в списке на промо.")
        return

    issued_codes = [r["code"] for r in rows]
    header = "Привет, твой промокод за недельный топ 🎉🎉🎉\n1.5к камней\n\n"
    promo_lines = [f"{i+1}. <code>{esc(c)}</code>" for i, c in enumerate(issued_codes)]
    footer = "\n\n👉 <a href=\"https://animestars.org/promo_codes\">animestars.org</a>\n👉 <a href=\"https://asstars.tv/promo_codes\">asstars.tv</a>"
    await message.answer(header + "\n".join(promo_lines) + footer)

# ---------------- PENDING: list + approve/reject callbacks ----------------
@dp.message(Command("pending"))
async def cmd_pending(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    c = get_cursor()
    if USE_POSTGRES:
        c.execute("SELECT tg_id, tg_username, site_username, registered_at FROM users WHERE status = 'pending' ORDER BY registered_at")
    else:
        c.execute("SELECT tg_id, tg_username, site_username, registered_at FROM users WHERE status = 'pending' ORDER BY registered_at")
    rows = c.fetchall()
    if not rows:
        await message.answer("Нет ожидающих подтверждения.")
        return
    for r in rows:
        tgid = r["tg_id"]
        site = esc(r["site_username"])
        tgname = esc(r["tg_username"])
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve:{tgid}"),
             InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject:{tgid}")]
        ])
        await message.answer(f"Заявка:\nsite: <code>{site}</code>\nid: <code>{esc(tgid)}</code>", reply_markup=kb)

@dp.callback_query(lambda c: c.data and c.data.startswith("approve:"))
async def cb_approve(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав")
        return
    parts = callback.data.split(":", 1)
    if len(parts) != 2:
        await callback.answer()
        return
    tgid = int(parts[1])
    c = get_cursor()
    if USE_POSTGRES:
        c.execute("UPDATE users SET status='approved', rejected_at = NULL WHERE tg_id = %s", (tgid,))
    else:
        c.execute("UPDATE users SET status='approved', rejected_at = NULL WHERE tg_id = ?", (tgid,))
    conn.commit()
    try:
        await callback.message.edit_text(f"Пользователь <code>{esc(tgid)}</code> одобрен.")
    except:
        pass
    try:
        await bot.send_message(tgid, "🎉 Ваша заявка одобрена! Теперь вы участвуете в еженедельных раздачах промо.")
    except:
        pass
    await callback.answer("Одобрен")

@dp.callback_query(lambda c: c.data and c.data.startswith("reject:"))
async def cb_reject(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав")
        return
    parts = callback.data.split(":", 1)
    if len(parts) != 2:
        await callback.answer()
        return
    tgid = int(parts[1])
    c = get_cursor()
    now_str = now_msk().isoformat()
    if USE_POSTGRES:
        c.execute("UPDATE users SET status='rejected', rejected_at = %s WHERE tg_id = %s", (now_str, tgid))
    else:
        c.execute("UPDATE users SET status='rejected', rejected_at = ? WHERE tg_id = ?", (now_str, tgid))
    conn.commit()
    try:
        await callback.message.edit_text(f"Пользователь <code>{esc(tgid)}</code> отклонён.")
    except:
        pass
    try:
        await bot.send_message(tgid, "❌ Ваша заявка на регистрацию отклонена. Повторно можно подать через 1 час.")
    except:
        pass
    await callback.answer("Отклонён")

# ---------------- ADD PROMO (3 promo + uses + reserve) ----------------
@dp.message(Command("addpromo"))
async def cmd_addpromo_start(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await state.clear()
    await message.answer("Добавление промо — шаг 1/4. Введите код первого промо:")
    await state.set_state(AddPromoState.waiting_for_code1)

@dp.message(AddPromoState.waiting_for_code1)
async def addpromo_code1(message: Message, state: FSMContext):
    await state.update_data(code1=message.text.strip())
    await message.answer("Введите код второго промо:")
    await state.set_state(AddPromoState.waiting_for_code2)

@dp.message(AddPromoState.waiting_for_code2)
async def addpromo_code2(message: Message, state: FSMContext):
    await state.update_data(code2=message.text.strip())
    await message.answer("Введите код третьего промо:")
    await state.set_state(AddPromoState.waiting_for_code3)

@dp.message(AddPromoState.waiting_for_code3)
async def addpromo_code3(message: Message, state: FSMContext):
    await state.update_data(code3=message.text.strip())
    await message.answer("Введите общее количество использований (целое число):")
    await state.set_state(AddPromoState.waiting_for_uses)

@dp.message(AddPromoState.waiting_for_uses)
async def addpromo_uses(message: Message, state: FSMContext):
    try:
        uses = int(message.text.strip())
        if uses < 0:
            raise ValueError()
    except:
        await message.answer("Введите положительное целое число.")
        return
    await state.update_data(uses=uses)
    await message.answer("Сколько из этих использований положить в резерв? (целое число, 0..uses)")
    await state.set_state(AddPromoState.waiting_for_reserve)

@dp.message(AddPromoState.waiting_for_reserve)
async def addpromo_reserve(message: Message, state: FSMContext):
    try:
        reserve_put = int(message.text.strip())
        data = await state.get_data()
        uses = int(data.get("uses", 0))
        if reserve_put < 0 or reserve_put > uses:
            raise ValueError()
    except:
        await message.answer("Введите корректное число (0 .. общее количество использований).")
        return
    data = await state.get_data()
    codes = [data.get("code1"), data.get("code2"), data.get("code3")]
    uses = int(data.get("uses"))
    add_promocodes(codes, uses)
    # increase reserve by reserve_put
    set_reserve(get_reserve() + reserve_put)
    lines = ["✅ Промокоды добавлены:"]
    for i, ccode in enumerate(codes, start=1):
        lines.append(f"{i}. <code>{esc(ccode)}</code>")
    lines.append(f"Всего использований: <code>{esc(uses)}</code>")
    lines.append(f"В резерв отправлено: <code>{esc(reserve_put)}</code>")
    await message.answer("\n".join(lines))
    await state.clear()
    # show promostats
    await cmd_promostats(message)

# ---------------- SETUSERS (upload .txt or paste) ----------------
@dp.message(Command("setusers"))
async def cmd_setusers(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Пришлите .txt файл со списком site_nicks (по одному в строке) или вставьте список в сообщении.")
    await state.set_state(SetUsersState.waiting_for_file)

@dp.message(SetUsersState.waiting_for_file)
async def process_setusers_file(message: Message, state: FSMContext):
    if message.document:
        doc = message.document
        if not doc.file_name.lower().endswith(".txt"):
            await message.answer("Ошибка: нужен файл с расширением .txt")
            await state.clear()
            return
        temp_path = f"tmp_{int(datetime.now().timestamp())}_{doc.file_name}"
        try:
            file = await bot.get_file(doc.file_id)
            await bot.download_file(file.file_path, destination=temp_path)
        except Exception:
            await message.answer("Ошибка при скачивании файла. Попробуйте ещё раз.")
            await state.clear()
            return
        try:
            with open(temp_path, "r", encoding="utf-8") as f:
                lines = [ln.strip() for ln in f if ln.strip()]
        except Exception:
            await message.answer("Ошибка при чтении файла. Убедитесь в кодировке UTF-8.")
            try:
                os.remove(temp_path)
            except:
                pass
            await state.clear()
            return
        try:
            os.remove(temp_path)
        except:
            pass
    else:
        if not message.text or message.text.strip() == "":
            await message.answer("Нет файла и нет текста. Отправьте .txt или вставьте список.")
            await state.clear()
            return
        lines = [ln.strip() for ln in message.text.splitlines() if ln.strip()]

    if not lines:
        await message.answer("Файл/текст пустой — ничего не добавлено.")
        await state.clear()
        return

    week = get_week_start()
    c = get_cursor()
    if USE_POSTGRES:
        c.execute("DELETE FROM weekly_users WHERE week_start = %s", (week,))
    else:
        c.execute("DELETE FROM weekly_users WHERE week_start = ?", (week,))
    added = 0
    missing = []
    for idx, nick in enumerate(lines, start=1):
        user = find_user_by_site(nick)
        user_id = user["tg_id"] if user and user["status"] == "approved" else None
        if USE_POSTGRES:
            c.execute("INSERT INTO weekly_users (week_start, position, site_username, user_id) VALUES (%s, %s, %s, %s)", (week, idx, nick, user_id))
        else:
            c.execute("INSERT INTO weekly_users (week_start, position, site_username, user_id) VALUES (?, ?, ?, ?)", (week, idx, nick, user_id))
        if user and user["status"] == "approved":
            added += 1
        else:
            missing.append((idx, nick))
    conn.commit()
    reply = (
        f"✅ Список обновлён\n"
        f"Позиции: <code>{esc(len(lines))}</code>\n"
        f"Привязано зарегистрированных: <code>{esc(added)}</code>\n"
        f"Непривязано (пустых): <code>{esc(len(missing))}</code>\n\n"
        f"ℹ️ Используйте /missing чтобы просмотреть пустые позиции."
    )
    await message.answer(reply)
    await state.clear()

# ---------------- MISSING ----------------
@dp.message(Command("missing"))
async def cmd_missing(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    week = get_week_start()
    c = get_cursor()
    if USE_POSTGRES:
        c.execute("SELECT position, site_username FROM weekly_users WHERE week_start = %s AND (user_id IS NULL OR user_id = '') ORDER BY position", (week,))
    else:
        c.execute("SELECT position, site_username FROM weekly_users WHERE week_start = ? AND (user_id IS NULL OR user_id = '') ORDER BY position", (week,))
    rows = c.fetchall()
    if not rows:
        await message.answer("Пустых позиций нет.")
        return
    out = ["📋 Пустые места на этой неделе:\n"]
    for r in rows:
        out.append(f"#{r['position']} — {esc(r['site_username'])}")
    out.append("\nℹ️ Используйте /assign чтобы закрепить зарегистрированного пользователя на место.")
    await message.answer("\n".join(out))

# ---------------- USERS (all / free) ----------------
@dp.message(Command("users"))
async def cmd_users(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟢 Все пользователи", callback_data="users_all")],
        [InlineKeyboardButton(text="🔵 Свободные пользователи", callback_data="users_free")]
    ])
    await message.answer("Выберите список:", reply_markup=kb)

@dp.callback_query(lambda c: c.data == "users_all")
async def cb_users_all(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав")
        return
    c = get_cursor()
    c.execute("SELECT tg_id, site_username, tg_username, status FROM users ORDER BY registered_at")
    rows = c.fetchall()
    if not rows:
        await callback.message.edit_text("Нет зарегистрированных пользователей.")
        return
    out = ["👥 Все пользователи (статусы показаны):\n"]
    for r in rows:
        out.append(f"👤 site: <code>{esc(r['site_username'] or '-')}</code>")
        out.append(f"🆔 id: <code>{esc(r['tg_id'])}</code>")
        out.append(f"🔗 <a href=\"tg://user?id={esc(r['tg_id'])}\">@{esc(r['tg_username'] or r['tg_id'])}</a>")
        out.append(f"📌 Статус: <code>{esc(r['status'])}</code>")
        out.append("───────────────")
    await callback.message.edit_text("\n".join(out))

@dp.callback_query(lambda c: c.data == "users_free")
async def cb_users_free(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав")
        return
    week = get_week_start()
    c = get_cursor()
    if USE_POSTGRES:
        c.execute("""
            SELECT u.tg_id, u.site_username, u.tg_username
            FROM users u
            WHERE u.status='approved' AND u.tg_id NOT IN (
                SELECT user_id FROM weekly_users WHERE week_start = %s AND user_id IS NOT NULL
            )
            ORDER BY u.registered_at
        """, (week,))
    else:
        c.execute("""
            SELECT u.tg_id, u.site_username, u.tg_username
            FROM users u
            WHERE u.status='approved' AND u.tg_id NOT IN (
                SELECT user_id FROM weekly_users WHERE week_start = ? AND user_id IS NOT NULL
            )
            ORDER BY u.registered_at
        """, (week,))
    rows = c.fetchall()
    if not rows:
        await callback.message.edit_text("Нет свободных зарегистрированных.")
        return
    out = ["👥 Свободные зарегистрированные (не в недельном списке):\n"]
    for r in rows:
        out.append(f"👤 site: {esc(r['site_username'] or '-')}")
        out.append(f"🆔 id: <code>{esc(r['tg_id'])}</code>")
        out.append(f"🔗 <a href=\"tg://user?id={esc(r['tg_id'])}\">@{esc(r['tg_username'] or r['tg_id'])}</a>")
        out.append("───────────────")
    await callback.message.edit_text("\n".join(out))

# ---------------- ASSIGN ----------------
@dp.message(Command("assign"))
async def cmd_assign_start(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    week = get_week_start()
    c = get_cursor()
    if USE_POSTGRES:
        c.execute("SELECT position, site_username FROM weekly_users WHERE week_start = %s AND (user_id IS NULL OR user_id = '') ORDER BY position", (week,))
    else:
        c.execute("SELECT position, site_username FROM weekly_users WHERE week_start = ? AND (user_id IS NULL OR user_id = '') ORDER BY position", (week,))
    rows = c.fetchall()
    if not rows:
        await message.answer("Нет пустых позиций для назначения.")
        return
    out = ["📌 Пустые позиции на этой неделе:"]
    for r in rows:
        out.append(f"#{r['position']} — {esc(r['site_username'])}")
    out.append("\nВведите номер позиции, которую хотите заполнить:")
    await message.answer("\n".join(out))
    await state.set_state(AssignState.waiting_for_position)

@dp.message(AssignState.waiting_for_position)
async def assign_got_pos(message: Message, state: FSMContext):
    try:
        pos = int(message.text.strip())
    except:
        await message.answer("Введите корректный номер позиции (целое число).")
        return
    week = get_week_start()
    c = get_cursor()
    if USE_POSTGRES:
        c.execute("SELECT * FROM weekly_users WHERE week_start = %s AND position = %s", (week, pos))
    else:
        c.execute("SELECT * FROM weekly_users WHERE week_start = ? AND position = ?", (week, pos))
    row = c.fetchone()
    if not row:
        await message.answer("Позиции с таким номером нет. Проверьте /missing.")
        await state.clear()
        return
    if row["user_id"]:
        await message.answer("Эта позиция уже занята.")
        await state.clear()
        return
    # list available users to choose
    c = get_cursor()
    if USE_POSTGRES:
        c.execute("""
            SELECT u.tg_id, u.site_username, u.tg_username
            FROM users u
            WHERE u.status='approved' AND u.tg_id NOT IN (
                SELECT user_id FROM weekly_users WHERE week_start = %s AND user_id IS NOT NULL
            )
            ORDER BY u.registered_at
        """, (week,))
    else:
        c.execute("""
            SELECT u.tg_id, u.site_username, u.tg_username
            FROM users u
            WHERE u.status='approved' AND u.tg_id NOT IN (
                SELECT user_id FROM weekly_users WHERE week_start = ? AND user_id IS NOT NULL
            )
            ORDER BY u.registered_at
        """, (week,))
    users = c.fetchall()
    if not users:
        await message.answer("Нет свободных зарегистрированных для назначения.")
        await state.clear()
        return
    buttons = []
    for u in users:
        label = f"{u['site_username']} — @{u['tg_username'] or '-'}"
        buttons.append([
            InlineKeyboardButton(
                text=label,
                callback_data=f"assign_choose:{pos}:{u['tg_id']}"
            )
        ])

    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await message.answer("Выберите пользователя для назначения (нажмите кнопку):", reply_markup=kb)
    await state.update_data(position=pos)
    await state.set_state(AssignState.waiting_for_choose_user)

@dp.callback_query(lambda c: c.data and c.data.startswith("assign_choose:"))
async def cb_assign_choose(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав")
        return
    parts = callback.data.split(":")
    if len(parts) != 3:
        await callback.answer()
        return
    pos = int(parts[1])
    tg_id = int(parts[2])
    week = get_week_start()
    c = get_cursor()
    if USE_POSTGRES:
        c.execute("SELECT * FROM users WHERE tg_id = %s", (tg_id,))
    else:
        c.execute("SELECT * FROM users WHERE tg_id = ?", (tg_id,))
    u = c.fetchone()
    if not u:
        await callback.answer("Пользователь не найден")
        return
    if USE_POSTGRES:
        c.execute("UPDATE weekly_users SET user_id = %s WHERE week_start = %s AND position = %s", (tg_id, week, pos))
    else:
        c.execute("UPDATE weekly_users SET user_id = ? WHERE week_start = ? AND position = ?", (tg_id, week, pos))
    conn.commit()
    try:
        await callback.message.edit_text(f"✅ Назначено: <code>{esc(u['site_username'])}</code> → позиция #{esc(pos)}")
    except:
        pass
    await callback.answer()

# ---------------- GIVEPROMO (simplified interactive) ----------------
@dp.message(Command("givepromo"))
async def cmd_givepromo_start(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Введите ник с сайта пользователя, которому хотите выдать промо:")
    await state.set_state(GivePromoState.waiting_for_site)

@dp.message(GivePromoState.waiting_for_site)
async def givepromo_site_entered(message: Message, state: FSMContext):
    site = message.text.strip()
    user = find_user_by_site(site)
    if not user or user["status"] != "approved":
        await message.answer("Пользователь не найден или не одобрен.")
        await state.clear()
        return
    tg_id = user["tg_id"]
    c = get_cursor()
    c.execute("SELECT id, code, total_uses, used FROM promocodes ORDER BY added_at ASC, id ASC")
    promos = c.fetchall()
    available_codes = []
    for p in promos:
        rem = p["total_uses"] - p["used"]
        if rem <= 0:
            continue
        if user_already_has_code(tg_id, p["code"]):
            continue
        available_codes.append(p["code"])
    text_lines = [f"Кому: <code>{esc(site)}</code> (id: <code>{esc(tg_id)}</code>)", ""]
    if available_codes:
        text_lines.append(f"Доступно уникальных промо: {len(available_codes)}")
        for code in available_codes:
            text_lines.append(f"<code>{esc(code)}</code>")
    else:
        text_lines.append("Доступных уникальных промо нет.")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟢 Выдать из резерва", callback_data=f"give_type:reserve:{tg_id}:{esc(site)}")],
        [InlineKeyboardButton(text="🔵 Выдать свободные", callback_data=f"give_type:free:{tg_id}:{esc(site)}")]
    ])
    await message.answer("\n".join(text_lines), reply_markup=kb)
    await state.update_data(site=site, tg_id=tg_id)
    await state.set_state(GivePromoState.waiting_for_choice)

@dp.callback_query(lambda c: c.data and c.data.startswith("give_type:"))
async def cb_give_type(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав")
        return
    parts = callback.data.split(":")
    if len(parts) < 4:
        await callback.answer()
        return
    give_type = parts[1]
    tg_id = int(parts[2])
    await callback.message.edit_text(f"Вы выбрали: {'резерв' if give_type=='reserve' else 'свободные'} для tg_id={tg_id}. Введите сколько промо выдать (1-3):")
    await state.update_data(give_type=give_type, give_tg_id=tg_id)
    await state.set_state(GivePromoState.waiting_for_qty)
    await callback.answer()

@dp.message(GivePromoState.waiting_for_qty)
async def givepromo_qty(message: Message, state: FSMContext):
    try:
        qty = int(message.text.strip())
    except:
        await message.answer("Введите число 1..3")
        return
    if qty < 1 or qty > 3:
        await message.answer("Можно выдать только 1..3 промо.")
        await state.clear()
        return
    data = await state.get_data()
    tg_id = int(data.get("give_tg_id"))
    c = get_cursor()
    c.execute("SELECT id, code, total_uses, used FROM promocodes ORDER BY added_at ASC, id ASC")
    promos = c.fetchall()
    choices = []
    for p in promos:
        rem = p["total_uses"] - p["used"]
        if rem <= 0:
            continue
        if user_already_has_code(tg_id, p["code"]):
            continue
        choices.append(p["code"])
    if not choices:
        await message.answer("Нет доступных уникальных промо для выдачи этому пользователю.")
        await state.clear()
        return
    await state.update_data(qty=qty)
    sample = "\n".join([f"{i+1}. <code>{esc(c)}</code>" for i,c in enumerate(choices[:50])])
    await message.answer(f"Доступные коды (выберите {qty} уникальных, введите через пробел или в новой строке):\n{sample}")
    await state.set_state(GivePromoState.waiting_for_codes)

@dp.message(GivePromoState.waiting_for_codes)
async def givepromo_codes_entered(message: Message, state: FSMContext):
    text = message.text.strip()
    parts = [p.strip() for p in text.replace("\n"," ").split(" ") if p.strip()]
    data = await state.get_data()
    qty = int(data.get("qty"))
    give_type = data.get("give_type")
    tg_id = int(data.get("give_tg_id"))
    if len(parts) != qty:
        await message.answer(f"Ошибка: нужно ввести ровно {qty} уникальных кодов.")
        return
    if len(set(parts)) != len(parts):
        await message.answer("Ошибка: нельзя выдавать одинаковые промо одному пользователю.")
        return
    c = get_cursor()
    valid = []
    for code in parts:
        if USE_POSTGRES:
            c.execute("SELECT id, total_uses, used FROM promocodes WHERE code = %s", (code,))
        else:
            c.execute("SELECT id, total_uses, used FROM promocodes WHERE code = ?", (code,))
        p = c.fetchone()
        if not p:
            await message.answer(f"Код <code>{esc(code)}</code> не найден в базе.")
            return
        rem = p["total_uses"] - p["used"]
        if rem <= 0:
            await message.answer(f"Код <code>{esc(code)}</code> исчерпан.")
            return
        if user_already_has_code(tg_id, code):
            await message.answer(f"Пользователь уже получал код <code>{esc(code)}</code> ранее.")
            return
        valid.append((p["id"], code))
    # commit issuance
    issued_codes = []
    now = now_msk().strftime("%Y-%m-%d %H:%M:%S")
    for pid, code in valid:
        if USE_POSTGRES:
            c.execute("INSERT INTO distribution (user_id, promo_id, code, count, source, given_at) VALUES (%s, %s, %s, %s, %s, %s)", (tg_id, pid, code, 1, give_type, now))
            c.execute("UPDATE promocodes SET used = used + 1 WHERE id = %s", (pid,))
        else:
            c.execute("INSERT INTO distribution (user_id, promo_id, code, count, source, given_at) VALUES (?, ?, ?, ?, ?, ?)", (tg_id, pid, code, 1, give_type, now))
            c.execute("UPDATE promocodes SET used = used + 1 WHERE id = ?", (pid,))
        issued_codes.append(code)
    if give_type == "reserve":
        set_reserve(max(0, get_reserve() - len(issued_codes)))
    conn.commit()
    # notify user
    try:
        header = "Привет, твой промокод за недельный топ 🎉🎉🎉\n1.5к камней\n\n"
        promo_lines = [f"{i+1}. <code>{esc(c)}</code>" for i,c in enumerate(issued_codes)]
        footer = "\n\n👉 <a href=\"https://animestars.org/promo_codes\">animestars.org</a>\n👉 <a href=\"https://asstars.tv/promo_codes\">asstars.tv</a>"
        await bot.send_message(tg_id, header + "\n".join(promo_lines) + footer)
    except:
        pass
    await message.answer("✅ Выдано пользователю:\n" + "\n".join([f"<code>{esc(c)}</code>" for c in issued_codes]) + f"\n\n📦 Резерв: <code>{esc(get_reserve())}</code>")
    await state.clear()

# ---------------- FINDUSER ----------------
@dp.message(Command("finduser"))
async def cmd_finduser_start(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Введите ник с сайта или Telegram ID для поиска:")
    await state.set_state(FindUserState.waiting_for_input)

@dp.message(FindUserState.waiting_for_input)
async def finduser_handle(message: Message, state: FSMContext):
    term = message.text.strip()
    c = get_cursor()
    user = None
    if term.isdigit():
        if USE_POSTGRES:
            c.execute("SELECT * FROM users WHERE tg_id = %s", (int(term),))
        else:
            c.execute("SELECT * FROM users WHERE tg_id = ?", (int(term),))
        user = c.fetchone()
    else:
        if USE_POSTGRES:
            c.execute("SELECT * FROM users WHERE site_username = %s", (term,))
        else:
            c.execute("SELECT * FROM users WHERE site_username = ?", (term,))
        user = c.fetchone()
    if not user:
        await message.answer("Пользователь не найден.")
        await state.clear()
        return
    site_v = esc(user["site_username"])
    tid = user["tg_id"]
    tg_v = esc(user["tg_username"])
    week = get_week_start()
    c.execute("SELECT position FROM weekly_users WHERE week_start = %s AND user_id = %s" if USE_POSTGRES else "SELECT position FROM weekly_users WHERE week_start = ? AND user_id = ?", (week, tid))
    pos = c.fetchone()
    in_list = ("✅ да (позиция #" + str(pos["position"]) + ")") if pos else "❌ нет"
    text = (
        "🔎 Найден пользователь:\n"
        f"👤 Ник: <code>{site_v}</code>\n"
        f"🆔 Telegram: <a href=\"tg://user?id={esc(tid)}\">{esc(tid)}</a>\n"
        f"📌 Статус: <code>{esc(user['status'])}</code>\n"
        f"📦 В недельном списке: {in_list}"
    )
    kb = None
    if not pos and user["status"] == "approved":
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📌 Назначить в список", callback_data=f"find_assign:{tid}")]
        ])
    await message.answer(text, reply_markup=kb)
    await state.clear()

@dp.callback_query(lambda c: c.data and c.data.startswith("find_assign:"))
async def cb_find_assign(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав")
        return
    tid = int(callback.data.split(":",1)[1])
    week = get_week_start()
    c = get_cursor()
    if USE_POSTGRES:
        c.execute("SELECT position, site_username FROM weekly_users WHERE week_start = %s AND (user_id IS NULL OR user_id = '') ORDER BY position", (week,))
    else:
        c.execute("SELECT position, site_username FROM weekly_users WHERE week_start = ? AND (user_id IS NULL OR user_id = '') ORDER BY position", (week,))
    rows = c.fetchall()
    if not rows:
        await callback.message.edit_text("Нет пустых позиций для назначения.")
        return
    out = ["📌 Пустые позиции на этой неделе:"]
    for r in rows:
        out.append(f"#{r['position']} — {esc(r['site_username'])}")
    out.append("\nВведите номер позиции, которую хотите назначить пользователю:")
    await callback.message.edit_text("\n".join(out))
    # store assign target mapping in settings to be used during assign flow
    db_set_setting(f"assign_target:{tid}", "1")
    await callback.answer()

# ---------------- PROMOSTATS ----------------
@dp.message(Command("promostats"))
async def cmd_promostats(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    c = get_cursor()
    c.execute("SELECT id, code, total_uses, used, added_at FROM promocodes ORDER BY added_at ASC, id ASC")
    rows = c.fetchall()
    if not rows:
        await message.answer("Промокоды не добавлены.")
        return
    lines = ["📊 <b>Статистика промокодов</b>\n"]
    for r in rows:
        left = r["total_uses"] - r["used"]
        status_emoji = "🟢" if left > 0 else "🔴"
        lines.append(f"{status_emoji} <code>{esc(r['code'])}</code> — осталось: <code>{esc(left)}</code> / всего: <code>{esc(r['total_uses'])}</code>")
    lines.append("───────────────")
    lines.append(f"📦 Резерв: <code>{esc(get_reserve())}</code>")
    await message.answer("\n".join(lines))

# ---------------- DISTRIBUTION ALGORITHM (same approach as earlier) ----------------
def compute_allocation_ordered() -> Dict[int, List[str]]:
    c = get_cursor()
    week = get_week_start()
    if USE_POSTGRES:
        c.execute("SELECT position, site_username, user_id FROM weekly_users WHERE week_start = %s ORDER BY position", (week,))
    else:
        c.execute("SELECT position, site_username, user_id FROM weekly_users WHERE week_start = ? ORDER BY position", (week,))
    positions = c.fetchall()
    if not positions:
        return {}
    ordered = [r["user_id"] for r in positions]
    n = len(ordered)
    c.execute("SELECT id, code, total_uses, used, added_at FROM promocodes ORDER BY added_at ASC, id ASC")
    promos = c.fetchall()
    total_available = sum(max(0, p["total_uses"] - p["used"]) for p in promos)
    reserve = get_reserve()
    distributable = total_available - reserve
    if distributable <= 0:
        return {}
    allocated = [0] * n
    # give first 15 up to 3
    for i in range(min(15, n)):
        if ordered[i]:
            give = min(3, distributable)
            allocated[i] += give
            distributable -= give
    # then others get at least 1 until distributed
    for i in range(15, n):
        if distributable <= 0:
            break
        if ordered[i]:
            allocated[i] += 1
            distributable -= 1
    # if anything left, distribute round-robin among eligible
    if distributable > 0:
        eligible = [i for i in range(15, n) if ordered[i]] or [i for i in range(n) if ordered[i]]
        idx = 0
        while distributable > 0 and eligible:
            i = eligible[idx % len(eligible)]
            allocated[i] += 1
            distributable -= 1
            idx += 1
    promo_iter = [{"id": p["id"], "code": p["code"], "remaining": p["total_uses"] - p["used"]} for p in promos if (p["total_uses"] - p["used"]) > 0]
    if not promo_iter:
        return {}

    distribution_plan = {}
    promo_idx = 0
    for pos_idx, cnt in enumerate(allocated):
        tg_id = ordered[pos_idx]
        if not tg_id or cnt <= 0:
            continue
        codes_given = []
        used_codes_for_user = set()  # ensure unique codes per user in this allocation
        for _ in range(cnt):
            found = False
            for offset in range(len(promo_iter)):
                idx = (promo_idx + offset) % len(promo_iter)
                if promo_iter[idx]["remaining"] <= 0:
                    continue
                candidate = promo_iter[idx]["code"]
                if candidate in used_codes_for_user:
                    continue
                if user_already_has_code(tg_id, candidate):
                    continue
                promo_iter[idx]["remaining"] -= 1
                codes_given.append(candidate)
                used_codes_for_user.add(candidate)
                promo_idx = idx
                found = True
                break
            if not found:
                break
        if codes_given:
            distribution_plan[tg_id] = distribution_plan.get(tg_id, []) + codes_given
    return distribution_plan

# ---------------- WEEKLY CONFIRMATION FLOW & DISTRIBUTION ----------------
async def send_weekly_confirmation():
    week = get_week_start()
    c = get_cursor()
    c.execute("SELECT MAX(added_at) AS last_promos FROM promocodes")
    last_promos = c.fetchone()["last_promos"] or "—"
    last_list = "последние обновления: см. /setusers"
    plan = compute_allocation_ordered()
    if not plan:
        preview_text = "Предварительное распределение: невозможно — либо список пуст, либо недостаточно промокодов (после учёта резерва)."
    else:
        parts = []
        c2 = conn.cursor()
        if USE_POSTGRES:
            c2.execute("SELECT position, site_username, user_id FROM weekly_users WHERE week_start = %s ORDER BY position", (week,))
        else:
            c2.execute("SELECT position, site_username, user_id FROM weekly_users WHERE week_start = ? ORDER BY position", (week,))
        positions = c2.fetchall()
        for pos in positions[:60]:
            uid = pos["user_id"]
            parts.append(f"{pos['position']}: {pos['site_username']} -> {len(plan.get(uid, []))} промо")
        preview_text = "\n".join(parts)
    planned_time = now_msk().replace(hour=21, minute=8, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    msg_text = (
        f"Запрос на подтверждение еженедельной раздачи промо\n\n"
        f"Планируемая раздача: {planned_time} (MSK)\n"
        f"Неделя: {week}\n"
        f"Промо (последнее обновление): {esc(last_promos)}\n"
        f"{last_list}\n\n"
        f"Предварительное распределение:\n{preview_text}\n\n"
        f"Нажмите 📊 План раздачи, затем ✅ Подтвердить или ❌ Ошибка."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 План раздачи", callback_data="weekly_plan")],
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data="weekly_confirm")],
        [InlineKeyboardButton(text="❌ Ошибка", callback_data="weekly_error")]
    ])
    db_set_setting("weekly_confirmed", "0")
    for admin in ADMIN_IDS:
        try:
            await bot.send_message(admin, msg_text, reply_markup=kb)
        except:
            pass
    now = now_msk()
    remind_end = now.replace(hour=21, minute=15, second=0, microsecond=0)
    if now < remind_end:
        job_id = f"weekly_reminder_{get_week_start()}"
        try:
            scheduler.remove_job(job_id)
        except Exception:
            pass
        scheduler.add_job(weekly_reminder_job, "interval", minutes=1, start_date=now, end_date=remind_end, args=[get_week_start()], id=job_id)

async def weekly_reminder_job(week):
    if db_get_setting("weekly_confirmed") == "1":
        try:
            scheduler.remove_job(f"weekly_reminder_{week}")
        except:
            pass
        return
    for admin in ADMIN_IDS:
        try:
            await bot.send_message(admin, "Напоминание: ожидается подтверждение еженедельной раздачи промо. Нажмите 📊 План раздачи или ✅ Подтвердить.")
        except:
            pass

@dp.callback_query(lambda c: c.data == "weekly_plan")
async def cb_weekly_plan(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав")
        return
    plan = compute_allocation_ordered()
    if not plan:
        await callback.answer("Невозможно построить план (недостаточно промокодов/пустой список).")
        return
    c = get_cursor()
    week = get_week_start()
    if USE_POSTGRES:
        c.execute("SELECT position, site_username, user_id FROM weekly_users WHERE week_start = %s ORDER BY position", (week,))
    else:
        c.execute("SELECT position, site_username, user_id FROM weekly_users WHERE week_start = ? ORDER BY position", (week,))
    positions = c.fetchall()
    out = ["📊 Предварительный план распределения:\n"]
    idx = 1
    full_counts = {"full":0,"partial":0,"none":0}
    for pos in positions:
        uid = pos["user_id"]
        if not uid:
            out.append(f"{idx}. {esc(pos['site_username'])} — ❌ пусто")
            full_counts["none"] += 1
        else:
            codes = plan.get(uid, [])
            if not codes:
                out.append(f"{idx}. {esc(pos['site_username'])} — ❌ не получит промо")
                full_counts["none"] += 1
            else:
                out.append(f"{idx}. {esc(pos['site_username'])}")
                for i, code in enumerate(codes, start=1):
                    out.append(f"   ├─ <code>{esc(code)}</code>")
                suffix = "✅ (полный комплект)" if len(codes) >= 3 else f"⚠️ ({len(codes)} шт.)"
                out.append(f"   {suffix}")
                if len(codes) >= 3:
                    full_counts["full"] += 1
                else:
                    full_counts["partial"] += 1
        idx += 1
        if len(out) > 400:
            out.append("... (обрезано для превью)")
            break
    out.append("\n📦 Итоги превью:")
    out.append(f"✅ Полный комплект: {full_counts['full']}")
    out.append(f"⚠️ Частично: {full_counts['partial']}")
    out.append(f"❌ Без промо: {full_counts['none']}")
    try:
        await callback.message.answer("\n".join(out))
        await callback.answer()
    except:
        await callback.answer("Ошибка при отправке плана.")

@dp.callback_query(lambda c: c.data == "weekly_confirm")
async def cb_weekly_confirm(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав")
        return
    db_set_setting("weekly_confirmed", "1")
    try:
        await callback.message.edit_text("Раздача подтверждена администратором. Раздача будет выполнена в 21:08 (MSK).")
    except:
        pass
    await callback.answer("Подтвержено")

@dp.callback_query(lambda c: c.data == "weekly_error")
async def cb_weekly_error(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав")
        return
    db_set_setting("weekly_confirmed", "0")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Обновить промо (используйте /addpromo)", callback_data="err_update_promos")],
        [InlineKeyboardButton(text="📋 Обновить список недели (используйте /setusers)", callback_data="err_update_list")],
        [InlineKeyboardButton(text="⚖️ Ошибка распределения (проверьте /distribution)", callback_data="err_distribution")]
    ])
    try:
        await callback.message.edit_text("Выберите причину ошибки и следуйте подсказкам:", reply_markup=kb)
    except:
        pass
    await callback.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("err_"))
async def cb_weekly_error_choice(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав")
        return
    choice = callback.data
    if choice == "err_update_promos":
        try:
            await callback.message.edit_text("Вы выбрали: Обновить промо.\nПожалуйста, загрузите новые промокоды через /addpromo (интерактивно).")
        except:
            pass
    elif choice == "err_update_list":
        try:
            await callback.message.edit_text("Вы выбрали: Обновить список недели.\nПожалуйста, загрузите новый .txt через /setusers.")
        except:
            pass
    elif choice == "err_distribution":
        try:
            await callback.message.edit_text("Вы выбрали: Ошибка в распределении.\nПроверьте /distribution и скорректируйте данные.")
        except:
            pass
    await callback.answer()

async def weekly_distribution_job():
    week = get_week_start()
    last = db_get_setting("last_distribution_date")
    if last == week:
        return
    if db_get_setting("weekly_confirmed") != "1":
        return
    plan = compute_allocation_ordered()
    if not plan:
        db_set_setting("weekly_confirmed", "0")
        return
    c = get_cursor()
    c.execute("SELECT id, code, total_uses, used FROM promocodes ORDER BY added_at ASC, id ASC")
    promos = c.fetchall()
    rem_map = {p["code"]:(p["id"], p["total_uses"] - p["used"]) for p in promos}
    now = now_msk().strftime("%Y-%m-%d %H:%M:%S")
    for tg_id, codes in plan.items():
        issued = []
        for code in codes:
            pid, rem = rem_map.get(code, (None,0))
            if pid is None or rem <= 0:
                continue
            if user_already_has_code(tg_id, code):
                continue
            if USE_POSTGRES:
                c.execute("INSERT INTO distribution (user_id, promo_id, code, count, source, given_at) VALUES (%s, %s, %s, %s, %s, %s)", (tg_id, pid, code, 1, "normal", now))
                c.execute("UPDATE promocodes SET used = used + 1 WHERE id = %s", (pid,))
            else:
                c.execute("INSERT INTO distribution (user_id, promo_id, code, count, source, given_at) VALUES (?, ?, ?, ?, ?, ?)", (tg_id, pid, code, 1, "normal", now))
                c.execute("UPDATE promocodes SET used = used + 1 WHERE id = ?", (pid,))
            issued.append(code)
        if issued:
            try:
                header = "Привет, твой промокод за недельный топ 🎉🎉🎉\n1.5к камней\n\n"
                promo_lines = [f"{i+1}. <code>{esc(c)}</code>" for i,c in enumerate(issued)]
                footer = "\n\n👉 <a href=\"https://animestars.org/promo_codes\">animestars.org</a>\n👉 <a href=\"https://asstars.tv/promo_codes\">asstars.tv</a>"
                await bot.send_message(tg_id, header + "\n".join(promo_lines) + footer)
            except:
                pass
    conn.commit()
    db_set_setting("last_distribution_date", week)
    db_set_setting("weekly_confirmed", "0")
    try:
        scheduler.remove_job(f"weekly_reminder_{week}")
    except:
        pass

# schedule weekly confirmation and distribution
scheduler.add_job(send_weekly_confirmation, "cron", day_of_week="sun", hour=21, minute=7)
scheduler.add_job(weekly_distribution_job, "cron", day_of_week="sun", hour=21, minute=8)

# ---------------- MANUAL DISTRIBUTE (/distribute_now) ----------------
@dp.message(Command("distribute_now"))
async def cmd_distribute_now(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Показать план", callback_data="manual_plan")],
        [InlineKeyboardButton(text="✅ Подтвердить немедленно", callback_data="manual_confirm")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="manual_cancel")]
    ])
    await message.answer("⚠️ Подтвердите немедленную раздачу (без ожидания 21:07). Сначала проверьте план.", reply_markup=kb)

@dp.callback_query(lambda c: c.data == "manual_plan")
async def cb_manual_plan(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав")
        return
    plan = compute_allocation_ordered()
    if not plan:
        await callback.answer("Невозможно построить план.")
        return
    out = ["📊 План распределения (ручная раздача):"]
    idx = 1
    c = get_cursor()
    week = get_week_start()
    if USE_POSTGRES:
        c.execute("SELECT position, site_username, user_id FROM weekly_users WHERE week_start = %s ORDER BY position", (week,))
    else:
        c.execute("SELECT position, site_username, user_id FROM weekly_users WHERE week_start = ? ORDER BY position", (week,))
    positions = c.fetchall()
    for pos in positions:
        uid = pos["user_id"]
        if not uid:
            out.append(f"{idx}. {esc(pos['site_username'])} — ❌ пусто")
        else:
            codes = plan.get(uid, [])
            if not codes:
                out.append(f"{idx}. {esc(pos['site_username'])} — ❌ не получит промо")
            else:
                out.append(f"{idx}. {esc(pos['site_username'])}")
                for i, code in enumerate(codes, start=1):
                    out.append(f"   ├─ <code>{esc(code)}</code>")
                suffix = "✅ (полный комплект)" if len(codes) >= 3 else f"⚠️ ({len(codes)} шт.)"
                out.append(f"   {suffix}")
        idx += 1
        if len(out) > 400:
            out.append("... (обрезано)")
            break
    await callback.message.answer("\n".join(out))
    await callback.answer()

@dp.callback_query(lambda c: c.data == "manual_confirm")
async def cb_manual_confirm(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав")
        return
    plan = compute_allocation_ordered()
    if not plan:
        await callback.message.edit_text("Раздача не может быть выполнена (пустой план).")
        await callback.answer()
        return
    await callback.message.edit_text("Запускаю ручную раздачу...")
    await asyncio.sleep(0.5)
    c = get_cursor()
    c.execute("SELECT id, code, total_uses, used FROM promocodes ORDER BY added_at ASC, id ASC")
    promos = c.fetchall()
    rem_map = {p["code"]:(p["id"], p["total_uses"] - p["used"]) for p in promos}
    now = now_msk().strftime("%Y-%m-%d %H:%M:%S")
    for tg_id, codes in plan.items():
        issued = []
        for code in codes:
            pid, rem = rem_map.get(code, (None,0))
            if pid is None or rem <= 0:
                continue
            if user_already_has_code(tg_id, code):
                continue
            if USE_POSTGRES:
                c.execute("INSERT INTO distribution (user_id, promo_id, code, count, source, given_at) VALUES (%s, %s, %s, %s, %s, %s)", (tg_id, pid, code, 1, "manual", now))
                c.execute("UPDATE promocodes SET used = used + 1 WHERE id = %s", (pid,))
            else:
                c.execute("INSERT INTO distribution (user_id, promo_id, code, count, source, given_at) VALUES (?, ?, ?, ?, ?, ?)", (tg_id, pid, code, 1, "manual", now))
                c.execute("UPDATE promocodes SET used = used + 1 WHERE id = ?", (pid,))
            issued.append(code)
        if issued:
            try:
                header = "Привет, твой промокод за недельный топ 🎉🎉🎉\n1.5к камней\n\n"
                promo_lines = [f"{i+1}. <code>{esc(c)}</code>" for i,c in enumerate(issued)]
                footer = "\n\n👉 <a href=\"https://animestars.org/promo_codes\">animestars.org</a>\n👉 <a href=\"https://asstars.tv/promo_codes\">asstars.tv</a>"
                await bot.send_message(tg_id, header + "\n".join(promo_lines) + footer)
            except:
                pass
    conn.commit()
    db_set_setting("last_distribution_date", get_week_start())
    await callback.message.edit_text("Ручная раздача выполнена.")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "manual_cancel")
async def cb_manual_cancel(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав")
        return
    try:
        await callback.message.edit_text("Ручная раздача отменена.")
    except:
        pass
    await callback.answer()

# ---------------- REPORT MENU (plan / results) ----------------
@dp.message(Command("report"))
async def cmd_report_menu(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 План раздачи", callback_data="report_plan")],
        [InlineKeyboardButton(text="✅ Итоги раздачи", callback_data="report_results")]
    ])
    await message.answer("📝 Отчёты по промо — выберите:", reply_markup=kb)

@dp.callback_query(lambda c: c.data == "report_plan")
async def cb_report_plan(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав")
        return
    plan = compute_allocation_ordered()
    if not plan:
        await callback.answer("План недоступен (пусто).")
        return
    out = ["📊 План раздачи:\n"]
    c = get_cursor()
    week = get_week_start()
    if USE_POSTGRES:
        c.execute("SELECT position, site_username, user_id FROM weekly_users WHERE week_start = %s ORDER BY position", (week,))
    else:
        c.execute("SELECT position, site_username, user_id FROM weekly_users WHERE week_start = ? ORDER BY position", (week,))
    positions = c.fetchall()
    idx = 1
    for pos in positions:
        uid = pos["user_id"]
        if not uid:
            out.append(f"{idx}. {esc(pos['site_username'])} — ❌ пусто")
        else:
            codes = plan.get(uid, [])
            if not codes:
                out.append(f"{idx}. {esc(pos['site_username'])} — ❌ не получит промо")
            else:
                out.append(f"{idx}. {esc(pos['site_username'])}")
                for i, code in enumerate(codes, start=1):
                    out.append(f"   ├─ <code>{esc(code)}</code>")
                suffix = "✅ (полный комплект)" if len(codes) >= 3 else f"⚠️ ({len(codes)} шт.)"
                out.append(f"   {suffix}")
        idx += 1
        if len(out) > 400:
            out.append("... (обрезано)")
            break
    await callback.message.answer("\n".join(out))
    await callback.answer()

@dp.callback_query(lambda c: c.data == "report_results")
async def cb_report_results(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав")
        return
    week = get_week_start()
    c = get_cursor()
    if USE_POSTGRES:
        c.execute("""
            SELECT d.given_at, COALESCE(u.site_username,'-') AS site, COALESCE(u.tg_username,'-') AS tg, d.code, d.source
            FROM distribution d
            LEFT JOIN users u ON u.tg_id = d.user_id
            WHERE d.given_at >= %s
            ORDER BY d.given_at DESC
        """, (week + " 00:00",))
    else:
        c.execute("""
            SELECT d.given_at, COALESCE(u.site_username,'-') AS site, COALESCE(u.tg_username,'-') AS tg, d.code, d.source
            FROM distribution d
            LEFT JOIN users u ON u.tg_id = d.user_id
            WHERE d.given_at >= ?
            ORDER BY d.given_at DESC
        """, (week + " 00:00",))
    rows = c.fetchall()
    if not rows:
        await callback.message.answer("За эту неделю выдач ещё не было.")
        await callback.answer()
        return
    parts = ["📝 Итоги раздачи за текущую неделю:\n"]
    grouped = {}
    for r in rows:
        key = (r["site"], r["tg"])
        grouped.setdefault(key, []).append((r["given_at"], r["code"], r["source"]))
    for (site, tg), items in grouped.items():
        parts.append(f"👤 {esc(site)} | @{esc(tg)}")
        for it in items:
            parts.append(f"   ├─ <code>{esc(it[1])}</code> | {esc(it[2])} | <code>{esc(it[0])}</code>")
        parts.append("───────────────")
    c.execute("SELECT id, code, total_uses, used FROM promocodes ORDER BY added_at ASC, id ASC")
    promos = c.fetchall()
    parts.append("\n📦 Остатки промокодов:")
    for p in promos:
        left = p["total_uses"] - p["used"]
        parts.append(f"🎟️ <code>{esc(p['code'])}</code> — осталось: <code>{esc(left)}</code> / всего: <code>{esc(p['total_uses'])}</code>")
    parts.append(f"\n📦 Резерв: <code>{esc(get_reserve())}</code>")
    await callback.message.answer("\n".join(parts))
    await callback.answer()

# ---------------- BOT COMMANDS SETUP ----------------
async def set_commands():
    user_cmds = [
        types.BotCommand(command="start", description="Запустить бота / заявка"),
        types.BotCommand(command="promo", description="Показать мои промо за неделю"),
    ]
    admin_cmds = [
        types.BotCommand(command="pending", description="Заявки на регистрацию"),
        types.BotCommand(command="addpromo", description="Добавить 3 промо (интерактивно)"),
        types.BotCommand(command="setusers", description="Загрузить список недели (.txt)"),
        types.BotCommand(command="missing", description="Пустые позиции недели"),
        types.BotCommand(command="users", description="Списки пользователей"),
        types.BotCommand(command="assign", description="Назначить пользователя на позицию"),
        types.BotCommand(command="givepromo", description="Выдать промо вручную"),
        types.BotCommand(command="finduser", description="Найти пользователя"),
        types.BotCommand(command="promostats", description="Статистика промо"),
        types.BotCommand(command="report", description="План / итоги раздачи"),
        types.BotCommand(command="distribute_now", description="Ручная раздача сейчас (подтвердить)")
    ]
    try:
        await bot.set_my_commands(user_cmds)
    except:
        pass
    # per-admin (chat scope)
    for aid in ADMIN_IDS:
        try:
            await bot.set_my_commands(user_cmds + admin_cmds, scope=types.BotCommandScopeChat(chat_id=aid))
        except:
            pass

# ---------------- RUN ----------------
async def main():
    # запускаем планировщик
    scheduler.start()
    # запускаем polling aiogram
    await dp.start_polling(bot, allowed_updates=AllowedUpdates.all())

if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        try:
            asyncio.run(bot.session.close())
        except:
            pass
