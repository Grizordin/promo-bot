import os
import asyncio
import html
from datetime import datetime, timedelta
from typing import List, Optional, Dict

from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from datetime import datetime, timedelta, timezone, date
from aiohttp import web
import re

# ---------------- CONFIG ----------------
# Token: keep fallback to original value so local usage doesn't break; you can set BOT_TOKEN in env on Render
BOT_TOKEN = os.getenv("BOT_TOKEN")
admin_ids_str = os.getenv("ADMIN_IDS", "")
try:
    ADMIN_IDS = [int(x.strip()) for x in admin_ids_str.split(",") if x.strip()]
except ValueError:
    ADMIN_IDS = []
PROMO_PATTERN = re.compile(r"^[A-Z0-9]{4}-[A-Z0-9]{4}$")

# ---------------- DB SETUP (Postgres if DATABASE_URL present, otherwise fallback to SQLite) ----------------
USE_POSTGRES = False
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    USE_POSTGRES = True
    import psycopg2
    from psycopg2.pool import SimpleConnectionPool
    import psycopg2.extras
    from contextlib import contextmanager
    import time

    # ---------------- PostgreSQL Connection Pool ----------------
    db_pool = None

    def init_db_pool():
        """Инициализирует пул соединений с повторными попытками"""
        global db_pool
        while True:
            try:
                db_pool = SimpleConnectionPool(
                    1, 10,  # минимум 1, максимум 10 соединений
                    dsn=DATABASE_URL,
                    sslmode="require"
                )
                print("[DB] ✅ PostgreSQL pool initialized.")
                break
            except Exception as e:
                print(f"[DB] ❌ Failed to init pool: {e}. Retrying in 5 seconds...")
                time.sleep(5)
    
    def get_connection():
        """Получает соединение из пула"""
        global db_pool
        if db_pool is None:
            init_db_pool()
        try:
            conn = db_pool.getconn()
            # Проверим, живо ли соединение
            try:
                with conn.cursor() as test_cur:
                    test_cur.execute("SELECT 1;")
            except Exception:
                print("[DB] ⚠️ Connection invalid, reinitializing pool...")
                init_db_pool()
                conn = db_pool.getconn()
            return conn
        except Exception as e:
            print(f"[DB] ⚠️ Failed to get connection: {e}. Reinitializing pool...")
            init_db_pool()
            return db_pool.getconn()

    def release_connection(conn):
        """Возвращает соединение обратно в пул"""
        global db_pool
        if db_pool and conn:
            try:
                db_pool.putconn(conn)
            except Exception as e:
                print(f"[DB] ⚠️ Failed to release connection: {e}")

    @contextmanager
    def get_cursor():
        """
        Контекстный менеджер:
        - Берёт соединение из пула
        - Проверяет соединение (SELECT 1)
        - Автоматически делает commit/rollback
        - При разрыве пересоздаёт пул и повторяет попытку 1 раз
        """
        conn = None
        cur = None
        try:
            conn = get_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            yield cur
            conn.commit()
        except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
            print(f"[DB] ⚠️ Lost connection during query: {e}. Reinitializing pool and retrying...")
            try:
                init_db_pool()
                conn = get_connection()
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                yield cur  # повторная попытка
                conn.commit()
            except Exception as e2:
                print(f"[DB] ❌ Retry failed: {e2}")
                if conn:
                    conn.rollback()
                raise
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            print(f"[DB] ❌ Query failed: {e}")
            raise
        finally:
            if cur:
                try:
                    cur.close()
                except:
                    pass
            if conn:
                try:
                    release_connection(conn)
                except:
                    pass

    # ---------------- Создание таблиц ----------------
    init_db_pool()

    with get_cursor() as c:
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

    # ---------------- Инициализация настроек ----------------
    with get_cursor() as c:
        c.execute("INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO NOTHING",
                  ("weekly_confirmed", "0"))
        c.execute("INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO NOTHING",
                  ("last_distribution_date", ""))

else:
    # fallback to sqlite for local/testing use
    import sqlite3
    DB_FILE = "telegram_promo_bot.db"
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # create tables (sqlite dialect)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        week_start DATE,
        position INTEGER,
        site_username TEXT,
        user_id BIGINT
    );
    """)

    # default settings initialization (sqlite style)
    cur.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", ("weekly_confirmed", "0"))
    cur.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", ("last_distribution_date", ""))

# ---------------- BOT / DISPATCHER / SCHEDULER ----------------
storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=storage)

# ---------------- HELPERS ----------------
def esc(s: Optional[str]) -> str:
    if s is None:
        return "-"
    return html.escape(str(s))

def db_get_setting(key: str) -> str:
    with get_cursor() as c:
        if USE_POSTGRES:
            c.execute("SELECT value FROM settings WHERE key = %s", (key,))
        else:
            c.execute("SELECT value FROM settings WHERE key = ?", (key,))
        r = c.fetchone()
        return r["value"] if r else ""

def db_set_setting(key: str, value: str):
    with get_cursor() as c:
        if USE_POSTGRES:
            c.execute("INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", (key, value))
        else:
            # sqlite: REPLACE INTO will insert or replace existing row
            c.execute("REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))

def get_week_start() -> date:
    today = datetime.now(timezone.utc).date()
    weekday = today.weekday()  # 0=Mon ... 6=Sun
    days_since_sunday = (weekday + 1) % 7
    return today - timedelta(days=days_since_sunday)

def find_user_by_site(site_username: str, c=None):
    if c is None:
        with get_cursor() as c2:
            return _find_user_by_site_inner(site_username, c2)
    return _find_user_by_site_inner(site_username, c)

def _find_user_by_site_inner(site_username, c):
    if USE_POSTGRES:
        c.execute("SELECT * FROM users WHERE site_username = %s", (site_username,))
    else:
        c.execute("SELECT * FROM users WHERE site_username = ?", (site_username,))
    return c.fetchone()

def find_user_by_tgid(tg_id: int):
    with get_cursor() as c:
        if USE_POSTGRES:
            c.execute("SELECT * FROM users WHERE tg_id = %s", (tg_id,))
        else:
            c.execute("SELECT * FROM users WHERE tg_id = ?", (tg_id,))
        return c.fetchone()

def user_already_has_code(tg_id: int, code: str, c=None) -> bool:
    if c is None:
        with get_cursor() as c2:
            return user_already_has_code(tg_id, code, c2)

    if USE_POSTGRES:
        c.execute("SELECT 1 FROM distribution WHERE user_id = %s AND code = %s", (tg_id, code))
    else:
        c.execute("SELECT 1 FROM distribution WHERE user_id = ? AND code = ?", (tg_id, code))
    return c.fetchone() is not None

def add_promocodes(codes: List[str], total_uses: int):
    with get_cursor() as c:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        for code in codes:
            if USE_POSTGRES:
                c.execute("INSERT INTO promocodes (code, total_uses, used, added_at) VALUES (%s, %s, 0, %s) ON CONFLICT (code) DO NOTHING", (code, total_uses, now))
            else:
                c.execute("INSERT OR IGNORE INTO promocodes (code, total_uses, used, added_at) VALUES (?, ?, 0, ?)", (code, total_uses, now))

# ---------------- FSM STATES ----------------
class RegisterState(StatesGroup):
    waiting_for_site_nick = State()

class AddPromoState(StatesGroup):
    waiting_for_code1 = State()
    waiting_for_code2 = State()
    waiting_for_code3 = State()
    waiting_for_uses = State()

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

# ---------------- UTILS ----------------
async def send_long_message(bot, chat_id, text, reply_markup=None, chunk_size=4000):
    """Отправляет длинное сообщение по частям, чтобы не превышать лимит Telegram."""
    lines = text.split("\n")
    chunk = ""
    for line in lines:
        # Если добавление следующей строки превысит лимит — отправляем предыдущий кусок
        if len(chunk) + len(line) + 1 > chunk_size:
            await bot.send_message(chat_id, chunk)
            chunk = ""
        chunk += line + "\n"
    # Отправляем оставшийся кусок, если есть
    if chunk:
        await bot.send_message(chat_id, chunk, reply_markup=reply_markup)

# ---------------- COMMANDS: /start (registration flow) ----------------
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    tg_id = message.from_user.id
    with get_cursor() as c:
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
                    ra_dt = datetime.now(timezone.utc) - timedelta(hours=2)
                if datetime.now(timezone.utc) < (ra_dt + timedelta(hours=1)):
                    remaining = (ra_dt + timedelta(hours=1)) - datetime.now(timezone.utc)
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
    with get_cursor() as c:
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
    with get_cursor() as c:
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
    # make explicit timestamp string YYYY-MM-DD HH:MM:SS (compatible with both Postgres and SQLite)
    week_start_dt = datetime.combine(week, datetime.min.time())
    week_start_str = week_start_dt.strftime("%Y-%m-%d %H:%M:%S")

    with get_cursor() as c:
        if USE_POSTGRES:
            c.execute("""
                SELECT code
                FROM distribution
                WHERE user_id = %s AND given_at >= %s
                ORDER BY given_at
            """, (tg_id, week_start_str))
        else:
            c.execute("""
                SELECT code
                FROM distribution
                WHERE user_id = ? AND given_at >= ?
                ORDER BY given_at
            """, (tg_id, week_start_str))
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
    with get_cursor() as c:
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
    with get_cursor() as c:
        if USE_POSTGRES:
            c.execute("UPDATE users SET status='approved', rejected_at = NULL WHERE tg_id = %s", (tgid,))
        else:
            c.execute("UPDATE users SET status='approved', rejected_at = NULL WHERE tg_id = ?", (tgid,))
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
    now_str = datetime.now(timezone.utc).isoformat()
    with get_cursor() as c:
        if USE_POSTGRES:
            c.execute("UPDATE users SET status='rejected', rejected_at = %s WHERE tg_id = %s", (now_str, tgid))
        else:
            c.execute("UPDATE users SET status='rejected', rejected_at = ? WHERE tg_id = ?", (now_str, tgid))
    try:
        await callback.message.edit_text(f"Пользователь <code>{esc(tgid)}</code> отклонён.")
    except:
        pass
    try:
        await bot.send_message(tgid, "❌ Ваша заявка на регистрацию отклонена. Повторно можно подать через 1 час.")
    except:
        pass
    await callback.answer("Отклонён")

# ---------------- ADD PROMO (3 promo + uses) ----------------
# --- Шаг 0: команда /addpromo ---
@dp.message(Command("addpromo"))
async def cmd_addpromo_start(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await state.clear()
    await message.answer("Добавление промо — шаг 1/4. Введите код первого промо:")
    await state.set_state(AddPromoState.waiting_for_code1)
    
@dp.message(AddPromoState.waiting_for_code1)
async def addpromo_code1(message: Message, state: FSMContext):
    code = message.text.strip().upper()
    if not PROMO_PATTERN.fullmatch(code):
        await message.answer("❌ Неверный формат. Пример правильного кода: <code>L725-8T33</code>")
        return
    await state.update_data(code1=code)
    await message.answer("Добавление промо — шаг 2/4. Введите код второго промо:")
    await state.set_state(AddPromoState.waiting_for_code2)

# --- Шаг 2: второй код ---
@dp.message(AddPromoState.waiting_for_code2)
async def addpromo_code2(message: Message, state: FSMContext):
    code = message.text.strip().upper()
    if not PROMO_PATTERN.fullmatch(code):
        await message.answer("❌ Неверный формат. Пример правильного кода: <code>L725-8T33</code>")
        return

    data = await state.get_data()
    if code == data.get("code1"):
        await message.answer("❌ Этот код уже был введён ранее. Введите другой промокод.")
        return

    await state.update_data(code2=code)
    await message.answer("Добавление промо — шаг 3/4. Введите код третьего промо:")
    await state.set_state(AddPromoState.waiting_for_code3)

# --- Шаг 3: третий код ---
@dp.message(AddPromoState.waiting_for_code3)
async def addpromo_code3(message: Message, state: FSMContext):
    code = message.text.strip().upper()
    if not PROMO_PATTERN.fullmatch(code):
        await message.answer("❌ Неверный формат. Пример правильного кода: <code>L725-8T33</code>")
        return

    data = await state.get_data()
    if code in [data.get("code1"), data.get("code2")]:
        await message.answer("❌ Этот код уже был введён ранее. Введите другой промокод.")
        return

    await state.update_data(code3=code)
    await message.answer("Добавление промо — шаг 4/4. Введите общее количество использований (целое число):")
    await state.set_state(AddPromoState.waiting_for_uses)

# --- Шаг 4: количество использований ---
@dp.message(AddPromoState.waiting_for_uses)
async def addpromo_uses(message: Message, state: FSMContext):
    try:
        uses = int(message.text.strip())
        if uses <= 0:
            raise ValueError()
    except:
        await message.answer("Введите положительное целое число.")
        return

    data = await state.get_data()
    codes = [data.get("code1"), data.get("code2"), data.get("code3")]
    codes = [c.strip().upper() for c in codes if c and c.strip()]

    # Проверка на уникальность (дополнительно)
    if len(set(codes)) < 3:
        await message.answer("❌ Промокоды не должны повторяться.")
        return

    # Добавление в БД
    try:
        add_promocodes(codes, uses)
    except Exception as e:
        await message.answer(f"❌ Ошибка при добавлении промокодов:\n<code>{esc(str(e))}</code>")
        return

    # Сообщение об успехе
    lines = ["✅ Промокоды успешно добавлены:"]
    for i, ccode in enumerate(codes, start=1):
        lines.append(f"{i}. <code>{esc(ccode)}</code>")
    lines.append(f"Всего использований: <code>{esc(uses)}</code>")

    await message.answer("\n".join(lines))
    await state.clear()

    # Показать статистику
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
    with get_cursor() as c:
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
    with get_cursor() as c:
        if USE_POSTGRES:
            c.execute("SELECT position, site_username FROM weekly_users WHERE week_start = %s AND user_id IS NULL ORDER BY position", (week,))
        else:
            c.execute("SELECT position, site_username FROM weekly_users WHERE week_start = ? AND (user_id IS NULL) ORDER BY position", (week,))
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
    with get_cursor() as c:
        c.execute("SELECT tg_id, site_username, tg_username, status FROM users ORDER BY registered_at")
        rows = c.fetchall()
    if not rows:
        await callback.message.edit_text("Нет зарегистрированных пользователей.")
        return

    batch = []
    for idx, r in enumerate(rows, start=1):
        batch.append(f"👤 site: <code>{esc(r['site_username'] or '-')}</code>")
        batch.append(f"🆔 id: <code>{esc(r['tg_id'])}</code>")
        batch.append(f"🔗 <a href=\"tg://user?id={esc(r['tg_id'])}\">@{esc(r['tg_username'] or r['tg_id'])}</a>")
        batch.append(f"📌 Статус: <code>{esc(r['status'])}</code>")
        batch.append("───────────────")

        if idx % 20 == 0:  # каждые 20 пользователей отправляем сообщение
            await callback.message.answer("\n".join(batch))
            batch = []

    if batch:
        await callback.message.answer("\n".join(batch))

@dp.callback_query(lambda c: c.data == "users_free")
async def cb_users_free(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав")
        return
    week = get_week_start()
    with get_cursor() as c:
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

    batch = []
    for idx, r in enumerate(rows, start=1):
        batch.append(f"👤 site: <code>{esc(r['site_username'] or '-')}</code>")
        batch.append(f"🆔 id: <code>{esc(r['tg_id'])}</code>")
        batch.append(f"🔗 <a href=\"tg://user?id={esc(r['tg_id'])}\">@{esc(r['tg_username'] or r['tg_id'])}</a>")
        batch.append("───────────────")

        if idx % 20 == 0:
            await callback.message.answer("\n".join(batch))
            batch = []

    if batch:
        await callback.message.answer("\n".join(batch))

# ---------------- ASSIGN ----------------
@dp.message(Command("assign"))
async def cmd_assign_start(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    week = get_week_start()
    with get_cursor() as c:
        if USE_POSTGRES:
            c.execute("SELECT position, site_username FROM weekly_users WHERE week_start = %s AND (user_id IS NULL) ORDER BY position", (week,))
        else:
            c.execute("SELECT position, site_username FROM weekly_users WHERE week_start = ? AND (user_id IS NULL) ORDER BY position", (week,))
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
    with get_cursor() as c:
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
    week = get_week_start()
    with get_cursor() as c:
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
    with get_cursor() as c:
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
    with get_cursor() as c:
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
        [InlineKeyboardButton(text="🟢 Выдать", callback_data=f"give_type:free:{tg_id}:{esc(site)}")]
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
    if len(parts) < 3:
        await callback.answer()
        return
    give_type = parts[1]  # ожидаем 'free'
    tg_id = int(parts[2])
    await callback.message.edit_text(f"Вы выбрали: выдавать промо пользователю tg_id={tg_id}. Введите сколько промо выдать (1-3):")
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
    with get_cursor() as c:
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
    with get_cursor() as c:
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
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    with get_cursor() as c:
        for pid, code in valid:
            if USE_POSTGRES:
                c.execute("INSERT INTO distribution (user_id, promo_id, code, count, source, given_at) VALUES (%s, %s, %s, %s, %s, %s)", (tg_id, pid, code, 1, give_type, now))
                c.execute("UPDATE promocodes SET used = used + 1 WHERE id = %s", (pid,))
            else:
                c.execute("INSERT INTO distribution (user_id, promo_id, code, count, source, given_at) VALUES (?, ?, ?, ?, ?, ?)", (tg_id, pid, code, 1, give_type, now))
                c.execute("UPDATE promocodes SET used = used + 1 WHERE id = ?", (pid,))
            issued_codes.append(code)
    # notify user
    try:
        header = "Привет, твой промокод за недельный топ 🎉🎉🎉\n1.5к камней\n\n"
        promo_lines = [f"{i+1}. <code>{esc(c)}</code>" for i,c in enumerate(issued_codes)]
        footer = "\n\n👉 <a href=\"https://animestars.org/promo_codes\">animestars.org</a>\n👉 <a href=\"https://asstars.tv/promo_codes\">asstars.tv</a>"
        await bot.send_message(tg_id, header + "\n".join(promo_lines) + footer)
    except:
        pass
    await message.answer("✅ Выдано пользователю:\n" + "\n".join([f"<code>{esc(c)}</code>" for c in issued_codes]))
    await state.clear()

# ---------------- LIMIT SETTINGS (global + per-user) ----------------

from aiogram.utils.keyboard import InlineKeyboardBuilder

# --- FSM состояния ---
class LimitState(StatesGroup):
    waiting_for_new_top = State()
    waiting_for_new_user_nick = State()
    waiting_for_new_user_limit = State()
    waiting_for_select_user = State()
    waiting_for_edit_limit = State()
    waiting_for_delete_confirm = State()


# --- Инициализация таблиц (вызывается при запуске) ---
def ensure_limit_tables():
    with get_cursor() as c:
        if USE_POSTGRES:
            c.execute("""
                CREATE TABLE IF NOT EXISTS promo_config (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS promo_limits (
                    id SERIAL PRIMARY KEY,
                    site TEXT UNIQUE,
                    limit_count INTEGER NOT NULL
                );
            """)
        else:
            c.execute("""
                CREATE TABLE IF NOT EXISTS promo_config (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS promo_limits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    site TEXT UNIQUE,
                    limit_count INTEGER NOT NULL
                );
            """)


# --- Получение/установка количества топов ---
def get_top_limit():
    with get_cursor() as c:
        if USE_POSTGRES:
            c.execute("SELECT value FROM promo_config WHERE key = 'top_limit'")
        else:
            c.execute("SELECT value FROM promo_config WHERE key = ?", ("top_limit",))
        row = c.fetchone()
        if not row:
            # если нет значения — создаем по умолчанию
            if USE_POSTGRES:
                c.execute("INSERT INTO promo_config (key, value) VALUES ('top_limit', '15')")
            else:
                c.execute("INSERT INTO promo_config (key, value) VALUES (?, ?)", ("top_limit", "15"))
            return 15
        return int(row["value"])


def set_top_limit(new_limit: int):
    with get_cursor() as c:
        if USE_POSTGRES:
            c.execute("""
                INSERT INTO promo_config (key, value)
                VALUES ('top_limit', %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """, (str(new_limit),))
        else:
            c.execute("""
                INSERT OR REPLACE INTO promo_config (key, value)
                VALUES (?, ?)
            """, ("top_limit", str(new_limit)))


# --- Работа с персональными лимитами ---
def get_all_personal_limits():
    with get_cursor() as c:
        c.execute("SELECT site, limit_count FROM promo_limits ORDER BY site ASC")
        return c.fetchall()


def set_user_limit(site: str, limit_count: int):
    with get_cursor() as c:
        if USE_POSTGRES:
            c.execute("""
                INSERT INTO promo_limits (site, limit_count)
                VALUES (%s, %s)
                ON CONFLICT (site) DO UPDATE SET limit_count = EXCLUDED.limit_count
            """, (site, limit_count))
        else:
            c.execute("""
                INSERT OR REPLACE INTO promo_limits (site, limit_count)
                VALUES (?, ?)
            """, (site, limit_count))


def delete_user_limit(site: str):
    with get_cursor() as c:
        if USE_POSTGRES:
            c.execute("DELETE FROM promo_limits WHERE site = %s", (site,))
        else:
            c.execute("DELETE FROM promo_limits WHERE site = ?", (site,))


# --- Команда /limit ---
@dp.message(Command("limit"))
async def cmd_limit_main(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Количество топов", callback_data="limit:top")],
        [InlineKeyboardButton(text="👤 Персональные лимиты", callback_data="limit:personal")]
    ])
    await message.answer("⚙️ Настройки лимитов промокодов:", reply_markup=kb)


# --- Обработка нажатий на основные кнопки ---
@dp.callback_query(lambda c: c.data and c.data.startswith("limit:"))
async def cb_limit_main(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав")
        return

    _, section = callback.data.split(":", 1)

    if section == "top":
        top_count = get_top_limit()
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Изменить количество", callback_data="limit:edit_top")]
        ])
        await callback.message.edit_text(
            f"3 промокода выдаются топ {top_count} из загружаемого списка.",
            reply_markup=kb
        )

    elif section == "personal":
        limits = get_all_personal_limits()
        if not limits:
            txt = "Персональные лимиты отсутствуют."
        else:
            lines = [f"{i+1}. {esc(l['site'])} — {l['limit_count']} промокод(а)" for i, l in enumerate(limits)]
            txt = "\n".join(lines)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить пользователя", callback_data="limit:add_user")],
            [InlineKeyboardButton(text="✏️ Изменить пользователя", callback_data="limit:edit_user")]
        ])
        await callback.message.edit_text(f"👤 Персональные лимиты:\n\n{txt}", reply_markup=kb)

    elif section == "edit_top":
        await callback.message.edit_text("Введите новое количество топов (целое число):")
        await state.set_state(LimitState.waiting_for_new_top)

    elif section == "add_user":
        await callback.message.edit_text("Введите ник пользователя с сайта, которому установить персональный лимит:")
        await state.set_state(LimitState.waiting_for_new_user_nick)

    elif section == "edit_user":
        limits = get_all_personal_limits()
        if not limits:
            await callback.answer("Список пуст.")
            return
        kb = InlineKeyboardBuilder()
        for l in limits:
            kb.button(text=l["site"], callback_data=f"limit:user:{l['site']}")
        kb.adjust(2)
        await callback.message.edit_text("Выберите пользователя для редактирования:", reply_markup=kb.as_markup())

    elif section.startswith("user:"):
        site = section.split(":", 1)[1]
        with get_cursor() as c:
            c.execute("SELECT limit_count FROM promo_limits WHERE site = %s" if USE_POSTGRES else "SELECT limit_count FROM promo_limits WHERE site = ?", (site,))
            row = c.fetchone()
        if not row:
            await callback.answer("Не найден.")
            return
        limit_count = row["limit_count"]
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Изменить лимит", callback_data=f"limit:change:{site}")],
            [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"limit:delete:{site}")]
        ])
        await callback.message.edit_text(f"{site} — {limit_count} промокод(а)", reply_markup=kb)

    elif section.startswith("change:"):
        site = section.split(":", 1)[1]
        await state.update_data(edit_site=site)
        await callback.message.edit_text("Введите новый лимит (1–3):")
        await state.set_state(LimitState.waiting_for_edit_limit)

    elif section.startswith("delete:"):
        site = section.split(":", 1)[1]
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить удаление", callback_data=f"limit:delete_confirm:{site}")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="limit:personal")]
        ])
        await callback.message.edit_text(f"Удалить лимит пользователя {site}?", reply_markup=kb)

    elif section.startswith("delete_confirm:"):
        site = section.split(":", 1)[1]
        delete_user_limit(site)
        await callback.message.edit_text(f"✅ Лимит пользователя {site} удалён.")
        await state.clear()

    await callback.answer()


# --- FSM шаги ---
@dp.message(LimitState.waiting_for_new_top)
async def limit_new_top_entered(message: Message, state: FSMContext):
    try:
        n = int(message.text.strip())
        if n <= 0:
            raise ValueError()
    except:
        await message.answer("Введите положительное целое число.")
        return
    set_top_limit(n)
    await message.answer(f"✅ Количество топов, получающих 3 промокода, изменено на {n}.")
    await state.clear()


@dp.message(LimitState.waiting_for_new_user_nick)
async def limit_add_user_nick(message: Message, state: FSMContext):
    site = message.text.strip()
    user = find_user_by_site(site)
    if not user or user["status"] != "approved":
        await message.answer("Пользователь не найден или не одобрен.")
        await state.clear()
        return
    await state.update_data(add_site=site)
    await message.answer("Введите лимит (1–3 промокода):")
    await state.set_state(LimitState.waiting_for_new_user_limit)


@dp.message(LimitState.waiting_for_new_user_limit)
async def limit_add_user_limit(message: Message, state: FSMContext):
    try:
        n = int(message.text.strip())
        if n < 1 or n > 3:
            raise ValueError()
    except:
        await message.answer("Введите число 1, 2 или 3.")
        return
    data = await state.get_data()
    site = data.get("add_site")
    set_user_limit(site, n)
    await message.answer(f"✅ Персональный лимит для {site} установлен: {n} промокод(а).")
    await state.clear()


@dp.message(LimitState.waiting_for_edit_limit)
async def limit_edit_limit_value(message: Message, state: FSMContext):
    try:
        n = int(message.text.strip())
        if n < 1 or n > 3:
            raise ValueError()
    except:
        await message.answer("Введите число 1, 2 или 3.")
        return
    data = await state.get_data()
    site = data.get("edit_site")
    set_user_limit(site, n)
    await message.answer(f"✅ Лимит пользователя {site} изменён на {n} промокод(а).")
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
    with get_cursor() as c:
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
    with get_cursor() as c:
        if USE_POSTGRES:
            c.execute("SELECT position FROM weekly_users WHERE week_start = %s AND user_id = %s", (week, tid))
        else:
            c.execute("SELECT position FROM weekly_users WHERE week_start = ? AND user_id = ?", (week, tid))
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
    with get_cursor() as c:
        if USE_POSTGRES:
            c.execute("SELECT position, site_username FROM weekly_users WHERE week_start = %s AND (user_id IS NULL) ORDER BY position", (week,))
        else:
            c.execute("SELECT position, site_username FROM weekly_users WHERE week_start = ? AND user_id IS NULL ORDER BY position", (week,))
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
    with get_cursor() as c:
        # distinct added_at values
        if USE_POSTGRES:
            c.execute("SELECT DISTINCT added_at FROM promocodes ORDER BY added_at DESC LIMIT 50")
        else:
            c.execute("SELECT DISTINCT added_at FROM promocodes ORDER BY added_at DESC LIMIT 50")
        rows = c.fetchall()
    if not rows:
        await message.answer("Промокоды не добавлены.")
        return
    buttons = []
    for r in rows:
        ts = r["added_at"]
        ts_str = ts if isinstance(ts, str) else ts.strftime("%Y-%m-%d %H:%M:%S")
        buttons.append([InlineKeyboardButton(text=f"📅 {ts_str}", callback_data=f"promostats:{ts_str}")])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await message.answer("Выберите загрузку промо для просмотра статистики:", reply_markup=kb)

@dp.callback_query(lambda c: c.data and c.data.startswith("promostats:"))
async def cb_promostats_show(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав")
        return
    ts_str = callback.data.split(":",1)[1]
    with get_cursor() as c:
        if USE_POSTGRES:
            c.execute("SELECT id, code, total_uses, used FROM promocodes WHERE added_at = %s ORDER BY id ASC", (ts_str,))
        else:
            c.execute("SELECT id, code, total_uses, used FROM promocodes WHERE added_at = ? ORDER BY id ASC", (ts_str,))
        rows = c.fetchall()
    if not rows:
        await callback.message.answer("Промокоды для этой загрузки не найдены.")
        await callback.answer()
        return
    lines = [f"📊 Статистика промо (загрузка {ts_str}):\n"]
    for r in rows:
        left = r["total_uses"] - r["used"]
        status_emoji = "🟢" if left > 0 else "🔴"
        lines.append(f"{status_emoji} <code>{esc(r['code'])}</code> — осталось: <code>{esc(left)}</code> / всего: <code>{esc(r['total_uses'])}</code>")
    kb_del = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 Удалить эту загрузку", callback_data=f"promostats_delete:{ts_str}")],
        [InlineKeyboardButton(text="Отмена", callback_data="noop")]
    ])
    await callback.message.answer("\n".join(lines), reply_markup=kb_del)
    await callback.answer()
@dp.callback_query(lambda c: c.data and c.data.startswith("promostats_delete:"))
async def cb_promostats_delete(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав")
        return
    ts_str = callback.data.split(":",1)[1]
    kb_confirm = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"promostats_delete_confirm:{ts_str}")],
        [InlineKeyboardButton(text="❌ Нет", callback_data="noop")]
    ])
    await callback.message.answer(f"Вы уверены, что хотите удалить все промокоды, загруженные {ts_str}? Это удалит строки из таблицы promocodes для этой даты.", reply_markup=kb_confirm)
    await callback.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("promostats_delete_confirm:"))
async def cb_promostats_delete_confirm(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав")
        return
    ts_str = callback.data.split(":",1)[1]
    with get_cursor() as c:
        try:
            if USE_POSTGRES:
                c.execute("DELETE FROM promocodes WHERE added_at = %s", (ts_str,))
            else:
                c.execute("DELETE FROM promocodes WHERE added_at = ?", (ts_str,))
            await callback.message.answer(f"Удаление промокодов, загруженных {ts_str}, выполнено.")
        except Exception as exc:
            await callback.message.answer(f"Ошибка при удалении: {exc}")
        await callback.answer()

@dp.callback_query(lambda c: c.data == "noop")
async def cb_noop(callback: types.CallbackQuery):
    try:
        await callback.answer()
    except:
        pass

@dp.callback_query(lambda c: c.data and c.data.startswith("report_delete:"))
async def cb_report_delete(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав")
        return
    d = callback.data.split(":",1)[1]
    kb_confirm = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"report_delete_confirm:{d}")],
        [InlineKeyboardButton(text="❌ Нет", callback_data="noop")]
    ])
    await callback.message.answer(f"Вы уверены, что хотите удалить все записи выдачи за {d}? Это удалит строки из таблицы distribution за эту дату.", reply_markup=kb_confirm)
    await callback.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("report_delete_confirm:"))
async def cb_report_delete_confirm(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав")
        return
    d = callback.data.split(":",1)[1]
    with get_cursor() as c:
        try:
            if USE_POSTGRES:
                c.execute("DELETE FROM distribution WHERE DATE(given_at) = %s", (d,))
            else:
                c.execute("DELETE FROM distribution WHERE DATE(given_at) = ?", (d,))
            await callback.message.answer(f"Удаление записей выдач за {d} выполнено.")
        except Exception as exc:
            await callback.message.answer(f"Ошибка при удалении: {exc}")
        await callback.answer()

        try:
            await callback.answer()
        except:
            pass

# ---------------- DISTRIBUTION ALGORITHM (with top + personal limits) ----------------

def compute_allocation_ordered() -> Dict[int, List[str]]:
    """
    Распределение промокодов с учетом:
    1) Топ пользователей (из promo_config.top_limit)
    2) Персональных лимитов (promo_limits)
    3) Остальной логики — 1 промо всем, потом +1 сверху списка
    """
    week = get_week_start()
    with get_cursor() as c:
        # 1) Список позиций текущей недели
        if USE_POSTGRES:
            c.execute("SELECT position, user_id FROM weekly_users WHERE week_start = %s ORDER BY position", (week,))
        else:
            c.execute("SELECT position, user_id FROM weekly_users WHERE week_start = ? ORDER BY position", (week,))
        positions = c.fetchall()
        if not positions:
            return {}

        n_positions = len(positions)

        # 2) Получаем свежие промокоды
        if USE_POSTGRES:
            c.execute("""
                SELECT id, code, total_uses, used
                FROM promocodes
                WHERE added_at = (SELECT MAX(added_at) FROM promocodes)
                ORDER BY id ASC
            """)
        else:
            c.execute("""
                SELECT id, code, total_uses, used
                FROM promocodes
                WHERE added_at = (SELECT MAX(added_at) FROM promocodes)
                ORDER BY id ASC
            """)
        promos = c.fetchall()

    promo_iter = [
        {"id": p["id"], "code": p["code"], "remaining": max(0, p["total_uses"] - p["used"])}
        for p in promos if (p["total_uses"] - p["used"]) > 0
    ]
    if not promo_iter:
        return {}

    total_available = sum(p["remaining"] for p in promo_iter)
    distributable = total_available
    if distributable <= 0:
        return {}

    # 3) Подготовка данных
    distribution_plan_by_pos: Dict[int, List[str]] = {}
    allocated = [0] * n_positions

    # --- (A) Получаем лимиты ---
    try:
        top_limit = get_top_limit()
    except Exception:
        top_limit = 15  # дефолт на случай отсутствия таблицы

    with get_cursor() as c:
        c.execute("SELECT site, limit_count FROM promo_limits ORDER BY site ASC")
        personal_limits = {r["site"]: r["limit_count"] for r in c.fetchall()}

    # --- (B) Персональные лимиты ---
    # Получаем user_id -> site из weekly_users
    user_site_map = {}
    with get_cursor() as c:
        c.execute("SELECT id, site_username FROM users")
        for r in c.fetchall():
            user_site_map[r["id"]] = r["site_username"]

    # применяем лимиты независимо от позиции
    manual_allocations: Dict[int, int] = {}
    for pos in positions:
        uid = pos["user_id"]
        site = user_site_map.get(uid)
        if not site:
            continue
        if site in personal_limits:
            manual_allocations[pos["position"]] = personal_limits[site]

    # --- (C) Топ пользователей ---
    top_count = min(top_limit, n_positions)
    for i in range(top_count):
        pos_num = positions[i]["position"]
        if pos_num in manual_allocations:
            continue  # у персонального уже задано
        give = min(3, distributable)
        allocated[i] = give
        distributable -= give
        if distributable <= 0:
            break

    # --- (D) Применяем персональные лимиты (если промо хватает) ---
    for pos_num, limit in manual_allocations.items():
        idx = next((i for i, p in enumerate(positions) if p["position"] == pos_num), None)
        if idx is None:
            continue
        give = min(limit, distributable)
        allocated[idx] = give
        distributable -= give
        if distributable <= 0:
            break

    # --- (E) Остальные пользователи — по 1 промо ---
    if distributable > 0:
        for i in range(n_positions):
            if distributable <= 0:
                break
            if allocated[i] == 0:  # не трогаем тех, кому уже дали
                allocated[i] = 1
                distributable -= 1

    # --- (F) Распределяем оставшиеся — +1 начиная сверху списка ---
    if distributable > 0:
        idx = 0
        while distributable > 0 and n_positions > 0:
            allocated[idx % n_positions] += 1
            distributable -= 1
            idx += 1

    # --- (G) Присваиваем коды ---
    promo_idx = 0
    for pos_idx, cnt in enumerate(allocated):
        pos_number = positions[pos_idx]["position"]
        if cnt <= 0:
            continue
        codes_for_pos = []
        used_codes_local = set()
        for _ in range(cnt):
            found = False
            for offset in range(len(promo_iter)):
                idx = (promo_idx + offset) % len(promo_iter)
                if promo_iter[idx]["remaining"] <= 0:
                    continue
                cand = promo_iter[idx]["code"]
                if cand in used_codes_local:
                    continue
                promo_iter[idx]["remaining"] -= 1
                codes_for_pos.append(cand)
                used_codes_local.add(cand)
                promo_idx = idx
                found = True
                break
            if not found:
                break
        if codes_for_pos:
            distribution_plan_by_pos[pos_number] = codes_for_pos

    return distribution_plan_by_pos

# ---------------- MANUAL DISTRIBUTE (/distribute_now) ----------------
@dp.message(Command("distribute_now"))
async def cmd_distribute_now(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    week = get_week_start()
    with get_cursor() as c:
        c.execute("SELECT MAX(week_start) AS last_list FROM weekly_users")
        last_list_row = c.fetchone()
        last_list = last_list_row["last_list"] if last_list_row else None
        c.execute("SELECT MAX(added_at) AS last_promos FROM promocodes")
        last_promos_row = c.fetchone()
        last_promos = last_promos_row["last_promos"] if last_promos_row else None

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Показать план", callback_data="manual_plan")],
        [InlineKeyboardButton(text="✅ Подтвердить немедленно", callback_data="manual_confirm")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="manual_cancel")]
    ])
    info = "⚠️ Подтвердите немедленную раздачу (без ожидания 21:07). Сначала проверьте план.\n\n"
    info += f"📋 Последний список: {last_list}\n"
    info += f"📦 Последние промо (added_at): {last_promos}\n\n"
    await message.answer(info, reply_markup=kb)

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
    week = get_week_start()
    with get_cursor() as c:
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
            codes = plan.get(pos['position'], [])
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
    week = get_week_start()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    with get_cursor() as c:
        # загружаем все промо
        c.execute("SELECT id, code, total_uses, used FROM promocodes ORDER BY added_at ASC, id ASC")
        promos = c.fetchall()

        rem_map = {p["code"]: (p["id"], p["total_uses"] - p["used"]) for p in promos}

        for pos_number, codes in plan.items():
            if USE_POSTGRES:
                c.execute(
                    "SELECT user_id FROM weekly_users WHERE week_start = %s AND position = %s",
                    (week, pos_number)
                )
            else:
                c.execute(
                    "SELECT user_id FROM weekly_users WHERE week_start = ? AND position = ?",
                    (week, pos_number)
                )

            row = c.fetchone()
            if not row or not row.get("user_id"):
                continue
            tg_id = row["user_id"]

            issued = []
            for code in codes:
                pid, rem = rem_map.get(code, (None, 0))
                if pid is None or rem <= 0:
                    continue
                if user_already_has_code(tg_id, code):
                    continue

                try:
                    if USE_POSTGRES:
                        c.execute(
                            "INSERT INTO distribution (user_id, promo_id, code, count, source, given_at) "
                            "VALUES (%s, %s, %s, %s, %s, %s)",
                            (tg_id, pid, code, 1, "manual", now)
                        )
                        c.execute("UPDATE promocodes SET used = used + 1 WHERE id = %s", (pid,))
                    else:
                        c.execute(
                            "INSERT INTO distribution (user_id, promo_id, code, count, source, given_at) "
                            "VALUES (?, ?, ?, ?, ?, ?)",
                            (tg_id, pid, code, 1, "manual", now)
                        )
                        c.execute("UPDATE promocodes SET used = used + 1 WHERE id = ?", (pid,))

                    issued.append(code)
                    rem_map[code] = (pid, rem - 1)

                except Exception:
                    continue

            if issued:
                try:
                    header = "Привет, твой промокод за недельный топ 🎉🎉🎉\n1.5к камней\n\n"
                    promo_lines = [f"{i+1}. <code>{esc(c)}</code>" for i, c in enumerate(issued)]
                    footer = "\n\n👉 <a href=\"https://animestars.org/promo_codes\">animestars.org</a>\n👉 <a href=\"https://asstars.tv/promo_codes\">asstars.tv</a>"
                    await bot.send_message(tg_id, header + "\n".join(promo_lines) + footer)
                except:
                    pass

    db_set_setting("last_distribution_date", str(get_week_start()))
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
    week = get_week_start()
    with get_cursor() as c:
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
                codes = plan.get(pos['position'], [])
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
        await send_long_message(bot, callback.message.chat.id, "\n".join(out))
        await callback.answer()

@dp.callback_query(lambda c: c.data == "report_results")
async def cb_report_results(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав")
        return
    with get_cursor() as c:
        # выбираем уникальные даты выдач (date part)
        if USE_POSTGRES:
            c.execute("SELECT DISTINCT DATE(given_at) AS d FROM distribution ORDER BY d DESC LIMIT 50")
        else:
            c.execute("SELECT DISTINCT DATE(given_at) AS d FROM distribution ORDER BY d DESC LIMIT 50")
        rows = c.fetchall()
    if not rows:
        await callback.message.answer("Выдач ещё не было.")
        await callback.answer()
        return
    buttons = []
    for r in rows:
        d = r["d"]
        buttons.append([InlineKeyboardButton(text=f"🗓 {d}", callback_data=f"report_results_show:{d}")])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await callback.message.answer("Выберите дату (день) выдач для показа итогов:", reply_markup=kb)
    await callback.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("report_results_show:"))
async def cb_report_results_show(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав")
        return
    d = callback.data.split(":",1)[1]
    with get_cursor() as c:
        if USE_POSTGRES:
            c.execute("""
                SELECT d.given_at, COALESCE(u.site_username,'-') AS site, COALESCE(u.tg_username,'-') AS tg, d.code, d.source
                FROM distribution d
                LEFT JOIN users u ON u.tg_id = d.user_id
                WHERE DATE(d.given_at) = %s
                ORDER BY d.given_at DESC
            """, (d,))
        else:
            c.execute("""
                SELECT d.given_at, COALESCE(u.site_username,'-') AS site, COALESCE(u.tg_username,'-') AS tg, d.code, d.source
                FROM distribution d
                LEFT JOIN users u ON u.tg_id = d.user_id
                WHERE DATE(d.given_at) = ?
                ORDER BY d.given_at DESC
            """, (d,))
        rows = c.fetchall()
        if not rows:
            await callback.message.answer("За выбранную дату выдач не найдено.")
            await callback.answer()
            return

        parts = [f"📝 Итоги раздачи за {d}:\n"]
    grouped = {}
    for r in rows:
        key = (r["site"], r["tg"])
        grouped.setdefault(key, []).append((r["given_at"], r["code"], r["source"]))
    for (site, tg), items in grouped.items():
        parts.append(f"👤 {site} / {tg}:")
        for it in items:
            parts.append(f"   • {it[0]} — <code>{esc(it[1])}</code> ({esc(it[2])})")
        parts.append("───────────────")

    kb_del = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 Удалить итоги этой выдачи", callback_data=f"report_delete:{d}")],
        [InlineKeyboardButton(text="Отмена", callback_data="noop")]
    ])

    # функция для безопасной отправки длинных сообщений
    async def send_long_message(chat_id, text, reply_markup=None, chunk_limit=4000):
        while text:
            chunk = text[:chunk_limit]
            # если не конец текста — обрезаем по последнему переводу строки
            if len(text) > chunk_limit:
                last_n = chunk.rfind("\n")
                if last_n > 0:
                    chunk = chunk[:last_n]
            await bot.send_message(chat_id, chunk, reply_markup=reply_markup)
            text = text[len(chunk):]

    # собираем весь текст в одну строку
    full_text = "\n".join(parts)

    # отправляем по кускам с клавиатурой только в последнем сообщении
    await send_long_message(
        chat_id=callback.message.chat.id,
        text=full_text,
        reply_markup=kb_del
    )

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
        types.BotCommand(command="givepromo", description="Выдать промо вручную"),
        types.BotCommand(command="limit", description="Настройки лимитов промокодов"),
        types.BotCommand(command="promostats", description="Статистика промо"),
        types.BotCommand(command="setusers", description="Загрузить список недели (.txt)"),
        types.BotCommand(command="missing", description="Пустые позиции недели"),
        types.BotCommand(command="assign", description="Назначить пользователя на позицию"),
        types.BotCommand(command="users", description="Списки пользователей"),
        types.BotCommand(command="finduser", description="Найти пользователя"),
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
PORT = int(os.getenv("PORT", 10000))  # Render сам задаёт PORT

async def handle(request):
    return web.Response(text="Bot is running!")

async def start_webserver():
    app = web.Application()
    app.router.add_get("/", handle)   # health-check на /
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"✅ Web server started on port {PORT}")

async def main():
    ensure_limit_tables()
    await set_commands()
    # запускаем webserver и polling одновременно
    await asyncio.gather(
        start_webserver(),
        dp.start_polling(bot)
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        try:
            asyncio.run(bot.session.close())
        except:
            pass
