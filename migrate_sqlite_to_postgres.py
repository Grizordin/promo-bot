import os
import sqlite3
import psycopg2
import psycopg2.extras

# ---- Настройки ----
SQLITE_FILE = "telegram_promo_bot.db"
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL is None:
    raise RuntimeError("DATABASE_URL is not set")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# ---- Подключения ----
sqlite_conn = sqlite3.connect(SQLITE_FILE)
sqlite_conn.row_factory = sqlite3.Row
sqlite_cur = sqlite_conn.cursor()

pg_conn = psycopg2.connect(DATABASE_URL)
pg_cur = pg_conn.cursor()

# ---- Маппинг: уникальные ключи для UPSERT ----
# Чтобы знать, по какому полю обновлять
unique_keys = {
    "users": ["tg_id"],
    "promocodes": ["code"],
    "settings": ["key"],
    "distribution": ["id"],     # тут по id (SERIAL PK)
    "weekly_users": ["id"],     # тут по id (SERIAL PK)
}

# ---- Перенос данных ----
for table, keys in unique_keys.items():
    print(f"Переносим таблицу: {table}")
    rows = sqlite_cur.execute(f"SELECT * FROM {table}").fetchall()
    if not rows:
        print("  → нет данных")
        continue

    columns = rows[0].keys()
    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))

    conflict_cols = ", ".join(keys)
    update_set = ", ".join([f"{col}=EXCLUDED.{col}" for col in columns if col not in keys])

    for row in rows:
        values = [row[c] for c in columns]
        query = f"""
            INSERT INTO {table} ({col_list})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_cols})
            DO UPDATE SET {update_set}
        """
        try:
            pg_cur.execute(query, values)
        except Exception as e:
            print(f"  ⚠️ ошибка при вставке строки {dict(row)}: {e}")

pg_conn.commit()
print("✅ Миграция завершена! Данные из SQLite теперь в PostgreSQL.")

# ---- Закрываем соединения ----
sqlite_conn.close()
pg_cur.close()
pg_conn.close()
