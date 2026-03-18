"""
database.py — SQLite база данных казино.
Файл БД: casino.db

Экспортируемые функции (используются другими модулями):

  Инициализация:
    init_db()                          — создаёт все таблицы
    import_users_from_json()           — импорт из users.json при потере БД

  Пользователи (payments.py):
    db_get_user(user_id)               -> dict
    db_get_all_users()                 -> list[dict]
    db_get_all_user_ids()              -> list[int]   (для broadcast)
    db_set_balance(user_id, amount)
    db_update_field(user_id, field, value)
    update_user_info(user_id, first_name, username)   async

  Игры (mines, tower, gold, game, duels):
    save_game_result(user_id, game_name, win_amount)  async

  Депозиты / выводы (payments.py):
    save_deposit(user_id, amount, crypto_invoice_id)  async
    save_withdrawal(user_id, amount)                  async

  Рефералы (referrals.py):
    register_referral(new_user_id, referrer_id)       async
    save_referral_commission(referrer_id, referral_id, amount)  async
    save_referral_withdrawal(user_id, amount)         async
"""

import sqlite3
import logging
from datetime import datetime
from typing import Optional

DB_PATH = "casino.db"


# ══════════════════════════════════════════════════════════════════
#  ПОДКЛЮЧЕНИЕ
# ══════════════════════════════════════════════════════════════════

def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _add_column_if_missing(conn, table: str, column: str, definition: str):
    """Добавляет колонку если её нет (безопасная миграция)."""
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
    except Exception:
        pass  # уже существует


# ══════════════════════════════════════════════════════════════════
#  ИНИЦИАЛИЗАЦИЯ ТАБЛИЦ
# ══════════════════════════════════════════════════════════════════

async def init_db():
    """Создаёт все таблицы если их нет. Безопасно при повторном вызове."""
    try:
        with _connect() as conn:

            # ── Пользователи ──────────────────────────────────────
            conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id           INTEGER PRIMARY KEY,
                    first_name        TEXT    DEFAULT '',
                    last_name         TEXT    DEFAULT '',
                    username          TEXT    DEFAULT '',
                    balance           REAL    DEFAULT 0.0,
                    total_deposits    REAL    DEFAULT 0.0,
                    total_withdrawals REAL    DEFAULT 0.0,
                    last_withdrawal   TEXT    DEFAULT NULL,
                    join_date         TEXT    DEFAULT ''
                )
            """)
            # Миграции (для старых БД где колонок могло не быть)
            _add_column_if_missing(conn, "users", "last_name",         "TEXT DEFAULT ''")
            _add_column_if_missing(conn, "users", "username",          "TEXT DEFAULT ''")
            _add_column_if_missing(conn, "users", "total_deposits",    "REAL DEFAULT 0.0")
            _add_column_if_missing(conn, "users", "total_withdrawals", "REAL DEFAULT 0.0")
            _add_column_if_missing(conn, "users", "last_withdrawal",   "TEXT DEFAULT NULL")

            # ── Результаты игр (mines, tower, gold, game, duels) ──
            conn.execute("""
                CREATE TABLE IF NOT EXISTS game_results (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id     INTEGER NOT NULL,
                    game_name   TEXT    DEFAULT '',
                    win_amount  REAL    DEFAULT 0.0,
                    created_at  TEXT    DEFAULT (datetime('now'))
                )
            """)

            # ── Депозиты ──────────────────────────────────────────
            conn.execute("""
                CREATE TABLE IF NOT EXISTS deposits (
                    id                INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id           INTEGER NOT NULL,
                    amount            REAL    NOT NULL,
                    crypto_invoice_id INTEGER DEFAULT NULL,
                    created_at        TEXT    DEFAULT (datetime('now'))
                )
            """)

            # ── Выводы ────────────────────────────────────────────
            conn.execute("""
                CREATE TABLE IF NOT EXISTS withdrawals (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id    INTEGER NOT NULL,
                    amount     REAL    NOT NULL,
                    created_at TEXT    DEFAULT (datetime('now'))
                )
            """)

            # ── Рефералы ──────────────────────────────────────────
            conn.execute("""
                CREATE TABLE IF NOT EXISTS referrals (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    new_user_id  INTEGER NOT NULL,
                    referrer_id  INTEGER NOT NULL,
                    created_at   TEXT    DEFAULT (datetime('now')),
                    UNIQUE(new_user_id)
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS referral_commissions (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    referrer_id INTEGER NOT NULL,
                    referral_id INTEGER NOT NULL,
                    amount      REAL    NOT NULL,
                    created_at  TEXT    DEFAULT (datetime('now'))
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS referral_withdrawals (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id    INTEGER NOT NULL,
                    amount     REAL    NOT NULL,
                    created_at TEXT    DEFAULT (datetime('now'))
                )
            """)

            # ── Таблица лидеров (leaders.py создаёт сам, но на случай) ──
            conn.execute("""
                CREATE TABLE IF NOT EXISTS leaders_stats (
                    user_id     INTEGER NOT NULL,
                    date        TEXT    NOT NULL,
                    name        TEXT    DEFAULT '',
                    turnover    REAL    DEFAULT 0.0,
                    wins        REAL    DEFAULT 0.0,
                    deposits    REAL    DEFAULT 0.0,
                    withdrawals REAL    DEFAULT 0.0,
                    PRIMARY KEY (user_id, date)
                )
            """)
            _add_column_if_missing(conn, "leaders_stats", "deposits",    "REAL DEFAULT 0.0")
            _add_column_if_missing(conn, "leaders_stats", "withdrawals", "REAL DEFAULT 0.0")

            conn.commit()

        logging.info("[DB] Все таблицы инициализированы.")
    except Exception as e:
        logging.error(f"[DB] Ошибка init_db: {e}")
        raise


# ══════════════════════════════════════════════════════════════════
#  ИМПОРТ ИЗ JSON (восстановление после потери БД)
# ══════════════════════════════════════════════════════════════════

async def import_users_from_json():
    """
    Импортирует пользователей из users.json в БД.
    Запускается при старте если файл существует.
    """
    import os, json
    path = "users.json"
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        count = 0
        with _connect() as conn:
            for uid_str, udata in data.items():
                try:
                    uid = int(uid_str)
                    conn.execute("""
                        INSERT INTO users
                            (user_id, first_name, username, balance,
                             total_deposits, total_withdrawals, join_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(user_id) DO UPDATE SET
                            first_name        = excluded.first_name,
                            username          = excluded.username,
                            balance           = excluded.balance,
                            total_deposits    = excluded.total_deposits,
                            total_withdrawals = excluded.total_withdrawals
                    """, (
                        uid,
                        udata.get("first_name", ""),
                        udata.get("username", ""),
                        float(udata.get("balance", 0.0)),
                        float(udata.get("total_deposits", 0.0)),
                        float(udata.get("total_withdrawals", 0.0)),
                        udata.get("join_date", datetime.now().strftime("%Y-%m-%d")),
                    ))
                    count += 1
                except Exception as e:
                    logging.warning(f"[DB] import uid={uid_str}: {e}")
            conn.commit()
        logging.info(f"[DB] Импортировано {count} пользователей из {path}")
    except Exception as e:
        logging.error(f"[DB] Ошибка import_users_from_json: {e}")


# ══════════════════════════════════════════════════════════════════
#  ПОЛЬЗОВАТЕЛИ
# ══════════════════════════════════════════════════════════════════

def _default_user(user_id: int) -> dict:
    return {
        "user_id":           user_id,
        "first_name":        "",
        "last_name":         "",
        "username":          "",
        "balance":           0.0,
        "total_deposits":    0.0,
        "total_withdrawals": 0.0,
        "last_withdrawal":   None,
        "join_date":         datetime.now().strftime("%Y-%m-%d"),
    }


def db_get_user(user_id: int) -> dict:
    """
    Возвращает данные пользователя.
    Если нет — создаёт запись с нулями и возвращает её.
    Используется в: payments.py (Storage._load_from_db, Storage.get_user)
    """
    try:
        with _connect() as conn:
            cur = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            row = cur.fetchone()
            if row:
                return dict(row)
            # Новый пользователь
            join_date = datetime.now().strftime("%Y-%m-%d")
            conn.execute(
                "INSERT OR IGNORE INTO users (user_id, join_date) VALUES (?, ?)",
                (user_id, join_date)
            )
            conn.commit()
            return _default_user(user_id)
    except Exception as e:
        logging.error(f"[DB] db_get_user user_id={user_id}: {e}")
        return _default_user(user_id)


def db_get_all_users() -> list:
    """
    Возвращает список всех пользователей как list[dict].
    Используется в: payments.py (Storage._load_from_db)
    """
    try:
        with _connect() as conn:
            cur = conn.execute("SELECT * FROM users")
            return [dict(row) for row in cur.fetchall()]
    except Exception as e:
        logging.error(f"[DB] db_get_all_users: {e}")
        return []


def db_get_all_user_ids() -> list:
    """
    Возвращает список всех user_id.
    Используется в: broadcast.py (рассылка)
    """
    try:
        with _connect() as conn:
            cur = conn.execute("SELECT user_id FROM users")
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        logging.error(f"[DB] db_get_all_user_ids: {e}")
        return []


def db_set_balance(user_id: int, amount: float):
    """
    Устанавливает баланс пользователя.
    Используется в: payments.py (Storage._save_balance_to_db)
    """
    try:
        with _connect() as conn:
            conn.execute("""
                INSERT INTO users (user_id, balance, join_date)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET balance = excluded.balance
            """, (user_id, round(float(amount), 8), datetime.now().strftime("%Y-%m-%d")))
            conn.commit()
    except Exception as e:
        logging.error(f"[DB] db_set_balance user_id={user_id}: {e}")


def db_update_field(user_id: int, field: str, value):
    """
    Обновляет произвольное поле пользователя.
    Используется в: payments.py, main.py (_save_username)
    Допустимые поля защищены whitelist-ом.
    """
    ALLOWED = {
        "first_name", "last_name", "username", "balance",
        "total_deposits", "total_withdrawals", "last_withdrawal"
    }
    if field not in ALLOWED:
        logging.warning(f"[DB] db_update_field: недопустимое поле '{field}'")
        return
    try:
        with _connect() as conn:
            conn.execute(
                f"UPDATE users SET {field} = ? WHERE user_id = ?",
                (value, user_id)
            )
            conn.commit()
    except Exception as e:
        logging.error(f"[DB] db_update_field user_id={user_id} field={field}: {e}")


async def update_user_info(user_id: int, first_name: str = "", username: str = ""):
    """
    Обновляет имя и юзернейм пользователя (async).
    Используется в: payments.py (_process_deposit)
    """
    try:
        with _connect() as conn:
            conn.execute("""
                INSERT INTO users (user_id, first_name, username, join_date)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    first_name = CASE WHEN excluded.first_name != '' THEN excluded.first_name ELSE first_name END,
                    username   = CASE WHEN excluded.username   != '' THEN excluded.username   ELSE username   END
            """, (
                user_id,
                first_name or "",
                username or "",
                datetime.now().strftime("%Y-%m-%d")
            ))
            conn.commit()
    except Exception as e:
        logging.error(f"[DB] update_user_info user_id={user_id}: {e}")


# ══════════════════════════════════════════════════════════════════
#  ИГРЫ (mines, tower, gold, game, duels)
# ══════════════════════════════════════════════════════════════════

async def save_game_result(user_id: int, game_name: str, win_amount: float):
    """
    Сохраняет результат игры.
    Используется в: mines.py, tower.py, gold.py, game.py, duels.py
    Импортируется как: from database import save_game_result as db_save_game_result
    """
    try:
        with _connect() as conn:
            conn.execute("""
                INSERT INTO game_results (user_id, game_name, win_amount)
                VALUES (?, ?, ?)
            """, (user_id, game_name, float(win_amount)))
            conn.commit()
    except Exception as e:
        logging.error(f"[DB] save_game_result user_id={user_id} game={game_name}: {e}")


async def update_balance(user_id: int, amount: float) -> Optional[float]:
    """
    Добавляет amount к балансу пользователя (может быть отрицательным).
    Используется в: mines.py, tower.py, game.py (импортируется но не вызывается напрямую — через storage)
    Возвращает новый баланс или None при ошибке.
    """
    try:
        with _connect() as conn:
            conn.execute("""
                UPDATE users SET balance = ROUND(balance + ?, 8) WHERE user_id = ?
            """, (float(amount), user_id))
            conn.commit()
            cur = conn.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
            row = cur.fetchone()
            return float(row[0]) if row else None
    except Exception as e:
        logging.error(f"[DB] update_balance user_id={user_id}: {e}")
        return None


# ══════════════════════════════════════════════════════════════════
#  ДЕПОЗИТЫ / ВЫВОДЫ
# ══════════════════════════════════════════════════════════════════

async def save_deposit(user_id: int, amount: float, crypto_invoice_id: int):
    """
    Сохраняет запись о депозите.
    Используется в: payments.py (check_payment_task)
    """
    try:
        with _connect() as conn:
            conn.execute("""
                INSERT INTO deposits (user_id, amount, crypto_invoice_id)
                VALUES (?, ?, ?)
            """, (user_id, float(amount), int(crypto_invoice_id)))
            conn.commit()
    except Exception as e:
        logging.error(f"[DB] save_deposit user_id={user_id}: {e}")


async def save_withdrawal(user_id: int, amount: float):
    """
    Сохраняет запись о выводе.
    Используется в: payments.py (_process_withdraw)
    """
    try:
        with _connect() as conn:
            conn.execute("""
                INSERT INTO withdrawals (user_id, amount)
                VALUES (?, ?)
            """, (user_id, float(amount)))
            conn.commit()
    except Exception as e:
        logging.error(f"[DB] save_withdrawal user_id={user_id}: {e}")


# ══════════════════════════════════════════════════════════════════
#  РЕФЕРАЛЫ
# ══════════════════════════════════════════════════════════════════

async def register_referral(new_user_id: int, referrer_id: int):
    """
    Регистрирует реферальную связь.
    Используется в: referrals.py (process_start_referral)
    Импортируется как: from database import register_referral as db_register_referral
    """
    try:
        with _connect() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO referrals (new_user_id, referrer_id)
                VALUES (?, ?)
            """, (new_user_id, referrer_id))
            conn.commit()
    except Exception as e:
        logging.error(f"[DB] register_referral new={new_user_id} ref={referrer_id}: {e}")


async def save_referral_commission(referrer_id: int, referral_id: int, amount: float):
    """
    Сохраняет реферальную комиссию.
    Используется в: referrals.py (notify_referrer_commission)
    """
    try:
        with _connect() as conn:
            conn.execute("""
                INSERT INTO referral_commissions (referrer_id, referral_id, amount)
                VALUES (?, ?, ?)
            """, (referrer_id, referral_id, float(amount)))
            conn.commit()
    except Exception as e:
        logging.error(f"[DB] save_referral_commission: {e}")


async def save_referral_withdrawal(user_id: int, amount: float):
    """
    Сохраняет вывод реферального баланса.
    Используется в: referrals.py (ref_withdraw_amount)
    """
    try:
        with _connect() as conn:
            conn.execute("""
                INSERT INTO referral_withdrawals (user_id, amount)
                VALUES (?, ?)
            """, (user_id, float(amount)))
            conn.commit()
    except Exception as e:
        logging.error(f"[DB] save_referral_withdrawal: {e}")
