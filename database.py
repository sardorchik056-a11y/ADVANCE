import sqlite3
import logging
from datetime import datetime
from typing import Optional

DB_PATH = "casino.db"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _add_column_if_missing(conn, table: str, column: str, definition: str):
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
    except Exception:
        pass


async def init_db():
    try:
        with _connect() as conn:

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
            _add_column_if_missing(conn, "users", "last_name",         "TEXT DEFAULT ''")
            _add_column_if_missing(conn, "users", "username",          "TEXT DEFAULT ''")
            _add_column_if_missing(conn, "users", "total_deposits",    "REAL DEFAULT 0.0")
            _add_column_if_missing(conn, "users", "total_withdrawals", "REAL DEFAULT 0.0")
            _add_column_if_missing(conn, "users", "last_withdrawal",   "TEXT DEFAULT NULL")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS game_results (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id     INTEGER NOT NULL,
                    game_name   TEXT    DEFAULT '',
                    win_amount  REAL    DEFAULT 0.0,
                    created_at  TEXT    DEFAULT (datetime('now'))
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS deposits (
                    id                INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id           INTEGER NOT NULL,
                    amount            REAL    NOT NULL,
                    crypto_invoice_id INTEGER DEFAULT NULL,
                    created_at        TEXT    DEFAULT (datetime('now'))
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS withdrawals (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id    INTEGER NOT NULL,
                    amount     REAL    NOT NULL,
                    created_at TEXT    DEFAULT (datetime('now'))
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS withdraw_requests (
                    req_id     INTEGER PRIMARY KEY,
                    user_id    INTEGER NOT NULL,
                    username   TEXT    DEFAULT '',
                    first_name TEXT    DEFAULT '',
                    amount     REAL    NOT NULL,
                    status     TEXT    DEFAULT 'pending',
                    created_at TEXT    DEFAULT (datetime('now')),
                    updated_at TEXT    DEFAULT (datetime('now'))
                )
            """)

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


async def import_users_from_json():
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
    try:
        with _connect() as conn:
            cur = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            row = cur.fetchone()
            if row:
                return dict(row)
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
    try:
        with _connect() as conn:
            cur = conn.execute("SELECT * FROM users")
            return [dict(row) for row in cur.fetchall()]
    except Exception as e:
        logging.error(f"[DB] db_get_all_users: {e}")
        return []


def db_get_all_user_ids() -> list:
    try:
        with _connect() as conn:
            cur = conn.execute("SELECT user_id FROM users")
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        logging.error(f"[DB] db_get_all_user_ids: {e}")
        return []


def db_set_balance(user_id: int, amount: float):
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


async def save_game_result(user_id: int, game_name: str, win_amount: float):
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


async def save_deposit(user_id: int, amount: float, crypto_invoice_id: int):
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
    try:
        with _connect() as conn:
            conn.execute("""
                INSERT INTO withdrawals (user_id, amount)
                VALUES (?, ?)
            """, (user_id, float(amount)))
            conn.commit()
    except Exception as e:
        logging.error(f"[DB] save_withdrawal user_id={user_id}: {e}")


def db_save_withdraw_request(req_id: int, user_id: int, username: str,
                              first_name: str, amount: float) -> None:
    try:
        with _connect() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO withdraw_requests
                    (req_id, user_id, username, first_name, amount, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 'pending', datetime('now'), datetime('now'))
            """, (req_id, user_id, username or '', first_name or '', float(amount)))
            conn.commit()
    except Exception as e:
        logging.error(f"[DB] db_save_withdraw_request req_id={req_id}: {e}")


def db_update_withdraw_request_status(req_id: int, status: str) -> None:
    try:
        with _connect() as conn:
            conn.execute("""
                UPDATE withdraw_requests
                SET status = ?, updated_at = datetime('now')
                WHERE req_id = ?
            """, (status, req_id))
            conn.commit()
    except Exception as e:
        logging.error(f"[DB] db_update_withdraw_request_status req_id={req_id}: {e}")


def db_get_pending_withdraw_requests() -> list:
    try:
        with _connect() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.execute("""
                SELECT req_id, user_id, username, first_name, amount, status, created_at
                FROM withdraw_requests
                WHERE status = 'pending'
                ORDER BY created_at ASC
            """)
            return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        logging.error(f"[DB] db_get_pending_withdraw_requests: {e}")
        return []


def db_get_withdrawal_history(limit: int = 500) -> list:
    """Возвращает последние `limit` заявок на вывод из всех статусов."""
    try:
        with _connect() as conn:
            cur = conn.execute("""
                SELECT
                    req_id,
                    user_id,
                    username,
                    first_name,
                    amount,
                    status,
                    created_at,
                    updated_at
                FROM withdraw_requests
                ORDER BY created_at DESC
                LIMIT ?
            """, (limit,))
            return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        logging.error(f"[DB] db_get_withdrawal_history: {e}")
        return []


async def register_referral(new_user_id: int, referrer_id: int):
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
    try:
        with _connect() as conn:
            conn.execute("""
                INSERT INTO referral_withdrawals (user_id, amount)
                VALUES (?, ?)
            """, (user_id, float(amount)))
            conn.commit()
    except Exception as e:
        logging.error(f"[DB] save_referral_withdrawal: {e}")
