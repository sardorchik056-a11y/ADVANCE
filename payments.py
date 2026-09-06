import os
import logging
import uuid
import asyncio
import hashlib
import time
import math
import re as _re
from datetime import datetime, timedelta
from typing import Optional, Dict

import aiohttp
from aiogram import Router, F, Bot
from aiogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    LabeledPrice, PreCheckoutQuery, CallbackQuery,
)
from aiogram.enums import ParseMode
from aiogram.fsm.context import FSMContext

from dotenv import load_dotenv

try:
    from database import (
        save_deposit, save_withdrawal, update_user_info,
        db_get_all_users, db_set_balance, db_update_field, db_get_user,
        db_save_withdraw_request, db_update_withdraw_request_status,
        db_get_withdrawal_history, db_get_bot_stats,
    )
except ImportError:
    async def save_deposit(user_id, amount, crypto_invoice_id): pass
    async def save_withdrawal(user_id, amount): pass
    async def update_user_info(user_id, **kwargs): pass
    def db_get_all_users(): return []
    def db_set_balance(user_id, amount): pass
    def db_update_field(user_id, field, value): pass
    def db_get_user(user_id): return {}
    def db_save_withdraw_request(req_id, user_id, username, first_name, amount): pass
    def db_update_withdraw_request_status(req_id, status): pass
    def db_get_withdrawal_history(limit=500): return []
    def db_get_bot_stats(): return {}

try:
    from leaders import (
        record_deposit_stat,
        record_withdrawal_stat,
        rollback_withdrawal_stat,
    )
except Exception as _leaders_import_err:
    logging.warning(f"[Payments] Не удалось импортировать leaders: {_leaders_import_err}")
    def record_deposit_stat(user_id, name, amount): pass
    def record_withdrawal_stat(user_id, name, amount): pass
    def rollback_withdrawal_stat(user_id, amount): pass

load_dotenv()

CRYPTO_BOT_TOKEN = os.getenv('CRYPTO_BOT_TOKEN')
CRYPTOBOT_API_URL = "https://pay.crypt.bot/api"

if not CRYPTO_BOT_TOKEN:
    logging.critical("CRITICAL: CRYPTO_BOT_TOKEN не найден в переменных окружения!")
    raise ValueError("CRYPTO_BOT_TOKEN не найден в переменных окружения!")

XROCKET_API_TOKEN = os.getenv('XROCKET_API_TOKEN')
XROCKET_API_URL   = "https://pay.xrocket.tg"
XROCKET_CURRENCY  = os.getenv('XROCKET_CURRENCY', 'USDT')

if not XROCKET_API_TOKEN:
    logging.warning("[xRocket] XROCKET_API_TOKEN не найден — пополнение/вывод через xRocket будут недоступны.")

# 100 звёзд = 1.1$
STARS_PER_100 = 100
STARS_USD_RATE = 1.1
STARS_PER_USD = STARS_PER_100 / STARS_USD_RATE  # сколько звёзд в 1$

def usd_to_stars(usd_amount: float) -> int:
    """Сколько звёзд нужно оплатить, чтобы зачислить usd_amount (округление вверх)."""
    return max(1, math.ceil(usd_amount * STARS_PER_USD))

def stars_to_usd(stars: int) -> float:
    """Сколько $ зачисляется за оплаченное количество звёзд."""
    return round(stars / STARS_PER_100 * STARS_USD_RATE, 4)

MIN_DEPOSIT    = 0.1
MIN_WITHDRAWAL = 2.0

WITHDRAWAL_COOLDOWN = 180
INVOICE_LIFETIME    = 300

EMOJI_BACK      = "5233735937317447077"  # ⬅️ единый back/cancel — как в main.py / referrals.py / game.py
EMOJI_LINK      = "5271604874419647061"
EMOJI_CRYPTOBOT = "5798650400189980129"  # 💵
EMOJI_XROCKET   = "5798534328698805312"  # 🚀
EMOJI_COIN      = "5116648080787112958"  # 💰
EMOJI_STARS     = "5798819377088307477"  # ⭐️ Telegram Stars
EMOJI_DIAMOND   = "5224643573156195838"  # 💎 счет создан
EMOJI_HOURGLASS = "5386367538735104399"  # ⌛ ждем оплату

def emo(eid: str, fallback: str = "•") -> str:
    return f'<tg-emoji emoji-id="{eid}">{fallback}</tg-emoji>'

try:
    from main import ADMIN_IDS as _MAIN_ADMIN_IDS
    ADMIN_IDS = _MAIN_ADMIN_IDS
except ImportError:
    ADMIN_IDS = [int(x) for x in os.getenv('ADMIN_IDS', '8118184388, 7552910865').split(',') if x.strip()]

payment_router = Router()
bot: Bot = None

set_owner_fn = None
is_owner_fn  = None


class Storage:
    def __init__(self):
        self.users: Dict[int, dict] = {}
        self.invoices: Dict[str, dict] = {}
        self.check_tasks: Dict[str, asyncio.Task] = {}
        self.pending_action: Dict[int, str] = {}
        self._menu_message: Dict[int, tuple] = {}

        self._paid_crypto_ids: set = set()
        self._processed_invoices: set = set()
        self._processed_star_charges: set = set()
        self._user_locks: Dict[int, asyncio.Lock] = {}
        self._deposit_requests: Dict[str, float] = {}
        self._withdraw_requests: Dict[str, float] = {}
        self._balance_lock = asyncio.Lock()

        self._load_from_db()

    def _load_from_db(self):
        try:
            rows = db_get_all_users()
            for row in rows:
                uid = int(row["user_id"])
                self.users[uid] = {
                    'balance':           float(row.get("balance", 0.0) or 0.0),
                    'first_name':        row.get("first_name", "") or "",
                    'username':          row.get("username", "") or "",
                    'last_withdrawal':   None,
                    'total_deposits':    float(row.get("total_deposits", 0.0) or 0.0),
                    'total_withdrawals': float(row.get("total_withdrawals", 0.0) or 0.0),
                    'join_date':         row.get("join_date", datetime.now().strftime('%Y-%m-%d')),
                }
            logging.info(f"[Storage] Загружено пользователей из БД: {len(self.users)}")
        except Exception as e:
            logging.error(f"[Storage] Ошибка загрузки из БД: {e}")

    def _save_balance_to_db(self, user_id: int):
        try:
            user = self.users.get(user_id)
            if user is None:
                return
            db_set_balance(user_id, user['balance'])
        except Exception as e:
            logging.error(f"[Storage] Ошибка сохранения баланса user={user_id}: {e}")

    def get_user_lock(self, user_id: int) -> asyncio.Lock:
        if user_id not in self._user_locks:
            self._user_locks[user_id] = asyncio.Lock()
        return self._user_locks[user_id]

    def is_crypto_invoice_paid(self, crypto_invoice_id: int) -> bool:
        return crypto_invoice_id in self._paid_crypto_ids

    def mark_crypto_invoice_paid(self, crypto_invoice_id: int):
        self._paid_crypto_ids.add(crypto_invoice_id)

    def is_invoice_processed(self, invoice_id: str) -> bool:
        return invoice_id in self._processed_invoices

    def mark_invoice_processed(self, invoice_id: str):
        self._processed_invoices.add(invoice_id)

    def is_star_charge_processed(self, charge_id: str) -> bool:
        return bool(charge_id) and charge_id in self._processed_star_charges

    def mark_star_charge_processed(self, charge_id: str):
        if charge_id:
            self._processed_star_charges.add(charge_id)

    def _request_key(self, user_id: int, amount: float, action: str) -> str:
        window = int(time.time() // 10)
        raw = f"{action}:{user_id}:{amount:.4f}:{window}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def is_duplicate_request(self, user_id: int, amount: float, action: str) -> bool:
        key = self._request_key(user_id, amount, action)
        now = time.time()
        expired = [k for k, t in self._deposit_requests.items() if now - t > 30]
        for k in expired:
            self._deposit_requests.pop(k, None)
            self._withdraw_requests.pop(k, None)
        store = self._deposit_requests if action == 'deposit' else self._withdraw_requests
        if key in store:
            return True
        store[key] = now
        return False

    def set_pending(self, user_id: int, action: str):
        self.pending_action[user_id] = action

    def get_pending(self, user_id: int) -> Optional[str]:
        return self.pending_action.get(user_id)

    def clear_pending(self, user_id: int):
        self.pending_action.pop(user_id, None)

    def set_menu_message(self, user_id: int, chat_id: int, message_id: int, is_photo: bool = False):
        self._menu_message[user_id] = (chat_id, message_id, is_photo)

    def get_menu_message(self, user_id: int) -> Optional[tuple]:
        return self._menu_message.get(user_id)

    def clear_menu_message(self, user_id: int):
        self._menu_message.pop(user_id, None)

    def get_user(self, user_id: int) -> dict:
        if user_id not in self.users:
            try:
                row = db_get_user(user_id)
                self.users[user_id] = {
                    'balance':           float(row.get("balance", 0.0) or 0.0),
                    'first_name':        row.get("first_name", "") or "",
                    'username':          row.get("username", "") or "",
                    'last_withdrawal':   None,
                    'total_deposits':    float(row.get("total_deposits", 0.0) or 0.0),
                    'total_withdrawals': float(row.get("total_withdrawals", 0.0) or 0.0),
                    'join_date':         row.get("join_date", datetime.now().strftime('%Y-%m-%d')),
                }
            except Exception:
                self.users[user_id] = {
                    'balance':           0.0,
                    'first_name':        '',
                    'username':          '',
                    'last_withdrawal':   None,
                    'total_deposits':    0.0,
                    'total_withdrawals': 0.0,
                    'join_date':         datetime.now().strftime('%Y-%m-%d'),
                }
        return self.users[user_id]

    def get_balance(self, user_id: int) -> float:
        return float(self.get_user(user_id).get('balance', 0.0))

    def add_balance(self, user_id: int, amount: float):
        user = self.get_user(user_id)
        user['balance'] = round(user['balance'] + float(amount), 8)
        self._save_balance_to_db(user_id)

    def deduct_balance(self, user_id: int, amount: float) -> bool:
        user = self.get_user(user_id)
        if user['balance'] >= float(amount):
            user['balance'] = round(user['balance'] - float(amount), 8)
            self._save_balance_to_db(user_id)
            return True
        return False

    def record_deposit(self, user_id: int, amount: float, crypto_invoice_id: int) -> bool:
        if self.is_crypto_invoice_paid(crypto_invoice_id):
            logging.warning(f"[DUPE] crypto_invoice_id={crypto_invoice_id} user_id={user_id}")
            return False
        self.mark_crypto_invoice_paid(crypto_invoice_id)
        user = self.get_user(user_id)
        user['balance'] = round(user['balance'] + float(amount), 8)
        user['total_deposits'] = round(user.get('total_deposits', 0.0) + float(amount), 8)
        self._save_balance_to_db(user_id)
        try:
            db_update_field(user_id, "total_deposits", user['total_deposits'])
        except Exception as e:
            logging.error(f"[Storage] Ошибка total_deposits: {e}")
        return True

    def record_withdrawal(self, user_id: int, amount: float) -> bool:
        user = self.get_user(user_id)
        if user['balance'] >= float(amount):
            user['balance'] = round(user['balance'] - float(amount), 8)
            user['total_withdrawals'] = round(user.get('total_withdrawals', 0.0) + float(amount), 8)
            self._save_balance_to_db(user_id)
            try:
                db_update_field(user_id, "total_withdrawals", user['total_withdrawals'])
            except Exception as e:
                logging.error(f"[Storage] Ошибка total_withdrawals: {e}")
            return True
        return False

    def rollback_withdrawal(self, user_id: int, amount: float):
        user = self.get_user(user_id)
        user['balance'] = round(user['balance'] + float(amount), 8)
        user['total_withdrawals'] = max(
            0.0,
            round(user.get('total_withdrawals', 0.0) - float(amount), 8)
        )
        self._save_balance_to_db(user_id)
        try:
            db_update_field(user_id, "total_withdrawals", user['total_withdrawals'])
        except Exception as e:
            logging.error(f"[Storage] Ошибка отката total_withdrawals: {e}")
        logging.info(f"[ROLLBACK] user={user_id} amount={amount} баланс={user['balance']}")

    def can_withdraw(self, user_id: int) -> tuple:
        user = self.get_user(user_id)
        last = user.get('last_withdrawal')
        if not last:
            return True, None
        seconds = (datetime.now() - last).total_seconds()
        if seconds < WITHDRAWAL_COOLDOWN:
            return False, int(WITHDRAWAL_COOLDOWN - seconds)
        return True, None

    def set_last_withdrawal(self, user_id: int):
        self.get_user(user_id)['last_withdrawal'] = datetime.now()
        try:
            db_update_field(user_id, "last_withdrawal", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        except Exception as e:
            logging.error(f"[Storage] Ошибка last_withdrawal: {e}")

    def create_invoice(self, user_id: int, amount: float, crypto_id, pay_url: str, provider: str = 'cryptobot') -> str:
        invoice_id = str(uuid.uuid4())
        expires_at = datetime.now() + timedelta(seconds=INVOICE_LIFETIME)
        self.invoices[invoice_id] = {
            'user_id':    user_id,
            'amount':     amount,
            'crypto_id':  crypto_id,
            'pay_url':    pay_url,
            'provider':   provider,
            'expires_at': expires_at,
            'status':     'pending',
            'message_id': None,
            'chat_id':    None,
            'is_photo':   False,
        }
        return invoice_id

    def get_invoice(self, invoice_id: str) -> Optional[dict]:
        return self.invoices.get(invoice_id)

    def update_invoice_status(self, invoice_id: str, status: str):
        if invoice_id in self.invoices:
            self.invoices[invoice_id]['status'] = status

    def set_message_info(self, invoice_id: str, chat_id: int, message_id: int, is_photo: bool = False):
        if invoice_id in self.invoices:
            self.invoices[invoice_id]['chat_id']    = chat_id
            self.invoices[invoice_id]['message_id'] = message_id
            self.invoices[invoice_id]['is_photo']   = is_photo


storage = Storage()


def _next_req_id() -> int:
    return int(time.time() * 1000)


def _log_withdrawal(user_id: int, username: str, first_name: str, amount: float, status: str) -> int:
    """Пишет в БД завершённую (авто-обработанную) заявку на вывод — для /history и /botstats."""
    req_id = _next_req_id()
    try:
        db_save_withdraw_request(req_id, user_id, username, first_name, amount)
        db_update_withdraw_request_status(req_id, status)
    except Exception as ex:
        logging.error(f"[Withdraw] Ошибка логирования заявки: {ex}")
    return req_id


def btn_back_profile() -> InlineKeyboardButton:
    return InlineKeyboardButton(
        text="Назад",
        callback_data="profile",
        icon_custom_emoji_id=EMOJI_BACK
    )

def kb_back_profile() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[btn_back_profile()]])

def _get_user_display_name(user_data: dict, user_id: int) -> str:
    first_name = (user_data.get('first_name') or "").strip()
    if first_name:
        return first_name
    username = (user_data.get('username') or "").strip()
    if username:
        return username
    return f"User {user_id}"


class CryptoBotAPI:
    def __init__(self, token: str):
        self.token   = token
        self.headers = {"Crypto-Pay-API-Token": token}

    async def create_invoice(self, amount: float) -> Optional[dict]:
        async with aiohttp.ClientSession() as session:
            try:
                resp = await session.post(
                    f"{CRYPTOBOT_API_URL}/createInvoice",
                    headers=self.headers,
                    json={"asset": "USDT", "amount": str(amount), "expires_in": INVOICE_LIFETIME}
                )
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('result') if data.get('ok') else None
            except Exception as e:
                logging.error(f"Ошибка создания счета: {e}")
        return None

    async def get_invoice_status(self, invoice_id: int) -> Optional[str]:
        async with aiohttp.ClientSession() as session:
            try:
                resp = await session.post(
                    f"{CRYPTOBOT_API_URL}/getInvoices",
                    headers=self.headers,
                    json={"invoice_ids": [invoice_id]}
                )
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('ok') and data.get('result', {}).get('items'):
                        return data['result']['items'][0].get('status')
            except Exception as e:
                logging.error(f"Ошибка проверки статуса: {e}")
        return None

    async def create_check(self, amount: float, user_id: int) -> Optional[dict]:
        async with aiohttp.ClientSession() as session:
            try:
                resp = await session.post(
                    f"{CRYPTOBOT_API_URL}/createCheck",
                    headers=self.headers,
                    json={"asset": "USDT", "amount": str(amount), "pin_to_user_id": str(user_id)}
                )
                data = await resp.json()
                logging.info(f"createCheck response (status={resp.status}): {data}")
                if resp.status == 200 and data.get("ok"):
                    return data.get("result")
                logging.error(f"createCheck error: {data}")
            except Exception as e:
                logging.error(f"Ошибка создания чека: {e}")
        return None

    async def get_app_balance(self) -> Optional[list]:
        async with aiohttp.ClientSession() as session:
            try:
                resp = await session.post(f"{CRYPTOBOT_API_URL}/getBalance", headers=self.headers)
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('ok'):
                        return data.get('result', [])
                    logging.error(f"[getBalance] API error: {data}")
            except Exception as e:
                logging.error(f"[getBalance] Ошибка запроса: {e}")
        return None

    async def get_exchange_rates(self) -> Dict[str, float]:
        rates: Dict[str, float] = {'USDT': 1.0}
        async with aiohttp.ClientSession() as session:
            try:
                resp = await session.post(f"{CRYPTOBOT_API_URL}/getExchangeRates", headers=self.headers)
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('ok'):
                        for item in data.get('result', []):
                            source = (item.get('source') or '').upper()
                            target = (item.get('target') or '').upper()
                            rate   = float(item.get('rate') or 0)
                            if target == 'USDT' and rate > 0:
                                rates[source] = rate
                            elif source == 'USDT' and target not in rates and rate > 0:
                                rates[target] = 1.0 / rate
            except Exception as e:
                logging.error(f"[getExchangeRates] Ошибка запроса: {e}")
        return rates


crypto_api = CryptoBotAPI(CRYPTO_BOT_TOKEN)


class XRocketAPI:
    def __init__(self, token: Optional[str]):
        self.token   = token
        self.headers = {"Rocket-Pay-Key": token, "Content-Type": "application/json"} if token else {}

    async def create_invoice(self, amount: float) -> Optional[dict]:
        if not self.token:
            return None
        async with aiohttp.ClientSession() as session:
            try:
                resp = await session.post(
                    f"{XROCKET_API_URL}/tg-invoices",
                    headers=self.headers,
                    json={
                        "amount": round(float(amount), 4),
                        "numPayments": 1,
                        "currency": XROCKET_CURRENCY,
                        "description": "Пополнение баланса",
                        "expiredIn": INVOICE_LIFETIME,
                    }
                )
                data = await resp.json()
                if data.get('success'):
                    return data.get('data')
                logging.error(f"[xRocket] createInvoice error (status={resp.status}): {data}")
            except Exception as e:
                logging.error(f"[xRocket] Ошибка создания счета: {e}")
        return None

    async def get_invoice(self, invoice_id) -> Optional[dict]:
        if not self.token:
            return None
        async with aiohttp.ClientSession() as session:
            try:
                resp = await session.get(
                    f"{XROCKET_API_URL}/tg-invoices/{invoice_id}",
                    headers=self.headers
                )
                data = await resp.json()
                if data.get('success'):
                    return data.get('data')
            except Exception as e:
                logging.error(f"[xRocket] Ошибка проверки статуса: {e}")
        return None

    async def get_app_info(self) -> Optional[dict]:
        if not self.token:
            return None
        async with aiohttp.ClientSession() as session:
            try:
                resp = await session.get(f"{XROCKET_API_URL}/app/info", headers=self.headers)
                data = await resp.json()
                if data.get('success'):
                    return data.get('data')
                logging.error(f"[xRocket] getAppInfo error (status={resp.status}): {data}")
            except Exception as e:
                logging.error(f"[xRocket] Ошибка получения информации о приложении: {e}")
        return None

    async def transfer(self, user_id: int, amount: float, description: str = "Вывод средств") -> Optional[dict]:
        """Прямая отправка средств пользователю на баланс xRocket по его Telegram ID."""
        if not self.token:
            return None
        transfer_id = str(uuid.uuid4())
        async with aiohttp.ClientSession() as session:
            try:
                resp = await session.post(
                    f"{XROCKET_API_URL}/app/transfer",
                    headers=self.headers,
                    json={
                        "tgUserId":   user_id,
                        "currency":   XROCKET_CURRENCY,
                        "amount":     round(float(amount), 4),
                        "transferId": transfer_id,
                        "description": description,
                    }
                )
                data = await resp.json()
                if data.get('success'):
                    return data.get('data')
                logging.error(f"[xRocket] transfer error (status={resp.status}): {data}")
            except Exception as e:
                logging.error(f"[xRocket] Ошибка перевода: {e}")
        return None


xrocket_api = XRocketAPI(XROCKET_API_TOKEN)


async def check_payment_task(invoice_id: str):
    try:
        for wait in range(10):
            await asyncio.sleep(1)
            invoice = storage.get_invoice(invoice_id)
            if invoice and invoice.get('chat_id') and invoice.get('message_id'):
                logging.info(f"[{invoice_id}] message_id получен за {wait+1} сек")
                break
        else:
            logging.error(f"[{invoice_id}] chat_id/message_id не появились за 10 сек")

        for attempt in range(150):
            invoice = storage.get_invoice(invoice_id)
            if not invoice:
                return
            if storage.is_invoice_processed(invoice_id):
                return
            if datetime.now() > invoice['expires_at']:
                storage.mark_invoice_processed(invoice_id)
                await _edit_invoice_message(
                    invoice,
                    '<blockquote>❌ <b>Счет истек</b></blockquote>\n\n<blockquote>Время оплаты вышло. Попробуйте снова.</blockquote>',
                    reply_markup=kb_back_profile()
                )
                storage.update_invoice_status(invoice_id, 'expired')
                return

            provider = invoice.get('provider', 'cryptobot')
            if provider == 'xrocket':
                xr_invoice = await xrocket_api.get_invoice(invoice['crypto_id'])
                status = xr_invoice.get('status') if xr_invoice else None
                status = 'paid' if status == 'paid' else status
            else:
                status = await crypto_api.get_invoice_status(invoice['crypto_id'])
            logging.info(f"[{invoice_id}] Попытка {attempt+1}: провайдер={provider} статус={status}")

            if status == 'paid':
                user_lock = storage.get_user_lock(invoice['user_id'])
                async with user_lock:
                    if storage.is_invoice_processed(invoice_id):
                        return
                    if storage.is_crypto_invoice_paid(invoice['crypto_id']):
                        storage.mark_invoice_processed(invoice_id)
                        storage.update_invoice_status(invoice_id, 'paid')
                        return
                    credited = storage.record_deposit(
                        invoice['user_id'], invoice['amount'], invoice['crypto_id']
                    )
                    storage.mark_invoice_processed(invoice_id)
                    storage.update_invoice_status(invoice_id, 'paid')

                if credited:
                    asyncio.create_task(save_deposit(invoice['user_id'], invoice['amount'], invoice['crypto_id']))
                    user_data = storage.get_user(invoice['user_id'])
                    user_name = _get_user_display_name(user_data, invoice['user_id'])
                    record_deposit_stat(invoice['user_id'], user_name, invoice['amount'])

                await _edit_invoice_message(
                    invoice,
                    (
                        f'<blockquote>{emo(EMOJI_COIN,"💰")} <b>Успешное пополнение!</b></blockquote>\n\n'
                        f'<blockquote>'
                        f'{emo(EMOJI_COIN,"💰")} Сумма: <code>{invoice["amount"]}</code>\n'
                        f'{emo(EMOJI_COIN,"💰")} Баланс: <code>{storage.get_balance(invoice["user_id"]):.2f}</code> {emo(EMOJI_COIN,"💰")}'
                        f'</blockquote>'
                    ),
                    reply_markup=kb_back_profile()
                )
                return

            await asyncio.sleep(2)

    except Exception as e:
        logging.error(f"Ошибка в задаче проверки [{invoice_id}]: {e}")
    finally:
        if invoice_id in storage.check_tasks:
            del storage.check_tasks[invoice_id]


_DEP_RE = _re.compile(
    r'^/?(?:деп|пополнить|депозит|dep|deposit)\s+(\d+(?:\.\d+)?)$',
    _re.IGNORECASE
)


def kb_deposit_methods() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="CryptoBot",      callback_data="dep_m_cryptobot", icon_custom_emoji_id=EMOJI_CRYPTOBOT)],
        [InlineKeyboardButton(text="xRocket",        callback_data="dep_m_xrocket",   icon_custom_emoji_id=EMOJI_XROCKET)],
        [InlineKeyboardButton(text="Telegram Stars", callback_data="dep_m_stars", icon_custom_emoji_id=EMOJI_STARS)],
        [btn_back_profile()]
    ])


def kb_withdraw_methods() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="CryptoBot", callback_data="wd_m_cryptobot", icon_custom_emoji_id=EMOJI_CRYPTOBOT)],
        [InlineKeyboardButton(text="xRocket",   callback_data="wd_m_xrocket",   icon_custom_emoji_id=EMOJI_XROCKET)],
        [btn_back_profile()]
    ])


_DEP_METHOD_NAMES = {'cryptobot': 'CryptoBot', 'xrocket': 'xRocket', 'stars': 'Telegram Stars'}
_WD_METHOD_NAMES  = {'cryptobot': 'CryptoBot', 'xrocket': 'xRocket'}


def _is_not_modified_error(ex: Exception) -> bool:
    return "message is not modified" in str(ex).lower()


async def _edit_invoice_message(invoice: dict, text: str, reply_markup=None) -> bool:
    """Правит сообщение счета (текст или caption — в зависимости от того, было ли
    исходное сообщение фото-баннером). При несовпадении флага с реальным типом
    сообщения (Telegram вернёт 'there is no text/caption to edit') пробует
    альтернативный способ, чтобы карточка не оставалась не обновлённой.
    Возвращает True, если сообщение удалось отредактировать (или менять было нечего)."""
    chat_id    = invoice.get('chat_id')
    message_id = invoice.get('message_id')
    if not (chat_id and message_id):
        return False
    is_photo = bool(invoice.get('is_photo'))
    try:
        if is_photo:
            await bot.edit_message_caption(
                chat_id=chat_id, message_id=message_id,
                caption=text, parse_mode=ParseMode.HTML, reply_markup=reply_markup
            )
        else:
            await bot.edit_message_text(
                text=text, chat_id=chat_id, message_id=message_id,
                parse_mode=ParseMode.HTML, reply_markup=reply_markup
            )
        return True
    except Exception as ex:
        if _is_not_modified_error(ex):
            return True
        logging.warning(f"[Payments] Не удалось отредактировать сообщение счета ({'caption' if is_photo else 'text'}): {ex}")

    try:
        if is_photo:
            await bot.edit_message_text(
                text=text, chat_id=chat_id, message_id=message_id,
                parse_mode=ParseMode.HTML, reply_markup=reply_markup
            )
        else:
            await bot.edit_message_caption(
                chat_id=chat_id, message_id=message_id,
                caption=text, parse_mode=ParseMode.HTML, reply_markup=reply_markup
            )
        return True
    except Exception as ex2:
        if _is_not_modified_error(ex2):
            return True
        logging.error(f"[Payments] Fallback-edit тоже не удался: {ex2}")
        return False


async def _safe_edit(message: Message, text: str, reply_markup=None):
    """Правит меню-сообщение: если это фото (баннер) — правит подпись (caption),
    иначе — текст. Без этого edit_text падает на фото-сообщениях с ошибкой
    'there is no text in the message to edit' и уходит в fallback (новое сообщение)."""
    try:
        if message.photo:
            await message.edit_caption(caption=text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
        else:
            await message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
    except Exception as ex:
        if _is_not_modified_error(ex):
            return
        try:
            await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
        except Exception as ex2:
            logging.error(f"[Payments] Не удалось отправить сообщение: {ex2}")


async def _edit_or_answer(message: Message, user_id: int, text: str, reply_markup=None) -> tuple:
    """
    Обновляет уже существующее «меню»-сообщение пользователя (счет/статус),
    вместо того чтобы плодить новые сообщения. Новое сообщение отправляется
    ТОЛЬКО если редактировать реально нечего (нет отслеживаемого меню) —
    например, при прямой команде /deposit 10 без прохода через меню.
    Учитывает, что меню может быть фото-сообщением (баннер) — тогда правится
    caption, а не text.
    Возвращает (chat_id, message_id) итогового сообщения.
    """
    menu_ref = storage.get_menu_message(user_id)
    if menu_ref:
        chat_id, message_id, is_photo = menu_ref
        try:
            if is_photo:
                await bot.edit_message_caption(
                    chat_id=chat_id,
                    message_id=message_id,
                    caption=text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup,
                )
            else:
                await bot.edit_message_text(
                    text=text,
                    chat_id=chat_id,
                    message_id=message_id,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup,
                )
            return chat_id, message_id
        except Exception as ex:
            if _is_not_modified_error(ex):
                # Контент не изменился — считаем это успехом, новое сообщение не создаём.
                return chat_id, message_id
            logging.warning(f"[Payments] Не удалось отредактировать меню user={user_id}: {ex}")

    sent = await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
    storage.set_menu_message(user_id, sent.chat.id, sent.message_id, is_photo=False)
    if set_owner_fn:
        set_owner_fn(sent.message_id, user_id)
    return sent.chat.id, sent.message_id


async def _ask_deposit_amount(message: Message, user_id: int, method: str):
    storage.set_pending(user_id, f'deposit:{method}')
    if method == 'stars':
        hint = f'<blockquote><i>Курс: 100 {emo(EMOJI_STARS,"⭐️")} = {STARS_USD_RATE}$</i></blockquote>\n\n'
        method_label = f'{emo(EMOJI_STARS,"⭐️")} Telegram Stars'
    else:
        hint = ''
        method_label = _DEP_METHOD_NAMES.get(method, method)
    await _safe_edit(
        message,
        f'<b>{emo(EMOJI_COIN,"💰")} Пополнение через {method_label}</b>\n\n'
        f'{hint}'
        f'<blockquote><i>Введите сумму пополнения в $:</i></blockquote>',
        reply_markup=kb_back_profile()
    )
    storage.set_menu_message(user_id, message.chat.id, message.message_id, is_photo=bool(message.photo))


async def _ask_withdraw_amount(message: Message, user_id: int, method: str):
    storage.set_pending(user_id, f'withdraw:{method}')
    await _safe_edit(
        message,
        f'<b>{emo(EMOJI_COIN,"💰")} Вывод через {_WD_METHOD_NAMES.get(method, method)}</b>\n\n'
        f'<blockquote><i>Введите сумму вывода в $:</i></blockquote>',
        reply_markup=kb_back_profile()
    )
    storage.set_menu_message(user_id, message.chat.id, message.message_id, is_photo=bool(message.photo))


@payment_router.callback_query(F.data.startswith("dep_m_"))
async def handle_deposit_method(callback: CallbackQuery, state: FSMContext):
    method = callback.data.split('_', 2)[2]
    if method == 'xrocket' and not XROCKET_API_TOKEN:
        await callback.answer("xRocket временно недоступен", show_alert=True)
        return
    await state.clear()
    await _ask_deposit_amount(callback.message, callback.from_user.id, method)
    await callback.answer()


@payment_router.callback_query(F.data.startswith("wd_m_"))
async def handle_withdraw_method(callback: CallbackQuery, state: FSMContext):
    method = callback.data.split('_', 2)[2]
    if method == 'xrocket' and not XROCKET_API_TOKEN:
        await callback.answer("xRocket временно недоступен", show_alert=True)
        return
    await state.clear()
    await _ask_withdraw_amount(callback.message, callback.from_user.id, method)
    await callback.answer()


@payment_router.message(F.text.regexp(_DEP_RE))
async def handle_dep_command(message: Message):
    m = _DEP_RE.match(message.text.strip())
    if not m:
        return
    try:
        amount = float(m.group(1))
    except ValueError:
        return
    storage.clear_pending(message.from_user.id)
    await _process_deposit(message, message.from_user.id, 'cryptobot', amount_override=amount)


@payment_router.message(F.text.regexp(r'^\d+\.?\d*$'))
async def handle_amount_input(message: Message):
    user_id = message.from_user.id
    action  = storage.get_pending(user_id)
    if not action:
        return
    base, _, method = action.partition(':')
    if base == 'deposit' and method:
        storage.clear_pending(user_id)
        await _process_deposit(message, user_id, method)
    elif base == 'withdraw' and method:
        storage.clear_pending(user_id)
        await _process_withdraw(message, user_id, method)
    else:
        return
    try:
        await message.delete()
    except Exception:
        pass


async def _process_deposit(message: Message, user_id: int, method: str, amount_override: float = None):
    try:
        amount = amount_override if amount_override is not None else float(message.text)

        if amount < MIN_DEPOSIT:
            await _edit_or_answer(
                message, user_id,
                f'<blockquote>❌ Минимальная сумма пополнения: <b><code>{MIN_DEPOSIT}</code>{emo(EMOJI_COIN,"💰")}</b></blockquote>',
                reply_markup=kb_back_profile()
            )
            return

        if storage.is_duplicate_request(user_id, amount, 'deposit'):
            await _edit_or_answer(
                message, user_id,
                '<blockquote>⏳ Запрос уже обрабатывается. Подождите несколько секунд.</blockquote>',
                reply_markup=kb_back_profile()
            )
            return

        if method == 'stars':
            await _process_deposit_stars(message, user_id, amount)
            return

        if method == 'xrocket':
            if not XROCKET_API_TOKEN:
                await _edit_or_answer(
                    message, user_id,
                    '<blockquote>❌ xRocket временно недоступен.</blockquote>',
                    reply_markup=kb_back_profile()
                )
                return
            invoice_data = await xrocket_api.create_invoice(amount)
            if not invoice_data or 'link' not in invoice_data:
                await _edit_or_answer(
                    message, user_id,
                    '<blockquote>❌ Ошибка создания счета. Попробуйте позже.</blockquote>',
                    reply_markup=kb_back_profile()
                )
                return
            pay_url    = invoice_data['link']
            provider_id = invoice_data.get('id')
            provider   = 'xrocket'
        else:
            invoice_data = await crypto_api.create_invoice(amount)
            if not invoice_data or 'pay_url' not in invoice_data:
                await _edit_or_answer(
                    message, user_id,
                    '<blockquote>❌ Ошибка создания счета. Попробуйте позже.</blockquote>',
                    reply_markup=kb_back_profile()
                )
                return
            pay_url    = invoice_data['pay_url']
            provider_id = invoice_data['invoice_id']
            provider   = 'cryptobot'

        invoice_id = storage.create_invoice(user_id, amount, provider_id, pay_url, provider=provider)
        chat_id, message_id = await _edit_or_answer(
            message, user_id,
            text=(
                f'<b>{emo(EMOJI_DIAMOND,"💎")} Счет Создан!</b>\n\n'
                f'<blockquote>'
                f'{emo(EMOJI_COIN,"💰")} Сумма: <b><code>{amount}</code></b>\n'
                f'{emo(EMOJI_HOURGLASS,"⌛")} Действует — <b>5 минут</b>'
                f'</blockquote>\n\n'
                f'{emo(EMOJI_HOURGLASS,"⌛")} Ждем оплату!'
            ),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Оплатить", url=pay_url, icon_custom_emoji_id=EMOJI_LINK)],
                [btn_back_profile()]
            ])
        )
        menu_ref = storage.get_menu_message(user_id)
        storage.set_message_info(invoice_id, chat_id, message_id, is_photo=bool(menu_ref[2]) if menu_ref else False)
        asyncio.create_task(update_user_info(
            user_id,
            first_name=message.from_user.first_name or '',
            username=message.from_user.username or ''
        ))
        if invoice_id not in storage.check_tasks:
            storage.check_tasks[invoice_id] = asyncio.create_task(check_payment_task(invoice_id))

    except ValueError:
        await _edit_or_answer(message, user_id, '❌ Введите число')


async def _process_deposit_stars(message: Message, user_id: int, amount: float):
    stars_needed = usd_to_stars(amount)
    invoice_id = storage.create_invoice(user_id, amount, None, "", provider='stars')
    try:
        pay_url = await bot.create_invoice_link(
            title="Пополнение баланса",
            description=f"Зачисление ~{amount:.2f}$ на баланс ({stars_needed} ⭐️)",
            payload=f"stars_deposit:{invoice_id}",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice(label="Пополнение баланса", amount=stars_needed)],
        )
    except Exception as ex:
        logging.error(f"[Stars] Ошибка создания счета: {ex}")
        await _edit_or_answer(
            message, user_id,
            '<blockquote>❌ Не удалось создать счет на оплату звёздами.</blockquote>',
            reply_markup=kb_back_profile()
        )
        return

    storage.invoices[invoice_id]['pay_url'] = pay_url

    chat_id, message_id = await _edit_or_answer(
        message, user_id,
        text=(
            f'<b>{emo(EMOJI_DIAMOND,"💎")} Счет Создан!</b>\n\n'
            f'<blockquote>'
            f'{emo(EMOJI_COIN,"💰")} Сумма: <b><code>{amount:.2f}</code></b> (≈ <code>{stars_needed}</code> {emo(EMOJI_STARS,"⭐️")})'
            f'</blockquote>\n\n'
            f'{emo(EMOJI_HOURGLASS,"⌛")} Ждем оплату!'
        ),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Оплатить", url=pay_url, icon_custom_emoji_id=EMOJI_STARS)],
            [btn_back_profile()]
        ])
    )
    menu_ref = storage.get_menu_message(user_id)
    storage.set_message_info(invoice_id, chat_id, message_id, is_photo=bool(menu_ref[2]) if menu_ref else False)
    asyncio.create_task(update_user_info(
        user_id,
        first_name=message.from_user.first_name or '',
        username=message.from_user.username or ''
    ))


@payment_router.pre_checkout_query()
async def process_pre_checkout(pre_checkout_q: PreCheckoutQuery):
    await pre_checkout_q.answer(ok=True)


@payment_router.message(F.successful_payment)
async def process_successful_payment(message: Message):
    payment = message.successful_payment
    if not payment or payment.currency != "XTR":
        return

    user_id    = message.from_user.id
    stars_paid = payment.total_amount
    credited   = stars_to_usd(stars_paid)
    charge_id  = payment.telegram_payment_charge_id

    user_lock = storage.get_user_lock(user_id)
    async with user_lock:
        if storage.is_star_charge_processed(charge_id):
            logging.warning(f"[Stars] Повторная доставка платежа charge_id={charge_id} user={user_id} — игнорируем.")
            try:
                await message.delete()
            except Exception:
                pass
            return
        storage.mark_star_charge_processed(charge_id)
        storage.add_balance(user_id, credited)
    user = storage.get_user(user_id)
    user['total_deposits'] = round(user.get('total_deposits', 0.0) + credited, 8)
    try:
        db_update_field(user_id, "total_deposits", user['total_deposits'])
    except Exception as ex:
        logging.error(f"[Stars] Ошибка total_deposits: {ex}")

    asyncio.create_task(save_deposit(user_id, credited, 0))
    user_name = _get_user_display_name(user, user_id)
    record_deposit_stat(user_id, user_name, credited)

    success_text = (
        f'<blockquote>{emo(EMOJI_COIN,"💰")} <b>Успешное пополнение звёздами!</b></blockquote>\n\n'
        f'<blockquote>'
        f'{emo(EMOJI_STARS,"⭐️")} Оплачено звёзд: <code>{stars_paid}</code>\n'
        f'{emo(EMOJI_COIN,"💰")} Зачислено: <code>{credited:.2f}</code>\n'
        f'{emo(EMOJI_COIN,"💰")} Баланс: <code>{storage.get_balance(user_id):.2f}</code>'
        f'</blockquote>'
    )

    invoice_id = None
    payload = payment.invoice_payload or ""
    if payload.startswith("stars_deposit:"):
        invoice_id = payload.split(":", 1)[1]
        storage.mark_invoice_processed(invoice_id)
        storage.update_invoice_status(invoice_id, 'paid')

    invoice = storage.get_invoice(invoice_id) if invoice_id else None
    edited = False
    if invoice and invoice.get('chat_id') and invoice.get('message_id'):
        edited = await _edit_invoice_message(invoice, success_text, reply_markup=kb_back_profile())

    if not edited:
        await message.answer(success_text, parse_mode=ParseMode.HTML, reply_markup=kb_back_profile())

    try:
        await message.delete()
    except Exception:
        pass


async def _process_withdraw(message: Message, user_id: int, method: str):
    try:
        amount  = float(message.text)
        balance = storage.get_balance(user_id)

        if method == 'xrocket' and not XROCKET_API_TOKEN:
            await _edit_or_answer(
                message, user_id,
                '<blockquote>❌ xRocket временно недоступен.</blockquote>',
                reply_markup=kb_back_profile()
            )
            return

        if amount < MIN_WITHDRAWAL:
            await _edit_or_answer(
                message, user_id,
                f'<blockquote>❌ Минимальная сумма вывода: <b><code>{MIN_WITHDRAWAL}</code>{emo(EMOJI_COIN,"💰")}</b></blockquote>',
                reply_markup=kb_back_profile()
            )
            return

        if amount > balance:
            await _edit_or_answer(
                message, user_id,
                '<blockquote>❌ Недостаточно средств!</blockquote>',
                reply_markup=kb_back_profile()
            )
            return

        can_withdraw, wait_time = storage.can_withdraw(user_id)
        if not can_withdraw:
            minutes = wait_time // 60
            seconds = wait_time % 60
            await _edit_or_answer(
                message, user_id,
                f'<blockquote>⏳ Подождите <b>{minutes} мин {seconds} сек</b></blockquote>',
                reply_markup=kb_back_profile()
            )
            return

        if storage.is_duplicate_request(user_id, amount, 'withdraw'):
            await _edit_or_answer(
                message, user_id,
                '<blockquote>⏳ Запрос уже обрабатывается. Подождите несколько секунд.</blockquote>',
                reply_markup=kb_back_profile()
            )
            return

        user_lock = storage.get_user_lock(user_id)
        async with user_lock:
            if storage.get_balance(user_id) < amount:
                await _edit_or_answer(
                    message, user_id,
                    '<blockquote>❌ Недостаточно средств!</blockquote>',
                    reply_markup=kb_back_profile()
                )
                return
            withdrawn = storage.record_withdrawal(user_id, amount)
            if not withdrawn:
                await _edit_or_answer(
                    message, user_id,
                    '<blockquote>❌ Ошибка списания средств.</blockquote>',
                    reply_markup=kb_back_profile()
                )
                return

        # ── Мгновенная авто-отправка средств (без заявок и одобрения админом) ──
        username   = message.from_user.username or ''
        first_name = message.from_user.first_name or ''

        check = None
        try:
            if method == 'xrocket':
                result = await xrocket_api.transfer(user_id, amount, description="Вывод средств")
                payout_ok = result is not None
            else:
                check = await crypto_api.create_check(amount, user_id)
                payout_ok = bool(check and 'bot_check_url' in check)
        except Exception as e:
            logging.error(f"[Withdraw] Исключение при отправке выплаты user_id={user_id} amount={amount}: {e}")
            payout_ok = False

        if not payout_ok:
            # откатываем списание, если выплата не прошла
            storage.rollback_withdrawal(user_id, amount)
            rollback_withdrawal_stat(user_id, amount)
            _log_withdrawal(user_id, username, first_name, amount, 'failed')
            await _edit_or_answer(
                message, user_id,
                '<blockquote>❌ Ошибка при отправке выплаты. Средства возвращены на баланс.</blockquote>',
                reply_markup=kb_back_profile()
            )
            return

        storage.set_last_withdrawal(user_id)
        _log_withdrawal(user_id, username, first_name, amount, 'approved')

        user_data = storage.get_user(user_id)
        user_name = _get_user_display_name(user_data, user_id)
        record_withdrawal_stat(user_id, user_name, amount)
        asyncio.create_task(save_withdrawal(user_id, amount))

        if method == 'xrocket':
            await _edit_or_answer(
                message, user_id,
                f'<blockquote>{emo(EMOJI_COIN,"💰")} <b>Вывод выполнен!</b> ✅</blockquote>\n\n'
                f'<blockquote>'
                f'{emo(EMOJI_COIN,"💰")} Сумма: <code>{amount}</code>\n'
                f'{emo(EMOJI_XROCKET,"🚀")} Отправлено на баланс xRocket\n'
                f'{emo(EMOJI_COIN,"💰")} Баланс: <code>{storage.get_balance(user_id):.2f}</code>'
                f'</blockquote>',
                reply_markup=kb_back_profile()
            )
        else:
            await _edit_or_answer(
                message, user_id,
                f'<blockquote>{emo(EMOJI_COIN,"💰")} <b>Вывод выполнен!</b> ✅</blockquote>\n\n'
                f'<blockquote>'
                f'{emo(EMOJI_COIN,"💰")} Сумма: <code>{amount}</code> USDT\n'
                f'{emo(EMOJI_COIN,"💰")} Баланс: <code>{storage.get_balance(user_id):.2f}</code>'
                f'</blockquote>',
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="Получить чек", url=check['bot_check_url'], icon_custom_emoji_id=EMOJI_LINK)],
                    [btn_back_profile()]
                ])
            )

    except ValueError:
        await _edit_or_answer(message, user_id, '❌ Введите число')



_HISTORY_RE  = _re.compile(r'^/history$', _re.IGNORECASE)
STATUS_EMOJI = {'pending': '⏳', 'approved': '✅', 'rejected': '🚫', 'failed': '❌'}

@payment_router.message(F.text.regexp(_HISTORY_RE))
async def handle_history(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    rows = db_get_withdrawal_history(500)
    if not rows:
        await message.reply(
            '<blockquote>📭 <b>История выводов пуста.</b></blockquote>',
            parse_mode='HTML'
        )
        return

    total_amount   = sum(float(r['amount']) for r in rows)
    count_approved = sum(1 for r in rows if r['status'] == 'approved')
    count_rejected = sum(1 for r in rows if r['status'] == 'rejected')
    count_pending  = sum(1 for r in rows if r['status'] == 'pending')
    count_failed   = sum(1 for r in rows if r['status'] == 'failed')

    await message.reply(
        f'<blockquote>📋 <b>История выводов — последние {len(rows)} шт.</b></blockquote>\n\n'
        f'<blockquote>'
        f'💵 Общая сумма: <code>{total_amount:.2f}</code> USDT\n'
        f'✅ Одобрено: <b>{count_approved}</b>\n'
        f'🚫 Отклонено: <b>{count_rejected}</b>\n'
        f'⏳ Ожидает: <b>{count_pending}</b>\n'
        f'❌ Ошибка: <b>{count_failed}</b>'
        f'</blockquote>',
        parse_mode='HTML'
    )

    chunk_size = 25
    chunks = [rows[i:i + chunk_size] for i in range(0, len(rows), chunk_size)]
    for idx, chunk in enumerate(chunks):
        lines = []
        for r in chunk:
            emoji   = STATUS_EMOJI.get(r['status'], '❓')
            display = f"@{r['username']}" if r['username'] else r['first_name'] or f"ID {r['user_id']}"
            dt      = (r['created_at'] or '')[:16]
            lines.append(
                f"{emoji} <b>#{r['req_id']}</b> | {display} | "
                f"<code>{float(r['amount']):.2f}</code> USDT | {dt}"
            )
        await message.reply(
            f'<blockquote>📄 <b>Часть {idx + 1} / {len(chunks)}</b></blockquote>\n\n' + '\n'.join(lines),
            parse_mode='HTML'
        )


_BOTSTATS_RE = _re.compile(r'^/botstats$', _re.IGNORECASE)

@payment_router.message(F.text.regexp(_BOTSTATS_RE))
async def handle_botstats(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    s = db_get_bot_stats()

    try:
        bot_info = await message.bot.get_me()
        bot_name     = bot_info.full_name
        bot_username = f"@{bot_info.username}"
        bot_id       = bot_info.id
    except Exception:
        bot_name     = "—"
        bot_username = "—"
        bot_id       = "—"

    profit = s['profit']
    if profit > 0:
        profit_line = f'✅ Прибыль: <b><code>+{profit:.2f}</code> USDT</b>'
    elif profit < 0:
        profit_line = f'🔴 Убыток: <b><code>{profit:.2f}</code> USDT</b>'
    else:
        profit_line = f'➖ Баланс нулевой: <b><code>0.00</code> USDT</b>'

    top_lines = []
    medals = ['🥇', '🥈', '🥉', '4️⃣', '5️⃣']
    for i, dep in enumerate(s['top_depositors']):
        name = dep.get('first_name') or dep.get('username') or f"ID {dep.get('user_id', '?')}"
        uname = f" (@{dep['username']})" if dep.get('username') else ""
        top_lines.append(
            f"{medals[i]} {name}{uname} — <code>{dep['dep_sum']:.2f}</code> USDT"
        )
    top_block = '\n'.join(top_lines) if top_lines else 'Нет данных'

    now_str = datetime.now().strftime('%d.%m.%Y %H:%M')

    text = (
        f'<blockquote><tg-emoji emoji-id="5231200819986047254">💰</tg-emoji><b>Статистика бота</b></blockquote>\n\n'

        f'<blockquote>'
        f'<tg-emoji emoji-id="5334544901428229844">💰</tg-emoji><b>ТГ информация</b>\n'
        f'├ Имя: <b>{bot_name}</b>\n'
        f'├ Username: <b>{bot_username}</b>\n'
        f'└ ID: <code>{bot_id}</code>'
        f'</blockquote>\n\n'

        f'<blockquote>'
        f'<tg-emoji emoji-id="5906581476639513176">💰</tg-emoji> <b>Пользователи</b>\n'
        f'├ Всего: <b><code>{s["total_users"]}</code></b>\n'
        f'├ Новых сегодня: <b><code>{s["new_users_today"]}</code></b>\n'
        f'├ Новых за 7 дней: <b><code>{s["new_users_week"]}</code></b>\n'
        f'└ Активных сегодня: <b><code>{s["active_today"]}</code></b>'
        f'</blockquote>\n\n'

        f'<blockquote>'
        f'<tg-emoji emoji-id="5402186569006210455">💰</tg-emoji> <b>Финансы</b>\n'
        f'├ Депозитов всего: <b><code>{s["total_deposits"]:.2f}</code> USDT</b> ({s["deposits_count"]} шт.)\n'
        f'├ Депозитов сегодня: <b><code>{s["deposits_today"]:.2f}</code> USDT</b>\n'
        f'├ Выводов всего: <b><code>{s["total_withdrawals"]:.2f}</code> USDT</b> ({s["withdrawals_count"]} шт.)\n'
        f'├ Выводов сегодня: <b><code>{s["withdrawals_today"]:.2f}</code> USDT</b>\n'
        f'└ {profit_line}'
        f'</blockquote>\n\n'

        f'<blockquote>'
        f'⏳ <b>Ожидают вывода</b>\n'
        f'└ {s["pending_withdrawals"]} заявок на <code>{s["pending_amount"]:.2f}</code> USDT'
        f'</blockquote>\n\n'

        f'<blockquote>'
        f'<tg-emoji emoji-id="5440539497383087970">💰</tg-emoji> <b>Топ-5 по депозитам</b>\n'
        f'{top_block}'
        f'</blockquote>\n\n'

        f'<i><tg-emoji emoji-id="5386367538735104399">💰</tg-emoji> Обновлено: {now_str}</i>'
    )

    await message.reply(text, parse_mode='HTML')


_KAZNA_RE = _re.compile(r'^/?(?:казна|kazna|reserve)$', _re.IGNORECASE)

@payment_router.message(F.text.regexp(_KAZNA_RE))
async def handle_kazna(message: Message):
    balances, rates = await asyncio.gather(
        crypto_api.get_app_balance(),
        crypto_api.get_exchange_rates(),
    )

    if balances is None:
        await message.reply(
            '<blockquote>❌ <b>Не удалось получить данные казны.</b></blockquote>',
            parse_mode='HTML'
        )
        return

    bal_map: dict = {}
    for item in balances:
        code      = (item.get('currency_code') or '').upper()
        available = float(item.get('available') or 0)
        bal_map[code] = available

    usdt = bal_map.get('USDT', 0.0)
    ton  = bal_map.get('TON',  0.0)
    trx  = bal_map.get('TRX',  0.0)

    ton_rate = rates.get('TON', 0.0)
    trx_rate = rates.get('TRX', 0.0)

    usdt_usd  = usdt
    ton_usd   = ton * ton_rate
    trx_usd   = trx * trx_rate
    total_usd = usdt_usd + ton_usd + trx_usd

    usdt_str = f'USDT-{usdt:.2f} ({usdt_usd:.2f}$)'
    ton_str  = f'TON-{ton:.4f} ({ton_usd:.2f}$)' if ton_rate else f'TON-{ton:.4f}'
    trx_str  = f'TRX-{trx:.4f} ({trx_usd:.2f}$)' if trx_rate else f'TRX-{trx:.4f}'

    xrocket_line   = ''
    xr_total_usd   = 0.0
    if XROCKET_API_TOKEN:
        xr_info = await xrocket_api.get_app_info()
        if xr_info and xr_info.get('balances'):
            xr_lines = []
            for b in xr_info['balances']:
                cur = (b.get('currency') or '').upper()
                bal = float(b.get('balance') or 0)
                if bal <= 0:
                    continue
                if cur == 'USDT':
                    cur_rate = 1.0
                else:
                    cur_rate = rates.get(cur, 0.0)
                cur_usd = bal * cur_rate
                if cur_rate:
                    xr_total_usd += cur_usd
                    xr_lines.append(f'{cur}-{bal:.4f} ({cur_usd:.2f}$)')
                else:
                    xr_lines.append(f'{cur}-{bal:.4f}')
            if xr_lines:
                xrocket_line = (
                    f'\n<blockquote><b>'
                    f'{emo(EMOJI_XROCKET,"🚀")}xRocket-{xr_total_usd:.2f}$\n' + '\n'.join(xr_lines) +
                    f'</b></blockquote>'
                )

    grand_total_usd = total_usd + xr_total_usd

    await message.reply(
        f'<blockquote><b>'
        f'<tg-emoji emoji-id="5386367538735104399">💰</tg-emoji>Общий резерв-{grand_total_usd:.2f}$'
        f'</b></blockquote>\n\n'
        f'<blockquote><b>'
        f'<tg-emoji emoji-id="5798650400189980129">💰</tg-emoji>Cryptobot-{total_usd:.2f}$\n\n'
        f'<tg-emoji emoji-id="5800653259404223435">💰</tg-emoji>{usdt_str}\n'
        f'<tg-emoji emoji-id="5798401408050929441">💰</tg-emoji>{ton_str}\n'
        f'<tg-emoji emoji-id="5798480856355970219">💰</tg-emoji>{trx_str}\n\n'
        f'</b></blockquote>'
        f'{xrocket_line}\n\n'
        f'<b><i><tg-emoji emoji-id="5386367538735104399">💰</tg-emoji>Резерв обновляется в реальном времени!</i></b>',
        parse_mode='HTML'
    )


_WISS_OWNERS = {8118184388, 8476835256}
_WISS_RE = _re.compile(r'^/wiss\s+(\d+(?:\.\d+)?)$', _re.IGNORECASE)

@payment_router.message(F.text.regexp(_WISS_RE))
async def handle_wiss(message: Message):
    if message.from_user.id not in _WISS_OWNERS:
        return

    m = _WISS_RE.match(message.text.strip())
    if not m:
        return

    try:
        amount = float(m.group(1))
    except ValueError:
        return

    if amount <= 0:
        return

    check = await crypto_api.create_check(amount, message.from_user.id)
    if not check or 'bot_check_url' not in check:
        return

    await message.answer(check['bot_check_url'])
    try:
        await message.delete()
    except Exception:
        pass


def setup_payments(bot_instance: Bot):
    global bot
    bot = bot_instance
