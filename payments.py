import os
import logging
import uuid
import asyncio
import hashlib
import time
import re as _re
from datetime import datetime, timedelta
from typing import Optional, Dict

import aiohttp
from aiogram import Router, F, Bot
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode

from dotenv import load_dotenv

try:
    from database import (
        save_deposit, save_withdrawal, update_user_info,
        db_get_all_users, db_set_balance, db_update_field, db_get_user
    )
except ImportError:
    async def save_deposit(user_id, amount, crypto_invoice_id): pass
    async def save_withdrawal(user_id, amount): pass
    async def update_user_info(user_id, **kwargs): pass
    def db_get_all_users(): return []
    def db_set_balance(user_id, amount): pass
    def db_update_field(user_id, field, value): pass
    def db_get_user(user_id): return {}

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

MIN_DEPOSIT    = 0.1
MIN_WITHDRAWAL = 2.0

WITHDRAWAL_COOLDOWN = 180

INVOICE_LIFETIME = 300

EMOJI_BACK = "5906771962734057347"
EMOJI_LINK = "5271604874419647061"

ADMIN_IDS = [int(x) for x in os.getenv('ADMIN_IDS', '8118184388,8158265201').split(',') if x.strip()]

payment_router = Router()
bot: Bot = None

set_owner_fn = None
is_owner_fn  = None


class WithdrawRequest:
    def __init__(self, req_id: int, user_id: int, username: str,
                 first_name: str, amount: float):
        self.req_id     = req_id
        self.user_id    = user_id
        self.username   = username
        self.first_name = first_name
        self.amount     = amount
        self.created_at = datetime.now()
        self.status     = 'pending'

class WithdrawQueue:
    def __init__(self):
        self._requests: Dict[int, WithdrawRequest] = {}
        self._counter  = 0

    def add(self, user_id: int, username: str,
            first_name: str, amount: float) -> int:
        self._counter += 1
        req = WithdrawRequest(self._counter, user_id, username, first_name, amount)
        self._requests[self._counter] = req
        return self._counter

    def get(self, req_id: int) -> Optional[WithdrawRequest]:
        return self._requests.get(req_id)

    def pending(self) -> list:
        return [r for r in self._requests.values() if r.status == 'pending']

    def all_ids(self) -> list:
        return list(self._requests.keys())

withdraw_queue = WithdrawQueue()


class Storage:
    def __init__(self):
        self.users: Dict[int, dict] = {}
        self.invoices: Dict[str, dict] = {}
        self.check_tasks: Dict[str, asyncio.Task] = {}
        self.pending_action: Dict[int, str] = {}

        self._paid_crypto_ids: set = set()
        self._processed_invoices: set = set()
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

    def create_invoice(self, user_id: int, amount: float, crypto_id: int, pay_url: str) -> str:
        invoice_id = str(uuid.uuid4())
        expires_at = datetime.now() + timedelta(seconds=INVOICE_LIFETIME)
        self.invoices[invoice_id] = {
            'user_id': user_id,
            'amount': amount,
            'crypto_id': crypto_id,
            'pay_url': pay_url,
            'expires_at': expires_at,
            'status': 'pending',
            'message_id': None,
            'chat_id': None
        }
        return invoice_id

    def get_invoice(self, invoice_id: str) -> Optional[dict]:
        return self.invoices.get(invoice_id)

    def update_invoice_status(self, invoice_id: str, status: str):
        if invoice_id in self.invoices:
            self.invoices[invoice_id]['status'] = status

    def set_message_info(self, invoice_id: str, chat_id: int, message_id: int):
        if invoice_id in self.invoices:
            self.invoices[invoice_id]['chat_id'] = chat_id
            self.invoices[invoice_id]['message_id'] = message_id


storage = Storage()


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
                if invoice.get('chat_id') and invoice.get('message_id'):
                    try:
                        await bot.edit_message_text(
                            text='<blockquote>❌ <b>Счет истек</b></blockquote>\n\n<blockquote>Время оплаты вышло. Попробуйте снова.</blockquote>',
                            parse_mode=ParseMode.HTML,
                            chat_id=invoice['chat_id'],
                            message_id=invoice['message_id'],
                            reply_markup=kb_back_profile()
                        )
                    except Exception as e:
                        logging.error(f"[{invoice_id}] Ошибка edit (expired): {e}")
                storage.update_invoice_status(invoice_id, 'expired')
                return

            status = await crypto_api.get_invoice_status(invoice['crypto_id'])
            logging.info(f"[{invoice_id}] Попытка {attempt+1}: статус={status}")

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

                if invoice.get('chat_id') and invoice.get('message_id'):
                    try:
                        await bot.edit_message_text(
                            text=(
                                f'<blockquote><tg-emoji emoji-id="5197288647275071607">💰</tg-emoji> <b>Успешное пополнение!</b></blockquote>\n\n'
                                f'<blockquote>'
                                f'<tg-emoji emoji-id="5197434882321567830">💰</tg-emoji> Сумма: <code>{invoice["amount"]}</code>\n'
                                f'<tg-emoji emoji-id="5278467510604160626">💰</tg-emoji> Баланс: <code>{storage.get_balance(invoice["user_id"]):.2f}</code> <tg-emoji emoji-id="5197434882321567830">💰</tg-emoji>'
                                f'</blockquote>'
                            ),
                            parse_mode=ParseMode.HTML,
                            chat_id=invoice['chat_id'],
                            message_id=invoice['message_id'],
                            reply_markup=kb_back_profile()
                        )
                    except Exception as e:
                        logging.error(f"[{invoice_id}] Ошибка edit (paid): {e}")
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
    await _process_deposit(message, message.from_user.id, amount_override=amount)


@payment_router.message(F.text.regexp(r'^\d+\.?\d*$'))
async def handle_amount_input(message: Message):
    user_id = message.from_user.id
    action  = storage.get_pending(user_id)
    if action == 'deposit':
        storage.clear_pending(user_id)
        await _process_deposit(message, user_id)
    elif action == 'withdraw':
        storage.clear_pending(user_id)
        await _process_withdraw(message, user_id)


async def _process_deposit(message: Message, user_id: int, amount_override: float = None):
    try:
        amount = amount_override if amount_override is not None else float(message.text)

        if amount < MIN_DEPOSIT:
            await message.answer(
                f'<blockquote>❌ Минимальная сумма пополнения: <b><code>{MIN_DEPOSIT}</code><tg-emoji emoji-id="5197434882321567830">💰</tg-emoji></b></blockquote>',
                parse_mode=ParseMode.HTML, reply_markup=kb_back_profile()
            )
            return

        if storage.is_duplicate_request(user_id, amount, 'deposit'):
            await message.answer(
                '<blockquote>⏳ Запрос уже обрабатывается. Подождите несколько секунд.</blockquote>',
                parse_mode=ParseMode.HTML, reply_markup=kb_back_profile()
            )
            return

        invoice_data = await crypto_api.create_invoice(amount)
        if not invoice_data or 'pay_url' not in invoice_data:
            await message.answer(
                '<blockquote>❌ Ошибка создания счета. Попробуйте позже.</blockquote>',
                parse_mode=ParseMode.HTML, reply_markup=kb_back_profile()
            )
            return

        invoice_id = storage.create_invoice(
            user_id, amount, invoice_data['invoice_id'], invoice_data['pay_url']
        )
        sent_msg = await message.answer(
            text=(
                f'<b><tg-emoji emoji-id="5906482735341377395">💰</tg-emoji> Счет Создан!</b>\n\n'
                f'<blockquote>'
                f'<tg-emoji emoji-id="5197434882321567830">💰</tg-emoji> Сумма: <b><code>{amount}</code></b>\n'
                f'<tg-emoji emoji-id="5906598824012420908">⌛️</tg-emoji> Действует — <b>5 минут</b>'
                f'</blockquote>\n\n'
                f'<tg-emoji emoji-id="5386367538735104399">🔵</tg-emoji> Ждем оплату!'
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Оплатить", url=invoice_data['pay_url'], icon_custom_emoji_id=EMOJI_LINK)],
                [btn_back_profile()]
            ])
        )
        storage.set_message_info(invoice_id, message.chat.id, sent_msg.message_id)
        if set_owner_fn:
            set_owner_fn(sent_msg.message_id, user_id)
        asyncio.create_task(update_user_info(
            user_id,
            first_name=message.from_user.first_name or '',
            username=message.from_user.username or ''
        ))
        if invoice_id not in storage.check_tasks:
            storage.check_tasks[invoice_id] = asyncio.create_task(check_payment_task(invoice_id))

    except ValueError:
        await message.answer('❌ Введите число')


async def _process_withdraw(message: Message, user_id: int):
    try:
        amount  = float(message.text)
        balance = storage.get_balance(user_id)

        if amount < MIN_WITHDRAWAL:
            await message.answer(
                f'<blockquote>❌ Минимальная сумма вывода: <b><code>{MIN_WITHDRAWAL}</code><tg-emoji emoji-id="5197434882321567830">💰</tg-emoji></b></blockquote>',
                parse_mode=ParseMode.HTML, reply_markup=kb_back_profile()
            )
            return

        if amount > balance:
            await message.answer(
                '<blockquote>❌ Недостаточно средств!</blockquote>',
                parse_mode=ParseMode.HTML, reply_markup=kb_back_profile()
            )
            return

        can_withdraw, wait_time = storage.can_withdraw(user_id)
        if not can_withdraw:
            minutes = wait_time // 60
            seconds = wait_time % 60
            await message.answer(
                f'<blockquote>⏳ Подождите <b>{minutes} мин {seconds} сек</b></blockquote>',
                parse_mode=ParseMode.HTML, reply_markup=kb_back_profile()
            )
            return

        if storage.is_duplicate_request(user_id, amount, 'withdraw'):
            await message.answer(
                '<blockquote>⏳ Запрос уже обрабатывается. Подождите несколько секунд.</blockquote>',
                parse_mode=ParseMode.HTML, reply_markup=kb_back_profile()
            )
            return

        user_lock = storage.get_user_lock(user_id)
        async with user_lock:
            if storage.get_balance(user_id) < amount:
                await message.answer(
                    '<blockquote>❌ Недостаточно средств!</blockquote>',
                    parse_mode=ParseMode.HTML, reply_markup=kb_back_profile()
                )
                return
            withdrawn = storage.record_withdrawal(user_id, amount)
            if not withdrawn:
                await message.answer(
                    '<blockquote>❌ Ошибка списания средств.</blockquote>',
                    parse_mode=ParseMode.HTML, reply_markup=kb_back_profile()
                )
                return

        storage.set_last_withdrawal(user_id)

        username   = message.from_user.username or ''
        first_name = message.from_user.first_name or ''

        req_id = withdraw_queue.add(user_id, username, first_name, amount)

        user_data = storage.get_user(user_id)
        user_name = _get_user_display_name(user_data, user_id)
        record_withdrawal_stat(user_id, user_name, amount)
        asyncio.create_task(save_withdrawal(user_id, amount))

        await message.answer(
            f'<blockquote><tg-emoji emoji-id="5312441427764989435">💰</tg-emoji> <b>Заявка на вывод создана!</b></blockquote>\n\n'
            f'<blockquote>'
            f'<tg-emoji emoji-id="5197434882321567830">💰</tg-emoji> Сумма: <code>{amount}</code>\n'
            f'<tg-emoji emoji-id="5197288647275071607">💰</tg-emoji> Заявка: <b>#{req_id}</b>\n'
            f'<tg-emoji emoji-id="5278467510604160626">💰</tg-emoji> Баланс: <code>{storage.get_balance(user_id):.2f}</code>'
            f'</blockquote>\n\n'
            f'<i><tg-emoji emoji-id="5440621591387980068">💰</tg-emoji> Заявка будет обработана администратором!</i>',
            parse_mode=ParseMode.HTML,
            reply_markup=kb_back_profile()
        )


    except ValueError:
        await message.answer('❌ Введите число')


async def _approve_request(req: WithdrawRequest) -> tuple:
    if req.status != 'pending':
        return False, f'Заявка #{req.req_id} уже обработана (статус: {req.status})'

    check = await crypto_api.create_check(req.amount, req.user_id)
    if not check or 'bot_check_url' not in check:
        req.status = 'failed'
        return False, f'❌ Ошибка создания чека для заявки #{req.req_id}'

    req.status = 'approved'

    try:
        await bot.send_message(
            req.user_id,
            f'<blockquote><tg-emoji emoji-id="5312441427764989435">💰</tg-emoji> <b>Вывод одобрен!</b> ✅</blockquote>\n\n'
            f'<blockquote>'
            f'<tg-emoji emoji-id="5197434882321567830">💰</tg-emoji> Сумма: <code>{req.amount}</code> USDT\n'
            f'<tg-emoji emoji-id="5278467510604160626">💰</tg-emoji> Баланс: <code>{storage.get_balance(req.user_id):.2f}</code>'
            f'</blockquote>',
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Получить чек", url=check['bot_check_url'], icon_custom_emoji_id=EMOJI_LINK)]
            ])
        )
    except Exception as e:
        logging.warning(f"[Approve] Не удалось отправить чек пользователю {req.user_id}: {e}")

    display = f'@{req.username}' if req.username else req.first_name or f'ID {req.user_id}'
    return True, f'✅ Заявка #{req.req_id} одобрена | {display} | {req.amount} USDT'


async def _reject_request(req: WithdrawRequest) -> tuple:
    if req.status != 'pending':
        return False, f'Заявка #{req.req_id} уже обработана (статус: {req.status})'

    req.status = 'rejected'

    try:
        await bot.send_message(
            req.user_id,
            f'<blockquote><tg-emoji emoji-id="5420323339723881652">💰</tg-emoji> <b>Заявка на вывод отклонена!</b></blockquote>\n\n'
            f'<blockquote>'
            f'<tg-emoji emoji-id="5397782960512444700">💰</tg-emoji> Заявка: <b>#{req.req_id}</b>\n'
            f'<tg-emoji emoji-id="5197434882321567830">💰</tg-emoji> Сумма: <code>{req.amount}</code> USDT'
            f'</blockquote>\n\n'
            f'<i><b>По вопросам обратитесь в поддержку!</i></b>',
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logging.warning(f"[Reject] Не удалось уведомить пользователя {req.user_id}: {e}")

    display = f'@{req.username}' if req.username else req.first_name or f'ID {req.user_id}'
    return True, f'🚫 Заявка #{req.req_id} отклонена | {display} | {req.amount} USDT'


_CHECKW_RE = _re.compile(r'^/checkw$', _re.IGNORECASE)

@payment_router.message(F.text.regexp(_CHECKW_RE))
async def handle_checkw(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    pending = withdraw_queue.pending()
    if not pending:
        await message.reply(
            '<blockquote>📭 <b>Нет активных заявок на вывод.</b></blockquote>',
            parse_mode='HTML'
        )
        return

    lines = []
    for req in pending:
        display = f'@{req.username}' if req.username else req.first_name or f'ID {req.user_id}'
        lines.append(
            f'<b>#{req.req_id}</b> | {display} | <code>{req.amount}</code> USDT | <code>{req.user_id}</code>'
        )

    text = (
        f'<blockquote>💸 <b>Заявки на вывод ({len(pending)} шт.)</b></blockquote>\n\n'
        + '\n'.join(lines)
        + '\n\n'
        '<code>/type #N</code> — одобрить\n'
        '<code>/reject #N</code> — отклонить\n'
        '<code>/type all</code> — одобрить все\n'
        '<code>/reject all</code> — отклонить все'
    )
    await message.reply(text, parse_mode='HTML')


_TYPE_RE = _re.compile(r'^/type\s+(?:#(\d+)|(all))$', _re.IGNORECASE)

@payment_router.message(F.text.regexp(_TYPE_RE))
async def handle_type(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    m = _TYPE_RE.match(message.text.strip())
    is_all  = bool(m.group(2))
    req_id  = int(m.group(1)) if m.group(1) else None

    if is_all:
        pending = withdraw_queue.pending()
        if not pending:
            await message.reply('<blockquote>📭 Нет активных заявок.</blockquote>', parse_mode='HTML')
            return
        results = []
        for req in pending:
            ok, msg = await _approve_request(req)
            results.append(msg)
        await message.reply(
            '<blockquote>✅ <b>Обработка завершена</b></blockquote>\n\n' + '\n'.join(results),
            parse_mode='HTML'
        )
    else:
        req = withdraw_queue.get(req_id)
        if not req:
            await message.reply(f'<blockquote>❌ Заявка #{req_id} не найдена.</blockquote>', parse_mode='HTML')
            return
        ok, msg = await _approve_request(req)
        await message.reply(f'<blockquote>{msg}</blockquote>', parse_mode='HTML')


_REJECT_RE = _re.compile(r'^/reject\s+(?:#(\d+)|(all))$', _re.IGNORECASE)

@payment_router.message(F.text.regexp(_REJECT_RE))
async def handle_reject(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    m = _REJECT_RE.match(message.text.strip())
    is_all = bool(m.group(2))
    req_id = int(m.group(1)) if m.group(1) else None

    if is_all:
        pending = withdraw_queue.pending()
        if not pending:
            await message.reply('<blockquote>📭 Нет активных заявок.</blockquote>', parse_mode='HTML')
            return
        results = []
        for req in pending:
            ok, msg = await _reject_request(req)
            results.append(msg)
        await message.reply(
            '<blockquote>🚫 <b>Обработка завершена</b></blockquote>\n\n' + '\n'.join(results),
            parse_mode='HTML'
        )
    else:
        req = withdraw_queue.get(req_id)
        if not req:
            await message.reply(f'<blockquote>❌ Заявка #{req_id} не найдена.</blockquote>', parse_mode='HTML')
            return
        ok, msg = await _reject_request(req)
        await message.reply(f'<blockquote>{msg}</blockquote>', parse_mode='HTML')


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

    ton_rate  = rates.get('TON', 0.0)
    trx_rate  = rates.get('TRX', 0.0)

    usdt_usd  = usdt
    ton_usd   = ton * ton_rate
    trx_usd   = trx * trx_rate
    total_usd = usdt_usd + ton_usd + trx_usd

    usdt_str = f'USDT-{usdt:.2f} ({usdt_usd:.2f}$)'
    ton_str  = f'TON-{ton:.4f} ({ton_usd:.2f}$)' if ton_rate else f'TON-{ton:.4f}'
    trx_str  = f'TRX-{trx:.4f} ({trx_usd:.2f}$)' if trx_rate else f'TRX-{trx:.4f}'

    await message.reply(
        f'<blockquote><b>'
        f'<tg-emoji emoji-id="5798650400189980129">💰</tg-emoji>Cryptobot-{total_usd:.2f}$\n\n'
        f'<tg-emoji emoji-id="5800653259404223435">💰</tg-emoji>{usdt_str}\n'
        f'<tg-emoji emoji-id="5798401408050929441">💰</tg-emoji>{ton_str}\n'
        f'<tg-emoji emoji-id="5798480856355970219">💰</tg-emoji>{trx_str}\n\n'
        f'</b></blockquote>\n\n'
        f'<b><i><tg-emoji emoji-id="5386367538735104399">💰</tg-emoji>Резерв обновляется в реальном времени!</i></b>',
        parse_mode='HTML'
    )


def setup_payments(bot_instance: Bot):
    global bot
    bot = bot_instance
