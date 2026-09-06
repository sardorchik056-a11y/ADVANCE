import asyncio
import logging
import os
import re
import json
from typing import Optional
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery,
    ReplyKeyboardMarkup, KeyboardButton
)
from aiogram.filters.command import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from payments import (
    payment_router, setup_payments, storage, MIN_DEPOSIT, MIN_WITHDRAWAL,
    kb_deposit_methods, kb_withdraw_methods,
)
from game import (
    BettingGame, show_dice_menu, show_basketball_menu, show_football_menu,
    show_darts_menu, show_bowling_menu, show_exact_number_menu, request_amount,
    cancel_bet, is_bet_command, handle_text_bet_command
)
from mines import (
    mines_router, MinesGame, show_mines_menu, process_mines_bet, process_mines_command
)
from tower import (
    tower_router, TowerGame, show_tower_menu, process_tower_bet, process_tower_command
)
from gold import (
    gold_router, GoldGame, show_gold_menu, process_gold_bet, process_gold_command
)
from referrals import (
    referral_router, referral_storage,
    setup_referrals, process_start_referral,
    ReferralWithdraw, ref_withdraw_amount
)
from leaders import leaders_router, show_leaders, update_user_name, init_leaders_db
import leaders as _leaders_module
import mines as _mines_module
import tower as _tower_module
import gold as _gold_module
import referrals as _referrals_module
import payments as _payments_module

from duels import (
    duels_router, setup_duels,
    is_duel_command, handle_duel_command,
    is_mygames_command, handle_mygames,
    is_del_command, handle_del,
)
import duels as _duels_module

from bonus import (
    bonus_router,
    setup_bonus,
    start_bonus_watchdog,
    is_bonus_command,
    handle_bonus,
)
import bonus as _bonus_module

from helper import helper_router
from broadcast import broadcast_router

try:
    from database import (
        init_db,
        import_users_from_json,
        db_get_user,
        db_get_user_last_games,
    )
except ImportError:
    async def init_db(): pass
    async def import_users_from_json(): pass
    def db_get_user(user_id): return {}
    def db_get_user_last_games(user_id, limit=10): return []

BOT_TOKEN = "8651956926:AAG3ML1uGBPQOgrM5WAMl3kXaRLvVxTHCsw"

LINK_NEWS     = "https://t.me/egonewsg"
LINK_CHAT     = "https://t.me/egogruf"
LINK_INSTRUCT = "https://t.me/egonewsg"
LINK_SUPPORT  = "https://t.me/Kazino_Bulda"

EMOJI_WELCOME    = "5436386989857320953"  # 🤑 кастомный эмодзи вместо приветственного стикера
EMOJI_PROFILE    = "5224361015847725432"  # 👤
EMOJI_PARTNERS   = "5906986955911993888"
EMOJI_GAMES      = "5224708079270013673"  # 🎮
EMOJI_LEADERS    = "5226892735859961563"  # 🏆
EMOJI_ABOUT      = "5226696692077731319"  # 💬
EMOJI_CRYPTOBOT  = "5798650400189980129"  # 💵
EMOJI_XROCKET    = "5798534328698805312"  # 🚀
EMOJI_BACK       = "5233735937317447077"
EMOJI_DEVELOPMENT= "5445355530111437729"
EMOJI_WALLET     = "5443127283898405358"
EMOJI_STATS      = "5197288647275071607"
EMOJI_WITHDRAWAL = "5445355530111437729"
EMOJI_MINES      = "5307996024738395492"
EMOJI_PROMO      = "5224336109332374398"  # 🎟
EMOJI_INSTRUCT   = "5224571477835162080"  # 📜
EMOJI_CHANNEL    = "5224450475721531885"  # 📢
EMOJI_CHAT       = "5226696692077731319"  # 💬
EMOJI_SUPORT     = "5226878755741410609"  # ⚠️
EMOJI_PEREXOD    = "5906839307821259375"
EMOJI_GOLD       = "5278467510604160626"
EMOJI_BONUS      = "5443127283898405358"
EMOJI_TEXT_CMD   = "5224571477835162080"  # 📜 — текстовые команды
EMOJI_EMOJI_GAMES= "5224519461486241288"  # ✨️ — эмодзи-игры
EMOJI_MINES_LABEL= "5224253822053952249"  # 🎯 — "Мины игры:"
EMOJI_TOWER_BTN  = "5224707005528188746"  # 🏰 — кнопка "Башня"
EMOJI_MINES_BTN  = "5226939456514203548"  # 💣 — кнопка "Мины"
EMOJI_GOLD_BTN   = "5226770767378688325"  # 🪙 — кнопка "Золото"
EMOJI_REPLY_MENU     = "5224409982769869331"  # 🏠 — иконка reply-кнопки «Меню»
EMOJI_REPLY_PARTNERS = "5226704646357165977"  # 👥 — иконка reply-кнопки «Партнёры»
EMOJI_REPLY_ABOUT    = "5226696692077731319"  # 💬 — иконка reply-кнопки «О проекте»

GAME_CALLBACKS = {
    'dice':         'custom_dice_001',
    'basketball':   'custom_basketball_002',
    'football':     'custom_football_003',
    'darts':        'custom_darts_004',
    'bowling':      'custom_bowling_005',
    'exact_number': 'custom_exact_006',
    'back_to_games':'custom_back_games_007'
}

WELCOME_STICKER_ID = "CAACAgIAAxkBAAIGUWmRflo7gmuMF5MNUcs4LGpyA93yAAKaDAAC753ZS6lNRCGaKqt5OgQ"

ADMIN_IDS    = [8118184388, 8872779375]
PROMO_FILE   = "promos.json"
MENU_IMAGE_FILE = "menu_image.json"
MIN_TRANSFER = 0.02
MAX_TRANSFER = 10000

TRANSFER_PATTERN  = re.compile(r'^(?:/)?(?:pay|дать)\s+([\d.,]+)$', re.IGNORECASE)
GAMES_PATTERN     = re.compile(r'^/?(?:игры|games)$', re.IGNORECASE)
DEP_PATTERN       = re.compile(
    r'^/?(?:деп|пополнить|депозит|dep|deposit)\s+(\d+(?:\.\d+)?)$',
    re.IGNORECASE
)
GOLD_PATTERN      = re.compile(r'^/?(?:gold|золото)\s+[\d.,]+$', re.IGNORECASE)
KAZNA_PATTERN     = re.compile(r'^/?(?:казна|kazna|reserve)$', re.IGNORECASE)
CHECKW_PATTERN    = re.compile(r'^/checkw$', re.IGNORECASE)
CHECK_PATTERN     = re.compile(r'^/check(?:\s|$)', re.IGNORECASE)
TYPE_PATTERN      = re.compile(r'^/type\s+(?:#\d+|all)$', re.IGNORECASE)
REJECT_PATTERN    = re.compile(r'^/reject\s+(?:#\d+|all)$', re.IGNORECASE)
HISTORY_PATTERN   = re.compile(r'^/history$', re.IGNORECASE)
BOTSTATS_PATTERN  = re.compile(r'^/botstats$', re.IGNORECASE)
WISS_PATTERN      = re.compile(r'^/wiss\s+\d+(?:\.\d+)?$', re.IGNORECASE)
IMG_PATTERN       = re.compile(r'^/img$', re.IGNORECASE)

_transfer_locks: dict = {}

def _get_transfer_lock(user_id: int) -> asyncio.Lock:
    if user_id not in _transfer_locks:
        _transfer_locks[user_id] = asyncio.Lock()
    return _transfer_locks[user_id]

router       = Router()
betting_game = None
_msg_owners: dict = {}

def _set_msg_owner(message_id: int, user_id: int):
    _msg_owners[message_id] = user_id

def _is_msg_owner(message_id: int, user_id: int) -> bool:
    owner = _msg_owners.get(message_id)
    if owner is None:
        return True
    return owner == user_id

def _inject_leaders_owner_fns():
    _leaders_module.set_owner_fn   = _set_msg_owner
    _leaders_module.is_owner_fn    = _is_msg_owner
    _mines_module.set_owner_fn     = _set_msg_owner
    _mines_module.is_owner_fn      = _is_msg_owner
    _tower_module.set_owner_fn     = _set_msg_owner
    _tower_module.is_owner_fn      = _is_msg_owner
    _gold_module.set_owner_fn      = _set_msg_owner
    _gold_module.is_owner_fn       = _is_msg_owner
    _referrals_module.set_owner_fn = _set_msg_owner
    _referrals_module.is_owner_fn  = _is_msg_owner
    _payments_module.set_owner_fn  = _set_msg_owner
    _payments_module.is_owner_fn   = _is_msg_owner
    _duels_module.set_owner_fn     = _set_msg_owner
    _duels_module.is_owner_fn      = _is_msg_owner


def _save_username(user_id: int, username: str, first_name: str = ""):
    try:
        user_data = storage.get_user(user_id)
        changed = False

        new_username = (username or "").strip()
        if new_username and user_data.get("username") != new_username:
            user_data["username"] = new_username
            changed = True

        new_name = (first_name or "").strip()
        if new_name and user_data.get("first_name") != new_name:
            user_data["first_name"] = new_name
            changed = True

        if changed:
            try:
                from database import db_update_field
                if new_username:
                    db_update_field(user_id, "username", new_username)
                if new_name:
                    db_update_field(user_id, "first_name", new_name)
            except Exception as e:
                logging.debug(f"[_save_username] db_update_field: {e}")
    except Exception as e:
        logging.error(f"[_save_username] user={user_id}: {e}")


class PromoState(StatesGroup):
    entering_code = State()


def load_promos() -> dict:
    if not os.path.exists(PROMO_FILE):
        return {}
    try:
        with open(PROMO_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_promos(data: dict):
    with open(PROMO_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_menu_image_id() -> Optional[str]:
    """Возвращает file_id картинки, установленной командой /img, или None, если не задана."""
    if not os.path.exists(MENU_IMAGE_FILE):
        return None
    try:
        with open(MENU_IMAGE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("file_id")
    except Exception:
        return None

def set_menu_image_id(file_id: str) -> None:
    """Сохраняет file_id картинки главного меню (используется при /start и по кнопке «Меню»)."""
    with open(MENU_IMAGE_FILE, "w", encoding="utf-8") as f:
        json.dump({"file_id": file_id}, f, ensure_ascii=False, indent=2)

def promo_create(code: str, amount: float, activations: int) -> bool:
    data = load_promos()
    code = code.upper().strip()
    if code in data:
        return False
    data[code] = {"amount": amount, "activations": activations, "used_by": []}
    save_promos(data)
    return True

def promo_use(code: str, user_id: int):
    data = load_promos()
    code = code.upper().strip()
    if code not in data:
        return False, 0, "not_found"
    promo = data[code]
    if user_id in promo["used_by"]:
        return False, 0, "already_used"
    if promo["activations"] <= 0:
        return False, 0, "expired"
    promo["used_by"].append(user_id)
    promo["activations"] -= 1
    save_promos(data)
    return True, promo["amount"], "ok"


def is_balance_command(text: str) -> bool:
    if not text:
        return False
    t = text.lstrip('/')
    return t.lower() in {'б', 'b', 'бал', 'bal', 'баланс', 'balance'}

def sync_balances(user_id: int):
    return storage.get_balance(user_id)

def links_line() -> str:
    return (
        f'<tg-emoji emoji-id="{EMOJI_SUPORT}">⚠️</tg-emoji> <b>'
        f'<a href="{LINK_SUPPORT}">Тех. поддержка</a> | '
        f'<a href="{LINK_CHAT}">Наш чат</a> | '
        f'<a href="{LINK_NEWS}">Новости</a></b>'
    )


def get_main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Профиль",   callback_data="profile", icon_custom_emoji_id=EMOJI_PROFILE),
            InlineKeyboardButton(text="О проекте", callback_data="about",   icon_custom_emoji_id=EMOJI_ABOUT)
        ],
        [
            InlineKeyboardButton(text="Лидеры",    callback_data="leaders",    icon_custom_emoji_id=EMOJI_LEADERS),
            InlineKeyboardButton(text="Промокоды", callback_data="promo_menu", icon_custom_emoji_id=EMOJI_PROMO)
        ],
        [
            InlineKeyboardButton(text="Инструкция", url=LINK_INSTRUCT, icon_custom_emoji_id=EMOJI_INSTRUCT)
        ]
    ])

def get_reply_menu() -> ReplyKeyboardMarkup:
    """Постоянная reply-клавиатура под полем ввода: Меню | Партнёры | Игры."""
    return ReplyKeyboardMarkup(
        keyboard=[[
            KeyboardButton(text="Меню",     icon_custom_emoji_id=EMOJI_REPLY_MENU),
            KeyboardButton(text="Партнёры", icon_custom_emoji_id=EMOJI_REPLY_PARTNERS),
            KeyboardButton(text="Игры",     icon_custom_emoji_id=EMOJI_GAMES),
        ]],
        resize_keyboard=True,
        is_persistent=True
    )

def get_games_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🎲 Кубик",     callback_data=GAME_CALLBACKS['dice']),
            InlineKeyboardButton(text="🏀 Баскетбол", callback_data=GAME_CALLBACKS['basketball'])
        ],
        [
            InlineKeyboardButton(text="⚽️ Футбол", callback_data=GAME_CALLBACKS['football']),
            InlineKeyboardButton(text="🎯 Дартс",  callback_data=GAME_CALLBACKS['darts'])
        ],
        [
            InlineKeyboardButton(text="🎳 Боулинг", callback_data=GAME_CALLBACKS['bowling'])
        ],
        [
            InlineKeyboardButton(text="Мины",  callback_data="mines_menu", icon_custom_emoji_id=EMOJI_MINES_BTN),
            InlineKeyboardButton(text="Башня", callback_data="tower_menu", icon_custom_emoji_id=EMOJI_TOWER_BTN)
        ],
        [
            InlineKeyboardButton(text="Золото", callback_data="gold_menu", icon_custom_emoji_id=EMOJI_GOLD_BTN)
        ],
        [
            InlineKeyboardButton(text="Назад", callback_data="back_to_main", icon_custom_emoji_id=EMOJI_BACK)
        ]
    ])

def get_profile_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Пополнить", callback_data="deposit",  icon_custom_emoji_id=EMOJI_WALLET),
            InlineKeyboardButton(text="Вывести",   callback_data="withdraw", icon_custom_emoji_id=EMOJI_WITHDRAWAL)
        ],
        [
            InlineKeyboardButton(text="Назад", callback_data="back_to_main", icon_custom_emoji_id=EMOJI_BACK)
        ]
    ])

def get_cancel_menu():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Отмена", callback_data="profile", icon_custom_emoji_id=EMOJI_BACK)
    ]])

def get_balance_menu():
    bot_username = os.getenv("BOT_USERNAME", "your_bot")
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Пополнить", url=f"https://t.me/{bot_username}?start=deposit", icon_custom_emoji_id=EMOJI_WALLET),
        InlineKeyboardButton(text="Вывести",   url=f"https://t.me/{bot_username}?start=withdraw", icon_custom_emoji_id=EMOJI_WITHDRAWAL)
    ]])

def get_promo_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Ввести промокод", callback_data="promo_enter", icon_custom_emoji_id=EMOJI_PROMO)],
        [InlineKeyboardButton(text="Назад", callback_data="back_to_main", icon_custom_emoji_id=EMOJI_BACK)]
    ])

def get_promo_cancel_menu():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Отмена", callback_data="promo_menu", icon_custom_emoji_id=EMOJI_BACK)
    ]])


def get_main_menu_text():
    return (
        f'<blockquote><tg-emoji emoji-id="5224483169012589725">⭐️</tg-emoji> <b>Честные игры — прозрачные правила и реальные шансы на победу.</b>\n'
        f'<b>Без скрытых условий, всё открыто и по-настоящему честно.</b></blockquote>\n\n'
        f'<blockquote><tg-emoji emoji-id="5226472688058411001">♻️</tg-emoji> <b>Быстрые выплаты — моментальный вывод средств без задержек.</b>\n'
        f'<tg-emoji emoji-id="5224350849660138496">👛</tg-emoji> <b>Выводы через <tg-emoji emoji-id="{EMOJI_CRYPTOBOT}">💵</tg-emoji> <a href="https://t.me/send">Cryptobot</a> и '
        f'<tg-emoji emoji-id="{EMOJI_XROCKET}">🚀</tg-emoji> <a href="https://t.me/xrocket">Xrocket</a></b></blockquote>\n\n'
        f'{links_line()}\n'
    )

def get_about_text():
    return (
        f'<tg-emoji emoji-id="{EMOJI_ABOUT}">💬</tg-emoji> <b>О проекте</b>\n\n'
        f'<blockquote><tg-emoji emoji-id="5224483169012589725">⭐️</tg-emoji> <b>Мы работаем честно: прозрачные правила и настоящие шансы на выигрыш.</b>\n'
        f'<b>Никаких скрытых условий — всё видно и понятно.</b></blockquote>\n\n'
        f'<blockquote><tg-emoji emoji-id="5226472688058411001">♻️</tg-emoji> <b>Выплаты приходят мгновенно, без ожидания.</b>\n'
        f'<tg-emoji emoji-id="5224350849660138496">👛</tg-emoji> <b>Пополнение и вывод через <tg-emoji emoji-id="{EMOJI_CRYPTOBOT}">💵</tg-emoji> <a href="https://t.me/send">Cryptobot</a> и '
        f'<tg-emoji emoji-id="{EMOJI_XROCKET}">🚀</tg-emoji> <a href="https://t.me/xrocket">Xrocket</a></b></blockquote>\n\n'
        f'{links_line()}\n'
    )


async def send_main_menu(message: Message):
    """Отправляет главное меню. Если картинка задана командой /img — отправляет
    фото с текстом меню в подписи, иначе — обычным текстовым сообщением."""
    image_id = get_menu_image_id()
    if image_id:
        return await message.answer_photo(
            photo=image_id,
            caption=get_main_menu_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_menu()
        )
    return await message.answer(
        get_main_menu_text(), parse_mode=ParseMode.HTML, reply_markup=get_main_menu(),
        disable_web_page_preview=True
    )

async def edit_menu(message: Message, text: str, reply_markup=None, disable_web_page_preview: Optional[bool] = None):
    """Универсально редактирует меню-сообщение.
    Если сообщение с фото (меню отправлено через /img) — правит подпись (edit_caption),
    иначе — правит текст (edit_text). Без этого кнопки под фото-меню не работают,
    т.к. edit_text на сообщении с фото падает с ошибкой "there is no text in the message to edit"."""
    if message.photo:
        return await message.edit_caption(
            caption=text,
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup
        )
    kwargs = {"parse_mode": ParseMode.HTML, "reply_markup": reply_markup}
    if disable_web_page_preview is not None:
        kwargs["disable_web_page_preview"] = disable_web_page_preview
    return await message.edit_text(text, **kwargs)


def get_games_menu_text(user_id: int):
    balance = sync_balances(user_id)
    return (
        f'<blockquote><tg-emoji emoji-id="5424972470023104089">👋</tg-emoji> <b>Игры</b></blockquote>\n\n'
        f'<blockquote><b><tg-emoji emoji-id="{EMOJI_TEXT_CMD}">📜</tg-emoji>Текстовые команды:</b>\n\n'
        f'  <tg-emoji emoji-id="{EMOJI_EMOJI_GAMES}">✨️</tg-emoji>Эмоджи игры:\n'
        f"  <code>куб (исход) (сумма)</code>\n"
        f"  <code>баскет (исход) (сумма)</code>\n"
        f"  <code>фут (исход) (сумма)</code>\n"
        f"  <code>дартс (исход) (сумма)</code>\n"
        f"  <code>боулинг (исход) (сумма)</code>\n"
        f"</blockquote>"
        f"<blockquote>"
        f'  <tg-emoji emoji-id="{EMOJI_MINES_LABEL}">🎯</tg-emoji>Мины игры:\n'
        f"  <code>мины (сумма) (сложность)</code>\n"
        f"  <code>башня (сумма) (сложность)</code>\n"
        f"  <code>золото (сумма)</code>\n"
        f"</blockquote>\n\n"
        f'{links_line()}\n'
    )

def get_profile_text(user_first_name: str, days_in_project: int, user_id: int):
    balance           = sync_balances(user_id)
    user_data         = storage.get_user(user_id)
    total_deposits    = user_data.get('total_deposits', 0)
    total_withdrawals = user_data.get('total_withdrawals', 0)

    if 11 <= days_in_project <= 19:
        days_text = "дней"
    elif days_in_project % 10 == 1:
        days_text = "день"
    elif days_in_project % 10 in [2, 3, 4]:
        days_text = "дня"
    else:
        days_text = "дней"

    return (
        f'<blockquote><b><tg-emoji emoji-id="{EMOJI_PROFILE}">👤</tg-emoji> Профиль</b></blockquote>\n\n'
        f'<blockquote>\n'
        f'├ <tg-emoji emoji-id="5224350849660138496">👛</tg-emoji> <b>Баланс: <code>{balance:,.2f}</code><tg-emoji emoji-id="5116648080787112958">💰</tg-emoji></b>\n'
        f'├ <tg-emoji emoji-id="5224380154221995303">💰</tg-emoji> <b>Депозитов: <code>{total_deposits:,.2f}</code><tg-emoji emoji-id="5116648080787112958">💰</tg-emoji></b>\n'
        f'├ <tg-emoji emoji-id="5224393683368978372">💵</tg-emoji> <b>Выводов: <code>{total_withdrawals:,.2f}</code><tg-emoji emoji-id="5116648080787112958">💰</tg-emoji></b>\n'
        f'└ <tg-emoji emoji-id="5226736338920842771">📋</tg-emoji> <b>В проекте: <code>{days_in_project} {days_text}</code></b>\n'
        f'</blockquote>\n\n'
        f'{links_line()}\n'
    )


# ─────────────────────────────────────────────────────────────────────────────
#  Команды
# ─────────────────────────────────────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(message: Message):
    try:
        args  = message.text.split(maxsplit=1)
        param = args[1] if len(args) > 1 else ""

        storage.get_user(message.from_user.id)
        _save_username(
            message.from_user.id,
            message.from_user.username or "",
            message.from_user.first_name or ""
        )

        if param == "deposit":
            storage.clear_pending(message.from_user.id)
            await message.answer(
                f'<b><tg-emoji emoji-id="{EMOJI_WALLET}">💰</tg-emoji> Пополнение баланса</b>\n\n'
                f'<blockquote><i>Выберите способ пополнения:</i></blockquote>',
                parse_mode=ParseMode.HTML, reply_markup=kb_deposit_methods()
            )
            return
        elif param == "withdraw":
            storage.clear_pending(message.from_user.id)
            await message.answer(
                f'<b><tg-emoji emoji-id="{EMOJI_WITHDRAWAL}">💸</tg-emoji> Вывод средств</b>\n\n'
                f'<blockquote><i>Выберите способ вывода:</i></blockquote>',
                parse_mode=ParseMode.HTML, reply_markup=kb_withdraw_methods()
            )
            return
        elif param.startswith("ref_"):
            await process_start_referral(message, param)
        else:
            referral_storage.mark_organic(message.from_user.id)

        sync_balances(message.from_user.id)
        update_user_name(storage, message.from_user.id, message.from_user.first_name or "")

        await message.answer(
            f'<tg-emoji emoji-id="{EMOJI_WELCOME}">🤑</tg-emoji>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_reply_menu()
        )
        sent = await send_main_menu(message)
        _set_msg_owner(sent.message_id, message.from_user.id)
    except Exception as e:
        logging.error(f"Error in start: {e}")
        await message.answer("Произошла ошибка. Попробуйте позже.")


@router.message(F.text.startswith("/add") & ~F.text.startswith("/addpromo"))
async def cmd_add_balance(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Нет доступа.")
        return

    parts = message.text.split()
    if len(parts) != 3:
        await message.answer(
            "<b>⚙️ Использование:</b>\n"
            "<code>/add [user_id] [сумма]</code>\n"
            "<code>/add [@username] [сумма]</code>\n\n"
            "<blockquote>➕ Выдать: <code>/add @user 50</code>\n"
            "➖ Снять: <code>/add @user -50</code></blockquote>",
            parse_mode=ParseMode.HTML
        )
        return

    target_input = parts[1]
    amount_str   = parts[2]

    try:
        amount = float(amount_str.replace(',', '.'))
    except ValueError:
        await message.answer("❌ Неверный формат суммы.", parse_mode=ParseMode.HTML)
        return

    if amount == 0:
        await message.answer("❌ Сумма не может быть равна нулю.", parse_mode=ParseMode.HTML)
        return

    target_id   = None
    target_name = target_input

    if target_input.startswith("@"):
        username_clean = target_input.lstrip("@").lower()
        for uid, udata in storage.users.items():
            stored = (udata.get("username") or "").lstrip("@").lower()
            if stored and stored == username_clean:
                target_id   = int(uid)
                target_name = udata.get("first_name") or udata.get("username") or str(uid)
                break
        if target_id is None:
            await message.answer(
                f"❌ Пользователь <code>{target_input}</code> не найден в базе.\n"
                f"<blockquote><i>Пользователь должен хотя бы раз нажать любую кнопку в боте.</i></blockquote>",
                parse_mode=ParseMode.HTML
            )
            return
    else:
        try:
            target_id = int(target_input)
            user_data = storage.get_user(target_id)
            target_name = (
                user_data.get("first_name")
                or user_data.get("username")
                or str(target_id)
            )
        except ValueError:
            await message.answer(
                "❌ Неверный формат: укажите числовой ID или @username.",
                parse_mode=ParseMode.HTML
            )
            return

    if amount < 0:
        current_balance = storage.get_balance(target_id)
        if current_balance + amount < 0:
            await message.answer(
                f"❌ Недостаточно средств для снятия.\n"
                f"<blockquote>Баланс: <code>{current_balance:.2f}</code></blockquote>",
                parse_mode=ParseMode.HTML
            )
            return

    storage.get_user(target_id)
    storage.add_balance(target_id, amount)
    new_balance = storage.get_balance(target_id)

    action_emoji = "➕" if amount > 0 else "➖"
    action_text  = "Выдано" if amount > 0 else "Снято"

    await message.answer(
        f"<b>✅ Баланс обновлён</b>\n\n<blockquote>"
        f"👤 Пользователь: <b>{target_name}</b> (<code>{target_id}</code>)\n"
        f"{action_emoji} {action_text}: <code>{abs(amount):.2f}</code>\n"
        f"💰 Новый баланс: <code>{new_balance:.2f}</code></blockquote>",
        parse_mode=ParseMode.HTML
    )


# ── /checkw больше не нужен: вывод обрабатывается автоматически ────────────

@router.message(F.text.regexp(CHECKW_PATTERN))
async def handle_checkw_main(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer(
        "<blockquote>ℹ️ Заявки на вывод больше не используются — все выводы обрабатываются автоматически.</blockquote>",
        parse_mode=ParseMode.HTML
    )


@router.message(F.text.regexp(CHECK_PATTERN))
async def cmd_check_user(message: Message):
    """Команда /check @username или /check user_id — для администраторов."""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Нет доступа.", parse_mode=ParseMode.HTML)
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer(
            "<b>⚙️ Использование:</b>\n"
            "<code>/check @username</code>\n"
            "<code>/check user_id</code>\n\n"
            "<blockquote><i>Показывает баланс, депозиты, выводы и последние 10 игр игрока.</i></blockquote>",
            parse_mode=ParseMode.HTML
        )
        return

    target_input = parts[1].strip()
    target_id    = None

    if target_input.startswith("@"):
        username_clean = target_input.lstrip("@").lower()
        for uid, udata in storage.users.items():
            stored = (udata.get("username") or "").lstrip("@").lower()
            if stored and stored == username_clean:
                target_id = int(uid)
                break
        if target_id is None:
            await message.answer(
                f"❌ Пользователь <code>{target_input}</code> не найден.\n"
                f"<blockquote><i>Пользователь должен хотя бы раз запустить бота.</i></blockquote>",
                parse_mode=ParseMode.HTML
            )
            return
    else:
        try:
            target_id = int(target_input)
        except ValueError:
            await message.answer(
                "❌ Укажите <code>@username</code> или числовой ID.",
                parse_mode=ParseMode.HTML
            )
            return

    user_row   = db_get_user(target_id)
    balance    = storage.get_balance(target_id)
    total_dep  = float(user_row.get("total_deposits", 0) or 0)
    total_wth  = float(user_row.get("total_withdrawals", 0) or 0)
    first_name = (user_row.get("first_name") or "").strip()
    username   = (user_row.get("username") or "").strip()
    join_date  = user_row.get("join_date") or "—"
    display    = first_name or username or f"ID {target_id}"
    username_str = f"@{username}" if username else "—"

    games = db_get_user_last_games(target_id, 10)

    total_won    = 0.0
    total_lost_n = 0
    games_lines  = []

    for g in games:
        win      = float(g.get("win_amount", 0) or 0)
        gname    = (g.get("game_name") or "—").strip()
        dt       = (g.get("created_at") or "")[:16]
        if win > 0:
            games_lines.append(
                f'✅ <b>{gname}</b> — <code>+{win:.2f}</code>'
                f'<tg-emoji emoji-id="5197434882321567830">💰</tg-emoji> | {dt}'
            )
            total_won += win
        else:
            games_lines.append(f'❌ <b>{gname}</b> — проигрыш | {dt}')
            total_lost_n += 1

    games_block = "\n".join(games_lines) if games_lines else "<i>Нет игр</i>"

    text = (
        f'<blockquote>'
        f'<tg-emoji emoji-id="{EMOJI_STATS}">🔍</tg-emoji> <b>Карточка игрока</b>'
        f'</blockquote>\n\n'

        f'<blockquote>'
        f'<tg-emoji emoji-id="{EMOJI_PROFILE}">👤</tg-emoji> Имя: <b>{display}</b>\n'
        f'<tg-emoji emoji-id="{EMOJI_PEREXOD}">🔗</tg-emoji> Username: <b>{username_str}</b>\n'
        f'🆔 ID: <code>{target_id}</code>\n'
        f'<tg-emoji emoji-id="5274055917766202507">📅</tg-emoji> В проекте с: <b>{join_date}</b>\n'
        f'</blockquote>\n\n'

        f'<blockquote>'
        f'<tg-emoji emoji-id="{EMOJI_GOLD}">💰</tg-emoji> Баланс: <b><code>{balance:.2f}</code>'
        f'<tg-emoji emoji-id="5197434882321567830">💰</tg-emoji></b>\n'
        f'<tg-emoji emoji-id="{EMOJI_WALLET}">📥</tg-emoji> Депозитов: <b><code>{total_dep:.2f}</code>'
        f'<tg-emoji emoji-id="5197434882321567830">💰</tg-emoji></b>\n'
        f'<tg-emoji emoji-id="{EMOJI_WITHDRAWAL}">📤</tg-emoji> Выводов: <b><code>{total_wth:.2f}</code>'
        f'<tg-emoji emoji-id="5197434882321567830">💰</tg-emoji></b>\n'
        f'</blockquote>\n\n'

        f'<blockquote>'
        f'<tg-emoji emoji-id="{EMOJI_GAMES}">🎮</tg-emoji> <b>Последние 10 игр:</b>\n'
        f'{games_block}\n\n'
        f'<tg-emoji emoji-id="5429651785352501917">✨</tg-emoji> Выиграно: <b><code>{total_won:.2f}</code>'
        f'<tg-emoji emoji-id="5197434882321567830">💰</tg-emoji></b>\n'
        f'❌ Проигрышей: <b><code>{total_lost_n}</code></b>'
        f'</blockquote>'
    )

    await message.answer(text, parse_mode=ParseMode.HTML)


@router.message(F.text.startswith("/addpromo"))
async def cmd_add_promo(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Нет доступа.")
        return
    parts = message.text.split()
    if len(parts) != 4:
        await message.answer(f'<b>Использование:</b> <code>/addpromo [код] [сумма] [активации]</code>', parse_mode=ParseMode.HTML)
        return
    code = parts[1].upper().strip()
    try:
        amount      = float(parts[2])
        activations = int(parts[3])
    except ValueError:
        await message.answer("❌ Неверный формат.", parse_mode=ParseMode.HTML)
        return
    if amount <= 0 or activations <= 0:
        await message.answer("❌ Сумма и кол-во активаций должны быть > 0.", parse_mode=ParseMode.HTML)
        return
    ok = promo_create(code, amount, activations)
    if not ok:
        await message.answer(f"❌ Промокод <code>{code}</code> уже существует.", parse_mode=ParseMode.HTML)
        return
    await message.answer(
        f'✅ <b>Промокод создан!</b>\n\n<blockquote>'
        f'<tg-emoji emoji-id="5262517101578443800">🎰</tg-emoji>Код: <code>{code}</code>\n'
        f'<tg-emoji emoji-id="5197288647275071607">🎰</tg-emoji>Сумма: <b><code>{amount:.2f}</code></b>\n'
        f'<tg-emoji emoji-id="5197288647275071607">🎰</tg-emoji>Активаций: <b><code>{activations}</code></b></blockquote>',
        parse_mode=ParseMode.HTML
    )


# ─────────────────────────────────────────────────────────────────────────────
#  /wiss — ВЫШЕ handle_text_message чтобы не перехватывалась
# ─────────────────────────────────────────────────────────────────────────────

@router.message(F.text.regexp(WISS_PATTERN))
async def handle_wiss_main(message: Message):
    from payments import handle_wiss
    await handle_wiss(message)


# ─────────────────────────────────────────────────────────────────────────────
#  Callback handlers
# ─────────────────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "promo_menu")
async def promo_menu_callback(callback: CallbackQuery, state: FSMContext):
    _save_username(callback.from_user.id, callback.from_user.username or "", callback.from_user.first_name or "")
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True)
        return
    await state.clear()
    await edit_menu(
        callback.message,
        f'<tg-emoji emoji-id="{EMOJI_PROMO}">💣</tg-emoji> <b>Промокоды</b>\n\n'
        f'<blockquote><i>Активируй промокод и получи бонус на баланс.\n'
        f'Промокоды публикуются в нашем <a href="{LINK_CHAT}">чате</a> и <a href="{LINK_NEWS}">канале</a>.</i></blockquote>\n\n{links_line()}',
        reply_markup=get_promo_menu(), disable_web_page_preview=True
    )
    _set_msg_owner(callback.message.message_id, callback.from_user.id)
    await callback.answer()

@router.callback_query(F.data == "promo_enter")
async def promo_enter_callback(callback: CallbackQuery, state: FSMContext):
    _save_username(callback.from_user.id, callback.from_user.username or "", callback.from_user.first_name or "")
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True)
        return
    await state.set_state(PromoState.entering_code)
    await edit_menu(
        callback.message,
        f'<b><tg-emoji emoji-id="5224446266653579710">🎁</tg-emoji> Введите промокод</b>\n\n<blockquote><i>Напишите код в чат — регистр не важен.</i></blockquote>',
        reply_markup=get_promo_cancel_menu()
    )
    _set_msg_owner(callback.message.message_id, callback.from_user.id)
    await callback.answer()


@router.callback_query(F.data == "profile")
async def profile_callback(callback: CallbackQuery, state: FSMContext):
    _save_username(callback.from_user.id, callback.from_user.username or "", callback.from_user.first_name or "")
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваш профиль!", show_alert=True)
        return
    await state.clear()
    from datetime import datetime
    user_data       = storage.get_user(callback.from_user.id)
    join_date_str   = user_data.get('join_date', datetime.now().strftime('%Y-%m-%d'))
    join_date       = datetime.strptime(join_date_str, '%Y-%m-%d')
    days_in_project = (datetime.now() - join_date).days
    update_user_name(storage, callback.from_user.id, callback.from_user.first_name or "")
    await edit_menu(
        callback.message,
        get_profile_text(callback.from_user.first_name, days_in_project, callback.from_user.id),
        reply_markup=get_profile_menu(), disable_web_page_preview=True
    )
    _set_msg_owner(callback.message.message_id, callback.from_user.id)
    await callback.answer()


@router.callback_query(F.data == "games")
async def games_callback(callback: CallbackQuery, state: FSMContext):
    _save_username(callback.from_user.id, callback.from_user.username or "", callback.from_user.first_name or "")
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True)
        return
    await state.clear()
    await edit_menu(
        callback.message,
        get_games_menu_text(callback.from_user.id),
        reply_markup=get_games_menu(), disable_web_page_preview=True
    )
    _set_msg_owner(callback.message.message_id, callback.from_user.id)
    await callback.answer()

@router.callback_query(F.data == "mines_menu")
async def mines_menu_callback(callback: CallbackQuery, state: FSMContext):
    _save_username(callback.from_user.id, callback.from_user.username or "", callback.from_user.first_name or "")
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True)
        return
    await state.clear()
    await show_mines_menu(callback, storage, betting_game)

@router.callback_query(F.data == "tower_menu")
async def tower_menu_callback(callback: CallbackQuery, state: FSMContext):
    _save_username(callback.from_user.id, callback.from_user.username or "", callback.from_user.first_name or "")
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True)
        return
    await state.clear()
    await show_tower_menu(callback, storage, betting_game)

@router.callback_query(F.data == "gold_menu")
async def gold_menu_callback(callback: CallbackQuery, state: FSMContext):
    _save_username(callback.from_user.id, callback.from_user.username or "", callback.from_user.first_name or "")
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True)
        return
    await state.clear()
    await show_gold_menu(callback, storage, state)

@router.callback_query(F.data == GAME_CALLBACKS['dice'])
async def dice_menu(callback: CallbackQuery, state: FSMContext):
    _save_username(callback.from_user.id, callback.from_user.username or "", callback.from_user.first_name or "")
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True); return
    await state.clear(); await show_dice_menu(callback)

@router.callback_query(F.data == GAME_CALLBACKS['basketball'])
async def basketball_menu(callback: CallbackQuery, state: FSMContext):
    _save_username(callback.from_user.id, callback.from_user.username or "", callback.from_user.first_name or "")
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True); return
    await state.clear(); await show_basketball_menu(callback)

@router.callback_query(F.data == GAME_CALLBACKS['football'])
async def football_menu(callback: CallbackQuery, state: FSMContext):
    _save_username(callback.from_user.id, callback.from_user.username or "", callback.from_user.first_name or "")
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True); return
    await state.clear(); await show_football_menu(callback)

@router.callback_query(F.data == GAME_CALLBACKS['darts'])
async def darts_menu(callback: CallbackQuery, state: FSMContext):
    _save_username(callback.from_user.id, callback.from_user.username or "", callback.from_user.first_name or "")
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True); return
    await state.clear(); await show_darts_menu(callback)

@router.callback_query(F.data == GAME_CALLBACKS['bowling'])
async def bowling_menu(callback: CallbackQuery, state: FSMContext):
    _save_username(callback.from_user.id, callback.from_user.username or "", callback.from_user.first_name or "")
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True); return
    await state.clear(); await show_bowling_menu(callback)

@router.callback_query(F.data == "bet_dice_exact")
async def exact_number_menu(callback: CallbackQuery, state: FSMContext):
    _save_username(callback.from_user.id, callback.from_user.username or "", callback.from_user.first_name or "")
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True); return
    await state.clear(); await show_exact_number_menu(callback)

@router.callback_query(F.data.startswith("bet_"))
async def handle_bet_selection(callback: CallbackQuery, state: FSMContext):
    _save_username(callback.from_user.id, callback.from_user.username or "", callback.from_user.first_name or "")
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True); return
    await request_amount(callback, state, betting_game)

@router.callback_query(F.data == "cancel_bet")
async def handle_cancel_bet(callback: CallbackQuery, state: FSMContext):
    _save_username(callback.from_user.id, callback.from_user.username or "", callback.from_user.first_name or "")
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True); return
    await cancel_bet(callback, state, betting_game)


@router.callback_query(F.data == "deposit")
async def deposit_callback(callback: CallbackQuery, state: FSMContext):
    _save_username(callback.from_user.id, callback.from_user.username or "", callback.from_user.first_name or "")
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваш профиль!", show_alert=True); return
    await state.clear()
    storage.clear_pending(callback.from_user.id)
    await edit_menu(
        callback.message,
        f'<b><tg-emoji emoji-id="{EMOJI_WALLET}">💰</tg-emoji> Пополнение баланса</b>\n\n'
        f'<blockquote><i>Выберите способ пополнения:</i></blockquote>',
        reply_markup=kb_deposit_methods()
    )
    await callback.answer()

@router.callback_query(F.data == "withdraw")
async def withdraw_callback(callback: CallbackQuery, state: FSMContext):
    _save_username(callback.from_user.id, callback.from_user.username or "", callback.from_user.first_name or "")
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваш профиль!", show_alert=True); return
    await state.clear()
    storage.clear_pending(callback.from_user.id)
    await edit_menu(
        callback.message,
        f'<b><tg-emoji emoji-id="{EMOJI_WITHDRAWAL}">💸</tg-emoji> Вывод средств</b>\n\n'
        f'<blockquote><i>Выберите способ вывода:</i></blockquote>',
        reply_markup=kb_withdraw_methods()
    )
    await callback.answer()


# ─────────────────────────────────────────────────────────────────────────────
#  Текстовые команды
# ─────────────────────────────────────────────────────────────────────────────

@router.message(F.text.regexp(r'(?i)^(?:/)?(?:pay|дать)\s+[\d.,]+$'))
async def handle_transfer(message: Message, state: FSMContext):
    _save_username(message.from_user.id, message.from_user.username or "", message.from_user.first_name or "")
    if not message.reply_to_message:
        await message.reply(
            f'❌<b>Команда должна быть ответом на сообщение игрока!</b>\n\n'
            f'<blockquote><i>Ответьте на сообщение нужного игрока:\n'
            f'<code>дать 100</code> или <code>/pay 100</code></i></blockquote>',
            parse_mode=ParseMode.HTML
        )
        return
    target = message.reply_to_message.from_user
    if target.id == message.from_user.id:
        await message.reply("<blockquote>❌<b>Нельзя переводить самому себе!</b></blockquote>", parse_mode=ParseMode.HTML); return
    if target.is_bot:
        await message.reply("<blockquote>❌<b>Нельзя переводить ботам!</b></blockquote>", parse_mode=ParseMode.HTML); return
    match = TRANSFER_PATTERN.match(message.text.strip())
    if not match:
        return
    try:
        amount = float(match.group(1).replace(',', '.'))
    except ValueError:
        await message.reply("<blockquote>❌<b>Неверный формат суммы!</b></blockquote>", parse_mode=ParseMode.HTML); return
    if amount < MIN_TRANSFER:
        await message.reply(f'<blockquote>❌<b>Мин. сумма перевода: <code>{MIN_TRANSFER}</code><tg-emoji emoji-id="5197434882321567830">💰</tg-emoji></b></blockquote>', parse_mode=ParseMode.HTML); return
    if amount > MAX_TRANSFER:
        await message.reply(f'<blockquote>❌<b>Макс. сумма перевода: <code>{MAX_TRANSFER:,.0f}</code><tg-emoji emoji-id="5197434882321567830">💰</tg-emoji></b></blockquote>', parse_mode=ParseMode.HTML); return
    if storage.get_balance(message.from_user.id) < amount:
        await message.reply("<blockquote>❌<b>Недостаточно средств!</b></blockquote>", parse_mode=ParseMode.HTML); return
    lock = _get_transfer_lock(message.from_user.id)
    if lock.locked():
        await message.reply("<blockquote>⏳<b>Перевод уже обрабатывается.</b></blockquote>", parse_mode=ParseMode.HTML); return
    async with lock:
        if storage.get_balance(message.from_user.id) < amount:
            await message.reply("<blockquote>❌<b>Недостаточно средств!</b></blockquote>", parse_mode=ParseMode.HTML); return
        storage.get_user(target.id)
        _save_username(target.id, target.username or "", target.first_name or "")
        storage.add_balance(message.from_user.id, -amount)
        storage.add_balance(target.id, amount)
    await message.reply(
        f'<tg-emoji emoji-id="5206607081334906820">💸</tg-emoji><b>Перевод выполнен!</b>\n\n<blockquote>'
        f'<tg-emoji emoji-id="5195033767969839232">💸</tg-emoji>Вы отправили <code>{amount:,.2f}</code><tg-emoji emoji-id="5197434882321567830">💸</tg-emoji> игроку <b>{target.first_name or "Игрок"}</b></blockquote>',
        parse_mode=ParseMode.HTML
    )


@router.message(F.text.regexp(r'(?i)^(?:/)?(?:mines|мины)\s+[\d.,]+\s+\d+$'))
async def mines_command_handler(message: Message, state: FSMContext):
    _save_username(message.from_user.id, message.from_user.username or "", message.from_user.first_name or "")
    await process_mines_command(message, state, storage)

@router.message(F.text.regexp(r'(?i)^(?:/)?(?:tower|башня)\s+[\d.,]+\s+\d+$'))
async def tower_command_handler(message: Message, state: FSMContext):
    _save_username(message.from_user.id, message.from_user.username or "", message.from_user.first_name or "")
    await process_tower_command(message, state, storage)

@router.message(F.text.regexp(GOLD_PATTERN))
async def gold_command_handler(message: Message, state: FSMContext):
    _save_username(message.from_user.id, message.from_user.username or "", message.from_user.first_name or "")
    await process_gold_command(message, state, storage)

@router.message(F.text.regexp(GAMES_PATTERN))
async def handle_games_command(message: Message, state: FSMContext):
    _save_username(message.from_user.id, message.from_user.username or "", message.from_user.first_name or "")
    await state.clear()
    sent = await message.answer(
        get_games_menu_text(message.from_user.id),
        parse_mode=ParseMode.HTML, reply_markup=get_games_menu(), disable_web_page_preview=True
    )
    _set_msg_owner(sent.message_id, message.from_user.id)

@router.message(F.text.regexp(DEP_PATTERN))
async def handle_dep_command_main(message: Message, state: FSMContext):
    _save_username(message.from_user.id, message.from_user.username or "", message.from_user.first_name or "")
    from payments import _process_deposit
    m = DEP_PATTERN.match(message.text.strip())
    if not m:
        return
    try:
        amount = float(m.group(1))
    except ValueError:
        return
    await state.clear()
    storage.clear_pending(message.from_user.id)
    await _process_deposit(message, message.from_user.id, 'cryptobot', amount_override=amount)

@router.message(F.text.regexp(KAZNA_PATTERN))
async def handle_kazna_main(message: Message):
    from payments import handle_kazna
    await handle_kazna(message)

@router.message(F.text.regexp(TYPE_PATTERN))
async def handle_type_main(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer(
        "<blockquote>ℹ️ Одобрение вывода больше не требуется — выводы выполняются автоматически.</blockquote>",
        parse_mode=ParseMode.HTML
    )

@router.message(F.text.regexp(REJECT_PATTERN))
async def handle_reject_main(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer(
        "<blockquote>ℹ️ Отклонение вывода больше не требуется — выводы выполняются автоматически.</blockquote>",
        parse_mode=ParseMode.HTML
    )

@router.message(F.text.regexp(HISTORY_PATTERN))
async def handle_history_main(message: Message):
    from payments import handle_history
    await handle_history(message)

@router.message(F.text.regexp(BOTSTATS_PATTERN))
async def handle_botstats_main(message: Message):
    from payments import handle_botstats
    await handle_botstats(message)


@router.message(F.text.regexp(IMG_PATTERN))
async def cmd_send_img_with_menu(message: Message):
    """Админ отвечает /img на сообщение с изображением — эта картинка становится
    постоянной картинкой главного меню: используется при /start и по кнопке «Меню»,
    пока не будет заменена новым /img."""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Нет доступа.", parse_mode=ParseMode.HTML)
        return

    reply = message.reply_to_message
    if not reply or not reply.photo:
        await message.answer(
            "⚙️ Ответьте командой <code>/img</code> на сообщение с изображением.",
            parse_mode=ParseMode.HTML
        )
        return

    photo_id = reply.photo[-1].file_id
    set_menu_image_id(photo_id)

    sent = await message.answer_photo(
        photo=photo_id,
        caption=get_main_menu_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_menu()
    )
    _set_msg_owner(sent.message_id, message.from_user.id)


# ─────────────────────────────────────────────────────────────────────────────
#  Reply-кнопки: Меню | Партнёры | Игры
#  (нажатие "Игры" ловится ниже через GAMES_PATTERN-хендлер handle_games_command —
#  текст кнопки "Игры" уже входит в этот паттерн, отдельный хендлер не нужен)
# ─────────────────────────────────────────────────────────────────────────────

@router.message(F.text == "Меню")
async def reply_menu_button(message: Message, state: FSMContext):
    _save_username(message.from_user.id, message.from_user.username or "", message.from_user.first_name or "")
    await state.clear()
    storage.clear_pending(message.from_user.id)
    sent = await send_main_menu(message)
    _set_msg_owner(sent.message_id, message.from_user.id)


@router.message(F.text == "О проекте")
async def reply_about_button(message: Message, state: FSMContext):
    _save_username(message.from_user.id, message.from_user.username or "", message.from_user.first_name or "")
    await state.clear()
    sent = await message.answer(
        get_about_text(),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Новости",    url=LINK_NEWS,     icon_custom_emoji_id=EMOJI_CHANNEL),
                InlineKeyboardButton(text="Чат",        url=LINK_CHAT,     icon_custom_emoji_id=EMOJI_CHAT),
                InlineKeyboardButton(text="Инструкция", url=LINK_INSTRUCT, icon_custom_emoji_id=EMOJI_INSTRUCT)
            ],
            [InlineKeyboardButton(text="Поддержка", url=LINK_SUPPORT, icon_custom_emoji_id=EMOJI_SUPORT)],
        ])
    )
    _set_msg_owner(sent.message_id, message.from_user.id)


@router.message(F.text == "Партнёры")
async def reply_partners_button(message: Message, state: FSMContext):
    _save_username(message.from_user.id, message.from_user.username or "", message.from_user.first_name or "")
    await state.clear()
    from referrals import text_referrals_main, kb_referrals_main
    sent = await message.answer(
        text_referrals_main(message.from_user.id),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_referrals_main()
    )
    _set_msg_owner(sent.message_id, message.from_user.id)


# ─────────────────────────────────────────────────────────────────────────────
#  Общий обработчик текста — САМЫЙ ПОСЛЕДНИЙ
# ─────────────────────────────────────────────────────────────────────────────

@router.message(F.text)
async def handle_text_message(message: Message, state: FSMContext):
    from payments import handle_amount_input

    _save_username(message.from_user.id, message.from_user.username or "", message.from_user.first_name or "")

    # /wiss — передаём напрямую (защита на случай если специфичный хендлер не сработал)
    if WISS_PATTERN.match(message.text.strip() if message.text else ""):
        from payments import handle_wiss
        await handle_wiss(message)
        return

    if is_balance_command(message.text):
        balance = sync_balances(message.from_user.id)
        await message.reply(
            f'<blockquote><b><tg-emoji emoji-id="5278467510604160626">💰</tg-emoji> '
            f'<code>{balance:,.2f}</code> '
            f'<tg-emoji emoji-id="5197434882321567830">💰</tg-emoji></b></blockquote>\n\n'
            f'<blockquote><i>Выберите действие ниже</i></blockquote>',
            parse_mode=ParseMode.HTML, reply_markup=get_balance_menu()
        )
        return

    if is_bonus_command(message.text):
        await handle_bonus(message, user_id=message.from_user.id)
        return

    if is_mygames_command(message.text):
        await handle_mygames(message)
        return

    if is_del_command(message.text):
        await handle_del(message)
        return

    current_state = await state.get_state()

    if current_state == PromoState.entering_code.state:
        code = message.text.strip()
        ok, amount, reason = promo_use(code, message.from_user.id)
        if ok:
            storage.get_user(message.from_user.id)
            storage.add_balance(message.from_user.id, amount)
            new_balance = storage.get_balance(message.from_user.id)
            await state.clear()
            await message.answer(
                f'✅ <b>Промокод активирован!</b>\n\n<blockquote>'
                f'Начислено: <b><code>+{amount:.2f}</code><tg-emoji emoji-id="5197434882321567830">💰</tg-emoji></b>\n'
                f'<tg-emoji emoji-id="5278467510604160626">💰</tg-emoji>: <b><code>{new_balance:.2f}</code><tg-emoji emoji-id="5197434882321567830">💰</tg-emoji></b></blockquote>',
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="На главную", callback_data="back_to_main", icon_custom_emoji_id=EMOJI_BACK)
                ]])
            )
        else:
            error_texts = {
                "not_found":    "Промокод не найден. Проверьте правильность ввода.",
                "already_used": "Вы уже активировали этот промокод.",
                "expired":      "Промокод больше не активен — все активации израсходованы.",
            }
            await message.answer(
                f"❌ <b>Ошибка активации</b>\n\n<blockquote>{error_texts.get(reason, 'Неизвестная ошибка.')}</blockquote>",
                parse_mode=ParseMode.HTML, reply_markup=get_promo_cancel_menu()
            )
        return

    if current_state == ReferralWithdraw.entering_amount.state:
        await ref_withdraw_amount(message, state)
        return

    if current_state == MinesGame.choosing_bet:
        await process_mines_bet(message, state, storage)
        return

    if current_state == TowerGame.choosing_bet:
        await process_tower_bet(message, state, storage)
        return

    if current_state == GoldGame.choosing_bet.state:
        await process_gold_bet(message, state, storage)
        return

    if is_duel_command(message.text):
        await handle_duel_command(message)
        return

    if is_bet_command(message.text):
        await handle_text_bet_command(message, betting_game)
        return

    try:
        float(message.text)
        if current_state:
            from game import process_bet_amount
            await process_bet_amount(message, state, betting_game)
        else:
            await handle_amount_input(message)
    except ValueError:
        pass


@router.callback_query(F.data == "leaders")
async def leaders_callback(callback: CallbackQuery, state: FSMContext):
    _save_username(callback.from_user.id, callback.from_user.username or "", callback.from_user.first_name or "")
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True); return
    await state.clear()
    await show_leaders(callback, storage)

@router.callback_query(F.data == "about")
async def about_callback(callback: CallbackQuery, state: FSMContext):
    _save_username(callback.from_user.id, callback.from_user.username or "", callback.from_user.first_name or "")
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True); return
    await state.clear()
    await edit_menu(
        callback.message,
        get_about_text(),
        disable_web_page_preview=True,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Новости",    url=LINK_NEWS,     icon_custom_emoji_id=EMOJI_CHANNEL),
                InlineKeyboardButton(text="Чат",        url=LINK_CHAT,     icon_custom_emoji_id=EMOJI_CHAT),
                InlineKeyboardButton(text="Инструкция", url=LINK_INSTRUCT, icon_custom_emoji_id=EMOJI_INSTRUCT)
            ],
            [InlineKeyboardButton(text="Поддержка", url=LINK_SUPPORT, icon_custom_emoji_id=EMOJI_SUPORT)],
        ])
    )
    _set_msg_owner(callback.message.message_id, callback.from_user.id)
    await callback.answer()

@router.callback_query(F.data == "back_to_main")
async def back_to_main_callback(callback: CallbackQuery, state: FSMContext):
    _save_username(callback.from_user.id, callback.from_user.username or "", callback.from_user.first_name or "")
    if not _is_msg_owner(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True); return
    await state.clear()
    storage.clear_pending(callback.from_user.id)
    await edit_menu(
        callback.message,
        get_main_menu_text(), reply_markup=get_main_menu(),
        disable_web_page_preview=True
    )
    _set_msg_owner(callback.message.message_id, callback.from_user.id)
    await callback.answer()


# ─────────────────────────────────────────────────────────────────────────────
#  Запуск
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    global betting_game

    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("=== БОТ НАЧИНАЕТ ЗАПУСК ===")  # Добавляем print для отладки
    
    try:
        print("Инициализация базы данных...")
        await init_db()
        await import_users_from_json()
        init_leaders_db()
        logging.info("База данных инициализирована")
        
        print("Создание бота...")
        bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        
        # ========== СБРОС ВСЕХ АКТИВНЫХ СЕАНСОВ ==========
        print("Сброс активных сеансов...")
        logging.info("Начинаю сброс активных сеансов...")
        try:
            # 1. Удаляем webhook (если был установлен)
            await bot.delete_webhook(drop_pending_updates=True)
            print("✓ Webhook удален")
            logging.info("✓ Webhook удален")
            
            # 2. Получаем и сбрасываем все ожидающие обновления
            updates = await bot.get_updates(offset=-1, timeout=1)
            if updates:
                last_update_id = updates[-1].update_id
                await bot.get_updates(offset=last_update_id + 1, timeout=1)
                print(f"✓ Сброшено {len(updates)} ожидающих обновлений")
                logging.info(f"✓ Сброшено {len(updates)} ожидающих обновлений")
            else:
                print("✓ Нет ожидающих обновлений")
                logging.info("✓ Нет ожидающих обновлений")
            
            # 3. Очищаем локальные кэши
            _transfer_locks.clear()
            _msg_owners.clear()
            
            print("✅ Все активные сеансы успешно сброшены")
            logging.info("✅ Все активные сеансы успешно сброшены")
        except Exception as e:
            print(f"❌ Ошибка при сбросе сеансов: {e}")
            logging.error(f"Ошибка при сбросе сеансов: {e}")
        # ========== КОНЕЦ СБРОСА ==========
        
        print("Создание диспетчера...")
        dp = Dispatcher(storage=MemoryStorage())
        
        print("Получение информации о боте...")
        bot_info = await bot.get_me()
        os.environ["BOT_USERNAME"] = bot_info.username
        print(f"Бот запущен как @{bot_info.username}")
        logging.info(f"Бот запущен как @{bot_info.username}")
        
        print("Инициализация игр...")
        betting_game = BettingGame(bot)
        
        print("Подключение роутеров...")
        dp.include_router(duels_router)
        dp.include_router(broadcast_router)
        dp.include_router(helper_router)
        dp.include_router(router)
        dp.include_router(mines_router)
        dp.include_router(tower_router)
        dp.include_router(gold_router)
        dp.include_router(referral_router)
        dp.include_router(payment_router)
        dp.include_router(leaders_router)
        dp.include_router(bonus_router)
        
        print("Настройка платежей и рефералов...")
        setup_payments(bot)
        setup_referrals(bot)
        setup_duels(bot, storage)
        setup_bonus(bot)
        _inject_leaders_owner_fns()
        
        print("Запуск watchdog...")
        asyncio.create_task(start_bonus_watchdog())
        
        print("Ожидание 1 секунду...")
        await asyncio.sleep(1)
        
        print("=== ЗАПУСК ПОЛЛИНГА ===")
        logging.info("Бот запущен в режиме поллинга")
        
        # Запускаем поллинг
        await dp.start_polling(bot)
        
    except Exception as e:
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        logging.error(f"Критическая ошибка: {e}", exc_info=True)
        raise
    
    finally:
        print("Закрытие сессии бота...")
        await asyncio.sleep(2)
        await bot.session.close()
        print("Бот остановлен")
        logging.info("Бот остановлен, сессия закрыта.")

if __name__ == "__main__":
    print("=== ЗАПУСК MAIN ===")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n=== БОТ ОСТАНОВЛЕН ПОЛЬЗОВАТЕЛЕМ ===")
    except Exception as e:
        print(f"=== ОШИБКА В MAIN: {e} ===")
        import traceback
        traceback.print_exc()
