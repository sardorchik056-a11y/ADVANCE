import json
import logging
import os
import asyncio
from datetime import datetime
from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.enums import ParseMode

try:
    from database import save_referral_commission, save_referral_withdrawal, register_referral as db_register_referral
except ImportError:
    async def save_referral_commission(referrer_id, referral_id, amount): pass
    async def save_referral_withdrawal(user_id, amount): pass
    async def db_register_referral(new_user_id, referrer_id): pass

REFERRAL_PERCENT   = 2
MIN_REF_WITHDRAWAL = 1.0
REFERRALS_FILE     = "referrals.json"

EMOJI_BACK       = "5233735937317447077"

EMOJI_INVITED       = "5224351111653139458"  # 📈 — Приглашено / текст про проценты
EMOJI_REF_BALANCE   = "5224380154221995303"  # 💰 — Реф-баланс
EMOJI_EARNED        = "5224393683368978372"  # 💵 — Заработано
EMOJI_WITHDRAWN_TXT = "5224643573156195838"  # 💎 — Выведено
EMOJI_LINK          = "5226832640677551956"  # 🔗 — Ваша ссылка / кнопка "Моя ссылка"
EMOJI_STATS_BTN     = "5233410275717198826"  # 📉 — кнопка "Статистика"
EMOJI_WALLET_BTN    = "5224350849660138496"  # 👛 — кнопка "Вывести"
EMOJI_TOTAL_REFS    = "5226704646357165977"  # 👥️ — всего рефералов / партнёры
EMOJI_EDIT          = "5226595189115625543"  # ✏️ — "Введите сумму"
EMOJI_COIN          = "5116648080787112958"  # 💰


class ReferralWithdraw(StatesGroup):
    entering_amount = State()


class ReferralStorage:
    def __init__(self, filepath: str = REFERRALS_FILE):
        self.filepath = filepath
        self._data: dict = {}
        self._load()

    def _load(self):
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, "r", encoding="utf-8") as f:
                    self._data = json.load(f)
            except Exception as ex:
                logging.error(f"[ReferralStorage] Ошибка загрузки: {ex}")
                self._data = {}

    def _save(self):
        try:
            with open(self.filepath, "w", encoding="utf-8") as f:
                json.dump(self._data, f, ensure_ascii=False, indent=2)
        except Exception as ex:
            logging.error(f"[ReferralStorage] Ошибка сохранения: {ex}")

    def _get(self, user_id: int) -> dict:
        key = str(user_id)
        if key not in self._data:
            self._data[key] = {
                "referrer_id":     None,
                "referrals":       [],
                "ref_balance":     0.0,
                "total_earned":    0.0,
                "total_withdrawn": 0.0,
                "join_date":       datetime.now().strftime("%Y-%m-%d"),
                "joined_organically": False,
            }
            self._save()
        return self._data[key]

    def mark_organic(self, user_id: int):
        key = str(user_id)
        if key not in self._data:
            self._data[key] = {
                "referrer_id":        None,
                "referrals":          [],
                "ref_balance":        0.0,
                "total_earned":       0.0,
                "total_withdrawn":    0.0,
                "join_date":          datetime.now().strftime("%Y-%m-%d"),
                "joined_organically": True,
            }
            self._save()
            logging.info(f"[Referral] {user_id} пришёл без реф-ссылки → заблокирован от реф-системы")

    def register_referral(self, new_user_id: int, referrer_id: int) -> bool:
        if new_user_id == referrer_id:
            logging.info(f"[Referral] {new_user_id} попытался стать рефералом самого себя")
            return False

        key = str(new_user_id)

        if key in self._data:
            record = self._data[key]

            if record.get("referrer_id") is not None:
                logging.info(f"[Referral] {new_user_id} уже является рефералом {record['referrer_id']}")
                return False


        referrer_key = str(referrer_id)
        if referrer_key not in self._data:
            self._data[referrer_key] = {
                "referrer_id":        None,
                "referrals":          [],
                "ref_balance":        0.0,
                "total_earned":       0.0,
                "total_withdrawn":    0.0,
                "join_date":          datetime.now().strftime("%Y-%m-%d"),
                "joined_organically": False,
            }
            logging.info(f"[Referral] Реферер {referrer_id} создан автоматически в базе")

        referrer_record = self._data[referrer_key]

        if new_user_id in referrer_record["referrals"]:
            logging.info(f"[Referral] {new_user_id} уже в списке рефералов {referrer_id}")
            return False

        record = self._get(new_user_id)
        record["referrer_id"]        = referrer_id
        record["joined_organically"] = False
        referrer_record["referrals"].append(new_user_id)
        self._save()
        logging.info(f"[Referral] {new_user_id} → реферал {referrer_id} ✅")
        return True

    def accrue_commission(self, referral_user_id: int, bet_amount: float) -> float:
        record = self._get(referral_user_id)
        referrer_id = record["referrer_id"]
        if referrer_id is None:
            return 0.0
        commission = round(bet_amount * REFERRAL_PERCENT / 100, 4)
        ref_record = self._get(referrer_id)
        ref_record["ref_balance"]  = round(ref_record["ref_balance"]  + commission, 4)
        ref_record["total_earned"] = round(ref_record["total_earned"] + commission, 4)
        self._save()
        logging.info(f"[Referral] +{commission} USDT → {referrer_id} (ставка {referral_user_id})")
        return commission

    def get_ref_balance(self, user_id: int) -> float:
        return self._get(user_id)["ref_balance"]

    def get_stats(self, user_id: int) -> dict:
        r = self._get(user_id)
        return {
            "referrals_count": len(r["referrals"]),
            "referrals_list":  r["referrals"],
            "ref_balance":     r["ref_balance"],
            "total_earned":    r["total_earned"],
            "total_withdrawn": r["total_withdrawn"],
        }

    def withdraw_ref_balance(self, user_id: int, amount: float) -> bool:
        record = self._get(user_id)
        if record["ref_balance"] < amount:
            return False
        record["ref_balance"]     = round(record["ref_balance"]     - amount, 4)
        record["total_withdrawn"] = round(record["total_withdrawn"] + amount, 4)
        self._save()
        return True

    def get_referrer_id(self, user_id: int):
        return self._get(user_id)["referrer_id"]


referral_storage = ReferralStorage()
_bot = None

def _noop_set_owner(message_id: int, user_id: int): pass
def _noop_is_owner(message_id: int, user_id: int) -> bool: return True
set_owner_fn = _noop_set_owner
is_owner_fn  = _noop_is_owner


def setup_referrals(bot: Bot):
    global _bot
    _bot = bot


def get_referral_link(user_id: int) -> str:
    bot_username = os.getenv("BOT_USERNAME", "YourBotUsername")
    return f"https://t.me/{bot_username}?start=ref_{user_id}"


def e(eid: str, fallback: str = "•") -> str:
    return f'<tg-emoji emoji-id="{eid}">{fallback}</tg-emoji>'


def kb_referrals_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="Статистика",
                callback_data="ref_stats",
                icon_custom_emoji_id=EMOJI_STATS_BTN
            ),
            InlineKeyboardButton(
                text="Вывести",
                callback_data="ref_withdraw",
                icon_custom_emoji_id=EMOJI_WALLET_BTN
            ),
        ],
        [
            InlineKeyboardButton(
                text="Моя ссылка",
                callback_data="ref_link",
                icon_custom_emoji_id=EMOJI_LINK
            ),
        ],
    ])


def kb_ref_back() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[])


def kb_ref_cancel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="Отмена",
            callback_data="referrals",
            icon_custom_emoji_id=EMOJI_BACK
        )
    ]])


def text_referrals_main(user_id: int) -> str:
    stats = referral_storage.get_stats(user_id)
    link  = get_referral_link(user_id)

    cnt = stats["referrals_count"]
    if 11 <= cnt % 100 <= 19:
        ref_word = "рефералов"
    elif cnt % 10 == 1:
        ref_word = "реферал"
    elif cnt % 10 in (2, 3, 4):
        ref_word = "реферала"
    else:
        ref_word = "рефералов"

    return (
        f"{e(EMOJI_TOTAL_REFS,'👥️')} <b>Реферальная программа</b>\n\n"
        f"<blockquote>"
        f"{e(EMOJI_INVITED,'📈')}<b>Приглашено:</b> <code>{cnt} {ref_word}</code>\n"
        f"{e(EMOJI_REF_BALANCE,'💰')}<b>Реф-баланс:</b> <code>{stats['ref_balance']:.4f}</code> "
        f"{e(EMOJI_COIN,'💰')}\n"
        f"{e(EMOJI_EARNED,'💵')}<b>Заработано:</b> <code>{stats['total_earned']:.4f}</code> "
        f"{e(EMOJI_COIN,'💰')}\n"
        f"{e(EMOJI_WITHDRAWN_TXT,'💎')} <b>Выведено:</b> <code>{stats['total_withdrawn']:.4f}</code> "
        f"{e(EMOJI_COIN,'💰')}\n"
        f"</blockquote>\n\n"
        f"<blockquote>"
        f"{e(EMOJI_INVITED,'📈')}<b>Получайте 2% от выигрышей друзей!</b>\n"
        f"</blockquote>\n\n"
        f"<blockquote>"
        f"{e(EMOJI_LINK,'🔗')}<b>Ваша ссылка:</b>\n"
        f"<code>{link}</code>"
        f"</blockquote>"
    )


def text_ref_stats(user_id: int) -> str:
    stats = referral_storage.get_stats(user_id)
    refs  = stats["referrals_list"]

    last_5 = list(reversed(refs[-5:])) if refs else []
    lines = [
        f"{e(EMOJI_TOTAL_REFS,'👥️')} <code>{uid}</code>"
        for uid in last_5
    ]
    refs_block = "\n".join(lines) if lines else "  <i>Рефералов пока нет</i>"
    more = f"\n{e(EMOJI_STATS_BTN,'📉')} <i>... и ещё {len(refs) - 5}</i>" if len(refs) > 5 else ""

    return (
        f"{e(EMOJI_STATS_BTN,'📉')} <b>Детальная статистика</b>\n\n"
        f"<blockquote>"
        f"{e(EMOJI_REF_BALANCE,'💰')}Реф-баланс: <code>{stats['ref_balance']:.4f}</code>\n"
        f"{e(EMOJI_EARNED,'💵')}Заработано: <code>{stats['total_earned']:.4f}</code>\n"
        f"{e(EMOJI_WITHDRAWN_TXT,'💎')}Выведено: <code>{stats['total_withdrawn']:.4f}</code>\n"
        f"{e(EMOJI_TOTAL_REFS,'👥️')}Всего рефералов: <code>{stats['referrals_count']}</code>\n"
        f"</blockquote>\n\n"
        f"<blockquote>"
        f"<b>Последние рефералы:</b>\n"
        f"{refs_block}{more}"
        f"</blockquote>"
    )


def text_ref_link(user_id: int) -> str:
    link = get_referral_link(user_id)
    return (
        f"<blockquote>{e(EMOJI_LINK,'🔗')}<b>Реферальная ссылка</b></blockquote>\n\n"
        f"<blockquote><code>{link}</code></blockquote>"
    )


referral_router = Router()


@referral_router.callback_query(F.data == "referrals")
async def referrals_main(callback: CallbackQuery, state: FSMContext):
    if not is_owner_fn(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True)
        return
    await state.clear()
    await callback.message.edit_text(
        text_referrals_main(callback.from_user.id),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_referrals_main()
    )
    set_owner_fn(callback.message.message_id, callback.from_user.id)
    await callback.answer()


@referral_router.callback_query(F.data == "ref_stats")
async def ref_stats(callback: CallbackQuery, state: FSMContext):
    if not is_owner_fn(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True)
        return
    await state.clear()
    await callback.message.edit_text(
        text_ref_stats(callback.from_user.id),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_ref_back()
    )
    set_owner_fn(callback.message.message_id, callback.from_user.id)
    await callback.answer()


@referral_router.callback_query(F.data == "ref_link")
async def ref_link(callback: CallbackQuery, state: FSMContext):
    if not is_owner_fn(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True)
        return
    await state.clear()
    await callback.message.edit_text(
        text_ref_link(callback.from_user.id),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_ref_back()
    )
    set_owner_fn(callback.message.message_id, callback.from_user.id)
    await callback.answer()


@referral_router.callback_query(F.data == "ref_withdraw")
async def ref_withdraw_start(callback: CallbackQuery, state: FSMContext):
    if not is_owner_fn(callback.message.message_id, callback.from_user.id):
        await callback.answer("🚫 Это не ваша кнопка!", show_alert=True)
        return
    ref_balance = referral_storage.get_ref_balance(callback.from_user.id)

    await state.set_state(ReferralWithdraw.entering_amount)
    await callback.message.edit_text(
        f"{e(EMOJI_WALLET_BTN,'👛')} <b>Вывод реферального баланса</b>\n\n"
        f"<blockquote><i>{e(EMOJI_EDIT,'✏️')}Введите сумму для вывода:</i></blockquote>",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_ref_cancel()
    )
    set_owner_fn(callback.message.message_id, callback.from_user.id)
    await callback.answer()


async def ref_withdraw_amount(message: Message, state: FSMContext):
    try:
        amount = float(message.text.replace(",", ".").strip())
    except ValueError:
        await message.answer(
            "❌ <b>Неверный формат.</b> Введите число, например: <code>5.00</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_ref_cancel()
        )
        return

    if amount < MIN_REF_WITHDRAWAL:
        await message.answer(
            f"❌ <b>Минимальная сумма:</b> <code>{MIN_REF_WITHDRAWAL:.2f}</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_ref_cancel()
        )
        return

    ref_balance = referral_storage.get_ref_balance(message.from_user.id)
    if amount > ref_balance:
        await message.answer(
            f"❌ <b>Недостаточно средств.</b>\n"
            f"Реф-баланс: <code>{ref_balance:.4f}</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_ref_cancel()
        )
        return

    success = referral_storage.withdraw_ref_balance(message.from_user.id, amount)
    if not success:
        await message.answer(
            "❌ Ошибка при выводе. Попробуйте позже.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_ref_cancel()
        )
        return

    asyncio.create_task(save_referral_withdrawal(message.from_user.id, amount))

    try:
        from payments import storage as pay_storage
        pay_storage.add_balance(message.from_user.id, amount)
        new_pay_balance = pay_storage.get_balance(message.from_user.id)
        try:
            from main import betting_game
            if betting_game:
                betting_game.user_balances[message.from_user.id] = new_pay_balance
                betting_game.save_balances()
        except Exception:
            pass
    except Exception as ex:
        logging.error(f"[Referral] Ошибка зачисления: {ex}")

    await state.clear()
    new_ref_balance = referral_storage.get_ref_balance(message.from_user.id)

    await message.answer(
        f"{e(EMOJI_WALLET_BTN,'👛')}<b>Успешно выведено!</b>\n\n",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_ref_back()
    )
    logging.info(f"[Referral] {message.from_user.id} вывел {amount} USDT с реф-баланса")


@referral_router.message(ReferralWithdraw.entering_amount, F.text)
async def ref_withdraw_amount_handler(message: Message, state: FSMContext):
    await ref_withdraw_amount(message, state)


async def notify_referrer_commission(referral_user_id: int, bet_amount: float):
    commission = referral_storage.accrue_commission(referral_user_id, bet_amount)
    if commission > 0:
        logging.info(f"[Referral] Комиссия {commission} USDT начислена тихо рефереру")
        referrer_id = referral_storage.get_referrer_id(referral_user_id)
        if referrer_id:
            asyncio.create_task(save_referral_commission(referrer_id, referral_user_id, commission))


async def process_start_referral(message: Message, start_param: str) -> bool:
    if not start_param.startswith("ref_"):
        return False
    try:
        referrer_id = int(start_param[4:])
    except ValueError:
        return False

    new_user_id = message.from_user.id
    registered  = referral_storage.register_referral(new_user_id, referrer_id)

    if registered:
        asyncio.create_task(db_register_referral(new_user_id, referrer_id))

    if registered and _bot is not None:
        try:
            await _bot.send_message(
                chat_id=referrer_id,
                text=(
                    f"<blockquote>{e(EMOJI_TOTAL_REFS,'👥️')}<b>Новый реферал!</b></blockquote>\n\n"
                ),
                parse_mode=ParseMode.HTML
            )
        except Exception as ex:
            logging.warning(f"[Referral] Не удалось уведомить {referrer_id}: {ex}")

    return registered
