import asyncio
from aiogram import Bot
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import logging
import re
from datetime import datetime, timedelta
from typing import Optional, Dict, Tuple

try:
    from database import save_game_result as db_save_game_result, update_balance as db_update_balance
except ImportError:
    async def db_save_game_result(user_id, game_name, score): pass
    async def db_update_balance(user_id, amount): return None

try:
    from referrals import notify_referrer_commission
except ImportError:
    async def notify_referrer_commission(user_id: int, bet_amount: float):
        pass

try:
    from leaders import record_game_result
except ImportError:
    def record_game_result(user_id, name, bet, win):
        pass

logging.basicConfig(level=logging.INFO)

MIN_BET = 0.1
MAX_BET = 10000.0

RATE_LIMIT_SECONDS = 3
user_last_bet_time: Dict[int, datetime] = {}

EMOJI_DICE = "5424972470023104089"
EMOJI_BASKETBALL = "5424972470023104089"
EMOJI_FOOTBALL = "5424972470023104089"
EMOJI_DARTS = "5424972470023104089"
EMOJI_BOWLING = "5424972470023104089"
EMOJI_BACK = "5906771962734057347"
EMOJI_WIN = "5199885118214255386"
EMOJI_LOSE = "5906986955911993888"
EMOJI_BALANCE = "5443127283898405358"
EMOJI_PROFILE = "5906581476639513176"
EMOJI_CHECK = "5197269100878907942"
EMOJI_CROSS = "5906949717859230132"
EMOJI_ARROW_UP = "5906856435426279601"
EMOJI_ARROW_DOWN = "5906856429256319396"
EMOJI_TARGET = "5907049601640308729"
EMOJI_CHET = "5330320040883411678"
EMOJI_NECHET = "5391032818111363540"
EMOJI_MORE = "5449683594425410231"
EMOJI_LESS = "5447183459602669338"
EMOJI_2MORE = "5429651785352501917"
EMOJI_2LESS = "5429518319243775957"
EMOJI_NUMBER = "5456140674028019486"
EMOJI_GOAL = "5206607081334906820"
EMOJI_3POINT = "5397782960512444700"
EMOJI_MISS = "5210952531676504517"

DICE_BET_TYPES = {
    'куб_нечет': {'name': '🎲 Нечетное', 'values': [1, 3, 5], 'multiplier': 1.9},
    'куб_чет': {'name': '🎲 Четное', 'values': [2, 4, 6], 'multiplier': 1.9},
    'куб_мал': {'name': '📉 Меньше (1-3)', 'values': [1, 2, 3], 'multiplier': 1.9},
    'куб_бол': {'name': '📈 Больше (4-6)', 'values': [4, 5, 6], 'multiplier': 1.9},
    'куб_2меньше': {'name': '🎲🎲 Оба меньше 4', 'multiplier': 3.8, 'special': 'double_dice'},
    'куб_2больше': {'name': '🎲🎲 Оба больше 3', 'multiplier': 3.8, 'special': 'double_dice'},
    'куб_1': {'name': '1️⃣', 'values': [1], 'multiplier': 5.7},
    'куб_2': {'name': '2️⃣', 'values': [2], 'multiplier': 5.7},
    'куб_3': {'name': '3️⃣', 'values': [3], 'multiplier': 5.7},
    'куб_4': {'name': '4️⃣', 'values': [4], 'multiplier': 5.7},
    'куб_5': {'name': '5️⃣', 'values': [5], 'multiplier': 5.7},
    'куб_6': {'name': '6️⃣', 'values': [6], 'multiplier': 5.7},
}

BASKETBALL_BET_TYPES = {
    'баскет_гол': {'name': '🏀 Гол (2 очка)', 'values': [4, 5], 'multiplier': 1.85},
    'баскет_мимо': {'name': '🏀 Мимо', 'values': [1, 2, 3], 'multiplier': 1.7},
    'баскет_3очка': {'name': '🏀 3-очковый', 'values': [5], 'multiplier': 5.7},
}

FOOTBALL_BET_TYPES = {
    'футбол_гол': {'name': '⚽ Гол', 'values': [3, 4, 5], 'multiplier': 1.35},
    'футбол_мимо': {'name': '⚽ Мимо', 'values': [1, 2], 'multiplier': 1.75},
}

DART_BET_TYPES = {
    'дартс_белое': {'name': '⚪ Белое', 'values': [3, 5], 'multiplier': 2.35},
    'дартс_красное': {'name': '🔴 Красное', 'values': [2, 4, 6], 'multiplier': 1.9},
    'дартс_мимо': {'name': '❌ Мимо', 'values': [1], 'multiplier': 5.7},
    'дартс_центр': {'name': '🎯 Центр', 'values': [6], 'multiplier': 5.7},
}

BOWLING_BET_TYPES = {
    'боулинг_поражение': {'name': '🎳 Поражение', 'values': [], 'multiplier': 1.8, 'special': 'bowling_vs'},
    'боулинг_победа': {'name': '🎳 Победа', 'values': [], 'multiplier': 1.8, 'special': 'bowling_vs'},
    'боулинг_страйк': {'name': '🎳 Страйк', 'values': [6], 'multiplier': 5.7},
}

COMMAND_MAPPING = {
    'фут': 'футбол',
    'fut': 'футбол',
    'foot': 'футбол',
    'футбол': 'футбол',
    'football': 'футбол',
    
    'баскет': 'баскет',
    'basket': 'баскет',
    'basketball': 'баскет',
    'баскетбол': 'баскет',
    'bask': 'баскет',
    
    'куб': 'куб',
    'dice': 'куб',
    'кубик': 'куб',
    'cube': 'куб',
    
    'дартс': 'дартс',
    'dart': 'дартс',
    'darts': 'дартс',
    'дарт': 'дартс',
    
    'боулинг': 'боулинг',
    'bowling': 'боулинг',
    'боул': 'боулинг',
    'bowl': 'боулинг',
}

BET_TYPE_MAPPING = {
    '3очка': 'баскет_3очка',
    '3points': 'баскет_3очка',
    '3': 'баскет_3очка',
    'три': 'баскет_3очка',
    'three': 'баскет_3очка',
    
    'нечет': 'куб_нечет',
    'odd': 'куб_нечет',
    'нечетное': 'куб_нечет',
    'нечётное': 'куб_нечет',
    
    'чет': 'куб_чет',
    'even': 'куб_чет',
    'четное': 'куб_чет',
    'чётное': 'куб_чет',
    
    'мал': 'куб_мал',
    'small': 'куб_мал',
    'меньше': 'куб_мал',
    'less': 'куб_мал',
    
    'бол': 'куб_бол',
    'big': 'куб_бол',
    'больше': 'куб_бол',
    'more': 'куб_бол',
    
    '2меньше': 'куб_2меньше',
    '2less': 'куб_2меньше',
    '2мал': 'куб_2меньше',
    'обаменьше': 'куб_2меньше',
    'bothless': 'куб_2меньше',
    
    '2больше': 'куб_2больше',
    '2more': 'куб_2больше',
    '2бол': 'куб_2больше',
    'обабольше': 'куб_2больше',
    'bothmore': 'куб_2больше',
    
    '1': 'куб_1',
    '2': 'куб_2',
    '3': 'куб_3',
    '4': 'куб_4',
    '5': 'куб_5',
    '6': 'куб_6',
    
    'белое': 'дартс_белое',
    'white': 'дартс_белое',
    'белый': 'дартс_белое',
    'бел': 'дартс_белое',
    
    'красное': 'дартс_красное',
    'red': 'дартс_красное',
    'красный': 'дартс_красное',
    'крас': 'дартс_красное',
    
    'центр': 'дартс_центр',
    'center': 'дартс_центр',
    'bull': 'дартс_центр',
    
    'победа': 'боулинг_победа',
    'win': 'боулинг_победа',
    'victory': 'боулинг_победа',
    'побед': 'боулинг_победа',
    
    'поражение': 'боулинг_поражение',
    'lose': 'боулинг_поражение',
    'loss': 'боулинг_поражение',
    'пораж': 'боулинг_поражение',
    
    'страйк': 'боулинг_страйк',
    'strike': 'боулинг_страйк',
    'стр': 'боулинг_страйк',
}

class BetStates(StatesGroup):
    waiting_for_amount = State()

class BettingGame:
    def __init__(self, bot: Bot):
        self.bot = bot
        self.pending_bets = {}
        self.active_games = {}
        self.referral_system = None

    @property
    def _storage(self):
        from payments import storage as pay_storage
        return pay_storage

    @property
    def user_balances(self):
        return {uid: d.get('balance', 0.0) for uid, d in self._storage.users.items()}

    def save_balances(self):
        pass

    def get_balance(self, user_id: int) -> float:
        return self._storage.get_balance(user_id)

    def add_balance(self, user_id: int, amount: float) -> float:
        self._storage.add_balance(user_id, amount)
        return self._storage.get_balance(user_id)

    def subtract_balance(self, user_id: int, amount: float) -> bool:
        return self._storage.deduct_balance(user_id, amount)

    def get_bet_config(self, bet_type: str):
        if bet_type.startswith('куб_'):
            return DICE_BET_TYPES.get(bet_type)
        elif bet_type.startswith('баскет_'):
            return BASKETBALL_BET_TYPES.get(bet_type)
        elif bet_type.startswith('футбол_'):
            return FOOTBALL_BET_TYPES.get(bet_type)
        elif bet_type.startswith('дартс_'):
            return DART_BET_TYPES.get(bet_type)
        elif bet_type.startswith('боулинг_'):
            return BOWLING_BET_TYPES.get(bet_type)
        return None

    def set_referral_system(self, referral_system):
        self.referral_system = referral_system
    
    def is_user_in_game(self, user_id: int) -> bool:
        return user_id in self.active_games
    
    def start_game(self, user_id: int):
        self.active_games[user_id] = datetime.now()
    
    def end_game(self, user_id: int):
        if user_id in self.active_games:
            del self.active_games[user_id]


def check_rate_limit(user_id: int) -> Tuple[bool, float]:
    now = datetime.now()
    if user_id in user_last_bet_time:
        time_passed = (now - user_last_bet_time[user_id]).total_seconds()
        if time_passed < RATE_LIMIT_SECONDS:
            return False, RATE_LIMIT_SECONDS - time_passed
    user_last_bet_time[user_id] = now
    return True, 0.0


def parse_bet_command(text: str) -> Optional[Tuple[str, float]]:
    text = text.strip()
    if text.startswith('/'):
        text = text[1:]
    text = text.lower()
    parts = text.split()
    if len(parts) < 3:
        return None
    game = parts[0]
    bet_type_key = parts[1]
    try:
        amount = float(parts[2])
    except (ValueError, IndexError):
        return None
    if amount < MIN_BET or amount > MAX_BET:
        return None
    game_prefix = COMMAND_MAPPING.get(game)
    if not game_prefix:
        return None
    if game_prefix == 'баскет':
        if bet_type_key in ['гол', 'goal']:
            full_bet_type = 'баскет_гол'
        elif bet_type_key in ['мимо', 'miss']:
            full_bet_type = 'баскет_мимо'
        else:
            full_bet_type = BET_TYPE_MAPPING.get(bet_type_key)
    elif game_prefix == 'футбол':
        if bet_type_key in ['гол', 'goal']:
            full_bet_type = 'футбол_гол'
        elif bet_type_key in ['мимо', 'miss']:
            full_bet_type = 'футбол_мимо'
        else:
            full_bet_type = BET_TYPE_MAPPING.get(bet_type_key)
    elif game_prefix == 'дартс':
        if bet_type_key in ['мимо', 'miss']:
            full_bet_type = 'дартс_мимо'
        else:
            full_bet_type = BET_TYPE_MAPPING.get(bet_type_key)
    else:
        full_bet_type = BET_TYPE_MAPPING.get(bet_type_key)
    if not full_bet_type:
        return None
    if not full_bet_type.startswith(game_prefix):
        return None
    return (full_bet_type, amount)


def is_bet_command(text: str) -> bool:
    if not text:
        return False
    text = text.strip().lower()
    if text.startswith('/'):
        text = text[1:]
    parts = text.split()
    if len(parts) < 3:
        return False
    game = parts[0]
    return game in COMMAND_MAPPING


async def _safe_reply(target_message: Message, text: str, parse_mode: str = 'HTML'):
    try:
        await target_message.reply(text, parse_mode=parse_mode)
    except Exception as e:
        logging.warning(f"[safe_reply] Не удалось отправить результат игры: {e}")


async def _delayed_safe_reply(target_message: Message, text: str, delay: float = 3.0, parse_mode: str = 'HTML'):
    await asyncio.sleep(delay)
    await _safe_reply(target_message, text, parse_mode=parse_mode)


def _apply_game_result(
    user_id: int,
    nickname: str,
    amount: float,
    is_win: bool,
    bet_config: dict,
    betting_game: BettingGame,
) -> float:
    if is_win:
        winnings = amount * bet_config['multiplier']
        betting_game.add_balance(user_id, winnings)
        record_game_result(user_id, nickname, amount, winnings)
        asyncio.create_task(db_save_game_result(user_id, 'game', winnings))
        logging.info(
            f"[game] user={user_id} WIN bet={amount} win={winnings:.2f}"
        )
        return winnings
    else:
        record_game_result(user_id, nickname, amount, 0.0)
        asyncio.create_task(db_save_game_result(user_id, 'game', 0.0))
        logging.info(f"[game] user={user_id} LOSE bet={amount}")
        return 0.0


def _build_win_text(nickname: str, winnings: float) -> str:
    return (
        f"<b>{nickname}-Вы выиграли"
        f"<tg-emoji emoji-id=\"5461151367559141950\">🎉</tg-emoji></b>\n\n"
        f"<blockquote><code>{winnings:.2f}</code>"
        f"<tg-emoji emoji-id=\"5197434882321567830\">🎉</tg-emoji> "
        f"Успешно зачислены на баланс!</blockquote>\n"
        f"<blockquote><tg-emoji emoji-id=\"5461151367559141950\">🎉</tg-emoji>"
        f"Поздравляем!</blockquote>"
    )


def _build_lose_text(nickname: str) -> str:
    return (
        f"<b>{nickname}-Вы проиграли"
        f"<tg-emoji emoji-id=\"5422858869372104873\">❌</tg-emoji></b>\n\n"
        f"<blockquote><b><i>Это не повод сдаваться! "
        f"Пробуй снова и снова до победного!</i></b></blockquote>\n"
        f"<blockquote><tg-emoji emoji-id=\"5305699699204837855\">🎉</tg-emoji>"
        f"Желаем удачи!</blockquote>"
    )


async def play_single_dice_game(
    chat_id: int,
    user_id: int,
    nickname: str,
    amount: float,
    bet_type: str,
    bet_config: dict,
    betting_game: BettingGame,
    reply_to_message: Message = None,
):
    if bet_type.startswith('куб_'):
        emoji = "🎲"
    elif bet_type.startswith('баскет_'):
        emoji = "🏀"
    elif bet_type.startswith('футбол_'):
        emoji = "⚽"
    elif bet_type.startswith('дартс_'):
        emoji = "🎯"
    elif bet_type.startswith('боулинг_'):
        emoji = "🎳"
    else:
        emoji = "🎲"

    send_kwargs = {'chat_id': chat_id, 'emoji': emoji}
    if reply_to_message:
        send_kwargs['reply_to_message_id'] = reply_to_message.message_id

    dice_message = await betting_game.bot.send_dice(**send_kwargs)
    dice_value = dice_message.dice.value

    is_win = dice_value in bet_config.get('values', [])
    winnings = _apply_game_result(user_id, nickname, amount, is_win, bet_config, betting_game)

    text = _build_win_text(nickname, winnings) if is_win else _build_lose_text(nickname)
    asyncio.create_task(_delayed_safe_reply(dice_message, text, delay=3.0))


async def play_double_dice_game(
    chat_id: int,
    user_id: int,
    nickname: str,
    amount: float,
    bet_type: str,
    bet_config: dict,
    betting_game: BettingGame,
    reply_to_message: Message = None,
):
    send_kwargs = {'chat_id': chat_id, 'emoji': '🎲'}
    if reply_to_message:
        send_kwargs['reply_to_message_id'] = reply_to_message.message_id

    dice1 = await betting_game.bot.send_dice(**send_kwargs)
    await asyncio.sleep(2)

    dice2 = await betting_game.bot.send_dice(chat_id=chat_id, emoji='🎲')

    dice1_value = dice1.dice.value
    dice2_value = dice2.dice.value

    if bet_type == 'куб_2меньше':
        is_win = dice1_value < 4 and dice2_value < 4
    else:
        is_win = dice1_value > 3 and dice2_value > 3

    winnings = _apply_game_result(user_id, nickname, amount, is_win, bet_config, betting_game)

    text = _build_win_text(nickname, winnings) if is_win else _build_lose_text(nickname)
    asyncio.create_task(_delayed_safe_reply(dice2, text, delay=3.0))


async def play_bowling_vs_game(
    chat_id: int,
    user_id: int,
    nickname: str,
    amount: float,
    bet_type: str,
    bet_config: dict,
    betting_game: BettingGame,
    reply_to_message: Message = None,
):
    send_kwargs = {'chat_id': chat_id, 'emoji': '🎳'}
    if reply_to_message:
        send_kwargs['reply_to_message_id'] = reply_to_message.message_id

    player_roll = await betting_game.bot.send_dice(**send_kwargs)
    await asyncio.sleep(2)
    bot_roll = await betting_game.bot.send_dice(chat_id=chat_id, emoji='🎳')
    await asyncio.sleep(3)

    player_value = player_roll.dice.value
    bot_value = bot_roll.dice.value

    while player_value == bot_value:
        asyncio.create_task(
            _safe_reply(player_roll, "<tg-emoji emoji-id=\"5402186569006210455\">🎉</tg-emoji>Ничья! Переброс...")
        )
        await asyncio.sleep(1)

        player_roll = await betting_game.bot.send_dice(chat_id=chat_id, emoji='🎳')
        await asyncio.sleep(2)
        bot_roll = await betting_game.bot.send_dice(chat_id=chat_id, emoji='🎳')
        await asyncio.sleep(3)

        player_value = player_roll.dice.value
        bot_value = bot_roll.dice.value

    if bet_type == 'боулинг_победа':
        is_win = player_value > bot_value
    elif bet_type == 'боулинг_поражение':
        is_win = player_value < bot_value
    else:
        is_win = False

    winnings = _apply_game_result(user_id, nickname, amount, is_win, bet_config, betting_game)

    if is_win:
        asyncio.create_task(_safe_reply(bot_roll, _build_win_text(nickname, winnings)))
    else:
        asyncio.create_task(_safe_reply(bot_roll, _build_lose_text(nickname)))


async def handle_text_bet_command(message: Message, betting_game: BettingGame):
    user_id = message.from_user.id

    allowed, wait_time = check_rate_limit(user_id)
    if not allowed:
        await message.answer(
            f"⏳ Подождите {wait_time:.1f} сек перед следующей ставкой",
            parse_mode='HTML'
        )
        return

    if betting_game.is_user_in_game(user_id):
        await message.answer("⏳ Дождитесь окончания текущей игры!")
        return

    parsed = parse_bet_command(message.text)
    if not parsed:
        await message.answer(
            "<blockquote>❌<b>Неверный формат команды!</b>\n\n"
            "Используйте /help для уточнения!</blockquote>",
            parse_mode='HTML'
        )
        return

    bet_type, amount = parsed

    balance = betting_game.get_balance(user_id)
    if balance < amount:
        await message.answer(
            f"<blockquote><b><tg-emoji emoji-id=\"5447183459602669338\">❌</tg-emoji> Недостаточно средств!</b></blockquote>\n\n",
            parse_mode='HTML'
        )
        return

    bet_config = betting_game.get_bet_config(bet_type)
    if not bet_config:
        await message.answer("❌ Ошибка конфигурации ставки")
        return

    if not betting_game.subtract_balance(user_id, amount):
        await message.answer("❌ Ошибка при снятии средств")
        return

    asyncio.create_task(notify_referrer_commission(user_id, amount))

    nickname = message.from_user.first_name or ""
    if message.from_user.last_name:
        nickname += f" {message.from_user.last_name}"
    nickname = nickname.strip() or message.from_user.username or "Игрок"

    betting_game.start_game(user_id)

    try:
        if bet_type in ['куб_2меньше', 'куб_2больше']:
            await play_double_dice_game(message.chat.id, user_id, nickname, amount, bet_type, bet_config, betting_game, message)
        elif bet_type.startswith('боулинг_') and bet_config.get('special') == 'bowling_vs':
            await play_bowling_vs_game(message.chat.id, user_id, nickname, amount, bet_type, bet_config, betting_game, message)
        else:
            await play_single_dice_game(message.chat.id, user_id, nickname, amount, bet_type, bet_config, betting_game, message)
    except Exception as e:
        logging.error(f"Ошибка при отправке кубика (до броска): {e}")
        betting_game.add_balance(user_id, amount)
        try:
            await message.answer("❌ Не удалось начать игру. Средства возвращены.")
        except Exception:
            pass
    finally:
        betting_game.end_game(user_id)


async def safe_edit_message(callback: CallbackQuery, text: str, reply_markup=None, parse_mode=None):
    try:
        await callback.message.edit_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
    except Exception as e:
        logging.error(f"Error editing message: {e}")
        try:
            await callback.message.answer(text, parse_mode=parse_mode, reply_markup=reply_markup)
        except Exception:
            pass


async def show_dice_menu(callback: CallbackQuery):
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Нечет (x1.9)", callback_data="bet_dice_куб_нечет", icon_custom_emoji_id=EMOJI_NECHET),
            InlineKeyboardButton(text="Чет (x1.9)", callback_data="bet_dice_куб_чет", icon_custom_emoji_id=EMOJI_CHET)
        ],
        [
            InlineKeyboardButton(text="Меньше (x1.9)", callback_data="bet_dice_куб_мал", icon_custom_emoji_id=EMOJI_LESS),
            InlineKeyboardButton(text="Больше (x1.9)", callback_data="bet_dice_куб_бол", icon_custom_emoji_id=EMOJI_MORE)
        ],
        [
            InlineKeyboardButton(text="2-меньше (x3.8)", callback_data="bet_dice_куб_2меньше", icon_custom_emoji_id=EMOJI_2LESS),
            InlineKeyboardButton(text="2-больше (x3.8)", callback_data="bet_dice_куб_2больше", icon_custom_emoji_id=EMOJI_2MORE)
        ],
        [
            InlineKeyboardButton(text="Точное число (x5.7)", callback_data="bet_dice_exact", icon_custom_emoji_id=EMOJI_NUMBER)
        ],
        [
            InlineKeyboardButton(text="Назад", callback_data="games", icon_custom_emoji_id=EMOJI_BACK)
        ]
    ])
    await safe_edit_message(callback,
        f"<blockquote><b>🎲 Кубик</b></blockquote>\n\n"
        f"<blockquote><b><i>Выберите тип ставки:</i></b></blockquote>\n\n",
        reply_markup=markup, parse_mode='HTML'
    )
    await callback.answer()


async def show_exact_number_menu(callback: CallbackQuery):
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="(x5.7)", callback_data="bet_dice_куб_1", icon_custom_emoji_id="5382322671679708881"),
            InlineKeyboardButton(text="(x5.7)", callback_data="bet_dice_куб_2", icon_custom_emoji_id="5381990043642502553"),
            InlineKeyboardButton(text="(x5.7)", callback_data="bet_dice_куб_3", icon_custom_emoji_id="5381879959335738545")
        ],
        [
            InlineKeyboardButton(text="(x5.7)", callback_data="bet_dice_куб_4", icon_custom_emoji_id="5382054253403577563"),
            InlineKeyboardButton(text="(x5.7)", callback_data="bet_dice_куб_5", icon_custom_emoji_id="5391197405553107640"),
            InlineKeyboardButton(text="(x5.7)", callback_data="bet_dice_куб_6", icon_custom_emoji_id="5390966190283694453")
        ],
        [
            InlineKeyboardButton(text="Назад", callback_data="custom_dice_001", icon_custom_emoji_id=EMOJI_BACK)
        ]
    ])
    await safe_edit_message(callback,
        f"<blockquote><b><tg-emoji emoji-id=\"5456140674028019486\">🎰</tg-emoji> Точное число</b></blockquote>\n\n"
        f"<blockquote><b><i>Выберите число:</i></b></blockquote>",
        reply_markup=markup, parse_mode='HTML'
    )
    await callback.answer()


async def show_basketball_menu(callback: CallbackQuery):
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="3-очковый (x5.7)", callback_data="bet_basketball_баскет_3очка", icon_custom_emoji_id=EMOJI_3POINT)
        ],
        [
            InlineKeyboardButton(text="Гол (x1.85)", callback_data="bet_basketball_баскет_гол", icon_custom_emoji_id=EMOJI_GOAL),
            InlineKeyboardButton(text="Мимо (x1.7)", callback_data="bet_basketball_баскет_мимо", icon_custom_emoji_id=EMOJI_MISS)
        ],
        [
            InlineKeyboardButton(text="Назад", callback_data="games", icon_custom_emoji_id=EMOJI_BACK)
        ]
    ])
    await safe_edit_message(callback,
        f"<blockquote><b>🏀 Баскетбол</b></blockquote>\n\n"
        f"<blockquote><b><i>Выберите исход:</i></b></blockquote>\n\n",
        reply_markup=markup, parse_mode='HTML'
    )
    await callback.answer()


async def show_football_menu(callback: CallbackQuery):
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Гол (x1.35)", callback_data="bet_football_футбол_гол", icon_custom_emoji_id=EMOJI_GOAL),
            InlineKeyboardButton(text="Мимо (x1.75)", callback_data="bet_football_футбол_мимо", icon_custom_emoji_id=EMOJI_MISS)
        ],
        [
            InlineKeyboardButton(text="Назад", callback_data="games", icon_custom_emoji_id=EMOJI_BACK)
        ]
    ])
    await safe_edit_message(callback,
        f"<blockquote><b>⚽ Футбол</b></blockquote>\n\n"
        f"<blockquote><b><i>Выберите исход:</i></b></blockquote>\n\n",
        reply_markup=markup, parse_mode='HTML'
    )
    await callback.answer()


async def show_darts_menu(callback: CallbackQuery):
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="⚪Белое (x2.35)", callback_data="bet_darts_дартс_белое"),
            InlineKeyboardButton(text="🔴Красное (x1.9)", callback_data="bet_darts_дартс_красное")
        ],
        [
            InlineKeyboardButton(text="Центр (x5.7)", callback_data="bet_darts_дартс_центр", icon_custom_emoji_id=EMOJI_3POINT)
        ],
        [
            InlineKeyboardButton(text="Мимо (x5.7)", callback_data="bet_darts_дартс_мимо", icon_custom_emoji_id=EMOJI_MISS)
        ],
        [
            InlineKeyboardButton(text="Назад", callback_data="games", icon_custom_emoji_id=EMOJI_BACK)
        ]
    ])
    await safe_edit_message(callback,
        f"<blockquote><b>🎯 Дартс</b></blockquote>\n\n"
        f"<blockquote><b><i>Выберите исход:</i></b></blockquote>\n\n",
        reply_markup=markup, parse_mode='HTML'
    )
    await callback.answer()


async def show_bowling_menu(callback: CallbackQuery):
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Победа (x1.8)", callback_data="bet_bowling_боулинг_победа", icon_custom_emoji_id=EMOJI_GOAL),
            InlineKeyboardButton(text="Поражение (x1.8)", callback_data="bet_bowling_боулинг_поражение", icon_custom_emoji_id=EMOJI_MISS)
        ],
        [
            InlineKeyboardButton(text="Страйк (x5.7)", callback_data="bet_bowling_боулинг_страйк", icon_custom_emoji_id=EMOJI_3POINT)
        ],
        [
            InlineKeyboardButton(text="Назад", callback_data="games", icon_custom_emoji_id=EMOJI_BACK)
        ]
    ])
    await safe_edit_message(callback,
        f"<blockquote><b>🎳 Боулинг</b></blockquote>\n\n"
        f"<blockquote><b><i>Выберите исход:</i></b></blockquote>\n\n",
        reply_markup=markup, parse_mode='HTML'
    )
    await callback.answer()


async def request_amount(callback: CallbackQuery, state: FSMContext, betting_game: BettingGame):
    bet_type = callback.data.split('_', 2)[2]
    user_id = callback.from_user.id

    allowed, wait_time = check_rate_limit(user_id)
    if not allowed:
        await callback.answer(f"⏳ Подождите {wait_time:.1f} сек", show_alert=True)
        return

    if betting_game.is_user_in_game(user_id):
        await callback.answer("⏳ Дождитесь окончания игры!", show_alert=True)
        return

    betting_game.pending_bets[user_id] = bet_type
    bet_config = betting_game.get_bet_config(bet_type)
    if not bet_config:
        await callback.answer("❌ Ошибка", show_alert=True)
        return

    await state.set_state(BetStates.waiting_for_amount)
    balance = betting_game.get_balance(user_id)

    markup = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Отмена", callback_data="cancel_bet", icon_custom_emoji_id=EMOJI_BACK)
    ]])
    await callback.message.edit_text(
        f"<blockquote><b><tg-emoji emoji-id=\"5197269100878907942\">🎰</tg-emoji> Введите сумму ставки</b></blockquote>\n\n",
        parse_mode='HTML',
        reply_markup=markup
    )
    await callback.answer()


async def process_bet_amount(message: Message, state: FSMContext, betting_game: BettingGame):
    user_id = message.from_user.id

    if user_id not in betting_game.pending_bets:
        await state.clear()
        return

    if betting_game.is_user_in_game(user_id):
        await message.answer("⏳ Дождитесь окончания текущей игры!")
        return

    try:
        amount = float(message.text)

        if amount < MIN_BET:
            await message.answer(f"<tg-emoji emoji-id=\"5447183459602669338\">❌</tg-emoji> Минимальная ставка: {MIN_BET}")
            return

        if amount > MAX_BET:
            await message.answer(f"<tg-emoji emoji-id=\"5447183459602669338\">❌</tg-emoji> Максимальная ставка: {MAX_BET}")
            return

        balance = betting_game.get_balance(user_id)
        if balance < amount:
            await message.answer(
                f"<blockquote><b><tg-emoji emoji-id=\"5447183459602669338\">❌</tg-emoji> Недостаточно средств!</b></blockquote>\n\n",
                parse_mode='HTML'
            )
            if user_id in betting_game.pending_bets:
                del betting_game.pending_bets[user_id]
            await state.clear()
            return

        bet_type = betting_game.pending_bets[user_id]
        bet_config = betting_game.get_bet_config(bet_type)
        if not bet_config:
            await message.answer("❌ Ошибка конфигурации ставки")
            if user_id in betting_game.pending_bets:
                del betting_game.pending_bets[user_id]
            await state.clear()
            return

        if not betting_game.subtract_balance(user_id, amount):
            await message.answer("❌ Ошибка при снятии средств")
            if user_id in betting_game.pending_bets:
                del betting_game.pending_bets[user_id]
            await state.clear()
            return

        asyncio.create_task(notify_referrer_commission(user_id, amount))

        nickname = message.from_user.first_name or ""
        if message.from_user.last_name:
            nickname += f" {message.from_user.last_name}"
        nickname = nickname.strip() or message.from_user.username or "Игрок"

        betting_game.start_game(user_id)

        try:
            if bet_type in ['куб_2меньше', 'куб_2больше']:
                await play_double_dice_game(message.chat.id, user_id, nickname, amount, bet_type, bet_config, betting_game, message)
            elif bet_type.startswith('боулинг_') and bet_config.get('special') == 'bowling_vs':
                await play_bowling_vs_game(message.chat.id, user_id, nickname, amount, bet_type, bet_config, betting_game, message)
            else:
                await play_single_dice_game(message.chat.id, user_id, nickname, amount, bet_type, bet_config, betting_game, message)
        except Exception as e:
            logging.error(f"Ошибка при отправке кубика (до броска): {e}")
            betting_game.add_balance(user_id, amount)
            try:
                await message.answer("❌ Не удалось начать игру. Средства возвращены.")
            except Exception:
                pass
        finally:
            if user_id in betting_game.pending_bets:
                del betting_game.pending_bets[user_id]
            await state.clear()
            betting_game.end_game(user_id)

    except ValueError:
        await message.answer("❌ Введите корректное число")
    except Exception as e:
        logging.error(f"Error: {e}")
        await message.answer("❌ Произошла ошибка")
        if user_id in betting_game.pending_bets:
            del betting_game.pending_bets[user_id]
        await state.clear()


async def cancel_bet(callback: CallbackQuery, state: FSMContext, betting_game: BettingGame):
    user_id = callback.from_user.id
    if user_id in betting_game.pending_bets:
        del betting_game.pending_bets[user_id]
    await state.clear()

    from main import games_callback
    await games_callback(callback, state)
