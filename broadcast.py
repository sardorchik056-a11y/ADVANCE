
import asyncio
import logging
from aiogram import Router, F, Bot
from aiogram.types import Message
from aiogram.enums import ParseMode

try:
    from database import db_get_all_user_ids
except ImportError:
    def db_get_all_user_ids() -> list:
        return []

BROADCAST_DELAY = 0.05

FLOOD_WAIT_EXTRA = 2

ADMIN_IDS = [8118184388, 8158265201]

broadcast_router = Router()

_active_broadcast: asyncio.Task | None = None


async def _do_broadcast(bot: Bot, admin_id: int, text: str):
    """Внутренняя корутина рассылки."""
    global _active_broadcast

    user_ids = db_get_all_user_ids()
    total    = len(user_ids)

    if total == 0:
        await bot.send_message(
            admin_id,
            "<blockquote>⚠️ <b>Нет пользователей в базе.</b></blockquote>",
            parse_mode=ParseMode.HTML
        )
        return

    await bot.send_message(
        admin_id,
        f"<blockquote>📣 <b>Рассылка начата</b>\n\n"
        f"👥 Получателей: <code>{total}</code>\n"
        f"⏳ Примерное время: ~<code>{int(total * BROADCAST_DELAY)}</code> сек</blockquote>",
        parse_mode=ParseMode.HTML
    )

    sent      = 0
    blocked   = 0
    errors    = 0
    cancelled = False

    for user_id in user_ids:
        if asyncio.current_task().cancelled():
            cancelled = True
            break

        try:
            await bot.send_message(
                user_id,
                text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
            sent += 1

        except Exception as e:
            err_str = str(e).lower()

            if "blocked" in err_str or "user is deactivated" in err_str or "chat not found" in err_str:
                blocked += 1
            elif "flood" in err_str or "too many requests" in err_str:
                # Flood wait — пауза и повторная попытка
                try:
                    wait_time = int(''.join(filter(str.isdigit, err_str))) + FLOOD_WAIT_EXTRA
                except Exception:
                    wait_time = 30
                logging.warning(f"[Broadcast] FloodWait {wait_time}s")
                await asyncio.sleep(wait_time)
                try:
                    await bot.send_message(
                        user_id,
                        text,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True
                    )
                    sent += 1
                except Exception:
                    errors += 1
            else:
                errors += 1
                logging.warning(f"[Broadcast] user_id={user_id}: {e}")

        await asyncio.sleep(BROADCAST_DELAY)

    status = "⛔️ Отменена" if cancelled else "✅ Завершена"
    await bot.send_message(
        admin_id,
        f"<blockquote>📊 <b>Рассылка {status}</b>\n\n"
        f"✅ Отправлено:    <code>{sent}</code>\n"
        f"🚫 Заблокировали: <code>{blocked}</code>\n"
        f"❌ Ошибок:        <code>{errors}</code>\n"
        f"👥 Всего:         <code>{total}</code></blockquote>",
        parse_mode=ParseMode.HTML
    )

    _active_broadcast = None


@broadcast_router.message(F.text.startswith("/reck"))
async def cmd_reck(message: Message):
    global _active_broadcast

    if message.from_user.id not in ADMIN_IDS:
        return

    text_raw = message.text.strip()
    if text_raw.lower() in ("/reck cancel", "/reck отмена"):
        if _active_broadcast and not _active_broadcast.done():
            _active_broadcast.cancel()
            await message.answer(
                "<blockquote>⛔️ <b>Рассылка отменена.</b></blockquote>",
                parse_mode=ParseMode.HTML
            )
        else:
            await message.answer(
                "<blockquote>ℹ️ Нет активной рассылки.</blockquote>",
                parse_mode=ParseMode.HTML
            )
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer(
            "<b>⚙️ Использование:</b>\n"
            "<code>/reck [текст сообщения]</code>\n\n"
            "<blockquote>"
            "Поддерживается HTML-форматирование:\n"
            "<code>&lt;b&gt;жирный&lt;/b&gt;</code>\n"
            "<code>&lt;i&gt;курсив&lt;/i&gt;</code>\n"
            "<code>&lt;blockquote&gt;цитата&lt;/blockquote&gt;</code>\n"
            "<code>&lt;tg-emoji emoji-id=\"ID\"&gt;🎰&lt;/tg-emoji&gt;</code>\n\n"
            "Отмена: <code>/reck cancel</code>"
            "</blockquote>",
            parse_mode=ParseMode.HTML
        )
        return

    broadcast_text = parts[1].strip()

    if _active_broadcast and not _active_broadcast.done():
        await message.answer(
            "<blockquote>⚠️ <b>Рассылка уже идёт!</b>\n\n"
            "Отменить: <code>/reck cancel</code></blockquote>",
            parse_mode=ParseMode.HTML
        )
        return

    await message.answer(
        "<blockquote>👁 <b>Предпросмотр сообщения:</b></blockquote>",
        parse_mode=ParseMode.HTML
    )
    try:
        await message.answer(broadcast_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except Exception as e:
        await message.answer(
            f"<blockquote>❌ <b>Ошибка в тексте сообщения:</b>\n<code>{e}</code>\n\n"
            f"Проверьте HTML-разметку.</blockquote>",
            parse_mode=ParseMode.HTML
        )
        return

    _active_broadcast = asyncio.create_task(
        _do_broadcast(message.bot, message.from_user.id, broadcast_text)
    )
