from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from app.bot.ui_models import BotAction, BotKeyboard, BotMessage
from app.channels.evolution_adapter import send_evolution_message
from app.bot.parser import escape_html, format_currency
from app.bot.recurring_flow import compute_next_due, get_today
from app.core.config import Settings
from app.core.logging import logger
from app.services.evolution import EvolutionClient
from app.services.repositories import DataRepo


def _today_for_timezone(tz_name: str) -> date:
    try:
        return datetime.now(ZoneInfo(tz_name)).date()
    except (ZoneInfoNotFoundError, ValueError):
        return datetime.now(ZoneInfo("America/Bogota")).date()


def _hour_for_timezone(tz_name: str) -> int:
    try:
        return datetime.now(ZoneInfo(tz_name)).hour
    except (ZoneInfoNotFoundError, ValueError):
        return datetime.now(ZoneInfo("America/Bogota")).hour


def _parse_reminder_hour(value: Any, default: int = 9) -> int:
    try:
        hour = int(value)
    except (TypeError, ValueError):
        return default
    if 0 <= hour <= 23:
        return hour
    return default


def _action_rows(actions: Iterable[tuple[str, str]], row_size: int = 3) -> list[list[tuple[str, str]]]:
    data = list(actions)
    size = max(1, int(row_size))
    return [data[i : i + size] for i in range(0, len(data), size)]


def _build_keyboard(actions: Iterable[tuple[str, str]], row_size: int = 3) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=label, callback_data=action_id) for action_id, label in row]
        for row in _action_rows(actions, row_size=row_size)
    ]
    return InlineKeyboardMarkup(rows)


def _build_bot_message(text: str, actions: Iterable[tuple[str, str]], row_size: int = 3) -> BotMessage:
    rows = [[BotAction(action_id, label) for action_id, label in row] for row in _action_rows(actions, row_size=row_size)]
    return BotMessage(text=text, keyboard=BotKeyboard(rows=rows))


def _reminder_text(recurring: Dict[str, Any], due_date: date, offset: int) -> str:
    raw_amount = recurring.get("amount")
    amount = "Por definir"
    try:
        if raw_amount is not None and float(raw_amount) > 0:
            amount = format_currency(float(raw_amount), str(recurring.get("currency", "COP")))
    except (TypeError, ValueError):
        amount = "Por definir"

    service_name = recurring.get("service_name") or recurring.get("normalized_merchant") or recurring.get("description") or "Pago recurrente"
    link = recurring.get("payment_link") or "—"
    reference = recurring.get("payment_reference") or "—"
    when = "hoy" if offset == 0 else f"en {offset} día(s)"
    return (
        "⏰ <b>Recordatorio de pago</b>\n"
        f"<b>Vence:</b> <code>{due_date.isoformat()}</code> ({escape_html(when)})\n"
        f"<b>Servicio:</b> {escape_html(str(service_name))}\n"
        f"<b>Monto:</b> {amount}\n"
        f"<b>Referencia:</b> {escape_html(str(reference))}\n"
        f"<b>Enlace:</b> {escape_html(str(link))}\n\n"
        "¿Ya realizaste el pago?"
    )


def _daily_expense_nudge_text() -> str:
    return (
        "🧾 <b>Recordatorio diario</b>\n"
        "Hoy no veo movimientos registrados.\n\n"
        "Si ya gastaste algo, escríbelo en lenguaje natural.\n"
        "Ejemplo: <code>almuerzo 18000</code>\n\n"
        "También puedes usar el menú para ver ejemplos, ayuda o tu resumen."
    )


def _daily_nudge_prefs(repo: DataRepo, user_id: str) -> tuple[bool, int]:
    pending = repo.get_pending_action(user_id, "daily_nudge_prefs")
    state: Dict[str, Any] = {}
    if pending:
        raw = pending.get("state") or {}
        if isinstance(raw, str):
            try:
                raw = __import__("json").loads(raw)
            except Exception:
                raw = {}
        if isinstance(raw, dict):
            state = raw
    enabled = bool(state.get("enabled", True))
    hour = _parse_reminder_hour(state.get("hour"), default=21)
    return enabled, hour


def _extract_anchor_date(recurring: Dict[str, Any]) -> Optional[date]:
    value = recurring.get("anchor_date")
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value:
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None


def _extract_next_due(recurring: Dict[str, Any]) -> Optional[date]:
    value = recurring.get("next_due")
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value:
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None


def _extract_offsets(recurring: Dict[str, Any]) -> list[int]:
    offsets = recurring.get("remind_offsets") or []
    if isinstance(offsets, list):
        values = []
        for item in offsets:
            try:
                values.append(int(item))
            except (TypeError, ValueError):
                continue
        return values
    if isinstance(offsets, str):
        try:
            data = __import__("json").loads(offsets)
            if isinstance(data, list):
                return [int(item) for item in data if str(item).isdigit()]
        except Exception:
            return []
    return []


async def _process_daily_expense_nudges(
    repo: DataRepo,
    bot,
    settings: Settings,
    evolution_client: Optional[EvolutionClient],
) -> None:
    scheduler_tz = str(settings.timezone or "America/Bogota")
    current_hour = _hour_for_timezone(scheduler_tz)
    today = _today_for_timezone(scheduler_tz)
    prompt_text = _daily_expense_nudge_text()
    actions = [
        ("dailynudge:examples", "✍️ Ejemplos"),
        ("/help", "ℹ️ Ayuda"),
        ("/list", "🧾 Ver movimientos"),
        ("/summary", "📊 Resumen"),
        ("dailynudge:set_hour", "🕖 Cambiar hora"),
        ("dailynudge:silence", "🔕 Silenciar"),
    ]

    channel_map: Dict[str, Dict[str, str]] = {}
    for item in repo.list_active_users_with_chat("telegram"):
        user_id = str(item.get("user_id") or "")
        chat_id = str(item.get("chat_id") or "")
        if user_id and chat_id:
            channel_map.setdefault(user_id, {})["telegram"] = chat_id
    for item in repo.list_active_users_with_chat("evolution"):
        user_id = str(item.get("user_id") or "")
        chat_id = str(item.get("chat_id") or "")
        if user_id and chat_id:
            channel_map.setdefault(user_id, {})["evolution"] = chat_id

    for user_id, channels in channel_map.items():
        try:
            enabled, preferred_hour = _daily_nudge_prefs(repo, user_id)
            if not enabled:
                continue
            if preferred_hour != current_hour:
                continue

            action_type = f"daily_nudge_{today.strftime('%Y%m%d')}"
            if repo.get_pending_action(user_id, action_type):
                continue
            if repo.has_expense_for_date(user_id, today.isoformat()):
                continue

            delivered = False
            telegram_chat = channels.get("telegram")
            if telegram_chat:
                try:
                    await bot.send_message(
                        chat_id=int(telegram_chat),
                        text=prompt_text,
                        parse_mode="HTML",
                        disable_web_page_preview=True,
                        reply_markup=_build_keyboard(actions, row_size=2),
                    )
                    delivered = True
                except Exception as exc:
                    logger.warning("Daily nudge telegram send failed user_id=%s error=%s", user_id, exc)

            evolution_chat = channels.get("evolution")
            if evolution_client and evolution_chat:
                try:
                    await send_evolution_message(
                        evolution_client,
                        str(evolution_chat),
                        _build_bot_message(prompt_text, actions, row_size=2),
                    )
                    delivered = True
                except Exception as exc:
                    logger.warning("Daily nudge evolution send failed user_id=%s error=%s", user_id, exc)

            if delivered:
                repo.upsert_pending_action(
                    user_id,
                    action_type,
                    {"kind": "daily_expense_nudge", "date": today.isoformat()},
                    expires_at=(datetime.now(timezone.utc) + timedelta(days=2)).isoformat(),
                )
        except Exception as exc:
            logger.warning("Daily expense nudge failed user_id=%s error=%s", user_id, exc)


async def process_recurring_reminders(
    repo: DataRepo,
    bot,
    settings: Settings,
    evolution_client: Optional[EvolutionClient] = None,
) -> None:
    await _process_daily_expense_nudges(repo, bot, settings, evolution_client)

    today = get_today(settings)
    repo.mark_overdue_bill_instances(today.isoformat())
    follow_ups = repo.list_due_follow_up_bill_instances(today.isoformat())
    for bill in follow_ups:
        try:
            tz_name = str(bill.get("timezone") or settings.timezone or "America/Bogota")
            if _hour_for_timezone(tz_name) != _parse_reminder_hour(bill.get("reminder_hour"), default=9):
                continue
            reminder_id = repo.create_bill_reminder_if_missing(
                int(bill["id"]),
                -1,
                today.isoformat(),
            )
            if not reminder_id:
                continue
            chat_id = repo.get_user_chat_id(str(bill["user_id"]), "telegram")
            recurring = {
                "amount": bill.get("amount"),
                "currency": bill.get("currency"),
                "service_name": bill.get("service_name"),
                "payment_link": bill.get("payment_link") or bill.get("recurring_payment_link"),
                "payment_reference": bill.get("reference_number") or bill.get("recurring_payment_reference"),
            }
            due_date = bill.get("due_date")
            due = due_date if isinstance(due_date, date) else date.fromisoformat(str(due_date))
            actions = [
                (f"recurring:paid:{bill['id']}", "✅ Sí"),
                (f"recurring:later:{bill['id']}", "⏳ Después"),
                (f"recurring:no:{bill['id']}", "❌ No"),
            ]
            text = _reminder_text(recurring, due, 0)
            delivered = False

            if chat_id:
                try:
                    await bot.send_message(
                        chat_id=int(chat_id),
                        text=text,
                        parse_mode="HTML",
                        disable_web_page_preview=True,
                        reply_markup=_build_keyboard(actions),
                    )
                    delivered = True
                except Exception as exc:
                    logger.warning("Recurring follow-up telegram send failed: %s", exc)

            if evolution_client:
                evolution_chat_id = repo.get_user_chat_id(str(bill["user_id"]), "evolution")
                if evolution_chat_id:
                    try:
                        await send_evolution_message(
                            evolution_client,
                            str(evolution_chat_id),
                            _build_bot_message(text, actions),
                        )
                        delivered = True
                    except Exception as exc:
                        logger.warning("Recurring follow-up evolution send failed: %s", exc)

            if delivered:
                repo.update_bill_instance(int(bill["id"]), {"follow_up_on": None})
                repo.update_bill_reminder(
                    int(reminder_id),
                    {"status": "sent", "sent_at": datetime.now(timezone.utc).isoformat()},
                )
        except Exception as exc:
            logger.warning("Recurring follow-up reminder failed: %s", exc)

    recurring_expenses = repo.list_active_recurring_expenses()
    for recurring in recurring_expenses:
        try:
            next_due = _extract_next_due(recurring)
            anchor_date = _extract_anchor_date(recurring)
            recurrence = str(recurring.get("recurrence") or "monthly").lower()
            billing_day = recurring.get("billing_day")
            billing_weekday = recurring.get("billing_weekday")
            billing_month = recurring.get("billing_month")
            tz_name = str(recurring.get("timezone") or settings.timezone or "America/Bogota")
            local_today = _today_for_timezone(tz_name)
            if _hour_for_timezone(tz_name) != _parse_reminder_hour(recurring.get("reminder_hour"), default=9):
                continue

            if next_due is None or next_due < local_today:
                next_due = compute_next_due(
                    recurrence,
                    local_today,
                    billing_day,
                    billing_weekday,
                    billing_month,
                    anchor_date,
                )
                repo.update_recurring_expense(int(recurring["id"]), {"next_due": next_due})

            bill_instance = repo.upsert_bill_instance(
                int(recurring["id"]),
                int(next_due.year),
                int(next_due.month),
                next_due.isoformat(),
                float(recurring.get("amount") or 0),
                recurring.get("payment_link"),
                recurring.get("payment_reference"),
            )

            offsets = _extract_offsets(recurring)
            if 0 not in offsets:
                offsets.append(0)

            for offset in offsets:
                reminder_date = next_due - timedelta(days=int(offset))
                if reminder_date != local_today:
                    continue

                reminder_id = repo.create_bill_reminder_if_missing(
                    int(bill_instance["id"]),
                    int(offset),
                    reminder_date.isoformat(),
                )
                if not reminder_id:
                    continue

                chat_id = repo.get_user_chat_id(str(recurring["user_id"]), "telegram")

                actions = [
                    (f"recurring:paid:{bill_instance['id']}", "✅ Sí"),
                    (f"recurring:later:{bill_instance['id']}", "⏳ Después"),
                    (f"recurring:no:{bill_instance['id']}", "❌ No"),
                ]
                text = _reminder_text(recurring, next_due, int(offset))
                delivered = False
                if chat_id:
                    try:
                        await bot.send_message(
                            chat_id=int(chat_id),
                            text=text,
                            parse_mode="HTML",
                            disable_web_page_preview=True,
                            reply_markup=_build_keyboard(actions),
                        )
                        delivered = True
                    except Exception as exc:
                        logger.warning("Recurring telegram send failed: %s", exc)

                if evolution_client:
                    evolution_chat_id = repo.get_user_chat_id(str(recurring["user_id"]), "evolution")
                    if evolution_chat_id:
                        try:
                            await send_evolution_message(
                                evolution_client,
                                str(evolution_chat_id),
                                _build_bot_message(text, actions),
                            )
                            delivered = True
                        except Exception as exc:
                            logger.warning("Recurring evolution send failed: %s", exc)

                if delivered:
                    repo.update_bill_reminder(
                        int(reminder_id),
                        {"status": "sent", "sent_at": datetime.now(timezone.utc).isoformat()},
                    )
        except Exception as exc:
            logger.warning("Recurring reminder failed: %s", exc)
