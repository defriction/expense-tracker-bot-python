from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from app.bot.parser import escape_html, format_currency
from app.bot.recurring_flow import compute_next_due, get_today
from app.core.config import Settings
from app.core.logging import logger
from app.services.repositories import DataRepo


def _build_keyboard(actions: Iterable[tuple[str, str]]) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=label, callback_data=action_id) for action_id, label in actions]]
    return InlineKeyboardMarkup(rows)


def _reminder_text(recurring: Dict[str, Any], due_date: date, offset: int) -> str:
    amount = format_currency(float(recurring.get("amount", 0)), str(recurring.get("currency", "COP")))
    merchant = recurring.get("normalized_merchant") or recurring.get("description") or "Gasto recurrente"
    link = recurring.get("payment_link") or "—"
    reference = recurring.get("payment_reference") or "—"
    when = "hoy" if offset == 0 else f"en {offset} día(s)"
    return (
        "⏰ <b>Recordatorio de pago</b>\n"
        f"<b>Vence:</b> <code>{due_date.isoformat()}</code> ({escape_html(when)})\n"
        f"<b>Monto:</b> {amount}\n"
        f"<b>Concepto:</b> {escape_html(str(merchant))}\n"
        f"<b>Enlace:</b> {escape_html(str(link))}\n"
        f"<b>Referencia:</b> {escape_html(str(reference))}\n\n"
        "¿Ya pagaste?"
    )


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


async def process_recurring_reminders(
    repo: DataRepo,
    bot,
    settings: Settings,
) -> None:
    today = get_today(settings)
    recurring_expenses = repo.list_active_recurring_expenses()
    for recurring in recurring_expenses:
        try:
            next_due = _extract_next_due(recurring)
            anchor_date = _extract_anchor_date(recurring)
            recurrence = str(recurring.get("recurrence") or "monthly").lower()
            billing_day = recurring.get("billing_day")
            billing_weekday = recurring.get("billing_weekday")
            billing_month = recurring.get("billing_month")
            if next_due is None or next_due < today:
                next_due = compute_next_due(
                    recurrence,
                    today,
                    billing_day,
                    billing_weekday,
                    billing_month,
                    anchor_date,
                )
                repo.update_recurring_expense(int(recurring["id"]), {"next_due": next_due})

            offsets = _extract_offsets(recurring)
            if 0 not in offsets:
                offsets.append(0)

            for offset in offsets:
                reminder_date = next_due - timedelta(days=int(offset))
                if reminder_date != today:
                    continue
                event_id = repo.create_recurring_event_if_missing(
                    int(recurring["id"]),
                    reminder_date.isoformat(),
                    int(offset),
                    next_due.isoformat(),
                )
                if not event_id:
                    continue
                chat_id = repo.get_user_chat_id(str(recurring["user_id"]), "telegram")
                if not chat_id:
                    continue
                actions = [
                    (f"recurring:paid:{event_id}", "✅ Pagado"),
                    (f"recurring:skip:{event_id}", "⏳ No pagado"),
                ]
                await bot.send_message(
                    chat_id=int(chat_id),
                    text=_reminder_text(recurring, next_due, int(offset)),
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                    reply_markup=_build_keyboard(actions),
                )
                repo.update_recurring_event(
                    int(event_id),
                    {"status": "sent", "sent_at": datetime.now(timezone.utc).isoformat()},
                )
        except Exception as exc:
            logger.warning("Recurring reminder failed: %s", exc)
