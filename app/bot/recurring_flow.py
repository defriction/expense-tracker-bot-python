from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from app.bot.parser import escape_html, format_currency
from app.core.config import Settings


PENDING_RECURRING_ACTION = "recurring_setup"
PENDING_RECURRING_OFFER_ACTION = "recurring_offer"


def _normalize_text(text: str) -> str:
    return (text or "").strip().lower()


def is_affirmative(text: str) -> bool:
    t = _normalize_text(text)
    return t in {"si", "sí", "s", "yes", "ok", "dale", "claro", "de una"}


def is_negative(text: str) -> bool:
    t = _normalize_text(text)
    return t in {"no", "ninguno", "ninguna", "nah", "na", "n"}


def _extract_link(text: str) -> Optional[str]:
    match = re.search(r"(https?://[^\s]+|www\.[^\s]+)", text or "", flags=re.IGNORECASE)
    return match.group(1) if match else None


def _parse_int(text: str) -> Optional[int]:
    match = re.search(r"\b(\d{1,2})\b", text or "")
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def parse_billing_day(text: str) -> Optional[int]:
    value = _parse_int(text)
    if value is None:
        return None
    if 1 <= value <= 31:
        return value
    return None


def parse_remind_offsets(text: str) -> list[int]:
    values = []
    for raw in re.findall(r"-?\d{1,2}", text or ""):
        try:
            value = abs(int(raw))
        except ValueError:
            continue
        if value not in values:
            values.append(value)
    values = [v for v in values if v >= 0]
    values.sort(reverse=True)
    return values


def parse_amount(text: str) -> Optional[float]:
    raw = (text or "").lower().replace("$", "").replace(".", "")
    raw = re.sub(
        r"(\d+(?:[.,]\d+)?)\s*(k|luka?s?|luca?s?)\b",
        lambda m: str(int(float(m.group(1).replace(",", ".")) * 1000)),
        raw,
    )
    raw = re.sub(
        r"(\d+(?:[.,]\d+)?)\s*(m|palo?s?)\b",
        lambda m: str(int(float(m.group(1).replace(",", ".")) * 1000000)),
        raw,
    )
    match = re.search(r"\b(\d+(?:[.,]\d+)?)\b", raw)
    if not match:
        return None
    try:
        return round(float(match.group(1).replace(",", ".")), 2)
    except ValueError:
        return None


def parse_recurrence(text: str) -> str:
    t = _normalize_text(text)
    if re.search(r"\b(quincenal|cada\s+15\s+d[ií]as)\b", t):
        return "biweekly"
    if re.search(r"\b(semanal|cada\s+semana|todos\s+los\s+(lunes|martes|miercoles|miércoles|jueves|viernes|sabado|sábado|domingo))\b", t):
        return "weekly"
    if re.search(r"\b(trimestral|cada\s+3\s+meses)\b", t):
        return "quarterly"
    if re.search(r"\b(anual|cada\s+a[nñ]o)\b", t):
        return "yearly"
    return "monthly"


def parse_service_name(text: str) -> Optional[str]:
    t = (text or "").strip()
    match = re.search(r"pagar\s+(.+)", t, flags=re.IGNORECASE)
    if not match:
        return None
    service = match.group(1)
    service = re.sub(r"\b(todos?\s+los\s+\d{1,2}|cada\s+mes|mensual|semanal|quincenal|trimestral|anual)\b", "", service, flags=re.IGNORECASE)
    service = re.sub(r"\s+", " ", service).strip(" .,")
    return service[:128] if service else None


_WEEKDAY_MAP = {
    "lunes": 0,
    "lun": 0,
    "martes": 1,
    "mar": 1,
    "miercoles": 2,
    "miércoles": 2,
    "mie": 2,
    "jueves": 3,
    "jue": 3,
    "viernes": 4,
    "vie": 4,
    "sabado": 5,
    "sábado": 5,
    "sab": 5,
    "domingo": 6,
    "dom": 6,
}


def parse_weekday(text: str) -> Optional[int]:
    t = _normalize_text(text)
    for key, value in _WEEKDAY_MAP.items():
        if re.search(rf"\b{re.escape(key)}\b", t):
            return value
    return None


def _month_range(year: int, month: int) -> int:
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    return (next_month - date(year, month, 1)).days


def _clamp_day(year: int, month: int, day: int) -> int:
    max_day = _month_range(year, month)
    return min(day, max_day)


def _add_months(source: date, months: int) -> date:
    month = source.month - 1 + months
    year = source.year + month // 12
    month = month % 12 + 1
    day = _clamp_day(year, month, source.day)
    return date(year, month, day)


def compute_next_due(
    recurrence: str,
    today: date,
    billing_day: Optional[int],
    billing_weekday: Optional[int],
    billing_month: Optional[int],
    anchor_date: Optional[date],
) -> date:
    recurrence = (recurrence or "monthly").lower()
    if recurrence in {"weekly", "biweekly"}:
        weekday = billing_weekday
        if weekday is None and anchor_date is not None:
            weekday = anchor_date.weekday()
        if weekday is None:
            weekday = today.weekday()

        if anchor_date is None:
            anchor_date = today

        candidate = today
        offset = (weekday - candidate.weekday()) % 7
        candidate = candidate + timedelta(days=offset)
        if recurrence == "biweekly":
            delta_days = (candidate - anchor_date).days
            if delta_days % 14 != 0:
                candidate = candidate + timedelta(days=(14 - (delta_days % 14)))
        return candidate

    if recurrence == "monthly":
        day = billing_day or (anchor_date.day if anchor_date else today.day)
        candidate = date(today.year, today.month, _clamp_day(today.year, today.month, day))
        if candidate < today:
            candidate = _add_months(candidate, 1)
        return candidate

    if recurrence == "quarterly":
        day = billing_day or (anchor_date.day if anchor_date else today.day)
        base_month = billing_month or (anchor_date.month if anchor_date else today.month)
        month_options = [(base_month - 1 + 3 * i) % 12 + 1 for i in range(4)]
        year = today.year
        candidates = []
        for month in month_options:
            y = year
            if month < base_month and today.month >= base_month:
                y += 1
            candidates.append(date(y, month, _clamp_day(y, month, day)))
        candidates.sort()
        for candidate in candidates:
            if candidate >= today:
                return candidate
        return candidates[0]

    if recurrence == "yearly":
        day = billing_day or (anchor_date.day if anchor_date else today.day)
        month = billing_month or (anchor_date.month if anchor_date else today.month)
        candidate = date(today.year, month, _clamp_day(today.year, month, day))
        if candidate < today:
            candidate = date(today.year + 1, month, _clamp_day(today.year + 1, month, day))
        return candidate

    return today


def build_setup_question(step: str, recurrence: str) -> str:
    if step == "ask_billing_day":
        if recurrence in {"weekly", "biweekly"}:
            return "¿Qué día de la semana se cobra? (ej: lunes)"
        return "¿Qué día del mes se cobra? (1-31)"
    if step == "ask_reminders":
        return "¿Cuándo quieres recordatorios? Ej: <code>3,1,0</code> (3 días antes, 1 día antes y el día)."
    if step == "ask_payment_link":
        return "¿Tienes enlace de pago? Envíalo o escribe <code>no</code>."
    if step == "ask_payment_reference":
        return "¿Tienes referencia de pago? Envíala o escribe <code>no</code>."
    return ""


def _format_recurrence_label(recurrence: str) -> str:
    mapping = {
        "weekly": "semanal",
        "biweekly": "quincenal",
        "monthly": "mensual",
        "quarterly": "trimestral",
        "yearly": "anual",
    }
    return mapping.get(recurrence, recurrence)


def build_setup_summary(recurring: Dict[str, Any], settings: Settings) -> str:
    amount = "Por definir"
    try:
        if float(recurring.get("amount") or 0) > 0:
            amount = format_currency(float(recurring.get("amount", 0)), str(recurring.get("currency", "COP")))
    except (TypeError, ValueError):
        amount = "Por definir"

    recurrence = _format_recurrence_label(str(recurring.get("recurrence", "monthly")))
    service_name = recurring.get("service_name") or recurring.get("normalized_merchant") or recurring.get("description") or "Pago recurrente"
    day = recurring.get("billing_day")
    weekday = recurring.get("billing_weekday")
    detail = ""
    if day:
        detail = f"día {day}"
    elif weekday is not None:
        detail = f"día {['lunes','martes','miércoles','jueves','viernes','sábado','domingo'][int(weekday)]}"
    link = recurring.get("payment_link") or "—"
    ref = recurring.get("payment_reference") or "—"
    tz = settings.timezone or "America/Bogota"
    offsets = recurring.get("remind_offsets") or [3, 1, 0]
    if isinstance(offsets, str):
        try:
            offsets = __import__("json").loads(offsets)
        except Exception:
            offsets = [3, 1, 0]
    offsets = [int(v) for v in offsets if isinstance(v, (int, float, str)) and str(v).isdigit()]
    offsets = sorted(set(offsets), reverse=True)
    offsets_label = ", ".join([f"-{v}" if v else "0" for v in offsets]) if offsets else "0"
    return (
        "✅ <b>Suscripción guardada</b>\n"
        f"<b>Servicio:</b> {escape_html(str(service_name))}\n"
        f"<b>Monto:</b> {amount}\n"
        f"<b>Frecuencia:</b> {escape_html(recurrence)}\n"
        f"<b>Vencimiento:</b> {escape_html(detail)}\n"
        f"<b>Recordatorios:</b> {escape_html(offsets_label)}\n"
        f"<b>Enlace:</b> {escape_html(str(link))}\n"
        f"<b>Referencia:</b> {escape_html(str(ref))}\n"
        f"<b>Zona horaria:</b> {escape_html(tz)}"
    )


@dataclass
class SetupResult:
    response: str
    done: bool = False
    updates: Optional[Dict[str, Any]] = None
    next_step: Optional[str] = None


def handle_setup_step(step: str, text: str, recurrence: str) -> SetupResult:
    if step == "ask_billing_day":
        if recurrence in {"weekly", "biweekly"}:
            weekday = parse_weekday(text)
            if weekday is None:
                return SetupResult("No entendí el día. Prueba con: lunes, martes, miércoles.")
            return SetupResult("", updates={"billing_weekday": weekday}, next_step="ask_reminders")
        day = parse_billing_day(text)
        if day is None:
            return SetupResult("No entendí el día. Escribe un número entre 1 y 31.")
        return SetupResult("", updates={"billing_day": day}, next_step="ask_reminders")

    if step == "ask_reminders":
        offsets = parse_remind_offsets(text)
        if not offsets:
            return SetupResult("No entendí los recordatorios. Ej: 3,1,0")
        return SetupResult("", updates={"remind_offsets": offsets}, next_step="ask_payment_link")

    if step == "ask_payment_link":
        if is_negative(text):
            return SetupResult("", updates={"payment_link": ""}, next_step="ask_payment_reference")
        link = _extract_link(text) or text.strip()
        return SetupResult("", updates={"payment_link": link[:500]}, next_step="ask_payment_reference")

    if step == "ask_payment_reference":
        if is_negative(text):
            return SetupResult("", updates={"payment_reference": ""}, next_step=None, done=True)
        return SetupResult("", updates={"payment_reference": text.strip()[:500]}, next_step=None, done=True)

    return SetupResult("No entendí. Intenta de nuevo.")


def get_today(settings: Settings) -> date:
    tz_name = settings.timezone or "America/Bogota"
    return datetime.now(ZoneInfo(tz_name)).date()
