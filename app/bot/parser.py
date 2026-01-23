from __future__ import annotations

import html
import json
import random
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from app.core.config import Settings


@dataclass
class ParsedCommand:
    route: str
    command: str
    invite_token: str
    text: str
    text_for_parsing: str
    chat_id: Optional[int]
    telegram_user_id: Optional[int]
    non_text_type: Optional[str]


def normalize_amount_slang(text: str) -> str:
    t = str(text or "")
    t = re.sub(r"(\d+(?:[.,]\d+)?)\s*(k|luka?s?|luca?s?)\b", _slang_mul(1000), t, flags=re.IGNORECASE)
    t = re.sub(r"(\d+(?:[.,]\d+)?)\s*(m|palo?s?)\b", _slang_mul(1_000_000), t, flags=re.IGNORECASE)
    return t


def _slang_mul(mult: int):
    def _repl(match: re.Match[str]) -> str:
        raw = match.group(1)
        try:
            value = float(raw.replace(",", "."))
        except ValueError:
            return match.group(0)
        return str(int(round(value * mult)))

    return _repl


def parse_command(text: Optional[str], chat_id: Optional[int], telegram_user_id: Optional[int], non_text_type: Optional[str]) -> ParsedCommand:
    if not text:
        return ParsedCommand(
            route="non_text",
            command="",
            invite_token="",
            text="",
            text_for_parsing="",
            chat_id=chat_id,
            telegram_user_id=telegram_user_id,
            non_text_type=non_text_type or "non_text",
        )

    clean = text.strip()
    first_token = clean.split()[0].split("@")[0].lower() if clean else ""
    args = " ".join(clean.split()[1:]).strip()

    route = "ai"
    invite_token = ""
    if first_token == "/start":
        if args:
            route = "onboarding"
            invite_token = args
        else:
            route = "help"
    elif first_token == "/help":
        route = "help"
    elif first_token == "/list":
        route = "list"
    elif first_token == "/summary":
        route = "summary"
    elif first_token == "/undo":
        route = "undo"

    return ParsedCommand(
        route=route,
        command=first_token,
        invite_token=invite_token,
        text=clean,
        text_for_parsing=normalize_amount_slang(clean),
        chat_id=chat_id,
        telegram_user_id=telegram_user_id,
        non_text_type=non_text_type,
    )


def _bogota_today() -> str:
    now = datetime.now(ZoneInfo("America/Bogota"))
    return now.strftime("%Y-%m-%d")


def _bogota_yesterday() -> str:
    now = datetime.now(ZoneInfo("America/Bogota")) - timedelta(days=1)
    return now.strftime("%Y-%m-%d")


def _user_provided_date(raw_text: str) -> bool:
    t = raw_text.lower()
    if re.search(r"\b\d{4}\b", t):
        return True
    if re.search(r"\b\d{1,2}[/-]\d{1,2}\b", t):
        return True
    if re.search(r"\b(hoy|ayer|anteayer|anoche)\b", t):
        return True
    if re.search(r"\b(\d{1,2})\s*(de)?\s*(ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)\b", t):
        return True
    return False


def _explicit_calendar_date(raw_text: str) -> bool:
    t = raw_text.lower()
    if re.search(r"\b\d{4}\b", t):
        return True
    if re.search(r"\b\d{1,2}[/-]\d{1,2}\b", t):
        return True
    if re.search(r"\b(\d{1,2})\s*(de)?\s*(ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)\b", t):
        return True
    return False


def _is_valid_iso_date(value: str) -> bool:
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", value))


def _current_year_bogota() -> int:
    return int(datetime.now(ZoneInfo("America/Bogota")).strftime("%Y"))


def _stable_id(prefix: str, value: str) -> str:
    base = re.sub(r"[^A-Z0-9_\-:.]", "", re.sub(r"\s+", "_", value.upper())).strip()
    base = base[:40]
    return f"{prefix}:{base}" if base else ""


def _norm_str(value: Any) -> str:
    return str(value or "").strip()


def normalize_ai_response(
    parsed: Dict[str, Any],
    raw_text: str,
    chat_id: Optional[int],
    settings: Settings,
    source: str,
) -> Dict[str, Any]:
    today = _bogota_today()
    yesterday = _bogota_yesterday()
    has_user_date = _user_provided_date(raw_text)
    current_year = _current_year_bogota()
    mentions_anoche = "anoche" in raw_text.lower()
    explicit_calendar_date = _explicit_calendar_date(raw_text)

    def _safe_float(value: Any, default: float = 0) -> float:
        try:
            return float(str(value).replace(",", "."))
        except ValueError:
            return default

    tx = {
        "intent": _norm_str(parsed.get("intent", "add_tx")).lower(),
        "type": _norm_str(parsed.get("type", "expense")).lower(),
        "transactionKind": _norm_str(parsed.get("transactionKind", "regular")).lower(),
        "amount": _safe_float(parsed.get("amount", 0), 0),
        "currency": _norm_str(parsed.get("currency", "COP")).upper(),
        "category": _norm_str(parsed.get("category", "misc")).lower(),
        "description": _norm_str(parsed.get("description", "")),
        "date": _norm_str(parsed.get("date", "")),
        "normalizedMerchant": _norm_str(parsed.get("normalizedMerchant", "")),
        "paymentMethod": _norm_str(parsed.get("paymentMethod", "cash")).lower(),
        "counterparty": _norm_str(parsed.get("counterparty", "")),
        "loanRole": _norm_str(parsed.get("loanRole", "")).lower(),
        "loanId": _norm_str(parsed.get("loanId", "")),
        "isRecurring": parsed.get("isRecurring") in {True, "true", 1, "1"},
        "recurrence": _norm_str(parsed.get("recurrence", "")).lower(),
        "recurrenceId": _norm_str(parsed.get("recurrenceId", "")),
        "parseConfidence": parsed.get("parseConfidence"),
        "parserVersion": "mvp-v1",
    }

    if not isinstance(tx["parseConfidence"], (int, float)):
        tx["parseConfidence"] = None

    if not (tx["amount"] >= 0):
        tx["amount"] = 0

    if tx["type"] not in {"income", "expense"}:
        tx["type"] = "expense"
    if tx["transactionKind"] not in {"regular", "loan", "transfer"}:
        tx["transactionKind"] = "regular"

    if mentions_anoche and not explicit_calendar_date:
        tx["date"] = yesterday
    elif not _is_valid_iso_date(tx["date"]):
        tx["date"] = today
    else:
        year = int(tx["date"][:4])
        if not has_user_date and year != current_year:
            tx["date"] = today

    if not tx["category"]:
        tx["category"] = "misc"

    blob = f"{raw_text} {tx['description']} {tx['normalizedMerchant']}".lower()
    if tx["category"] == "misc":
        if re.search(r"\b(pan|leche|huevo|huevos|arroz|pasta|arepa|cafe|café|agua|jugo|fruta|verdura|carne|pollo|mercado|supermercado|tienda|d1|ara|éxito|exito|carulla|jumbo)\b", blob):
            tx["category"] = "food_home"
        elif re.search(r"\b(restaurante|almuerzo|cena|hamburguesa|pizza|domicilio|rappi|rapi|ubereats|uber eats|didi food|corrientazo)\b", blob):
            tx["category"] = "food_out"
        elif re.search(r"\b(uber|didi|taxi|bus|transmi|metro|gasolina|parqueadero|peaje)\b", blob):
            tx["category"] = "transport"

    if tx["transactionKind"] == "loan":
        if tx["loanRole"] not in {"lent", "borrowed", "repayment"}:
            tx["loanRole"] = "repayment" if tx["type"] == "income" else "lent"
        if not tx["loanId"]:
            tx["loanId"] = _stable_id("LOAN", tx["counterparty"] or tx["normalizedMerchant"] or "GENERAL")
    else:
        tx["loanRole"] = ""
        tx["loanId"] = ""

    if tx["isRecurring"]:
        if tx["recurrence"] not in {"weekly", "biweekly", "monthly", "yearly"}:
            tx["recurrence"] = "monthly"
        if not tx["recurrenceId"]:
            tx["recurrenceId"] = _stable_id("REC", tx["normalizedMerchant"] or tx["description"] or tx["category"])
    else:
        tx["recurrence"] = ""
        tx["recurrenceId"] = ""

    now = datetime.now(timezone.utc).isoformat()
    tx["source"] = source
    tx["sourceMessageId"] = ""
    tx["rawText"] = raw_text
    tx["createdAt"] = now
    tx["updatedAt"] = now
    tx["isDeleted"] = False
    tx["deletedAt"] = ""
    tx["chatId"] = chat_id
    return tx


def normalize_types(tx: Dict[str, Any]) -> Dict[str, Any]:
    def to_bool(value: Any) -> bool:
        return value in {True, "true", 1, "1"}

    def to_num(value: Any, default: float) -> float:
        try:
            return float(str(value).replace(",", "."))
        except ValueError:
            return default

    if "isRecurring" in tx:
        tx["isRecurring"] = to_bool(tx["isRecurring"])
    if "isDeleted" in tx:
        tx["isDeleted"] = to_bool(tx["isDeleted"])

    if "amount" in tx:
        tx["amount"] = to_num(tx["amount"], 0)
    if "parseConfidence" in tx:
        tx["parseConfidence"] = to_num(tx["parseConfidence"], 0.7)

    tx["type"] = str(tx.get("type", "expense"))
    tx["transactionKind"] = str(tx.get("transactionKind", "regular"))
    tx["currency"] = str(tx.get("currency", "COP"))
    tx["category"] = str(tx.get("category", ""))
    tx["description"] = str(tx.get("description", ""))

    tx["source"] = str(tx.get("source", "unknown"))
    tx["sourceMessageId"] = str(tx.get("sourceMessageId", ""))
    tx["parserVersion"] = str(tx.get("parserVersion", "v1"))

    tx["normalizedMerchant"] = str(tx.get("normalizedMerchant", ""))
    tx["paymentMethod"] = str(tx.get("paymentMethod", "cash"))
    if not tx["paymentMethod"] or tx["paymentMethod"] == "unknown":
        tx["paymentMethod"] = "cash"
    tx["counterparty"] = str(tx.get("counterparty", ""))
    tx["loanRole"] = str(tx.get("loanRole", ""))
    tx["loanId"] = str(tx.get("loanId", ""))

    tx["isRecurring"] = bool(tx.get("isRecurring", False))

    return tx


def build_system_prompt(settings: Settings) -> str:
    today = datetime.now(ZoneInfo(settings.timezone)).strftime("%Y-%m-%d")
    return (
        "You are a financial assistant. Extract structured data from a single user message.\n\n"
        "Return JSON ONLY. No markdown, no backticks.\n\n"
        "General rules:\n"
        "- Default currency is COP.\n"
        "- paymentMethod defaults to 'cash' when unspecified.\n"
        "- Date must be YYYY-MM-DD. Use Current Date (America/Bogota) when user did not specify a date.\n"
        "- Relative time: interpret \"anoche\" as yesterday's date (America/Bogota).\n"
        "- Amount: support slang: k/lukas=1,000; m/palo(s)=1,000,000.\n\n"
        "Intent rules (for natural language):\n"
        "- intent: 'add_tx' by default.\n"
        "- If user asks for help => intent='help'.\n"
        "- If user asks to list movements => intent='list'.\n"
        "- If user asks for monthly summary => intent='summary'.\n\n"
        "Type:\n"
        "- type: 'expense' or 'income'.\n"
        "- If verbs like \"me pagaron\", \"recibi\", \"reembolso\" => type='income'.\n"
        "- If verbs like \"compre\", \"pague\", \"gaste\" => type='expense'.\n\n"
        "transactionKind:\n"
        "- 'regular' (default)\n"
        "- 'loan' when lending/borrowing/repaying (e.g., 'le presté', 'me prestaron', 'me pagó', 'le pagué')\n"
        "- 'transfer' when moving money between own accounts (e.g., \"pase a mi cuenta\", \"traspaso entre cuentas\").\n\n"
        "Loans (only if transactionKind='loan'):\n"
        "- counterparty: person/entity name if any.\n"
        "- loanRole: 'lent' | 'borrowed' | 'repayment'.\n"
        "- loanId: stable id like 'LOAN:<COUNTERPARTY>' when possible.\n\n"
        "Recurring:\n"
        "- isRecurring: true if periodic payment is indicated (mensual, cada mes, semanal, anual, suscripción, cada quincena).\n"
        "- recurrence: 'weekly'|'biweekly'|'monthly'|'yearly' when isRecurring=true.\n"
        "- recurrenceId: stable id like 'REC:<NORMALIZED_MERCHANT>'.\n\n"
        "Categories (choose ONE, avoid 'misc' unless nothing fits):\n"
        "- food_home (pan, café, snacks, mercado, supermercado, D1, Ara, Éxito)\n"
        "- food_out (restaurante, hamburguesa, pizza, domicilio, Rappi)\n"
        "- transport (uber, didi, taxi, bus, gasolina, parqueadero)\n"
        "- housing (arriendo, hipoteca)\n"
        "- utilities (luz, agua, gas, internet, celular)\n"
        "- health (medicina, doctor, farmacia)\n"
        "- shopping (ropa, compras)\n"
        "- entertainment (cine, juegos)\n"
        "- education (curso, universidad)\n"
        "- subscriptions (netflix, spotify, suscripción)\n"
        "- debt (cuota, interés)\n"
        "- travel (hotel, vuelo, viaje)\n"
        "- misc\n\n"
        "normalizedMerchant: short normalized merchant name if possible (Uber, Netflix, Exito).\n"
        "paymentMethod: one of 'cash'|'card'|'transfer'|'wallet'.\n"
        "- If text mentions \"tarjeta\", \"debito\", \"credito\" => paymentMethod='card'.\n"
        "- If text mentions \"transferencia\", \"transferi\", \"traspaso\" => paymentMethod='transfer'.\n"
        "- If text mentions \"nequi\", \"daviplata\" => paymentMethod='wallet'.\n"
        "parseConfidence: number 0..1 indicating your confidence. Use low values when amount/date are missing or ambiguous.\n\n"
        "Output fields (all in one JSON object):\n"
        "intent, type, transactionKind, amount, currency, category, description, date,\n"
        "normalizedMerchant, paymentMethod,\n"
        "counterparty, loanRole, loanId,\n"
        "isRecurring, recurrence, recurrenceId,\n"
        "parseConfidence\n\n"
        f"Current Date (America/Bogota): {today}"
    )


def escape_html(text: str) -> str:
    return html.escape(text or "")


def format_currency(amount: float, currency: str = "COP") -> str:
    sign = "-" if amount < 0 else ""
    value = abs(int(round(amount)))
    formatted = f"{value:,}".replace(",", ".")
    if currency.upper() == "COP":
        return f"{sign}${formatted}"
    return f"{sign}{currency.upper()} {formatted}"


def generate_tx_id() -> str:
    millis = int(datetime.now(timezone.utc).timestamp() * 1000)
    return f"TX-{millis}-{random.randint(0, 9999)}"
