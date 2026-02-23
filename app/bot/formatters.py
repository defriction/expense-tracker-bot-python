from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from .parser import escape_html, format_currency


HELP_MESSAGE = (
    "ℹ️ <b>Asistente financiero</b>\n\n"
    "<b>Registrar movimientos</b>\n"
    "• <code>comí un pan 5k</code>\n"
    "• <code>uber 12000</code>\n"
    "• <code>salario 2500000</code> (ingreso)\n\n"
    "<b>Préstamos</b>\n"
    "• <code>le presté 200k a Juan</code>\n"
    "• <code>Juan me pagó 50k</code>\n\n"
    "<b>Recurrentes</b>\n"
    "• <code>Netflix 39900 mensual</code>\n"
    "• <code>Recuérdame pagar todos los 5 el internet</code>\n"
    "• <code>/recurrings</code> (ver códigos)\n"
    "• <code>para código 12 avísame 3 días antes y el mismo día</code>\n"
    "• <code>monto código 12 45000</code>\n"
    "• <code>pausa netflix</code> / <code>sube luz a 70k</code> (lenguaje natural)\n"
    "• <code>pausar código 12</code> / <code>activar código 12</code> / <code>cancelar código 12</code>\n\n"
    "<b>Múltiples movimientos</b>\n"
    "• <code>me gasté 5k en comida y 60k en ropa</code>\n"
    "• Si hay ambigüedad, te pediré confirmar con <code>sí</code> o <code>no</code>\n\n"
    "<b>Menú rápido</b>\n"
    "• <code>/list</code> últimos movimientos\n"
    "• <code>/summary</code> resumen del mes\n"
    "• <code>/recurrings</code> ver recurrentes\n"
    "• <code>/download</code> o <code>/descargar</code> transacciones\n"
    "• <code>/undo</code> deshacer último\n"
    "• <code>/clear</code> eliminar todas (con confirmación)\n"
    "• <code>/clear_recurrings</code> eliminar recurrentes de tu lista (con confirmación)\n"
    "• <code>/start TU-TOKEN</code> activar cuenta\n\n"
    "<b>Notas</b>\n"
    "• Moneda por defecto: COP\n"
    "• Método por defecto: cash\n"
    "• Fechas admitidas: 2025-01-18, hoy, ayer"
)

NON_TEXT_MESSAGE = (
    "<b>📎 Solo puedo leer texto por ahora</b>\n\n"
    "Envíame un mensaje con texto (ej: <code>comí un pan 5k</code>)."
)

RATE_LIMIT_MESSAGE = (
    "⏳ <b>Muchos intentos</b>\n"
    "Espera un momento y vuelve a intentar."
)

LONG_MESSAGE = (
    "✂️ <b>Mensaje muy largo</b>\n"
    "Reduce el texto e intenta de nuevo."
)

UNAUTHORIZED_MESSAGE = (
    "🔒 <b>Acceso no autorizado</b>\n"
    "Tu usuario no está activo.\n\n"
    "Activa tu cuenta con: <code>/start TU-TOKEN</code>\n"
    "Si no tienes token, pídelo al administrador."
)

ONBOARDING_SUCCESS_MESSAGE = (
    "🎉 <b>Cuenta activada</b>\n\n"
    "Listo, ya puedes registrar movimientos:\n"
    "• <code>Café 6000</code>\n"
    "• <code>Me pagaron 3m</code>"
)


def format_add_tx_message(tx: Dict[str, object]) -> str:
    amount = format_currency(float(tx.get("amount", 0)), str(tx.get("currency", "COP")))
    kind = str(tx.get("transactionKind", "regular")).lower()
    tx_type = str(tx.get("type", "expense")).lower()

    type_emoji = "🟢" if tx_type == "income" else "🔴"
    header = "✅ <b>Movimiento guardado</b>"
    if kind == "loan":
        kind_label = "🤝 <b>Préstamo</b>"
    elif kind == "transfer":
        kind_label = "🔁 <b>Transferencia</b>"
    else:
        kind_label = f"{type_emoji} <b>{'Ingreso' if tx_type == 'income' else 'Gasto'}</b>"

    lines = [
        header,
        kind_label,
        f"<b>Monto:</b> {amount}",
        f"<b>Categoría:</b> {escape_html(str(tx.get('category', 'misc')))}",
        f"<b>Fecha:</b> <code>{escape_html(str(tx.get('date', '')))}</code>",
    ]

    if tx.get("normalizedMerchant"):
        lines.append(f"<b>Comercio:</b> {escape_html(str(tx.get('normalizedMerchant')))}")
    if tx.get("paymentMethod") and tx.get("paymentMethod") != "unknown":
        lines.append(f"<b>Método:</b> {escape_html(str(tx.get('paymentMethod')))}")
    if tx.get("description"):
        lines.append(f"<b>Detalle:</b> <i>{escape_html(str(tx.get('description')))}</i>")

    if kind == "loan":
        if tx.get("counterparty"):
            lines.append(f"<b>Con:</b> {escape_html(str(tx.get('counterparty')))}")
        if tx.get("loanRole"):
            lines.append(f"<b>Tipo préstamo:</b> {escape_html(str(tx.get('loanRole')))}")

    if tx.get("isRecurring"):
        lines.append(f"🔁 <b>Recurrente:</b> {tx.get('recurrence') or 'monthly'}")

    return "\n".join(lines)


def format_multi_tx_preview_message(txs: List[Dict[str, object]]) -> str:
    lines = [
        f"🧠 <b>Detecté {len(txs)} movimientos</b>",
        "<i>Revisa antes de guardar</i>",
        "",
    ]
    for idx, tx in enumerate(txs, start=1):
        amount = format_currency(float(tx.get("amount", 0)), str(tx.get("currency", "COP")))
        category = escape_html(str(tx.get("category", "misc")))
        detail = escape_html(str(tx.get("description") or tx.get("normalizedMerchant") or ""))
        line = f"{idx}. <b>{amount}</b> · <b>{category}</b>"
        if detail:
            line += f" · {detail}"
        lines.append(line)
    lines.append("")
    lines.append("Responde <code>sí</code> para guardar o <code>no</code> para cancelar.")
    return "\n".join(lines)


def format_multi_tx_saved_message(txs: List[Dict[str, object]]) -> str:
    lines = [f"✅ <b>Guardé {len(txs)} movimientos</b>", ""]
    for idx, tx in enumerate(txs, start=1):
        amount = format_currency(float(tx.get("amount", 0)), str(tx.get("currency", "COP")))
        category = escape_html(str(tx.get("category", "misc")))
        detail = escape_html(str(tx.get("description") or tx.get("normalizedMerchant") or ""))
        line = f"{idx}. {amount} · <b>{category}</b>"
        if detail:
            line += f" · {detail}"
        lines.append(line)
    return "\n".join(lines)


def format_list_message(transactions: List[Dict[str, object]]) -> str:
    def to_ts(item: Dict[str, object]) -> float:
        date_value = str(item.get("date") or "")
        if date_value and len(date_value) == 10:
            try:
                return datetime.fromisoformat(date_value + "T00:00:00+00:00").timestamp()
            except ValueError:
                pass
        created_at = str(item.get("createdAt") or "")
        try:
            return datetime.fromisoformat(created_at.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return float("-inf")

    filtered = [tx for tx in transactions if not bool(tx.get("isDeleted"))]
    filtered.sort(key=to_ts, reverse=True)
    last10 = filtered[:10]

    if not last10:
        return "📭 <b>Sin movimientos</b>\nAún no tienes movimientos registrados."

    message = [
        "🧾 <b>Movimientos recientes</b>",
        f"<i>Últimas {len(last10)}</i>",
        "",
    ]

    for tx in last10:
        icon = "🟢" if str(tx.get("type", "expense")).lower() == "income" else "🔴"
        kind = str(tx.get("transactionKind", "regular")).lower()
        kicon = "🤝" if kind == "loan" else "🔁" if kind == "transfer" else ""
        date = escape_html(str(tx.get("date", "")))
        category = escape_html(str(tx.get("category", "misc")))
        description = escape_html(str(tx.get("description", "")))
        merchant = escape_html(str(tx.get("normalizedMerchant", "")))
        payment = escape_html(str(tx.get("paymentMethod", "")))
        amount = format_currency(float(tx.get("amount", 0)), str(tx.get("currency", "COP")))

        line = f"{icon}{(' ' + kicon) if kicon else ''} <b>{amount}</b> · <b>{category}</b>"
        if merchant:
            line += f" · {merchant}"
        if tx.get("isRecurring"):
            line += " · 🔁"
        if payment:
            line += f" · {payment}"
        message.append(line)

        if description:
            message.append(f"<i>{description}</i>")

        if kind == "loan" and tx.get("counterparty"):
            role = f" ({escape_html(str(tx.get('loanRole') or ''))})" if tx.get("loanRole") else ""
            message.append(f"Con: <b>{escape_html(str(tx.get('counterparty')))}</b>{role}")

        if date:
            message.append(f"<code>{date}</code>")
        message.append("")

    return "\n".join(message).strip()


def format_recurring_list_message(items: List[Dict[str, object]]) -> str:
    visible_items = [item for item in items if str(item.get("status") or "").lower() != "canceled"]
    if not visible_items:
        return "📭 <b>Sin recurrentes</b>\nNo tienes recordatorios recurrentes."

    message = [
        "🔁 <b>Recurrentes</b>",
        "",
    ]
    for item in visible_items:
        rid = item.get("id")
        amount_value = float(item.get("amount", 0))
        amount = "Por definir" if amount_value <= 0 else format_currency(amount_value, str(item.get("currency", "COP")))
        merchant = escape_html(
            str(item.get("service_name") or item.get("normalized_merchant") or item.get("description") or "Gasto recurrente")
        )
        recurrence_raw = str(item.get("recurrence") or "monthly").lower()
        recurrence = {
            "weekly": "semanal",
            "biweekly": "quincenal",
            "monthly": "mensual",
            "quarterly": "trimestral",
            "yearly": "anual",
        }.get(recurrence_raw, recurrence_raw)
        status_raw = str(item.get("status") or "pending").lower()
        status = {
            "active": "activo",
            "paused": "pausado",
            "pending": "pendiente",
            "canceled": "cancelado",
        }.get(status_raw, status_raw)
        next_due = escape_html(str(item.get("next_due") or "—"))
        reminder_hour = item.get("reminder_hour")
        try:
            reminder_hour_label = f"{int(reminder_hour):02d}:00"
        except (TypeError, ValueError):
            reminder_hour_label = "09:00"
        message.append(f"🔹 <b>{merchant}</b>")
        message.append(f"<b>Código:</b> <code>{rid}</code>")
        message.append(f"<b>Monto:</b> {amount}")
        message.append(f"<b>Frecuencia:</b> {escape_html(recurrence)}")
        message.append(f"<b>Estado:</b> <b>{escape_html(status)}</b>")
        message.append(f"<b>Próximo cobro:</b> <code>{next_due}</code>")
        message.append(f"<b>Hora recordatorio:</b> <code>{escape_html(reminder_hour_label)}</code>")
        message.append("")

    message.append("Opciones para actualizar:")
    message.append("• En lenguaje natural: <code>sube internet a 70k y avísame a las 6 pm</code>")
    message.append("• Recordatorios: <code>para código 2 avísame 3 días antes y el mismo día</code>")
    message.append("• Estado: <code>pausa netflix</code>, <code>activar código 3</code> o <code>cancelar spotify</code>")
    message.append("• Limpiar todos: <code>/clear_recurrings</code>")
    return "\n".join(message).strip()


@dataclass
class SummaryAgg:
    totals_income: Dict[str, float]
    totals_expense: Dict[str, float]
    sum_income: float
    sum_expense: float
    count_income: int
    count_expense: int
    largest: Optional[Dict[str, object]]
    largest_expense: Optional[Dict[str, object]]


def _empty_agg() -> SummaryAgg:
    return SummaryAgg({}, {}, 0, 0, 0, 0, None, None)


def _add_tx(agg: SummaryAgg, tx: Dict[str, object]) -> None:
    tx_type = str(tx.get("type", "")).lower()
    if tx_type not in {"income", "expense"}:
        return

    try:
        amount = abs(float(tx.get("amount", 0)))
    except ValueError:
        return

    category = str(tx.get("category") or "sin_categoria")
    desc = str(tx.get("description") or tx.get("rawText") or "")
    tx_date = str(tx.get("date") or tx.get("createdAt") or "")

    if tx_type == "income":
        agg.totals_income[category] = agg.totals_income.get(category, 0) + amount
        agg.sum_income += amount
        agg.count_income += 1
    else:
        agg.totals_expense[category] = agg.totals_expense.get(category, 0) + amount
        agg.sum_expense += amount
        agg.count_expense += 1

    if not agg.largest or amount > float(agg.largest.get("amount", 0)):
        agg.largest = {"type": tx_type, "amount": amount, "category": category, "description": desc, "date": tx_date}

    if tx_type == "expense" and (not agg.largest_expense or amount > float(agg.largest_expense.get("amount", 0))):
        agg.largest_expense = {
            "type": tx_type,
            "amount": amount,
            "category": category,
            "description": desc,
            "date": tx_date,
        }


def _get_ymd_bogota(value: str) -> Optional[tuple[int, int, int]]:
    if not value:
        return None
    try:
        if len(value) >= 10 and value[4] == "-":
            y, m, d = value[:10].split("-")
            return int(y), int(m) - 1, int(d)
    except ValueError:
        return None

    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None

    bogota = dt.astimezone(ZoneInfo("America/Bogota"))
    return bogota.year, bogota.month - 1, bogota.day


def _format_delta_abs(value: float) -> str:
    sign = "+" if value > 0 else "−" if value < 0 else ""
    return f"{sign}{format_currency(abs(value))}"


def _format_delta_pct(curr: float, prev: float) -> str:
    if not prev:
        return "0%" if curr == 0 else "∞"
    pct = (curr - prev) / prev
    sign = "+" if pct > 0 else ""
    return f"{sign}{round(pct * 100)}%"


def _delta_icon(curr: float, prev: float) -> str:
    if not prev:
        return "➖" if curr == 0 else "🆕"
    diff = curr - prev
    if diff > 0:
        return "📈"
    if diff < 0:
        return "📉"
    return "➖"


def _short_label(label: str, max_len: int) -> str:
    label = label.strip()
    if len(label) <= max_len:
        return label
    if max_len <= 3:
        return label[:max_len]
    return label[: max_len - 1] + "…"


def _render_top_list(
    curr: Dict[str, float],
    curr_total: float,
    prev: Dict[str, float],
    *,
    compact: bool = False,
) -> str:
    entries = sorted(curr.items(), key=lambda item: item[1], reverse=True)
    if not entries:
        return "<pre>— Sin movimientos</pre>"

    top = entries[:4] if compact else entries[:5]
    rest = entries[5:]
    others = sum(value for _, value in rest)
    rows = top + ([("Otros", others)] if others > 0 else [])

    max_label = 12 if compact else min(18, max([len(str(label)) for label, _ in rows] + [8]))
    if compact:
        header = f"{'Cat'.ljust(max_label)}  {'Valor'.rjust(12)}  {'%'.rjust(4)}"
    else:
        header = f"{'Categoría'.ljust(max_label)}  {'Valor'.rjust(14)}  {'%'.rjust(4)}  {'Δ%'.rjust(4)}"

    lines = []
    for label, value in rows:
        prev_val = float(prev.get(label, 0))
        pct = f"{round((value / curr_total) * 100)}%" if curr_total else "0%"
        label_text = escape_html(_short_label(str(label), max_label)).ljust(max_label)
        if compact:
            line = f"{label_text}  {format_currency(value).rjust(12)}  {pct.rjust(4)}"
        else:
            d_pct = _format_delta_pct(value, prev_val)
            line = f"{label_text}  {format_currency(value).rjust(14)}  {pct.rjust(4)}  {str(d_pct).rjust(4)}"
        lines.append(line)

    return f"<pre>{header}\n" + "\n".join(lines) + "</pre>"


def _safe_short(value: str, max_len: int = 70) -> str:
    if not value:
        return ""
    value = value.strip()
    if len(value) > max_len:
        value = value[: max_len - 1] + "…"
    return escape_html(value)


def _render_largest(title: str, tx: Optional[Dict[str, object]]) -> str:
    if not tx:
        return f"<b>{title}:</b> —"
    desc = _safe_short(str(tx.get("description", "")))
    date = _safe_short(str(tx.get("date", "")), 30)
    category = escape_html(str(tx.get("category", "")))
    type_tag = "IN" if tx.get("type") == "income" else "OUT"
    message = f"<b>{title} ({type_tag}):</b> {format_currency(float(tx.get('amount', 0)))} · <b>{category}</b>"
    if date:
        message += f"\n<code>{date}</code>"
    if desc:
        message += f"\n<i>{desc}</i>"
    return message


def format_summary_message(transactions: List[Dict[str, object]], *, compact: bool = False) -> str:
    filtered = [
        tx
        for tx in transactions
        if not tx.get("isDeleted") and str(tx.get("transactionKind", "regular")).lower() not in {"loan", "transfer"}
    ]

    now = datetime.now(ZoneInfo("America/Bogota"))
    current_year = now.year
    current_month = now.month - 1
    current_day = now.day
    days_in_month = (datetime(now.year + (1 if now.month == 12 else 0), (now.month % 12) + 1, 1) - timedelta(days=1)).day

    prev_month = 11 if current_month == 0 else current_month - 1
    prev_year = current_year - 1 if current_month == 0 else current_year

    curr = _empty_agg()
    prev = _empty_agg()

    for tx in filtered:
        date_candidate = str(tx.get("date") or tx.get("createdAt") or "")
        ymd = _get_ymd_bogota(date_candidate)
        if not ymd:
            continue
        year, month_index, _day = ymd
        if year == current_year and month_index == current_month:
            _add_tx(curr, tx)
        if year == prev_year and month_index == prev_month:
            _add_tx(prev, tx)

    meses = [
        "Enero",
        "Febrero",
        "Marzo",
        "Abril",
        "Mayo",
        "Junio",
        "Julio",
        "Agosto",
        "Septiembre",
        "Octubre",
        "Noviembre",
        "Diciembre",
    ]

    if compact:
        header = f"📊 <b>Resumen · {meses[current_month]} {current_year}</b>\n<i>Día {current_day}/{days_in_month} · TZ: America/Bogota</i>"
    else:
        header = (
            f"📊 <b>Resumen · {meses[current_month]} {current_year}</b>\n"
            f"<i>Comparativo vs {meses[prev_month]} {prev_year} · Día {current_day}/{days_in_month} · TZ: America/Bogota</i>"
        )

    entradas_total = (
        f"<b>Total entradas:</b> {format_currency(curr.sum_income)}  "
        f"({_delta_icon(curr.sum_income, prev.sum_income)} {_format_delta_abs(curr.sum_income - prev.sum_income)} · {_format_delta_pct(curr.sum_income, prev.sum_income)})"
    )
    salidas_total = (
        f"<b>Total salidas:</b> {format_currency(curr.sum_expense)}  "
        f"({_delta_icon(curr.sum_expense, prev.sum_expense)} {_format_delta_abs(curr.sum_expense - prev.sum_expense)} · {_format_delta_pct(curr.sum_expense, prev.sum_expense)})"
    )

    entradas_block = (
        "🟢 <b>Entradas</b>\n"
        f"{_render_top_list(curr.totals_income, curr.sum_income, prev.totals_income, compact=compact)}\n"
        f"{entradas_total}\n"
        f"<b># Transacciones:</b> {curr.count_income} · <b>Promedio:</b> {format_currency(curr.sum_income / curr.count_income if curr.count_income else 0)}"
    )
    salidas_block = (
        "🔴 <b>Salidas</b>\n"
        f"{_render_top_list(curr.totals_expense, curr.sum_expense, prev.totals_expense, compact=compact)}\n"
        f"{salidas_total}\n"
        f"<b># Transacciones:</b> {curr.count_expense} · <b>Promedio:</b> {format_currency(curr.sum_expense / curr.count_expense if curr.count_expense else 0)}"
    )

    curr_net = curr.sum_income - curr.sum_expense
    prev_net = prev.sum_income - prev.sum_expense
    net_icon = "✅" if curr_net >= 0 else "⚠️"
    save_rate = (curr_net / curr.sum_income) if curr.sum_income else 0
    burn_rate = (curr.sum_expense / current_day) if current_day else 0
    projected_expense = burn_rate * days_in_month
    projected_net = curr.sum_income - projected_expense

    kpis = (
        "📌 <b>KPIs</b>\n"
        f"• <b>Neto:</b> {net_icon} {format_currency(curr_net)}  ({_delta_icon(curr_net, prev_net)} {_format_delta_abs(curr_net - prev_net)} · {_format_delta_pct(curr_net, prev_net)})\n"
        f"• <b>Tasa de ahorro:</b> {round(save_rate * 100)}%\n"
        f"• <b>Burn rate:</b> {format_currency(burn_rate)}/día\n"
        f"• <b>Proyección fin de mes:</b> Gasto {format_currency(projected_expense)} · Neto {format_currency(projected_net)}"
    )

    top_expense = sorted(curr.totals_expense.items(), key=lambda item: item[1], reverse=True)
    top_income = sorted(curr.totals_income.items(), key=lambda item: item[1], reverse=True)
    insights_lines = []
    if top_expense:
        insights_lines.append(
            f"• Mayor gasto: <b>{escape_html(str(top_expense[0][0]))}</b> ({round((top_expense[0][1] / curr.sum_expense) * 100) if curr.sum_expense else 0}%)"
        )
    if top_income:
        insights_lines.append(
            f"• Mayor ingreso: <b>{escape_html(str(top_income[0][0]))}</b> ({round((top_income[0][1] / curr.sum_income) * 100) if curr.sum_income else 0}%)"
        )
    if not insights_lines:
        insights_lines.append("• Sin movimientos este mes.")
    insights = "🔎 <b>Insights</b>\n" + "\n".join(insights_lines)

    destacados = (
        "🏷️ <b>Movimientos destacados</b>\n"
        f"{_render_largest('Mayor movimiento', curr.largest)}\n"
        f"{_render_largest('Mayor salida', curr.largest_expense)}"
    )

    message = "\n\n".join([header, entradas_block, salidas_block, kpis, insights, destacados])
    return message


def format_undo_message(result: Dict[str, object]) -> str:
    if not result.get("ok"):
        return "↩️ <b>Nada para deshacer</b>\nNo encontré movimientos recientes."

    amount = format_currency(float(result.get("amount", 0)), str(result.get("currency", "COP")))
    lines = [
        "↩️ <b>Último movimiento deshecho</b>",
        f"<b>Monto:</b> {amount}",
        f"<b>Categoría:</b> {escape_html(str(result.get('category', 'misc')))}",
    ]

    if result.get("date"):
        lines.append(f"<b>Fecha:</b> <code>{escape_html(str(result.get('date')))}</code>")
    if result.get("description"):
        lines.append(f"<b>Detalle:</b> <i>{escape_html(str(result.get('description')))}</i>")

    return "\n".join(lines)
