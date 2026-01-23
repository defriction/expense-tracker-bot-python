from __future__ import annotations

from typing import Any, Dict

from app.bot.ui_models import BotKeyboard, BotMessage


def build_whatsapp_payload(to: str, message: BotMessage) -> Dict[str, Any]:
    keyboard = _normalize_keyboard(message.keyboard)
    if not keyboard:
        return {
            "messaging_product": "whatsapp",
            "to": to,
            "type": "text",
            "text": {"body": message.text},
        }

    buttons = []
    for idx, action in enumerate(keyboard, start=1):
        buttons.append(
            {
                "type": "reply",
                "reply": {"id": action["id"], "title": action["label"]},
            }
        )

    return {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": message.text},
            "action": {"buttons": buttons},
        },
    }


def _normalize_keyboard(keyboard: BotKeyboard | None) -> list[dict[str, str]]:
    if not keyboard or not keyboard.rows:
        return []
    actions = []
    for row in keyboard.rows:
        for action in row:
            actions.append({"id": action.id, "label": action.label})
            if len(actions) >= 3:
                return actions
    return actions
