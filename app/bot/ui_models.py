from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass(frozen=True)
class BotAction:
    id: str
    label: str


@dataclass(frozen=True)
class BotKeyboard:
    rows: List[List[BotAction]] = field(default_factory=list)


@dataclass(frozen=True)
class BotMessage:
    text: str
    keyboard: Optional[BotKeyboard] = None
    disable_web_preview: bool = True
    document_bytes: Optional[bytes] = None
    document_name: Optional[str] = None
    document_mime: Optional[str] = None


@dataclass(frozen=True)
class BotInput:
    channel: str
    chat_id: Optional[str | int]
    user_id: Optional[str | int]
    text: Optional[str]
    message_id: Optional[str]
    audio_bytes: Optional[bytes]
    non_text_type: Optional[str]
