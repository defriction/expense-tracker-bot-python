from __future__ import annotations

import logging
import uuid
from contextvars import ContextVar
from typing import Optional

_trace_id: ContextVar[str] = ContextVar("trace_id", default="-")
_client_ip: ContextVar[str] = ContextVar("client_ip", default="-")
_channel: ContextVar[str] = ContextVar("channel", default="-")
_chat_id: ContextVar[str] = ContextVar("chat_id", default="-")
_user_id: ContextVar[str] = ContextVar("user_id", default="-")
_message_id: ContextVar[str] = ContextVar("message_id", default="-")

logger = logging.getLogger("expense_bot")


def set_trace_id(value: Optional[str] = None) -> str:
    trace = value or uuid.uuid4().hex
    _trace_id.set(trace)
    return trace


def get_trace_id() -> str:
    return _trace_id.get()


def set_client_ip(value: Optional[str] = None) -> str:
    ip = value or "-"
    _client_ip.set(ip)
    return ip


def get_client_ip() -> str:
    return _client_ip.get()


def set_log_context(
    channel: Optional[str] = None,
    chat_id: Optional[str | int] = None,
    user_id: Optional[str | int] = None,
    message_id: Optional[str | int] = None,
) -> None:
    _channel.set(channel or "-")
    _chat_id.set(str(chat_id) if chat_id is not None else "-")
    _user_id.set(str(user_id) if user_id is not None else "-")
    _message_id.set(str(message_id) if message_id is not None else "-")


def get_channel() -> str:
    return _channel.get()


def get_chat_id() -> str:
    return _chat_id.get()


def get_user_id() -> str:
    return _user_id.get()


def get_message_id() -> str:
    return _message_id.get()


class TraceIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.trace_id = get_trace_id()
        record.client_ip = get_client_ip()
        record.channel = get_channel()
        record.chat_id = get_chat_id()
        record.user_id = get_user_id()
        record.message_id = get_message_id()
        return True


def setup_logging(level: str = "INFO") -> None:
    if logger.handlers:
        return
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s trace_id=%(trace_id)s channel=%(channel)s "
        "chat_id=%(chat_id)s user_id=%(user_id)s message_id=%(message_id)s "
        "client_ip=%(client_ip)s - %(message)s"
    )
    handler.setFormatter(formatter)
    handler.addFilter(TraceIdFilter())
    logger.addHandler(handler)
    logger.setLevel(level)
