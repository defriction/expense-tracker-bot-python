from __future__ import annotations

import logging
import uuid
from contextvars import ContextVar
from typing import Optional

_trace_id: ContextVar[str] = ContextVar("trace_id", default="-")
_client_ip: ContextVar[str] = ContextVar("client_ip", default="-")

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


class TraceIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.trace_id = get_trace_id()
        record.client_ip = get_client_ip()
        return True


def setup_logging(level: str = "INFO") -> None:
    if logger.handlers:
        return
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s trace_id=%(trace_id)s client_ip=%(client_ip)s - %(message)s"
    )
    handler.setFormatter(formatter)
    handler.addFilter(TraceIdFilter())
    logger.addHandler(handler)
    logger.setLevel(level)
