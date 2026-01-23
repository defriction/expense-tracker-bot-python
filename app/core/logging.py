from __future__ import annotations

import logging
import uuid
from contextvars import ContextVar
from typing import Optional

_trace_id: ContextVar[str] = ContextVar("trace_id", default="-")

logger = logging.getLogger("expense_bot")


def set_trace_id(value: Optional[str] = None) -> str:
    trace = value or uuid.uuid4().hex
    _trace_id.set(trace)
    return trace


def get_trace_id() -> str:
    return _trace_id.get()


class TraceIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.trace_id = get_trace_id()
        return True


def setup_logging(level: str = "INFO") -> None:
    if logger.handlers:
        return
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s trace_id=%(trace_id)s - %(message)s")
    handler.setFormatter(formatter)
    handler.addFilter(TraceIdFilter())
    logger.addHandler(handler)
    logger.setLevel(level)
