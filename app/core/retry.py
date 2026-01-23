from __future__ import annotations

import asyncio
import time
from typing import Callable, Tuple, Type


def sync_retry(
    fn: Callable,
    retries: int = 2,
    backoff_seconds: float = 0.5,
    retry_exceptions: Tuple[Type[BaseException], ...] = (Exception,),
    on_retry: Callable[[int, BaseException], None] | None = None,
):
    last_exc = None
    for attempt in range(retries + 1):
        try:
            return fn()
        except retry_exceptions as exc:
            last_exc = exc
            if attempt >= retries:
                break
            if on_retry:
                on_retry(attempt + 1, exc)
            time.sleep(backoff_seconds * (2**attempt))
    if last_exc:
        raise last_exc


async def async_retry(
    fn: Callable,
    retries: int = 2,
    backoff_seconds: float = 0.5,
    retry_exceptions: Tuple[Type[BaseException], ...] = (Exception,),
    on_retry: Callable[[int, BaseException], None] | None = None,
):
    last_exc = None
    for attempt in range(retries + 1):
        try:
            return await fn()
        except retry_exceptions as exc:
            last_exc = exc
            if attempt >= retries:
                break
            if on_retry:
                on_retry(attempt + 1, exc)
            await asyncio.sleep(backoff_seconds * (2**attempt))
    if last_exc:
        raise last_exc
