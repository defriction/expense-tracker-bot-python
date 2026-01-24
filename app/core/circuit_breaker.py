from __future__ import annotations

import time
from typing import Callable, Optional


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 3,
        recovery_seconds: int = 30,
        success_threshold: int = 2,
        on_state_change: Optional[Callable[[str, str], None]] = None,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_seconds = recovery_seconds
        self.success_threshold = success_threshold
        self._state = "closed"
        self._failure_count = 0
        self._success_count = 0
        self._opened_at: Optional[float] = None
        self._on_state_change = on_state_change

    @property
    def state(self) -> str:
        return self._state

    def allow(self) -> bool:
        if self._state == "closed":
            return True
        if self._state == "open":
            if self._opened_at is None:
                return False
            if time.time() - self._opened_at >= self.recovery_seconds:
                self._transition("half_open")
                self._success_count = 0
                return True
            return False
        return True

    def record_success(self) -> None:
        if self._state == "half_open":
            self._success_count += 1
            if self._success_count >= self.success_threshold:
                self._transition("closed")
                self._failure_count = 0
                self._success_count = 0
        elif self._state == "closed":
            self._failure_count = 0

    def record_failure(self) -> None:
        if self._state == "half_open":
            self._open()
            return
        self._failure_count += 1
        if self._failure_count >= self.failure_threshold:
            self._open()

    def _open(self) -> None:
        self._transition("open")
        self._opened_at = time.time()
        self._failure_count = 0
        self._success_count = 0

    def _transition(self, new_state: str) -> None:
        if new_state == self._state:
            return
        old_state = self._state
        self._state = new_state
        if self._on_state_change:
            self._on_state_change(old_state, new_state)


def guarded_call(breaker: CircuitBreaker, fn: Callable, *args, **kwargs):
    if not breaker.allow():
        raise RuntimeError("Circuit breaker is open")
    try:
        result = fn(*args, **kwargs)
    except Exception:
        breaker.record_failure()
        raise
    breaker.record_success()
    return result
