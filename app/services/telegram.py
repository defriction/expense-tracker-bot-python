from __future__ import annotations

from typing import Iterable, Optional

from telegram.ext import Application, BaseHandler


def build_telegram_app(
    token: str,
    handlers: Iterable[BaseHandler],
    error_handler: Optional[callable] = None,
) -> Application:
    telegram_app = Application.builder().token(token).build()
    for handler in handlers:
        telegram_app.add_handler(handler)
    if error_handler:
        telegram_app.add_error_handler(error_handler)
    return telegram_app
