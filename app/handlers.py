from telegram.ext import CommandHandler

from .bot import start

def get_handlers():
    return [
        CommandHandler("start", start),
    ]
