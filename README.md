# Telegram Webhook Bot (FastAPI) + Traefik

## What this is
Minimal Telegram bot that receives updates via webhook at `/webhook` and replies to `/start`.

## Local quick run (optional)
- Copy `.env.example` to `.env` and set `BOT_TOKEN`.
- `docker compose up -d --build`

## Production
This project is designed to sit behind Traefik and expose:
- https://bot.srv1153123.hstgr.cloud/webhook
