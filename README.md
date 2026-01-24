# Telegram Webhook Bot (FastAPI) + Traefik

## What this is
Telegram webhook bot (FastAPI) that mirrors the n8n Finance Bot workflow:
onboarding with invite tokens, Google Sheets storage, AI parsing (Groq), voice
transcription, list/summary/undo commands.

## Project structure
- `app/core`: settings + shared utilities
- `app/services`: reusable integrations (Groq, Google Sheets, Telegram)
- `app/routers`: reusable FastAPI routers (Telegram webhook)
- `app/bot`: bot-specific workflow (parser, formatters, pipeline, handlers)

## Local quick run (optional)
- Copy `.env.example` to `.env` and set:
  - `BOT_TOKEN`
  - `GROQ_API_KEY`
  - `GOOGLE_SHEETS_ID`
  - `GOOGLE_SERVICE_ACCOUNT_JSON` (inline JSON) or `GOOGLE_SERVICE_ACCOUNT_FILE`
- `ADMIN_TELEGRAM_CHAT_ID` is optional (admin notifications).
- `docker compose up -d --build`

Local run without Docker:
```
set PYTHONPATH=.
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## Production
This project is designed to sit behind Traefik and expose:
- https://bot.srv1153123.hstgr.cloud/webhook

## Sheets schema
This bot expects the following sheets (same as the n8n workflow):
- `Users` (userId, telegramUserId, chatId, status, createdAt, lastSeenAt)
- `Invites` (inviteToken, status, expiresAt, usedAt, createdAt)
- `Transactions` (txId, userId, type, transactionKind, date, amount, currency, category, description, rawText, normalizedMerchant, paymentMethod, counterparty, loanRole, loanId, isRecurring, recurrence, recurrenceId, source, sourceMessageId, parseConfidence, parserVersion, createdAt, updatedAt, isDeleted, deletedAt)
- `ErrorLogs` (timestamp, workflow, node, message)

## Google auth
Create a Google service account, enable the Sheets API, and share the target
spreadsheet with the service account email. Then set credentials via
`GOOGLE_SERVICE_ACCOUNT_JSON` or `GOOGLE_SERVICE_ACCOUNT_FILE`.
