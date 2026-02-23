# Expense Tracker Bot

Este proyecto es un bot para registrar gastos e ingresos.
Funciona con FastAPI y recibe mensajes desde:

- Telegram
- WhatsApp (usando Evolution API)

También puede:

- Entender texto libre (con Groq)
- Transcribir notas de voz
- Exportar transacciones a Excel
- Recordarte pagos recurrentes

## Qué hace

- Guarda gastos, ingresos, préstamos y transferencias.
- Activa usuarios con token: `/start TU-TOKEN`.
- Muestra lista de movimientos: `/list`.
- Muestra resumen del mes: `/summary`.
- Muestra recurrentes: `/recurrings` (también acepta `/recurrentes`).
- Descarga Excel: `/download` o `/descargar`.
- Deshace el último movimiento: `/undo`.
- Elimina todas las transacciones (con confirmación): `/clear`.
- Cancela todos los recurrentes (con confirmación): `/clear_recurrings`.

## Rutas del webhook

Internas (dentro del servicio):

- `POST /webhook` (Telegram)
- `POST /evolution/webhook` (Evolution)

Con Traefik en producción:

- `POST /expense/v1/webhook`
- `POST /expense/v1/evolution/webhook`

En `develop` usa `/expense-dev/v1`.

## Requisitos

- Python 3.11+
- PostgreSQL
- Token de Telegram

Opcional:

- Groq API key (para IA y voz)
- Redis (rate limit compartido) 

## Variables de entorno

Copia `.env.example` a `.env`.

Obligatorias:

- `BOT_TOKEN`
- `DATABASE_URL`

Recomendadas:

- `GROQ_API_KEY`
- `TELEGRAM_WEBHOOK_SECRET`

Opcionales:

- `ADMIN_TELEGRAM_CHAT_ID`
- `INVITE_ADMIN_API_KEY` (protege `GET /admin/invites`)
- `REDIS_URL`
- `EVOLUTION_API_URL`
- `EVOLUTION_API_KEY`
- `EVOLUTION_INSTANCE_NAME`
- `DB_SCHEMA`
- `MAX_INPUT_CHARS` (default `1200`)
- `GROQ_MAX_OUTPUT_TOKENS` (default `400`)
- `RATE_LIMIT_USER_PER_MIN` (default `60`)
- `RATE_LIMIT_IP_PER_MIN` (default `120`)
- `RATE_LIMIT_ONBOARDING_PER_MIN` (default `10`)

## Crear invite

Endpoint interno para emitir tokens de onboarding:

- `GET /admin/invites`
- Auth (cualquiera de las dos):
  - Header: `X-Admin-Api-Key: <INVITE_ADMIN_API_KEY>`
  - Query param: `api_key=<INVITE_ADMIN_API_KEY>`
- Query param opcional: `actor_user_id` (máx. 64 chars)

Ejemplo browser: `GET /admin/invites?api_key=TU_KEY&actor_user_id=USR-ADMIN-123`

Respuesta:

```json
{
  "ok": true,
  "inviteToken": "INV-...",
  "status": "unused",
  "startCommand": "/start INV-..."
}
```

## Cómo correrlo

Con Docker:

```bash
docker compose up -d --build
```

Sin Docker:

```bash
export PYTHONPATH=.
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## Migraciones

Recomendado (Alembic):

```bash
alembic upgrade head
```

Notas:

- `DATABASE_URL` es obligatorio para migrar.
- Si usas otro esquema, define `DB_SCHEMA`.
- También hay scripts SQL manuales en `migrations/sql/`.

## Ejemplos de mensajes

- `comí un pan 5k`
- `uber 12000 ayer`
- `salario 2500000`
- `le presté 200k a Juan`
- `me gasté 5k en comida y 60k en ropa y 80k en estuche`
- `Recuérdame pagar todos los 5 el internet`
- `recordatorios 12 3,1,0`
- `monto 12 45000`
- `pausar 12` / `activar 12` / `cancelar 12`
- `pausa netflix`
- `sube luz a 70k`
- `/clear_recurrings`

Si detecta varios montos en un solo mensaje, intentará crear múltiples transacciones.
Cuando haya baja confianza, pedirá confirmación con `sí` o `no` antes de guardar.

## Deploy automático

El workflow `.github/workflows/deploy.yml` despliega por rama:

- En `main` usa nombres base (sin sufijo):
  `APP_DIR=/root/apps/automations/expense-tracker-bot`,
  `IMAGE_NAME=expense-tracker-bot:latest`,
  `CONTAINER_NAME=expense-tracker-bot`,
  `COMPOSE_PROJECT_NAME=expense-bot`.
- En ramas no `main` usa sufijo por branch:
  `APP_DIR=/root/apps/automations/expense-tracker-bot-<branch>`,
  `IMAGE_NAME=expense-tracker-bot:<branch>`,
  `CONTAINER_NAME=expense-tracker-bot-<branch>`,
  `COMPOSE_PROJECT_NAME=expense-bot-<branch>`.

Notas:

- `main` mantiene `TRAEFIK_PATH_PREFIX=/expense/v1`.
- Las demás ramas usan `TRAEFIK_PATH_PREFIX=/expense-<branch>/v1`.
