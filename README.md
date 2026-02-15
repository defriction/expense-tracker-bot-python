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
- Muestra recurrentes: `/recurrentes`.
- Descarga Excel: `/download` o `/descargar`.
- Deshace el último movimiento: `/undo`.

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
- `INVITE_ADMIN_API_KEY` (protege `POST /admin/invites`)
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

- `POST /admin/invites`
- Header requerido: `X-Admin-Api-Key: <INVITE_ADMIN_API_KEY>`
- Body opcional:

```json
{
  "actor_user_id": "USR-ADMIN-123"
}
```

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

Si detecta varios montos en un solo mensaje, intentará crear múltiples transacciones.
Cuando haya baja confianza, pedirá confirmación con `sí` o `no` antes de guardar.

## Deploy automático

El workflow `.github/workflows/deploy.yml` despliega por rama:

- `main`/`master`: producción
- `develop`: desarrollo

Hace copia al VPS y levanta con Docker Compose.
