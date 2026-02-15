-- Manual SQL migration 0002_recurring (current schema)

BEGIN;

CREATE TABLE IF NOT EXISTS recurring_expenses (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    recurrence_id VARCHAR(64) NOT NULL,
    normalized_merchant VARCHAR(128) NULL,
    description TEXT NULL,
    category VARCHAR(64) NOT NULL,
    amount NUMERIC(18,2) NOT NULL,
    currency VARCHAR(8) NOT NULL,
    recurrence VARCHAR(16) NOT NULL,
    billing_day INTEGER NULL,
    billing_weekday INTEGER NULL,
    billing_month INTEGER NULL,
    anchor_date DATE NULL,
    timezone VARCHAR(64) NOT NULL DEFAULT 'America/Bogota',
    payment_link TEXT NULL,
    payment_reference TEXT NULL,
    remind_offsets JSONB NOT NULL DEFAULT '[]'::jsonb,
    next_due DATE NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'pending',
    source_tx_id VARCHAR(64) NULL,
    last_confirmed_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT uq_recurring_user_recurrence_id UNIQUE (user_id, recurrence_id)
);

CREATE INDEX IF NOT EXISTS ix_recurring_expenses_user_id
    ON recurring_expenses (user_id);

CREATE TABLE IF NOT EXISTS recurring_events (
    id SERIAL PRIMARY KEY,
    recurring_id INTEGER NOT NULL REFERENCES recurring_expenses(id) ON DELETE CASCADE,
    reminder_date DATE NOT NULL,
    reminder_offset INTEGER NOT NULL,
    due_date DATE NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'pending',
    sent_at TIMESTAMPTZ NULL,
    paid_at TIMESTAMPTZ NULL,
    tx_id VARCHAR(64) NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT uq_recurring_event_once UNIQUE (recurring_id, reminder_date, reminder_offset)
);

CREATE INDEX IF NOT EXISTS ix_recurring_events_recurring_id
    ON recurring_events (recurring_id);

CREATE INDEX IF NOT EXISTS ix_recurring_events_reminder_date
    ON recurring_events (reminder_date);

CREATE TABLE IF NOT EXISTS bot_pending_actions (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    action_type VARCHAR(64) NOT NULL,
    state JSONB NOT NULL,
    expires_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT uq_pending_action_user_type UNIQUE (user_id, action_type)
);

COMMIT;
