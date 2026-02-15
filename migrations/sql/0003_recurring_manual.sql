-- Manual SQL migration 0003_recurring_billing_instances (current schema)

BEGIN;

CREATE TABLE IF NOT EXISTS alembic_version (
    version_num VARCHAR(32) NOT NULL PRIMARY KEY
);

ALTER TABLE recurring_expenses ADD COLUMN IF NOT EXISTS service_name VARCHAR(128);
ALTER TABLE recurring_expenses ADD COLUMN IF NOT EXISTS auto_add_transaction BOOLEAN NOT NULL DEFAULT true;
ALTER TABLE recurring_expenses ADD COLUMN IF NOT EXISTS canceled_at TIMESTAMPTZ NULL;

UPDATE recurring_expenses
SET service_name = COALESCE(NULLIF(normalized_merchant, ''), NULLIF(description, ''), 'Pago recurrente')
WHERE service_name IS NULL;

ALTER TABLE recurring_expenses
    ALTER COLUMN service_name SET NOT NULL;

CREATE TABLE IF NOT EXISTS bill_instances (
    id SERIAL PRIMARY KEY,
    recurring_id INTEGER NOT NULL REFERENCES recurring_expenses(id) ON DELETE CASCADE,
    period_year INTEGER NOT NULL,
    period_month INTEGER NOT NULL,
    due_date DATE NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'pending',
    amount NUMERIC(18,2) NULL,
    payment_link TEXT NULL,
    reference_number TEXT NULL,
    paid_at TIMESTAMPTZ NULL,
    tx_id VARCHAR(64) NULL,
    follow_up_on DATE NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT uq_bill_instance_period UNIQUE (recurring_id, period_year, period_month)
);

CREATE INDEX IF NOT EXISTS ix_bill_instances_due_date
    ON bill_instances (due_date);

CREATE INDEX IF NOT EXISTS ix_bill_instances_status
    ON bill_instances (status);

CREATE TABLE IF NOT EXISTS bill_instance_reminders (
    id SERIAL PRIMARY KEY,
    bill_instance_id INTEGER NOT NULL REFERENCES bill_instances(id) ON DELETE CASCADE,
    reminder_offset INTEGER NOT NULL,
    scheduled_for DATE NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'pending',
    sent_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT uq_bill_reminder_once UNIQUE (bill_instance_id, reminder_offset, scheduled_for)
);

CREATE INDEX IF NOT EXISTS ix_bill_instance_reminders_scheduled_for
    ON bill_instance_reminders (scheduled_for);

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
