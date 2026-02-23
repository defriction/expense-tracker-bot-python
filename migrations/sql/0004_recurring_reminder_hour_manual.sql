-- Manual SQL migration 0004_recurring_reminder_hour

BEGIN;

ALTER TABLE recurring_expenses
    ADD COLUMN IF NOT EXISTS reminder_hour INTEGER;

UPDATE recurring_expenses
SET reminder_hour = 9
WHERE reminder_hour IS NULL;

ALTER TABLE recurring_expenses
    ALTER COLUMN reminder_hour SET DEFAULT 9,
    ALTER COLUMN reminder_hour SET NOT NULL;

COMMIT;
