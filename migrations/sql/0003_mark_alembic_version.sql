-- Mark current schema as migrated to 0003_recurring_billing_instances

BEGIN;

CREATE TABLE IF NOT EXISTS alembic_version (
    version_num VARCHAR(32) NOT NULL PRIMARY KEY
);

DELETE FROM alembic_version;
INSERT INTO alembic_version(version_num)
VALUES ('0003_recurring_billing_instances');

COMMIT;
