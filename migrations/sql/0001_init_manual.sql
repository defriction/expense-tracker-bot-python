-- Manual SQL migration 0001_init (current schema)

BEGIN;

CREATE TABLE IF NOT EXISTS alembic_version (
    version_num VARCHAR(32) NOT NULL PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS users (
    id VARCHAR(64) PRIMARY KEY,
    status VARCHAR(16) NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NULL
);

CREATE TABLE IF NOT EXISTS user_identities (
    id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    channel VARCHAR(32) NOT NULL,
    external_user_id VARCHAR(128) NOT NULL,
    external_chat_id VARCHAR(128) NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_user_identity_channel_user
    ON user_identities (channel, external_user_id);

CREATE TABLE IF NOT EXISTS invites (
    invite_token VARCHAR(128) PRIMARY KEY,
    status VARCHAR(16) NOT NULL DEFAULT 'unused',
    created_at TIMESTAMPTZ NOT NULL,
    used_at TIMESTAMPTZ NULL,
    used_by_user_id VARCHAR(64) NULL REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS transactions (
    tx_id VARCHAR(64) PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    type VARCHAR(16) NOT NULL,
    transaction_kind VARCHAR(16) NOT NULL,
    amount NUMERIC(18,2) NOT NULL,
    currency VARCHAR(8) NOT NULL,
    category VARCHAR(64) NOT NULL,
    description TEXT NULL,
    date DATE NULL,
    normalized_merchant VARCHAR(128) NULL,
    payment_method VARCHAR(32) NULL,
    counterparty VARCHAR(128) NULL,
    loan_role VARCHAR(32) NULL,
    loan_id VARCHAR(64) NULL,
    is_recurring BOOLEAN NOT NULL DEFAULT false,
    recurrence VARCHAR(16) NULL,
    recurrence_id VARCHAR(64) NULL,
    parse_confidence NUMERIC(4,2) NULL,
    parser_version VARCHAR(32) NULL,
    source VARCHAR(32) NULL,
    source_message_id VARCHAR(64) NULL,
    raw_text TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    is_deleted BOOLEAN NOT NULL DEFAULT false,
    deleted_at TIMESTAMPTZ NULL,
    chat_id VARCHAR(64) NULL
);

CREATE INDEX IF NOT EXISTS ix_transactions_user_created
    ON transactions (user_id, created_at);

CREATE INDEX IF NOT EXISTS ix_transactions_user_date
    ON transactions (user_id, date);

CREATE TABLE IF NOT EXISTS error_logs (
    id BIGSERIAL PRIMARY KEY,
    occurred_at TIMESTAMPTZ NOT NULL,
    workflow VARCHAR(128) NOT NULL,
    node VARCHAR(64) NOT NULL,
    message TEXT NOT NULL,
    user_id VARCHAR(64) NULL,
    chat_id VARCHAR(64) NULL
);

CREATE TABLE IF NOT EXISTS audit_events (
    id BIGSERIAL PRIMARY KEY,
    entity_type VARCHAR(64) NOT NULL,
    entity_id VARCHAR(128) NOT NULL,
    action VARCHAR(32) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    actor_user_id VARCHAR(64) NULL,
    source VARCHAR(32) NULL
);

CREATE INDEX IF NOT EXISTS ix_audit_entity
    ON audit_events (entity_type, entity_id);

COMMIT;
