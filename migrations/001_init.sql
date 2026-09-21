CREATE TABLE IF NOT EXISTS events (
    id UUID PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    source VARCHAR(100) NOT NULL DEFAULT 'api',
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    retry_count INT NOT NULL DEFAULT 0,
    error_message TEXT,
    produced_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_events_status ON events (status);
CREATE INDEX IF NOT EXISTS idx_events_event_type ON events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_produced_at ON events (produced_at);

-- Append-only audit trail of everything that exhausted retries, independent
-- of the mutable `events.status` field, so DLQ history survives even if a
-- row is later reprocessed and its status changes back to completed.
CREATE TABLE IF NOT EXISTS dead_letter_log (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    error_context TEXT,
    dead_lettered_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dead_letter_log_event_id ON dead_letter_log (event_id);
