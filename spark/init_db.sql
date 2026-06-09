-- Runs automatically the first time the postgres container starts.
-- Creates the live-view tables the dashboard reads from.

CREATE TABLE IF NOT EXISTS alerts (
    id          SERIAL PRIMARY KEY,
    window_start TIMESTAMP   NOT NULL,
    window_end   TIMESTAMP   NOT NULL,
    ip           VARCHAR(64) NOT NULL,
    rule         VARCHAR(64) NOT NULL,
    count        INTEGER     NOT NULL,
    severity     VARCHAR(16) NOT NULL,
    created_at   TIMESTAMP   DEFAULT now()
);

CREATE TABLE IF NOT EXISTS event_counts (
    id           SERIAL PRIMARY KEY,
    window_start TIMESTAMP   NOT NULL,
    window_end   TIMESTAMP   NOT NULL,
    action       VARCHAR(32) NOT NULL,
    count        INTEGER     NOT NULL,
    created_at   TIMESTAMP   DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_alerts_created ON alerts (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_counts_window  ON event_counts (window_start DESC);
