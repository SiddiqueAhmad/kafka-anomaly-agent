CREATE TABLE IF NOT EXISTS orders_raw (
    id           TEXT,
    event        TEXT,
    customer_id  TEXT,
    total        FLOAT,
    status       TEXT,
    raw_payload  JSONB,
    consumed_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS anomalies (
    id            SERIAL PRIMARY KEY,
    order_id      TEXT,
    anomaly_type  TEXT,
    severity      TEXT,
    detail        JSONB,
    processed     BOOLEAN DEFAULT FALSE,
    detected_at   TIMESTAMP DEFAULT NOW()
);

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS incident_reports (
    id             SERIAL PRIMARY KEY,
    severity       TEXT,
    anomaly_types  TEXT[],
    report_text    TEXT,
    embedding      vector(1536),   -- matches embedder output_dimensionality=1536
    confidence_score INTEGER,      -- parsed 0-100 score from the report
    created_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS llm_usage (
    id            SERIAL PRIMARY KEY,
    run_id        TEXT,
    agent         TEXT,
    input_tokens  INTEGER,
    output_tokens INTEGER,
    total_tokens  INTEGER,
    cost_usd      NUMERIC(10, 6),
    created_at    TIMESTAMP DEFAULT NOW()
);