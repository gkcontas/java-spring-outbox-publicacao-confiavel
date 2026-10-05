CREATE TABLE products (
    id         UUID           PRIMARY KEY,
    sku        VARCHAR(40)    NOT NULL UNIQUE,
    name       VARCHAR(160)   NOT NULL,
    price      NUMERIC(12, 2) NOT NULL CHECK (price > 0),
    created_at TIMESTAMPTZ    NOT NULL,
    updated_at TIMESTAMPTZ    NOT NULL
);

-- The outbox. One row per event, written in the same transaction as the change it
-- describes, which is the whole point: either both are committed or neither is.
CREATE TABLE outbox_event (
    id             UUID         PRIMARY KEY,
    aggregate_type VARCHAR(40)  NOT NULL,
    aggregate_id   VARCHAR(64)  NOT NULL,
    event_type     VARCHAR(60)  NOT NULL,
    payload        TEXT         NOT NULL,
    status         VARCHAR(20)  NOT NULL,
    attempts       INT          NOT NULL DEFAULT 0,
    last_error     TEXT,
    created_at     TIMESTAMPTZ  NOT NULL,
    next_attempt_at TIMESTAMPTZ NOT NULL,
    published_at   TIMESTAMPTZ
);

-- The publisher's only query: the oldest pending rows that are due. A partial index keeps
-- it reading just those, instead of scanning a table that grows with every event the
-- system has ever emitted.
CREATE INDEX idx_outbox_pending
    ON outbox_event (next_attempt_at, created_at)
    WHERE status = 'PENDING';

-- Used by the retention job, which deletes what has already been delivered.
CREATE INDEX idx_outbox_published ON outbox_event (published_at)
    WHERE status = 'PUBLISHED';

-- The downstream view, maintained only by consuming events. Nothing writes here from the
-- catalogue side, so anything lost in publication is visibly missing from this table.
CREATE TABLE product_projection (
    product_id     UUID           PRIMARY KEY,
    sku            VARCHAR(40)    NOT NULL,
    name           VARCHAR(160)   NOT NULL,
    price          NUMERIC(12, 2) NOT NULL,
    last_event_id  UUID           NOT NULL,
    updated_at     TIMESTAMPTZ    NOT NULL
);

-- Deduplication for the consumer: at-least-once delivery means the same event id will
-- eventually arrive twice, and the consumer has to make the second one a no-op.
CREATE TABLE processed_event (
    event_id     UUID        PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL
);
