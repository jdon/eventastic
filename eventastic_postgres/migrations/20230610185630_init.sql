CREATE TABLE if not exists events (
    event_id uuid PRIMARY KEY,
		version bigint NOT NULL,
    aggregate_id uuid NOT NULL,
    event jsonb NOT NULL,
    created_at timestamptz NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS aggregate_version ON events (version, aggregate_id);

CREATE TABLE if not exists snapshots (
  aggregate_id uuid PRIMARY KEY,
  snapshot jsonb NOT NULL,
  created_at timestamptz NOT NULL
);

CREATE TABLE if not exists outbox (
  id uuid PRIMARY KEY,
  message jsonb NOT NULL,
  retries integer NOT NULL,
  requeue boolean NOT NULL,
  created_at timestamptz NOT NULL
);