CREATE TABLE if not exists events (
    aggregate_id uuid NOT NULL,
    version bigint NOT NULL CHECK (version >= 0),
    event_id uuid NOT NULL,
    event jsonb NOT NULL,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (aggregate_id, version)
);

CREATE UNIQUE INDEX IF NOT EXISTS events_event_id ON events (event_id);

CREATE TABLE if not exists snapshots (
  aggregate_id uuid PRIMARY KEY,
  aggregate jsonb NOT NULL,
  version bigint NOT NULL CHECK (version >= 0),
  snapshot_version bigint NOT NULL,
  created_at timestamptz NOT NULL
);

CREATE TABLE if not exists outbox (
  id uuid PRIMARY KEY,
  message jsonb NOT NULL,
  retries integer NOT NULL CHECK (retries >= 0),
  requeue boolean NOT NULL,
  created_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS snapshots_lookup ON snapshots(aggregate_id, snapshot_version);