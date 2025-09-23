use std::sync::Arc;

/// Configuration for database tables used by an aggregate type.
///
/// This struct contains pre-computed SQL queries to avoid string allocation
/// during query execution.
#[derive(Debug, Clone)]
pub struct TableConfig {
    pub(crate) stream_events_query: Arc<str>,
    pub(crate) get_event_query: String,
    pub(crate) get_snapshot_query: String,
    pub(crate) insert_events_query: String,
    pub(crate) upsert_snapshot_query: String,
}

impl TableConfig {
    /// Create a new TableConfig with pre-computed queries.
    pub fn new(events: impl Into<String>, snapshots: impl Into<String>) -> Self {
        let events = events.into();
        let snapshots = snapshots.into();

        Self {
            stream_events_query: format!(
                "SELECT event, event_id, version FROM {} WHERE aggregate_id = $1 AND version >= $2 ORDER BY version ASC",
                &events
            ).into(),
            get_event_query: format!(
                "SELECT event, event_id, version FROM {} WHERE aggregate_id = $1 AND event_id = $2",
                &events
            ),
            get_snapshot_query: format!(
                "SELECT aggregate, version, snapshot_version FROM {} WHERE aggregate_id = $1 AND snapshot_version = $2",
                &snapshots
            ),
            insert_events_query: format!(
                "INSERT INTO {} (event_id, version, aggregate_id, event, created_at) \
                 SELECT * FROM UNNEST($1::uuid[], $2::bigint[], $3::uuid[], $4::bytea[], $5::timestamptz[]) \
                 ON CONFLICT DO NOTHING returning event_id",
                &events
            ),
            upsert_snapshot_query: format!(
                "INSERT INTO {} (aggregate_id, aggregate, version, snapshot_version, created_at) \
                 VALUES ($1, $2, $3, $4, $5) \
                 ON CONFLICT (aggregate_id, snapshot_version) DO UPDATE SET aggregate = $2, version = $3, created_at = $5",
                &snapshots
            ),
        }
    }
}
