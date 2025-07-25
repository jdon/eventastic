use eventastic::aggregate::Aggregate;
use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Arc;

/// Configuration for database tables used by an aggregate type.
///
/// This struct contains pre-computed SQL queries to avoid string allocation
/// during query execution.
#[derive(Debug, Clone)]
pub struct TableConfig {
    pub(crate) stream_events_query: String,
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
            ),
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

/// Registry that maps aggregate types to their table configurations.
///
/// This allows different aggregate types to use different tables while
/// supporting runtime configuration.
#[derive(Debug, Clone, Default)]
pub struct TableRegistry {
    tables: HashMap<TypeId, Arc<TableConfig>>,
}

impl TableRegistry {
    /// Create a new empty table registry.
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Register table configuration for an aggregate type.
    pub fn register<T: Aggregate + 'static>(&mut self, config: TableConfig) {
        self.tables.insert(TypeId::of::<T>(), Arc::new(config));
    }

    /// Get the stream events query for an aggregate type.
    pub fn stream_events_query<T: Aggregate + 'static>(&self) -> Option<&str> {
        self.tables
            .get(&TypeId::of::<T>())
            .map(|config| config.stream_events_query.as_str())
    }

    /// Get the get event query for an aggregate type.
    pub fn get_event_query<T: Aggregate + 'static>(&self) -> Option<&str> {
        self.tables
            .get(&TypeId::of::<T>())
            .map(|config| config.get_event_query.as_str())
    }

    /// Get the get snapshot query for an aggregate type.
    pub fn get_snapshot_query<T: Aggregate + 'static>(&self) -> Option<&str> {
        self.tables
            .get(&TypeId::of::<T>())
            .map(|config| config.get_snapshot_query.as_str())
    }

    /// Get the insert events query for an aggregate type.
    pub fn insert_events_query<T: Aggregate + 'static>(&self) -> Option<&str> {
        self.tables
            .get(&TypeId::of::<T>())
            .map(|config| config.insert_events_query.as_str())
    }

    /// Get the upsert snapshot query for an aggregate type.
    pub fn upsert_snapshot_query<T: Aggregate + 'static>(&self) -> Option<&str> {
        self.tables
            .get(&TypeId::of::<T>())
            .map(|config| config.upsert_snapshot_query.as_str())
    }
}

/// Builder for creating a TableRegistry with a fluent API.
pub struct TableRegistryBuilder {
    registry: TableRegistry,
}

impl TableRegistryBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self {
            registry: TableRegistry::new(),
        }
    }

    /// Register table configuration for an aggregate type.
    pub fn register<T: Aggregate + 'static>(mut self, config: TableConfig) -> Self {
        self.registry.register::<T>(config);
        self
    }

    /// Register table configuration for an aggregate type with explicit table names.
    pub fn register_with_tables<T: Aggregate + 'static>(
        mut self,
        events: impl Into<String>,
        snapshots: impl Into<String>,
    ) -> Self {
        self.registry
            .register::<T>(TableConfig::new(events, snapshots));
        self
    }

    /// Build the TableRegistry.
    pub fn build(self) -> TableRegistry {
        self.registry
    }
}

impl Default for TableRegistryBuilder {
    fn default() -> Self {
        Self::new()
    }
}
