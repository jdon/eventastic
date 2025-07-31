//! Common data structures and utilities shared across PostgreSQL implementations.
//!
//! This module contains shared code used by [`PostgresTransaction`] to avoid
//! duplication and ensure consistency.

use crate::DbError;
use crate::pickle::Pickle;
use anyhow::Context;
use eventastic::aggregate::Aggregate;
use eventastic::event::{DomainEvent, EventStoreEvent};
use eventastic::repository::Snapshot;
use sqlx::types::Uuid;

/// Internal representation of a database row containing event data.
///
/// This struct is used to deserialize event rows from the database
/// before converting them to the full [`EventStoreEvent`] type.
#[derive(Debug, sqlx::FromRow)]
pub(crate) struct PartialEventRow {
    pub event_id: Uuid,
    pub version: i64,
    pub event: Vec<u8>,
}

impl PartialEventRow {
    /// Converts a [`PartialEventRow`] to an [`EventStoreEvent`].
    ///
    /// This function handles deserialization of the JSON event data and
    /// validation of the version number, providing consistent error handling
    /// across different database operations.
    ///
    /// # Type Parameters
    ///
    /// - `Evt` - The domain event type that implements [`DomainEvent`]
    ///
    /// # Errors
    ///
    /// Returns [`DbError::InvalidVersionNumber`] if the version cannot be converted to u64.
    /// Returns [`DbError::PicklingError`] if the event JSON cannot be deserialized.
    pub fn to_event<Evt, E>(row: PartialEventRow) -> Result<EventStoreEvent<Evt>, DbError<E>>
    where
        Evt: DomainEvent<EventId = Uuid> + Pickle,
    {
        let row_version = u64::try_from(row.version).map_err(|_| DbError::InvalidVersionNumber)?;

        Evt::unpickle(&row.event)
            .map(|e| EventStoreEvent {
                id: row.event_id,
                event: e,
                version: row_version,
            })
            .context("Failed to unpickle event")
            .map_err(DbError::PicklingError)
    }
}

/// Internal representation of a database row containing snapshot data.
///
/// This struct is used to deserialize snapshot rows from the database
/// before converting them to the full [`Snapshot`] type.
#[derive(sqlx::FromRow)]
pub(crate) struct PartialSnapshotRow {
    pub aggregate: Vec<u8>,
    pub snapshot_version: i64,
    pub version: i64,
}

impl PartialSnapshotRow {
    /// Converts a [`PartialSnapshotRow`] to a [`Snapshot`].
    ///
    /// This function handles deserialization of the JSON aggregate data and
    /// validation of version numbers, providing consistent error handling
    /// across different database operations.
    ///
    /// # Type Parameters
    ///
    /// - `T` - The aggregate type that implements [`Aggregate`]
    ///
    /// # Errors
    ///
    /// Returns [`DbError::InvalidVersionNumber`] if the version cannot be converted to u64.
    /// Returns [`DbError::InvalidSnapshotVersion`] if the snapshot version cannot be converted to u64.
    /// Returns [`DbError::PicklingError`] if the aggregate JSON cannot be deserialized.
    pub fn to_snapshot<T, E>(row: PartialSnapshotRow) -> Result<Snapshot<T>, DbError<E>>
    where
        T: Aggregate + Pickle,
    {
        let version = u64::try_from(row.version).map_err(|_| DbError::InvalidVersionNumber)?;
        let snapshot_version =
            u64::try_from(row.snapshot_version).map_err(|_| DbError::InvalidSnapshotVersion)?;
        let aggregate: T = T::unpickle(&row.aggregate)
            .context("Failed to unpickle aggregate")
            .map_err(DbError::PicklingError)?;

        Ok(Snapshot {
            aggregate,
            version,
            snapshot_version,
        })
    }
}

/// Utility functions for common validation and conversion operations.
pub(crate) mod utils {
    use crate::DbError;

    /// Converts a u64 version to i64 for database storage.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::InvalidVersionNumber`] if the conversion fails.
    pub fn version_to_i64<E>(version: u64) -> Result<i64, DbError<E>> {
        i64::try_from(version).map_err(|_| DbError::InvalidVersionNumber)
    }

    /// Converts a u64 snapshot version to i64 for database storage.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::InvalidSnapshotVersion`] if the conversion fails.
    pub fn snapshot_version_to_i64<E>(version: u64) -> Result<i64, DbError<E>> {
        i64::try_from(version).map_err(|_| DbError::InvalidSnapshotVersion)
    }
}
