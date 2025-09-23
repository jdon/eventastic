use eventastic::aggregate::Aggregate;
use thiserror::Error;

use crate::{EncryptionProvider, Pickle};

#[allow(type_alias_bounds)]
pub type EventSourcingDbError<E: EncryptionProvider, T: Aggregate> = DbError<
    E::Error,
    <T::DomainEvent as Pickle>::Error,
    <T as Pickle>::Error,
    <T::SideEffect as Pickle>::Error,
>;

#[derive(Error, Debug)]
pub enum DbError<
    EncryptionError,
    EventPicklingError,
    SnapshotPicklingError,
    SideEffectPicklingError,
> {
    /// A database operation failed.
    #[error("DB Error {0}")]
    DbError(sqlx::Error),
    /// Failed to pickle data.
    #[error("Pickling Error {0}")]
    EventPicklingError(EventPicklingError),
    /// Failed to pickle snapshot data.
    #[error("Snapshot Pickling Error {0}")]
    SnapshotPicklingError(SnapshotPicklingError),
    /// Failed to pickle side effect data.
    #[error("Side Effect Pickling Error {0}")]
    SideEffectPicklingError(SideEffectPicklingError),
    /// An invalid version number was encountered (e.g., negative value where positive expected).
    #[error("Invalid Version Number")]
    InvalidVersionNumber,
    /// An invalid snapshot version number was encountered.
    #[error("Invalid Snapshot Version Number")]
    InvalidSnapshotVersion,
    /// A concurrent modification was detected (optimistic locking failure).
    #[error("Optimistic Concurrency Error")]
    OptimisticConcurrencyError,
    /// Failed to encrypt or decrypt data.
    #[error("Encryption Error {0}")]
    Encryption(EncryptionError),
    /// Failed to encrypt or decrypt data.
    #[error("Encryption provider returned wrong number of items")]
    EncryptionProviderReturnedWrongNumberOfItems,
}

/// Errors that can occur during side effect storage operations.
///
/// This is a specialized error type for side effect operations that only
/// includes the errors relevant to storing and retrieving side effects.
#[derive(Error, Debug)]
pub enum SideEffectDbError<EncryptionError, SideEffectPicklingError> {
    /// A database operation failed.
    #[error("DB Error {0}")]
    DbError(sqlx::Error),
    /// Failed to pickle side effect data.
    #[error("Side Effect Pickling Error {0}")]
    SideEffectPicklingError(SideEffectPicklingError),
    /// Failed to encrypt or decrypt data.
    #[error("Encryption Error {0}")]
    Encryption(EncryptionError),
    /// Encryption provider returned wrong number of items.
    #[error("Encryption provider returned wrong number of items")]
    EncryptionProviderReturnedWrongNumberOfItems,
}

impl<EP, E, SS, SE> From<sqlx::Error> for DbError<EP, E, SS, SE> {
    fn from(e: sqlx::Error) -> Self {
        if let Some(db_error) = e.as_database_error() {
            if let Some(code) = db_error.code() {
                if code == "23505" && db_error.message().contains("aggregate_version") {
                    return DbError::OptimisticConcurrencyError;
                }
            }
        }
        DbError::DbError(e)
    }
}

impl<EP, SE> From<sqlx::Error> for SideEffectDbError<EP, SE> {
    fn from(e: sqlx::Error) -> Self {
        SideEffectDbError::DbError(e)
    }
}

impl<EP, E, SS, SE> From<SideEffectDbError<EP, SE>> for DbError<EP, E, SS, SE> {
    fn from(e: SideEffectDbError<EP, SE>) -> Self {
        match e {
            SideEffectDbError::DbError(err) => DbError::DbError(err),
            SideEffectDbError::SideEffectPicklingError(err) => {
                DbError::SideEffectPicklingError(err)
            }
            SideEffectDbError::Encryption(err) => DbError::Encryption(err),
            SideEffectDbError::EncryptionProviderReturnedWrongNumberOfItems => {
                DbError::EncryptionProviderReturnedWrongNumberOfItems
            }
        }
    }
}
