mod outbox;
mod repository;
mod transaction;
pub use repository::PostgresRepository;
use thiserror::Error;
pub use transaction::PostgresTransaction;

#[derive(Error, Debug)]
pub enum DbError {
    #[error("DB Error {0}")]
    DbError(#[from] sqlx::Error),
    #[error("Serialization Error {0}")]
    SerializationError(#[source] serde_json::Error),
    #[error("Invalid Version Number")]
    InvalidVersionNumber,
}
