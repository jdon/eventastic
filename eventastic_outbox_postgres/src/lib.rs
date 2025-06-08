//! # Eventastic PostgreSQL Outbox Pattern Implementation
//!
//! This crate provides a PostgreSQL-based implementation of the transactional outbox pattern
//! for the eventastic event sourcing framework.
//!
//! ## Overview
//!
//! The outbox pattern ensures reliable delivery of side effects by storing them in the same
//! database transaction as the domain events. A background worker processes these side effects
//! asynchronously, providing guaranteed delivery semantics.
//!
//! ## Components
//!
//! - [`TableOutbox`] - Default outbox implementation using a PostgreSQL table
//! - [`OutboxMessage`] - Wrapper for side effects stored in the outbox
//! - [`SideEffectHandler`] - Trait for processing side effects from the outbox
//! - [`RepositoryOutboxExt`] - Extension methods for running outbox workers
//!
//! ## Example
//!
//! ```rust,ignore
//! use eventastic_outbox_postgres::{TableOutbox, RepositoryOutboxExt, SideEffectHandler};
//! use eventastic_postgres::PostgresRepository;
//!
//! // Setup repository with outbox
//! let repository = PostgresRepository::new(
//!     connect_options,
//!     pool_options,
//!     TableOutbox,
//! ).await?;
//!
//! // Define side effect handler
//! struct EmailHandler;
//!
//! #[async_trait]
//! impl SideEffectHandler for EmailHandler {
//!     type SideEffect = EmailSideEffect;
//!     type Error = EmailError;
//!
//!     async fn handle(&self, msg: &Self::SideEffect, retries: u16)
//!         -> Result<(), (bool, Self::Error)> {
//!         // Process the side effect
//!         todo!()
//!     }
//! }
//!
//! // Start outbox worker
//! repository.start_outbox(
//!     EmailHandler,
//!     std::time::Duration::from_secs(5)
//! ).await?;
//! ```

mod outbox;
mod outbox_message;

pub use outbox::*;
pub use outbox_message::OutboxMessage;
