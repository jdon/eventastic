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
mod outbox;
mod outbox_message;

pub use outbox::*;
pub use outbox_message::OutboxMessage;
