//! # Eventastic
//!
//! A Rust library for implementing Event Sourcing and CQRS patterns.
//!
//! ## Overview
//!
//! Eventastic provides the core abstractions and patterns needed to build
//! event-sourced applications. It includes:
//!
//! - [`aggregate::Aggregate`] trait for modelling domain aggregates
//! - [`event::DomainEvent`] trait for representing domain events
//! - [`repository::RepositoryTransaction`] trait for persisting and loading aggregates
//! - Context management for recording and applying events
//!
//! ## Example
//!
//! ```rust,ignore
//! use eventastic::aggregate::{Aggregate, Root};
//! use eventastic::event::DomainEvent;
//!
//! // Define your domain events
//! #[derive(Clone, PartialEq, Eq)]
//! enum UserEvent {
//!     Created { id: String, email: String },
//!     EmailChanged { new_email: String },
//! }
//!
//! impl DomainEvent for UserEvent {
//!     type EventId = String;
//!     fn id(&self) -> &Self::EventId {
//!         // Return event ID implementation
//!         todo!()
//!     }
//! }
//!
//! // Define your aggregate
//! #[derive(Clone)]
//! struct User {
//!     id: String,
//!     email: String,
//! }
//!
//! impl Aggregate for User {
//!     const SNAPSHOT_VERSION: u64 = 1;
//!     type AggregateId = String;
//!     type DomainEvent = UserEvent;
//!     type ApplyError = String;
//!     type SideEffect = ();
//!
//!     fn aggregate_id(&self) -> &Self::AggregateId {
//!         &self.id
//!     }
//!
//!     fn apply_new(event: &Self::DomainEvent) -> Result<Self, Self::ApplyError> {
//!         // Create new aggregate from first event
//!         todo!()
//!     }
//!
//!     fn apply(&mut self, event: &Self::DomainEvent) -> Result<(), Self::ApplyError> {
//!         // Apply event to existing aggregate
//!         todo!()
//!     }
//! }
//!
//! // Use the aggregate
//! let event = UserEvent::Created {
//!     id: "user-1".to_string(),
//!     email: "user@example.com".to_string(),
//! };
//!
//! let mut user_context = User::record_new(event)?;
//! ```

pub mod aggregate;
pub mod event;
pub mod repository;
