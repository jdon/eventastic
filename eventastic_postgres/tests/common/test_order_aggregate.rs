use eventastic::aggregate::Aggregate;
use eventastic::aggregate::SideEffect;
use eventastic::event::DomainEvent;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;
use uuid::Uuid;

// Define our Order aggregate - different from Account
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct Order {
    pub order_id: Uuid,
    pub customer_id: Uuid,
    pub total_amount: i64,
    pub status: OrderStatus,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum OrderStatus {
    Pending,
    Confirmed,
    Shipped,
    Delivered,
    Cancelled,
}

// Define our domain events for Order
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub enum OrderEvent {
    Created {
        order_id: Uuid,
        event_id: Uuid,
        customer_id: Uuid,
        total_amount: i64,
    },
    Confirmed {
        event_id: Uuid,
    },
    Shipped {
        event_id: Uuid,
        tracking_number: String,
    },
    Delivered {
        event_id: Uuid,
    },
    Cancelled {
        event_id: Uuid,
        reason: String,
    },
}

impl DomainEvent for OrderEvent {
    type EventId = Uuid;
    fn id(&self) -> &Uuid {
        match self {
            OrderEvent::Created { event_id, .. }
            | OrderEvent::Confirmed { event_id, .. }
            | OrderEvent::Shipped { event_id, .. }
            | OrderEvent::Delivered { event_id, .. }
            | OrderEvent::Cancelled { event_id, .. } => event_id,
        }
    }
}

// Define our domain error for Order
#[derive(Error, Debug)]
pub enum OrderDomainError {
    #[error("This event can't be applied given the current state of the order")]
    InvalidState,
    #[error("Order is already in final state")]
    AlreadyFinalized,
}

// Define our side effects for Order - different from Account side effects
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub enum OrderSideEffects {
    SendConfirmationEmail {
        id: Uuid,
        customer_email: String,
        order_id: Uuid,
    },
    NotifyWarehouse {
        id: Uuid,
        order_id: Uuid,
        items: Vec<String>,
    },
    UpdateInventory {
        id: Uuid,
        product_ids: Vec<Uuid>,
        quantities: Vec<i32>,
    },
}

impl SideEffect for OrderSideEffects {
    type SideEffectId = Uuid;

    fn id(&self) -> &Self::SideEffectId {
        match self {
            OrderSideEffects::SendConfirmationEmail { id, .. }
            | OrderSideEffects::NotifyWarehouse { id, .. }
            | OrderSideEffects::UpdateInventory { id, .. } => id,
        }
    }
}

// Implement the aggregate trait for our Order struct
impl Aggregate for Order {
    const SNAPSHOT_VERSION: u64 = 1;

    type AggregateId = Uuid;
    type DomainEvent = OrderEvent;
    type ApplyError = OrderDomainError;
    type SideEffect = OrderSideEffects;

    fn aggregate_id(&self) -> &Self::AggregateId {
        &self.order_id
    }

    fn apply(&mut self, event: &Self::DomainEvent) -> Result<(), Self::ApplyError> {
        match event {
            OrderEvent::Confirmed { .. } => {
                if self.status != OrderStatus::Pending {
                    return Err(Self::ApplyError::InvalidState);
                }
                self.status = OrderStatus::Confirmed;
            }
            OrderEvent::Shipped { .. } => {
                if self.status != OrderStatus::Confirmed {
                    return Err(Self::ApplyError::InvalidState);
                }
                self.status = OrderStatus::Shipped;
            }
            OrderEvent::Delivered { .. } => {
                if self.status != OrderStatus::Shipped {
                    return Err(Self::ApplyError::InvalidState);
                }
                self.status = OrderStatus::Delivered;
            }
            OrderEvent::Cancelled { .. } => {
                if matches!(self.status, OrderStatus::Delivered | OrderStatus::Cancelled) {
                    return Err(Self::ApplyError::AlreadyFinalized);
                }
                self.status = OrderStatus::Cancelled;
            }
            OrderEvent::Created { .. } => return Err(Self::ApplyError::InvalidState),
        }
        Ok(())
    }

    fn apply_new(event: &Self::DomainEvent) -> Result<Self, Self::ApplyError> {
        match event {
            OrderEvent::Created {
                order_id,
                customer_id,
                total_amount,
                ..
            } => Ok(Self {
                order_id: *order_id,
                customer_id: *customer_id,
                total_amount: *total_amount,
                status: OrderStatus::Pending,
            }),
            _ => Err(Self::ApplyError::InvalidState),
        }
    }

    fn side_effects(&self, event: &Self::DomainEvent) -> Option<Vec<Self::SideEffect>> {
        let side_effect = match event {
            OrderEvent::Created { event_id, .. } => Some(OrderSideEffects::SendConfirmationEmail {
                id: *event_id,
                customer_email: "customer@example.com".to_string(),
                order_id: self.order_id,
            }),
            OrderEvent::Confirmed { event_id } => Some(OrderSideEffects::NotifyWarehouse {
                id: *event_id,
                order_id: self.order_id,
                items: vec!["item1".to_string(), "item2".to_string()],
            }),
            OrderEvent::Shipped { .. }
            | OrderEvent::Delivered { .. }
            | OrderEvent::Cancelled { .. } => None,
        };
        side_effect.map(|s| vec![s])
    }
}
