use eventastic::aggregate::SideEffect;

/// Message stored in the transactional outbox.
#[derive(Debug, Clone)]
pub struct OutboxMessage<T>
where
    T: SideEffect,
{
    /// The side effect payload.
    pub message: T,
    pub retries: u16,
    /// Whether the message should be requeued on failure.
    pub requeue: bool,
}

impl<T> OutboxMessage<T>
where
    T: SideEffect,
{
    pub fn new(message: T, retries: u16, requeue: bool) -> Self {
        Self {
            message,
            retries,
            requeue,
        }
    }

    /// Returns the retry count for this message.
    pub fn retries(&self) -> u16 {
        self.retries
    }
}
