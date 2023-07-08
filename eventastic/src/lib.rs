use aggregate::OutBoxMessage;
use async_trait::async_trait;

pub mod aggregate;
pub mod event;

pub type Version = u32;

#[async_trait]
pub trait SideEffectHandler<Id, T, E>: Send + Sync {
    /// Handle a side effect
    /// If Ok(()) is returned, the side effect is complete and it will be deleted from the repository.
    async fn handle(&self, msg: &OutBoxMessage<Id, T>) -> Result<(), (bool, E)>;
}
