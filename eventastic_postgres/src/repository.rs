use crate::{PostgresTransaction, TransactionalOutbox};
use sqlx::{
    Pool, Postgres,
    postgres::{PgConnectOptions, PgPoolOptions},
};

#[derive(Clone)]
pub struct PostgresRepository<O>
where
    O: TransactionalOutbox + Clone,
{
    pub(crate) inner: Pool<Postgres>,
    pub(crate) outbox: O,
}

impl<O> PostgresRepository<O>
where
    O: TransactionalOutbox + Clone,
{
    pub async fn new(
        connect_options: PgConnectOptions,
        pool_options: PgPoolOptions,
        outbox: O,
    ) -> Result<Self, sqlx::Error> {
        let pool = pool_options.connect_with(connect_options).await?;

        Ok(Self { inner: pool, outbox })
    }

    /// Start a new transaction using the default isolation level
    pub async fn begin_transaction(&self) -> Result<PostgresTransaction<'_, O>, sqlx::Error> {
        Ok(PostgresTransaction {
            inner: self.inner.begin().await?,
            outbox: &self.outbox,
        })
    }

    /// Run migrations on the database
    pub async fn run_migrations(&self) -> Result<(), sqlx::Error> {
        sqlx::migrate!("./migrations").run(&self.inner).await?;

        Ok(())
    }
}
