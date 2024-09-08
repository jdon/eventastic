use crate::PostgresTransaction;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Pool, Postgres,
};

#[derive(Clone)]
pub struct PostgresRepository {
    pub(crate) inner: Pool<Postgres>,
}

impl PostgresRepository {
    pub async fn new(
        connect_options: PgConnectOptions,
        pool_options: PgPoolOptions,
    ) -> Result<Self, sqlx::Error> {
        let pool = pool_options.connect_with(connect_options).await?;

        Ok(Self { inner: pool })
    }

    /// Start a new transaction using the default isolation level
    pub async fn begin_transaction(&self) -> Result<PostgresTransaction<'_>, sqlx::Error> {
        Ok(PostgresTransaction {
            inner: self.inner.begin().await?,
        })
    }

    /// Run migrations on the database
    pub async fn run_migrations(&self) -> Result<(), sqlx::Error> {
        sqlx::migrate!("./migrations").run(&self.inner).await?;

        Ok(())
    }
}
