use sqlx::PgPool;

pub type DbPool = PgPool;

pub async fn create_pool(database_url: &str) -> Result<DbPool, sqlx::Error> {
    PgPool::connect(database_url).await
}

pub mod models;
