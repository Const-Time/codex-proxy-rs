use sqlx::{PgPool, postgres::PgPoolOptions};

// Real stores and coordination, isolated from every other fixture/production database.
pub(crate) struct Database {
    admin: PgPool,
    name: String,
}
impl Database {
    pub(crate) async fn create() -> Option<(Self, String, String)> {
        fn env(name: &str) -> Option<String> {
            let value = std::env::var(name).ok();
            let ci = std::env::var("CI")
                .ok()
                .is_some_and(|v| !matches!(v.as_str(), "" | "false" | "0"));
            assert!(
                value.is_some() || !ci,
                "{name} is required for native plugin integration tests in CI"
            );
            value
        }
        let database_url = env("CPR_TEST_DATABASE_URL")?;
        let redis_url = env("CPR_TEST_REDIS_URL")?;
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .unwrap();
        let name = format!("cpr_plugin_fixture_{}", uuid::Uuid::new_v4().simple());
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!("create database \"{name}\"")))
            .execute(&admin)
            .await
            .unwrap();
        let mut url = url::Url::parse(&database_url).unwrap();
        url.set_path(&format!("/{name}"));
        Some((Self { admin, name }, url.into(), redis_url))
    }
    pub(crate) async fn close(self) {
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "drop database \"{}\" with (force)",
            self.name
        )))
        .execute(&self.admin)
        .await
        .unwrap();
        self.admin.close().await;
    }
}
