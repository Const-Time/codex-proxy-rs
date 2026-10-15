use std::{collections::BTreeMap, str::FromStr};

use gateway_store::postgres::{
    PgRuntimeSnapshotRepository, RuntimeSnapshotRepository, connect_and_migrate,
};
use serde_json::Value;
use sqlx::{
    ConnectOptions as _, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};

// v3.11.4 shipped 1..=26 and 32,33. Applying a contiguous prefix would miss
// the actual upgrade path: plugin migrations must fill the earlier holes.
#[tokio::test]
async fn v3_11_4_upgrades_through_production_migrator_without_rewriting_existing_business_rows() {
    let Some(url) = crate::support::test_env("CPR_TEST_DATABASE_URL") else {
        return;
    };
    let name = format!("cpr_upgrade_{}", uuid::Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("create database {name}")))
        .execute(&admin)
        .await
        .unwrap();
    let isolated = PgConnectOptions::from_str(&url)
        .unwrap()
        .database(&name)
        .to_url_lossy()
        .to_string();
    let old = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated)
        .await
        .unwrap();
    sqlx::migrate::Migrator::with_migrations(
        super::TEST_MIGRATOR
            .iter()
            .filter(|migration| migration.version <= 26 || matches!(migration.version, 32 | 33))
            .cloned()
            .collect(),
    )
    .run(&old)
    .await
    .unwrap();
    let versions: Vec<i64> =
        sqlx::query_scalar("select version from _sqlx_migrations order by version")
            .fetch_all(&old)
            .await
            .unwrap();
    assert_eq!(versions, (1..=26).chain([32, 33]).collect::<Vec<_>>());
    sqlx::raw_sql("\
        insert into users(id,username,password_hash,role,created_at,updated_at) values ('upgrade-user','upgrade-user','isolated-fixture','admin',now(),now());
        insert into account_groups(id,name,color,daily_limit_usd,weekly_limit_usd,model_multipliers,created_at,updated_at)
            values ('grp_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','Upgrade group','#64748BFF',10,50,'{\"gpt-5.4\":{\"multiplier\":1.5,\"scope\":\"all\"}}',now(),now());
        insert into user_account_groups(user_id,account_group_id,quota_multiplier) values ('upgrade-user','grp_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',2);
        insert into client_api_keys(id,owner_user_id,name,key,enabled,max_concurrency,requests_per_minute,created_at,updated_at)
            values ('key_upgrade','upgrade-user','Upgrade key','sk_uuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuu',true,2,60,now(),now());
        insert into client_api_key_groups(client_api_key_id,account_group_id,created_at) values ('key_upgrade','grp_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',now());
        insert into model_requests(id,client_api_key_ref,config_revision,protocol,operation,endpoint,client_transport,started_at,deadline_at,outcome,completed_at,routing_scope,cost_amount,billing_multiplier,cost_source,cost_currency)
            values ('req_upgrade','key_upgrade',1,'openai','responses','/v1/responses','http_sse',now(),now()+interval '1 hour','failed',now(),'all',0.2,1.5,'calculated','USD');
        insert into user_group_charge_events(request_id,user_id,account_group_id,client_api_key_ref,amount_usd,raw_amount_usd,billing_multiplier,completed_at)
            values ('req_upgrade','upgrade-user','grp_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','key_upgrade',0.3,0.2,1.5,now());
        insert into user_group_budget_windows(user_id,account_group_id,daily_start,daily_end,weekly_start,weekly_end,daily_used_usd,weekly_used_usd)
            values ('upgrade-user','grp_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',now(),now()+interval '1 day',now(),now()+interval '7 days',0.3,0.3);
    ").execute(&old).await.unwrap();
    let columns = sqlx::query_as::<_,(String,String)>("select table_name,column_name from information_schema.columns where table_schema='public' and table_name <> '_sqlx_migrations' order by table_name,ordinal_position")
        .fetch_all(&old).await.unwrap();
    let mut tables: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (table, column) in columns {
        tables.entry(table).or_default().push(column);
    }
    let before = snapshot(&old, &tables).await;
    old.close().await;
    for _ in 0..2 {
        let pool = connect_and_migrate(&isolated, gateway_store::StorePoolConfig::default())
            .await
            .unwrap();
        assert_eq!(
            snapshot(&pool, &tables).await,
            before,
            "upgrade/reopen must preserve every legacy column and row"
        );
        let count: i64 = sqlx::query_scalar("select count(*) from _sqlx_migrations where success")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            count,
            i64::try_from(super::TEST_MIGRATOR.iter().count()).unwrap()
        );
        let plugins: i64 = sqlx::query_scalar("select count(*) from plugin_instances")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(plugins, 0, "upgrade must not silently install a plugin");
        let runtime = PgRuntimeSnapshotRepository::new(pool.clone())
            .load_runtime_snapshot()
            .await
            .unwrap();
        assert_eq!(runtime.client_api_keys.len(), 1);
        assert_eq!(
            runtime.client_api_keys[0].plaintext_key.expose_for_auth(),
            "sk_uuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuu"
        );
        assert_eq!(runtime.client_api_keys[0].group_ids.len(), 1);
        pool.close().await;
    }
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("drop database {name}")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

async fn snapshot(
    pool: &PgPool,
    tables: &BTreeMap<String, Vec<String>>,
) -> BTreeMap<String, Vec<Value>> {
    let quote = |name: &str| format!("\"{}\"", name.replace('"', "\"\""));
    let mut result = BTreeMap::new();
    for (table, columns) in tables {
        let fields = columns
            .iter()
            .map(|column| quote(column))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "select to_jsonb(row) from (select {fields} from {}) row order by to_jsonb(row)::text",
            quote(table)
        );
        let rows = sqlx::query_scalar::<_, Value>(sqlx::AssertSqlSafe(sql))
            .fetch_all(pool)
            .await
            .unwrap();
        result.insert(table.clone(), rows);
    }
    result
}
