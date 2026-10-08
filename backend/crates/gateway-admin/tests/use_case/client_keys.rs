use async_trait::async_trait;
use gateway_core::policy::ClientApiKeyId;

use gateway_admin::{
    model::{
        AdminErrorKind, MutationContext, Revision,
        client_keys::{
            ClientKeyCursor, ClientKeyCursorValue, ClientKeyListQuery, ClientKeyPage,
            ClientKeyPageSize, ClientKeyRecord, ClientKeySecret, ClientKeySort, ClientKeySortField,
            DeleteClientKey, NewClientKey, SetClientKeyEnabled, SortDirection, UpdateClientKey,
        },
    },
    ports::store::{AdminStoreError, AdminStoreErrorKind, AdminStoreResult, ClientKeyStore},
};

struct UnusedClientKeyStore;

#[async_trait]
impl ClientKeyStore for UnusedClientKeyStore {
    async fn ensure_plugin_key(
        &self,
        scope: gateway_admin::model::plugins::PluginMutationScope,
        command: gateway_admin::model::plugins::PluginKeyCreate,
    ) -> AdminStoreResult<
        gateway_admin::model::plugins::PluginResourceMutation<
            gateway_admin::model::plugins::PluginManagedResource,
        >,
    > {
        assert_eq!(
            scope.actor.actor,
            gateway_admin::model::MutationActor::AdminSession {
                admin_user_id: "admin".into()
            }
        );
        assert_eq!(command.group_resource_key, "main");
        assert_eq!(command.key.plaintext.len(), 46);
        assert!(command.key.plaintext.starts_with("sk_"));
        assert!(command.key.group_ids.is_empty());
        assert_eq!(command.budget.daily_usd.canonical(), "2");
        Ok(gateway_admin::model::plugins::PluginResourceMutation {
            revision: Revision::new(2).unwrap(),
            value: gateway_admin::model::plugins::PluginManagedResource {
                id: command.key.id.as_str().into(),
                name: command.key.name,
                enabled: true,
            },
        })
    }
    async fn list_client_keys(
        &self,
        _owner: &str,
        query: ClientKeyListQuery,
    ) -> AdminStoreResult<ClientKeyPage> {
        assert_eq!(query.page_size.get(), u16::MAX);
        Ok(ClientKeyPage {
            config_revision: Revision::new(1).expect("revision"),
            items: Vec::new(),
            total: 0,
            next_cursor: None,
        })
    }

    async fn reveal_client_key(
        &self,
        _owner: &str,
        _: &ClientApiKeyId,
    ) -> AdminStoreResult<Option<ClientKeySecret>> {
        Err(unused())
    }

    async fn create_client_key(
        &self,
        _: NewClientKey,
        _: &MutationContext,
    ) -> AdminStoreResult<(Revision, ClientKeyRecord)> {
        Err(unused())
    }

    async fn update_client_key(
        &self,
        _: UpdateClientKey,
        _: &MutationContext,
    ) -> AdminStoreResult<(Revision, ClientKeyRecord)> {
        Err(unused())
    }

    async fn set_client_key_enabled(
        &self,
        _: SetClientKeyEnabled,
        _: &MutationContext,
    ) -> AdminStoreResult<(Revision, ClientKeyRecord)> {
        Err(unused())
    }

    async fn delete_client_key(
        &self,
        _: DeleteClientKey,
        _: &MutationContext,
    ) -> AdminStoreResult<Revision> {
        Err(unused())
    }
}

#[tokio::test]
async fn client_key_cursor_should_reject_value_that_does_not_match_sort() {
    let services = super::AdminHarness::new()
        .client_keys(std::sync::Arc::new(UnusedClientKeyStore))
        .build()
        .await;
    let sort = ClientKeySort {
        field: ClientKeySortField::Name,
        direction: SortDirection::Asc,
    };
    let error = services
        .client_keys()
        .list(
            "admin",
            ClientKeyListQuery {
                cursor: Some(ClientKeyCursor {
                    sort,
                    value: ClientKeyCursorValue::Enabled(true),
                    id: ClientApiKeyId::new("key_cursor").expect("key ID"),
                }),
                page_size: ClientKeyPageSize::new(50).expect("page size"),
                search: None,
                sort,
            },
        )
        .await
        .expect_err("mismatched cursor must fail");

    assert_eq!(error.kind(), AdminErrorKind::Invalid);
}

#[tokio::test]
async fn client_key_list_should_forward_the_full_nonzero_u16_page_size() {
    let services = super::AdminHarness::new()
        .client_keys(std::sync::Arc::new(UnusedClientKeyStore))
        .build()
        .await;
    let page = services
        .client_keys()
        .list(
            "admin",
            ClientKeyListQuery {
                cursor: None,
                page_size: ClientKeyPageSize::new(u16::MAX).expect("maximum page size"),
                search: None,
                sort: ClientKeySort {
                    field: ClientKeySortField::CreatedAt,
                    direction: SortDirection::Desc,
                },
            },
        )
        .await
        .expect("maximum page size should reach store");

    assert_eq!(page.total, 0);
}

fn unused() -> AdminStoreError {
    AdminStoreError::new(
        AdminStoreErrorKind::Unavailable,
        "client key",
        "unused in this test",
    )
}

#[tokio::test]
async fn plugin_key_creation_uses_personal_identity_and_validates_budget_and_single_group() {
    use gateway_admin::model::{MutationActor, plugins::*};
    let service = super::AdminHarness::new()
        .client_keys(std::sync::Arc::new(UnusedClientKeyStore))
        .build()
        .await
        .plugin_resources();
    let scope = |actor| PluginMutationScope {
        instance_id: "plugin".into(),
        revision: 1,
        actor: MutationContext {
            actor,
            request_id: "key-create".into(),
        },
    };
    let actor = || MutationActor::AdminSession {
        admin_user_id: "admin".into(),
    };
    let command = || PluginKeyEnsure {
        resource_key: "key".into(),
        name: "Key".into(),
        group_resource_keys: vec!["main".into()],
        max_concurrency: 1,
        requests_per_minute: 2,
        daily_limit_usd: "2".into(),
        weekly_limit_usd: "10".into(),
    };
    for kind in [MutationActor::System, MutationActor::AdminApiKey] {
        assert!(service.ensure_key(scope(kind), command()).await.is_err());
    }
    for groups in [vec![], vec!["main".into(), "other".into()]] {
        let mut c = command();
        c.group_resource_keys = groups;
        assert!(service.ensure_key(scope(actor()), c).await.is_err());
    }
    let mut c = command();
    c.daily_limit_usd = "-1".into();
    assert!(service.ensure_key(scope(actor()), c).await.is_err());
    let a = service.ensure_key(scope(actor()), command()).await.unwrap();
    let b = service.ensure_key(scope(actor()), command()).await.unwrap();
    assert!(a.id.starts_with("key_"));
    assert_ne!(a.id, b.id);
    assert_eq!(a.name, "Key");
}
