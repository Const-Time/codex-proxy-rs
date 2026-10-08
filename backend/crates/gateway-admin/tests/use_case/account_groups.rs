use std::{
    collections::BTreeMap,
    str::FromStr as _,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use chrono::{Duration, Utc};
use gateway_admin::{
    model::{
        MutationContext, PageSize, Revision,
        account_groups::{
            AccountGroupAccountSummary, AccountGroupCapacity, AccountGroupColor,
            AccountGroupListQuery, AccountGroupMemberFact, AccountGroupMutation, AccountGroupPage,
            AccountGroupRecord, AccountGroupUsage, DeleteAccountGroup, NewAccountGroup,
            SetAccountGroupEnabled, UpdateAccountGroup,
        },
        accounts::AccountRuntimeSnapshot,
        observability::DecimalAmount,
    },
    ports::store::{
        AccountGroupStore, AccountRuntimeStore, AdminStoreError, AdminStoreErrorKind,
        AdminStoreResult,
    },
};
use gateway_core::{
    account::{AccountStatusFacts, CredentialState, QuotaState},
    routing::AccountGroupId,
};

use super::AdminHarness;
use gateway_admin::model::plugins::*;

const GROUP_ID: &str = "grp_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[tokio::test]
async fn group_delete_conflict_explains_references_without_masking_other_errors() {
    use gateway_admin::model::{AdminErrorKind, MutationActor};
    for (store_kind, expected_kind) in [
        (AdminStoreErrorKind::Conflict, AdminErrorKind::Conflict),
        (
            AdminStoreErrorKind::Unavailable,
            AdminErrorKind::Unavailable,
        ),
        (AdminStoreErrorKind::NotFound, AdminErrorKind::NotFound),
    ] {
        let groups = Arc::new(FakeGroupStore {
            delete_error: Some(store_kind),
            ..Default::default()
        });
        let service = AdminHarness::new().account_groups(groups).build().await;
        let error = service
            .account_groups()
            .delete(
                &MutationContext {
                    actor: MutationActor::AdminSession {
                        admin_user_id: "test-owner".into(),
                    },
                    request_id: "delete-conflict".into(),
                },
                DeleteAccountGroup { id: group_id() },
            )
            .await
            .expect_err("injected deletion failure");
        assert_eq!(error.kind(), expected_kind);
        assert!(!error.message().contains("private database context"));
        if expected_kind == AdminErrorKind::Conflict {
            assert!(error.message().contains("仍被密钥引用"));
            assert!(error.message().contains("历史额度和消费记录会保留"));
        } else {
            assert!(!error.message().contains("禁用"));
        }
    }
}

#[tokio::test]
async fn group_query_service_enriches_only_current_page_members_with_runtime_facts() {
    let groups = Arc::new(FakeGroupStore::default());
    let runtime = Arc::new(FakeRuntimeStore::default());
    let service = AdminHarness::new()
        .account_groups(groups.clone())
        .account_runtime(runtime.clone())
        .build()
        .await;

    let page = service
        .account_groups()
        .list(AccountGroupListQuery {
            page: 1,
            page_size: PageSize::new(20).expect("page size"),
            search: None,
            enabled: None,
        })
        .await
        .expect("list projected account groups");

    assert_eq!(
        groups
            .requested_groups
            .lock()
            .expect("requested groups")
            .as_slice(),
        [GROUP_ID]
    );
    assert_eq!(
        runtime
            .requested_accounts
            .lock()
            .expect("requested accounts")
            .as_slice(),
        ["acct_available", "acct_limited"]
    );
    let group = &page.items[0];
    assert_eq!(
        group.account_summary,
        AccountGroupAccountSummary {
            available: 1,
            limited: 1,
            total: 2,
        }
    );
    assert_eq!(
        group.capacity,
        AccountGroupCapacity {
            used_slots: Some(2),
            total_slots: 4,
        }
    );
}

#[derive(Default)]
struct FakeGroupStore {
    plugin_calls: Mutex<Vec<PluginMutationScope>>,
    plugin_members: Mutex<Vec<PluginGroupMembers>>,
    gated: bool,
    started: tokio::sync::Notify,
    released: tokio::sync::Notify,
    requested_groups: Mutex<Vec<String>>,
    delete_error: Option<AdminStoreErrorKind>,
}

#[async_trait]
impl AccountGroupStore for FakeGroupStore {
    async fn ensure_plugin_group(
        &self,
        scope: PluginMutationScope,
        _key: String,
        command: NewAccountGroup,
    ) -> AdminStoreResult<PluginResourceMutation<PluginManagedResource>> {
        self.plugin_calls.lock().unwrap().push(scope);
        if self.gated {
            self.started.notify_one();
            self.released.notified().await;
        }
        Ok(PluginResourceMutation {
            revision: Revision::new(44).unwrap(),
            value: PluginManagedResource {
                id: command.id.as_str().into(),
                name: command.name,
                enabled: true,
            },
        })
    }
    async fn change_plugin_group_members(
        &self,
        scope: PluginMutationScope,
        command: PluginGroupMembers,
    ) -> AdminStoreResult<PluginResourceMutation<PluginMembersChanged>> {
        self.plugin_calls.lock().unwrap().push(scope);
        let added = command.add.len() as u64;
        let removed = command.remove.len() as u64;
        self.plugin_members.lock().unwrap().push(command);
        Ok(PluginResourceMutation {
            revision: Revision::new(45).unwrap(),
            value: PluginMembersChanged { added, removed },
        })
    }

    async fn list_account_groups(
        &self,
        query: AccountGroupListQuery,
    ) -> AdminStoreResult<AccountGroupPage> {
        Ok(AccountGroupPage {
            config_revision: Revision::new(1).expect("revision"),
            items: vec![group_record()],
            total: 1,
            page: query.page,
            page_size: query.page_size.get(),
        })
    }

    async fn load_account_group_members(
        &self,
        group_ids: &[AccountGroupId],
    ) -> AdminStoreResult<Vec<AccountGroupMemberFact>> {
        *self.requested_groups.lock().expect("requested groups") = group_ids
            .iter()
            .map(|group_id| group_id.as_str().to_owned())
            .collect();
        Ok(vec![member("acct_available", 4), member("acct_limited", 3)])
    }

    async fn create_account_group(
        &self,
        _: NewAccountGroup,
        _: &MutationContext,
    ) -> AdminStoreResult<AccountGroupMutation> {
        Err(unused())
    }

    async fn update_account_group(
        &self,
        _: UpdateAccountGroup,
        _: &MutationContext,
    ) -> AdminStoreResult<AccountGroupMutation> {
        Err(unused())
    }

    async fn set_account_group_enabled(
        &self,
        _: SetAccountGroupEnabled,
        _: &MutationContext,
    ) -> AdminStoreResult<AccountGroupMutation> {
        Err(unused())
    }

    async fn delete_account_group(
        &self,
        _: DeleteAccountGroup,
        _: &MutationContext,
    ) -> AdminStoreResult<AccountGroupMutation> {
        Err(AdminStoreError::new(
            self.delete_error
                .unwrap_or(AdminStoreErrorKind::Unavailable),
            "account group",
            "private database context",
        ))
    }
}

#[derive(Default)]
struct FakeRuntimeStore {
    requested_accounts: Mutex<Vec<String>>,
}

#[async_trait]
impl AccountRuntimeStore for FakeRuntimeStore {
    async fn active_rate_limits(&self) -> AdminStoreResult<AccountRuntimeSnapshot> {
        Ok(AccountRuntimeSnapshot::default())
    }

    async fn account_runtime(
        &self,
        account_ids: &[String],
    ) -> AdminStoreResult<AccountRuntimeSnapshot> {
        *self.requested_accounts.lock().expect("requested accounts") = account_ids.to_vec();
        Ok(AccountRuntimeSnapshot {
            rate_limited_until: BTreeMap::from([(
                "acct_limited".to_owned(),
                Utc::now() + Duration::minutes(5),
            )]),
            in_flight: Some(BTreeMap::from([("acct_available".to_owned(), 2)])),
        })
    }
}

fn group_record() -> AccountGroupRecord {
    let now = Utc::now();
    AccountGroupRecord {
        model_multipliers: Default::default(),
        budget: Default::default(),
        id: group_id(),
        name: "Primary".to_owned(),
        description: None,
        color: AccountGroupColor::parse("#2563EBFF").expect("color"),
        enabled: true,
        member_count: 2,
        user_count: 3,
        provider_counts: BTreeMap::from([("openai".to_owned(), 2)]),
        client_key_count: 1,
        account_summary: AccountGroupAccountSummary {
            available: 0,
            limited: 0,
            total: 0,
        },
        capacity: AccountGroupCapacity {
            used_slots: None,
            total_slots: 0,
        },
        usage: AccountGroupUsage {
            today_usd: DecimalAmount::from_str("1").expect("today usage"),
            retained_total_usd: DecimalAmount::from_str("3").expect("retained usage"),
        },
        created_at: now,
        updated_at: now,
    }
}

fn member(account_id: &str, total_slots: u64) -> AccountGroupMemberFact {
    AccountGroupMemberFact {
        group_id: group_id(),
        account_id: account_id.to_owned(),
        status: AccountStatusFacts {
            enabled: true,
            credential_state: CredentialState::Ready,
            access_token_expires_at: None,
            quota: QuotaState::default(),
            rate_limited_until: None,
            last_error_reason: None,
            last_error_message: None,
        },
        total_slots,
    }
}

fn group_id() -> AccountGroupId {
    AccountGroupId::new(GROUP_ID).expect("group ID")
}

fn unused() -> AdminStoreError {
    AdminStoreError::new(
        AdminStoreErrorKind::Unavailable,
        "account group",
        "unused test operation",
    )
}

#[derive(Default)]
struct ResourceSnapshot(tokio::sync::Notify, std::sync::atomic::AtomicU64);
impl gateway_core::runtime::SnapshotControl for ResourceSnapshot {
    fn publish_committed(
        &self,
        revision: gateway_core::routing::ConfigRevision,
    ) -> futures::future::BoxFuture<'_, ()> {
        Box::pin(async move {
            self.1
                .store(revision.get(), std::sync::atomic::Ordering::SeqCst);
            self.0.notify_one();
        })
    }
}
fn plugin_scope() -> PluginMutationScope {
    PluginMutationScope {
        instance_id: "plugin".into(),
        revision: 1,
        actor: MutationContext {
            actor: gateway_admin::model::MutationActor::System,
            request_id: "plugin-test".into(),
        },
    }
}
fn plugin_group() -> PluginGroupEnsure {
    PluginGroupEnsure {
        resource_key: "main".into(),
        name: "Group".into(),
        color: "#2563EBFF".into(),
        description: None,
    }
}
#[tokio::test]
async fn plugin_resource_commit_and_publication_survive_cancelled_caller() {
    let groups = Arc::new(FakeGroupStore {
        gated: true,
        ..Default::default()
    });
    let snapshot = Arc::new(ResourceSnapshot::default());
    let mut harness = AdminHarness::new().account_groups(groups.clone());
    harness.snapshot = snapshot.clone();
    let service = harness.build().await.plugin_resources();
    let task =
        tokio::spawn(async move { service.ensure_group(plugin_scope(), plugin_group()).await });
    tokio::time::timeout(std::time::Duration::from_secs(2), groups.started.notified())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    groups.released.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(2), snapshot.0.notified())
        .await
        .unwrap();
    assert_eq!(snapshot.1.load(std::sync::atomic::Ordering::SeqCst), 44);
    assert_eq!(groups.plugin_calls.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn plugin_resources_validate_scope_inputs_and_normalize_incremental_members() {
    let groups = Arc::new(FakeGroupStore::default());
    let service = AdminHarness::new()
        .account_groups(groups.clone())
        .build()
        .await
        .plugin_resources();
    for key in ["", " main", "main\n"] {
        let mut command = plugin_group();
        command.resource_key = key.into();
        assert!(service.ensure_group(plugin_scope(), command).await.is_err());
    }
    let mut command = plugin_group();
    command.color = "bad".into();
    assert!(service.ensure_group(plugin_scope(), command).await.is_err());
    for command in [
        PluginGroupMembers {
            resource_key: "main".into(),
            add: vec!["acct_test".into()],
            remove: vec!["acct_test".into()],
        },
        PluginGroupMembers {
            resource_key: "main".into(),
            add: vec!["".into()],
            remove: vec![],
        },
        PluginGroupMembers {
            resource_key: "main".into(),
            add: vec!["acct_test".into(); 1001],
            remove: vec![],
        },
    ] {
        assert!(
            service
                .change_members(plugin_scope(), command)
                .await
                .is_err()
        );
    }
    assert!(groups.plugin_calls.lock().unwrap().is_empty());
    let result = service
        .change_members(
            plugin_scope(),
            PluginGroupMembers {
                resource_key: "main".into(),
                add: vec!["acct_test".into(), "acct_test".into()],
                remove: vec![],
            },
        )
        .await
        .unwrap();
    assert_eq!(result.added, 1);
    assert_eq!(groups.plugin_members.lock().unwrap()[0].add, ["acct_test"]);
}
