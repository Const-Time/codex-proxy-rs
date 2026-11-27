use std::{
    collections::BTreeSet,
    num::NonZeroU32,
    sync::Mutex,
    time::{Duration, SystemTime},
};

use futures::future::BoxFuture;
use gateway_core::{
    account::{AccountSelectionPolicy, ProviderAccountId, RotationStrategy},
    engine::{
        AccountAttemptContext, AttemptContext, ContinuationAttempt, ModelRequestId,
        ProviderAccountStateOwner, RequestAttemptContext,
    },
    lifecycle::CancellationToken,
    operation::{GenerateRequest, Operation, ProtocolPayload, ProviderSessionState},
    policy::ClientApiKeyId,
    provider_ports::{ProviderSessionAffinityKey, ProviderSessionAffinityPort, ProviderStoreError},
    routing::{ProviderKind, UpstreamModelId},
};
use gateway_protocol::{chat_completions::CHAT_PROTOCOL, genai::GENAI_PROTOCOL};
use provider_antigravity::{
    SessionAffinity, apply_upstream_session_id, locked_account, session_key, upstream_session_id,
};
use serde_json::{Map, json};

const PROVIDER: &str = "antigravity";

fn context(account: AccountAttemptContext) -> AttemptContext {
    AttemptContext::new(
        RequestAttemptContext::new(
            ModelRequestId::new("req_attempt-test").unwrap(),
            ClientApiKeyId::new("test-client").unwrap(),
        ),
        NonZeroU32::new(2).unwrap(),
        SystemTime::now() + Duration::from_secs(5),
        AccountSelectionPolicy::new(
            RotationStrategy::RoundRobin,
            NonZeroU32::MIN,
            Duration::ZERO,
        ),
        account,
        None,
        CancellationToken::new(),
    )
}

#[test]
fn only_diagnostic_or_request_state_pins_account_selection() {
    let provider = ProviderKind::new(PROVIDER).unwrap();
    let first = ProviderAccountId::new("acct_first").unwrap();
    let second = ProviderAccountId::new("acct_second").unwrap();
    let owner = ProviderAccountStateOwner::new(provider.clone(), first.clone());
    let retry = context(AccountAttemptContext::new(
        BTreeSet::from([first.clone()]),
        None,
        Some(owner.clone()),
    ));
    let body = json!({"contents":[{"parts":[{"text":"Question"}]}]});
    let operation = Operation::Generate(GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object(GENAI_PROTOCOL, body.as_object().unwrap().clone()).unwrap(),
    ));

    // The coordinator excludes the failed account while retaining its owner fact.
    let pin = locked_account(&provider, &operation, &retry).unwrap();
    let eligible: Vec<_> = [&first, &second]
        .into_iter()
        .filter(|id| pin.is_none_or(|pin| pin == *id) && !retry.excluded_accounts().contains(*id))
        .collect();
    assert_eq!(eligible, [&second]);

    let replay = retry
        .clone()
        .with_continuation_attempt(ContinuationAttempt::ReplayOwner);
    assert_eq!(
        locked_account(&provider, &operation, &replay).unwrap(),
        Some(&first)
    );
    let diagnostic = context(AccountAttemptContext::diagnostic(
        BTreeSet::new(),
        second.clone(),
        Some(owner),
    ));
    assert_eq!(
        locked_account(&provider, &operation, &diagnostic).unwrap(),
        Some(&second)
    );
    let state = ProviderSessionState::new(PROVIDER, Map::new()).unwrap();
    let stateful = operation.with_provider_session_state(state);
    assert_eq!(
        locked_account(&provider, &stateful, &retry).unwrap(),
        Some(&first)
    );
}

#[test]
fn session_keys_are_stable_and_isolated_by_client_model_and_protocol() {
    let context = json!({
        "opaque_request_headers": [["X-HTTP-Session-Id", "c2FtZS1zZXNzaW9u"]]
    });
    let chat = ProtocolPayload::json_object(CHAT_PROTOCOL, Map::new())
        .unwrap()
        .with_context(context.as_object().unwrap().clone());
    let native_body = json!({"sessionId": "same-session"});
    let native =
        ProtocolPayload::json_object(GENAI_PROTOCOL, native_body.as_object().unwrap().clone())
            .unwrap();
    let no_session = ProtocolPayload::json_object(CHAT_PROTOCOL, Map::new()).unwrap();
    let client = ClientApiKeyId::new("first-client").unwrap();
    let other_client = ClientApiKeyId::new("second-client").unwrap();
    let model = UpstreamModelId::new("model-one").unwrap();
    let other_model = UpstreamModelId::new("model-two").unwrap();
    let key = session_key(&chat, &client, &model).unwrap();
    assert_eq!(session_key(&chat, &client, &model), Some(key.clone()));
    assert!(session_key(&no_session, &client, &model).is_none());
    let projected = upstream_session_id(&key);
    assert!(projected.parse::<i64>().is_ok());
    for other in [
        session_key(&chat, &other_client, &model).unwrap(),
        session_key(&chat, &client, &other_model).unwrap(),
        session_key(&native, &client, &model).unwrap(),
    ] {
        assert_ne!(key, other);
        assert_ne!(projected, upstream_session_id(&other));
    }
}

#[test]
fn invalid_session_header_cannot_forward_an_unscoped_body_session() {
    let body = json!({"sessionId":"session-from-body","session_id":"legacy-session","contents":[]});
    let mut body = body.as_object().unwrap().clone();
    let context = json!({"opaque_request_headers":[["x-http-session-id","IA=="]]});
    let request = ProtocolPayload::json_object(GENAI_PROTOCOL, body.clone())
        .unwrap()
        .with_context(context.as_object().unwrap().clone());
    let client = ClientApiKeyId::new("client").unwrap();
    let model = UpstreamModelId::new("model").unwrap();
    let key = session_key(&request, &client, &model);
    assert_eq!(key, None);
    apply_upstream_session_id(&mut body, key.as_ref());
    assert_eq!(body, json!({"contents":[]}).as_object().unwrap().clone());
}

#[derive(Default)]
struct AffinityStore(Mutex<Option<ProviderAccountId>>);

impl ProviderSessionAffinityPort for AffinityStore {
    fn load<'a>(
        &'a self,
        _: &'a ProviderKind,
        _: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Option<ProviderAccountId>, ProviderStoreError>> {
        Box::pin(async { Ok(self.0.lock().unwrap().clone()) })
    }

    fn bind<'a>(
        &'a self,
        _: &'a ProviderKind,
        _: &'a ProviderSessionAffinityKey,
        _: &'a ProviderAccountId,
        _: Duration,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        panic!("unconditional writes can overwrite a concurrent binding")
    }

    fn claim_or_load<'a>(
        &'a self,
        _: &'a ProviderKind,
        _: &'a ProviderSessionAffinityKey,
        candidate: &'a ProviderAccountId,
        _: Duration,
    ) -> BoxFuture<'a, Result<ProviderAccountId, ProviderStoreError>> {
        Box::pin(async move {
            Ok(self
                .0
                .lock()
                .unwrap()
                .get_or_insert_with(|| candidate.clone())
                .clone())
        })
    }

    fn compare_and_bind<'a>(
        &'a self,
        _: &'a ProviderKind,
        _: &'a ProviderSessionAffinityKey,
        expected: &'a ProviderAccountId,
        replacement: &'a ProviderAccountId,
        _: Duration,
    ) -> BoxFuture<'a, Result<ProviderAccountId, ProviderStoreError>> {
        Box::pin(async move {
            let mut bound = self.0.lock().unwrap();
            if bound.as_ref().is_none_or(|account| account == expected) {
                *bound = Some(replacement.clone());
            }
            Ok(bound.clone().unwrap())
        })
    }

    fn clear<'a>(
        &'a self,
        _: &'a ProviderKind,
        _: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<bool, ProviderStoreError>> {
        panic!("unconditional clears can remove a concurrent binding")
    }
}

#[tokio::test]
async fn concurrent_claims_converge_and_late_success_cannot_undo_migration() {
    let store = AffinityStore::default();
    let provider = ProviderKind::new(PROVIDER).unwrap();
    let key = ProviderSessionAffinityKey::try_new("test-session").unwrap();
    let first = ProviderAccountId::new("acct_first").unwrap();
    let second = ProviderAccountId::new("acct_second").unwrap();
    let mut early = SessionAffinity::load(&store, &provider, key.clone())
        .await
        .unwrap();
    let mut concurrent = SessionAffinity::load(&store, &provider, key.clone())
        .await
        .unwrap();

    assert_eq!(early.claim(&store, &provider, &first).await, None);
    assert_eq!(
        concurrent.claim(&store, &provider, &second).await,
        Some(first.clone())
    );
    assert_eq!(concurrent.bound(), Some(&first));
    concurrent.record_success(&store, &provider, &second).await;
    early.record_success(&store, &provider, &first).await;
    assert_eq!(store.load(&provider, &key).await.unwrap(), Some(second));
}
