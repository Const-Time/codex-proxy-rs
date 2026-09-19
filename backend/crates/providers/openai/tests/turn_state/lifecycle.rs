use super::*;

async fn rotate(accounts: &Arc<MemoryAccountStore>, cookie_only: bool) {
    let account = accounts.account("acct_state").unwrap();
    if cookie_only {
        let selector = crate::credential::contract::selector(
            accounts,
            Arc::new(crate::support::TestLeaseCoordinator::default()),
        );
        selector
            .capture_response_cookies(
                &account,
                &url::Url::parse("https://chatgpt.com/backend-api/codex/responses").unwrap(),
                &[
                    "cf_clearance=updated; Path=/; Domain=chatgpt.com; Secure; Max-Age=3600"
                        .to_owned(),
                ],
            )
            .await
            .unwrap();
    } else {
        let repository = accounts.repository();
        let mut data = repository.load_complete_data(&account).await.unwrap();
        data.oauth_mut().unwrap().access_token = "rotated-access-token".to_owned();
        repository
            .compare_and_swap_data(&account, data)
            .await
            .unwrap();
    }
    assert_ne!(
        account.revision(),
        accounts.account("acct_state").unwrap().revision()
    );
}

#[tokio::test]
async fn real_cookie_capture_preserves_candidate_and_absolute_expiry_but_auth_rotation_rejects_it()
{
    let server = MockServer::start().await;
    let candidate = token(10, 93);
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("x-codex-turn-state", candidate.as_str())
                .set_body_raw(COMPLETED_SESSION_SSE, "text/event-stream"),
        )
        .mount(&server)
        .await;
    let mut config = valid_config();
    config.config.api.base_url = server.uri();
    let accounts = accounts().await;
    let store = Arc::new(StateStore::default());
    let bundle = bundle(&config.config, &accounts, &store).await;
    let service = bundle.turn_state_service().unwrap();
    setup(service.as_ref()).await;
    set_account(service.as_ref(), false, true).await;
    execute(&bundle, operation(None, false), "acct_state").await;
    let expiry = service.view().await.unwrap().targets[0].expires_at;
    rotate(&accounts, true).await;
    server.reset().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(COMPLETED_SESSION_SSE, "text/event-stream"),
        )
        .mount(&server)
        .await;
    let metadata = execute(&bundle, operation(None, false), "acct_state").await;
    assert_eq!(metadata["turnStateSent"], candidate);
    assert_eq!(metadata["turnStateSentSource"], "managed");
    assert_eq!(service.view().await.unwrap().targets[0].expires_at, expiry);
    rotate(&accounts, false).await;
    execute(&bundle, operation(Some(&candidate), false), "acct_state").await;
    assert!(
        server.received_requests().await.unwrap()[1]
            .headers
            .get("x-codex-turn-state")
            .is_none()
    );
}

#[tokio::test]
async fn in_flight_natural_response_survives_cookie_cas_but_not_auth_rotation() {
    for cookie_only in [true, false] {
        let server = MockServer::start().await;
        let sent = Arc::new(tokio::sync::Notify::new());
        let signal = sent.clone();
        Mock::given(method("POST"))
            .respond_with(move |_: &wiremock::Request| {
                signal.notify_one();
                ResponseTemplate::new(200)
                    .insert_header("x-codex-turn-state", token(10, 94))
                    .set_body_raw(COMPLETED_SESSION_SSE, "text/event-stream")
                    .set_delay(Duration::from_millis(300))
            })
            .mount(&server)
            .await;
        let mut config = valid_config();
        config.config.api.base_url = server.uri();
        let accounts = accounts().await;
        let store = Arc::new(StateStore::default());
        let bundle = bundle(&config.config, &accounts, &store).await;
        let service = bundle.turn_state_service().unwrap();
        setup(service.as_ref()).await;
        set_account(service.as_ref(), false, true).await;
        tokio::join!(
            execute(&bundle, operation(None, false), "acct_state"),
            async {
                tokio::time::timeout(Duration::from_secs(5), sent.notified())
                    .await
                    .unwrap();
                rotate(&accounts, cookie_only).await;
            }
        );
        assert_eq!(
            service.view().await.unwrap().targets[0].candidate_count,
            usize::from(cookie_only)
        );
    }
}
