use provider_antigravity::{GoogleOAuth, OAuthRegistration};
use serde_json::json;
use url::Url;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_string_contains, header, method, path},
};

fn stub_oauth(server: &MockServer) -> GoogleOAuth {
    let origin = server.uri();
    GoogleOAuth::with_endpoints(
        OAuthRegistration::new(
            "operator-client".to_owned(),
            "operator-secret".to_owned(),
            "http://localhost:8085/auth/callback".to_owned(),
            vec!["test-scope".to_owned()],
            Url::parse(&origin).unwrap(),
        ),
        &format!("{origin}/token"),
        &format!("{origin}/userinfo"),
        &format!("{origin}/tokeninfo"),
    )
}

#[test]
fn authorization_uses_the_registration_in_the_build() {
    let oauth = GoogleOAuth::from_build_configuration().unwrap();
    let authorization = oauth.authorize_url("test-state", "test-verifier");
    let client_id = option_env!("CPR_BUILD_ANTIGRAVITY_OAUTH_CLIENT_ID")
        .filter(|value| !value.is_empty())
        .unwrap_or("1071006060591-tmhssin2h21lcre235vtolojh4g403ep.apps.googleusercontent.com");

    let url = Url::parse(&authorization.unwrap()).unwrap();
    let params: std::collections::HashMap<_, _> = url.query_pairs().collect();
    assert_eq!(params["client_id"], client_id);
    assert_eq!(params["code_challenge_method"], "S256");
    assert!(!params.contains_key("client_secret"));
    assert_eq!(
        params["redirect_uri"],
        option_env!("CPR_BUILD_ANTIGRAVITY_OAUTH_REDIRECT_URI")
            .filter(|value| !value.is_empty())
            .unwrap_or("http://localhost:8085/auth/callback")
    );
    if let Some(scopes) =
        option_env!("CPR_BUILD_ANTIGRAVITY_OAUTH_SCOPES").filter(|value| !value.is_empty())
    {
        for scope in scopes.split_ascii_whitespace() {
            assert!(
                params["scope"]
                    .split_ascii_whitespace()
                    .any(|item| item == scope)
            );
        }
    }
}

#[tokio::test]
async fn verified_exchange_and_refresh_preserve_subject() {
    let server = MockServer::start().await;
    let oauth = stub_oauth(&server);
    let verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
    let state = "test-state";
    let authorize = Url::parse(&oauth.authorize_url(state, verifier).unwrap()).unwrap();
    let params: std::collections::HashMap<_, _> = authorize.query_pairs().collect();
    assert_eq!(params["code_challenge_method"], "S256");
    assert_eq!(
        params["code_challenge"],
        "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
    );
    assert_eq!(params["state"], state);
    let callback = format!("http://localhost:8085/auth/callback?state={state}&code=fake-code");
    assert!(
        oauth
            .callback_code(&format!("{callback}&code=other"), state)
            .is_err()
    );
    assert!(
        oauth
            .callback_code(&callback.replace("localhost", "example.test"), state)
            .is_err()
    );
    assert!(oauth.callback_code(&callback, "wrong-state").is_err());
    let code = oauth.callback_code(&callback, state).unwrap();

    Mock::given(method("POST"))
        .and(path("/token"))
        .and(body_string_contains("grant_type=authorization_code"))
        .and(body_string_contains("code=fake-code"))
        .and(body_string_contains(format!("code_verifier={verifier}")))
        .and(body_string_contains("client_id=operator-client"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token":"fake-first-access", "refresh_token":"fake-refresh",
            "token_type":"Bearer", "expires_in":3600, "scope":"test-scope",
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/userinfo"))
        .and(header("authorization", "Bearer fake-first-access"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id":"1001", "email":"user@example.test", "verified_email":true,
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(body_string_contains("grant_type=refresh_token"))
        .and(body_string_contains("refresh_token=fake-refresh"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token":"fake-refresh-access", "token_type":"Bearer", "expires_in":3600,
        })))
        .expect(1)
        .mount(&server)
        .await;

    let grant = oauth.exchange(&code, verifier, None).await.unwrap();
    assert_eq!(grant.material().subject(), "1001");
    assert_eq!(grant.email(), Some("user@example.test"));
    let refreshed = oauth.refresh(grant.material(), None).await.unwrap();
    assert_eq!(refreshed.material().subject(), "1001");
    assert_eq!(refreshed.material().access_token(), "fake-refresh-access");
    assert_eq!(refreshed.material().refresh_token(), "fake-refresh");
}
