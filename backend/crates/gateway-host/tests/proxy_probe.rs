use gateway_admin::ports::proxy::ProxyProbe;
use gateway_core::account::OutboundProxy;
use gateway_host::proxy_probe::HttpProxyProbe;
use serde_json::json;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{any, header},
};

#[tokio::test]
async fn proxy_probe_supports_ipv4_and_ipv6_proxies_and_exit_addresses() {
    for (listen_address, exit_ip) in [
        ("127.0.0.1:0", "203.0.113.8"),
        ("127.0.0.1:0", "2001:db8::8"),
        ("[::1]:0", "203.0.113.8"),
        ("[::1]:0", "2001:db8::8"),
    ] {
        let listener = std::net::TcpListener::bind(listen_address).unwrap();
        let proxy_server = MockServer::builder().listener(listener).start().await;
        Mock::given(header("proxy-authorization", "Basic dXNlcjpwYXNzd29yZA=="))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ip": exit_ip})))
            .expect(1)
            .mount(&proxy_server)
            .await;
        let proxy =
            OutboundProxy::parse(&format!("http://user:password@{}", proxy_server.address()))
                .unwrap();
        let result = HttpProxyProbe::new("http://unresolvable.invalid/ip")
            .test(&proxy)
            .await;
        assert!(
            result.success,
            "{listen_address} -> {exit_ip}: {}",
            result.message
        );
        assert_eq!(result.exit_ip.unwrap().to_string(), exit_ip);
    }
}

#[tokio::test]
async fn proxy_location_matches_tested_ip_and_failure_preserves_connectivity() {
    use wiremock::matchers::path;
    for (location, valid) in [
        (
            json!({"success":true,"ip":"203.0.113.8","country_code":"US","region":"California","city":"Los Angeles","timezone":{"id":"America/Los_Angeles"}}),
            true,
        ),
        (
            json!({"success":true,"ip":"203.0.113.9","country_code":"US","region":"California","city":"Los Angeles","timezone":{"id":"America/Los_Angeles"}}),
            false,
        ),
        (
            json!({"success":true,"ip":"203.0.113.8","country_code":"US","region":"California","city":"Los Angeles","timezone":{"id":"invalid/timezone"}}),
            false,
        ),
        (json!({"success":false}), false),
        (json!({"oversized":"x".repeat(8193)}), false),
    ] {
        let proxy_server = MockServer::start().await;
        Mock::given(path("/ip"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ip":"203.0.113.8"})))
            .expect(1)
            .mount(&proxy_server)
            .await;
        Mock::given(path("/geo/203.0.113.8"))
            .respond_with(ResponseTemplate::new(200).set_body_json(location))
            .expect(1)
            .mount(&proxy_server)
            .await;
        let result = HttpProxyProbe::new("http://unresolvable.invalid/ip")
            .with_location_endpoint("http://unresolvable.invalid/geo/")
            .test(&OutboundProxy::parse(&proxy_server.uri()).unwrap())
            .await;
        assert!(result.success);
        assert_eq!(result.location.is_some(), valid);
        if let Some(location) = result.location {
            assert_eq!(location.timezone, "America/Los_Angeles");
        }
    }
}

#[tokio::test]
async fn proxy_probe_rejects_auth_errors_redirects_and_invalid_or_oversized_responses() {
    for response in [
        ResponseTemplate::new(407),
        ResponseTemplate::new(302).insert_header("Location", "http://127.0.0.1/"),
        ResponseTemplate::new(200).set_body_json(json!({"ip": "not-an-ip"})),
        ResponseTemplate::new(200).set_body_string("a".repeat(1025)),
    ] {
        let proxy_server = MockServer::start().await;
        Mock::given(any())
            .respond_with(response)
            .expect(1)
            .mount(&proxy_server)
            .await;
        let result = HttpProxyProbe::new("http://unresolvable.invalid/ip")
            .test(&OutboundProxy::parse(&proxy_server.uri()).unwrap())
            .await;
        assert!(!result.success);
        assert!(result.exit_ip.is_none());
    }
}

#[tokio::test]
async fn unavailable_proxy_never_falls_back_to_direct_connection() {
    let target = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ip":"203.0.113.8"})))
        .expect(0)
        .mount(&target)
        .await;
    let unused = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = OutboundProxy::parse(&format!("http://{}", unused.local_addr().unwrap())).unwrap();
    drop(unused);
    let result = HttpProxyProbe::new(target.uri()).test(&proxy).await;
    assert!(!result.success);
}

#[tokio::test]
async fn invalid_certificate_configuration_should_not_fall_back_or_expose_details() {
    let target = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&target)
        .await;
    let result = HttpProxyProbe::new(target.uri())
        .with_client_builder(|_| Err("private-certificate-path"))
        .test(&OutboundProxy::parse(&target.uri()).unwrap())
        .await;
    assert!(!result.success);
    assert!(!result.message.contains("private-certificate-path"));
}

#[tokio::test]
async fn quality_report_distinguishes_reachability_restrictions_and_challenges() {
    use gateway_admin::model::proxies::ProxyQualityStatus as Status;
    use wiremock::matchers::path;
    let proxy_server = MockServer::start().await;
    Mock::given(path("/ip"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ip":"203.0.113.8"})))
        .expect(1)
        .mount(&proxy_server)
        .await;
    let cases = [
        (200, false, Status::Passed),
        (401, false, Status::Passed),
        (405, false, Status::Passed),
        (403, false, Status::Warning),
        (403, true, Status::Challenge),
        (429, false, Status::Warning),
        (302, false, Status::Warning),
        (407, false, Status::Failed),
        (503, false, Status::Failed),
    ];
    let mut targets = Vec::new();
    for (index, (code, challenge, _)) in cases.iter().enumerate() {
        let path_value = format!("/target-{index}");
        let mut response = ResponseTemplate::new(*code)
            .insert_header("Location", "http://unresolvable.invalid/ip");
        if *challenge {
            response = response.insert_header("cf-mitigated", "challenge");
        }
        Mock::given(path(path_value.clone()))
            .respond_with(response)
            .expect(1)
            .mount(&proxy_server)
            .await;
        targets.push((
            format!("target-{index}"),
            format!("http://unresolvable.invalid{path_value}"),
        ));
    }
    let report = HttpProxyProbe::new("http://unresolvable.invalid/ip")
        .with_quality_targets(targets)
        .quality(&OutboundProxy::parse(&proxy_server.uri()).unwrap())
        .await;
    assert_eq!(report.basic.exit_ip.unwrap().to_string(), "203.0.113.8");
    assert_eq!(report.checks.len(), cases.len() + 2);
    for (check, (code, _, expected)) in report.checks.iter().skip(1).zip(cases) {
        assert_eq!(check.status, expected);
        assert_eq!(check.http_status, Some(code));
    }
    assert_eq!(report.checks.last().unwrap().status, Status::Warning);
}

#[tokio::test]
async fn quality_target_does_not_bypass_unavailable_proxy_or_leak_secrets() {
    let target = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&target)
        .await;
    let unused = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = OutboundProxy::parse(&format!(
        "http://private-user:private-password@{}",
        unused.local_addr().unwrap()
    ))
    .unwrap();
    drop(unused);
    let report = HttpProxyProbe::new(target.uri())
        .with_quality_targets(vec![("local".to_owned(), target.uri())])
        .quality(&proxy)
        .await;
    assert!(!report.basic.success);
    assert_eq!(
        report.checks[1].status,
        gateway_admin::model::proxies::ProxyQualityStatus::Failed
    );
    assert!(!format!("{report:?}").contains("private-"));
}
