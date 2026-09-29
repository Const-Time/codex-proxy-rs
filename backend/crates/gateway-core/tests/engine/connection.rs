use gateway_core::engine::connection::ConnectionBudget;

#[test]
fn all_transports_share_one_opening_budget_but_other_requests_do_not() {
    let request = ConnectionBudget::default();
    let http = request.clone();
    let websocket = request.clone();
    for transport in [&http, &websocket, &http, &websocket] {
        assert!(transport.begin().unwrap().is_some());
    }
    assert!(request.exhausted());
    assert!(http.begin().is_err());
    assert!(websocket.retry_delay(0, "req_shared").is_none());
    assert!(ConnectionBudget::default().begin().is_ok());
}

#[test]
fn successful_opening_does_not_limit_later_generation_or_protocol_recovery() {
    let request = ConnectionBudget::default();
    request.begin().unwrap();
    request.complete();
    assert!(request.startup_remaining().is_none());
    assert!(request.retry_delay(0, "req_sent").is_none());
    assert!(!request.exhausted());
    assert!(request.begin().unwrap().is_none());
}
