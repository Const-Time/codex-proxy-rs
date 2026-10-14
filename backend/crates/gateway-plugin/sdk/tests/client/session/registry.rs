//! 宿主回调关联、退休回调与强类型调用合同测试

use super::*;
use std::future::Future as _;

struct NextHandler {
    park_callback: bool,
}
impl PluginHandler for NextHandler {
    fn call(&self, call: PluginCall) -> CallFuture<'_> {
        Box::pin(async move {
            if call.method == "echo" {
                return Ok(CallReply::unary(call.params, call.payload));
            }
            let mut next = Box::pin(call.host.call(NEXT_METHOD, json!({}), vec![]));
            if self.park_callback {
                // Start next once, then leave its future unpolled. Receipt must still
                // resume the parent processing budget in the session registry.
                std::future::poll_fn(|cx| {
                    assert!(next.as_mut().poll(cx).is_pending());
                    std::task::Poll::Ready(())
                })
                .await;
                pending::<()>().await;
            }
            let reply = next.await.map_err(SessionError::into_plugin_fault)?;
            Ok(CallReply::unary(reply.result, reply.payload))
        })
    }
}

async fn pending_next(host: &mut HostPeer) -> u64 {
    send_call_with_timeout(
        host,
        1,
        "next",
        json!({}),
        vec![],
        Duration::from_millis(100),
    )
    .await;
    let Message::Callback {
        id,
        parent_id: 1,
        method,
        ..
    } = receive(host).await.message
    else {
        panic!("expected next callback")
    };
    assert_eq!(method, NEXT_METHOD);
    id
}

#[tokio::test(start_paused = true)]
async fn managed_next_outlives_processing_budget_and_returns_the_original_reply() {
    let (mut host, task) = start_session(NextHandler {
        park_callback: false,
    })
    .await;
    let id = pending_next(&mut host).await;
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    send_control(
        &mut host,
        Message::Result {
            id,
            result: json!("after-next"),
        },
    )
    .await;
    assert!(
        matches!(receive(&mut host).await.message, Message::Result { id: 1, result } if result == json!("after-next"))
    );
    shutdown(&mut host, task).await;
}

#[tokio::test(start_paused = true)]
async fn receiving_next_reply_resumes_budget_even_if_plugin_never_polls_callback_again() {
    let (mut host, task) = start_session(NextHandler {
        park_callback: true,
    })
    .await;
    let id = pending_next(&mut host).await;
    tokio::time::advance(Duration::from_secs(1)).await;
    send_control(
        &mut host,
        Message::Result {
            id,
            result: json!({}),
        },
    )
    .await;
    let Message::Error { id: 1, error } = receive(&mut host).await.message else {
        panic!("parent budget must resume at receipt")
    };
    assert_eq!(error.code, ErrorCode::Timeout);
    shutdown(&mut host, task).await;
}

#[tokio::test(start_paused = true)]
async fn cancelling_paused_next_retires_callback_without_ending_the_session() {
    let (mut host, task) = start_session(NextHandler {
        park_callback: false,
    })
    .await;
    let id = pending_next(&mut host).await;
    tokio::time::advance(Duration::from_secs(1)).await;
    send_control(&mut host, Message::Cancel { id: 1 }).await;
    assert!(matches!(
        receive(&mut host).await.message,
        Message::Cancelled { id: 1 }
    ));
    send_control(
        &mut host,
        Message::Result {
            id,
            result: json!({}),
        },
    )
    .await;
    send_call(&mut host, 3, "echo", json!("still-ready"), vec![]).await;
    assert!(
        matches!(receive(&mut host).await.message, Message::Result { id: 3, result } if result == json!("still-ready"))
    );
    shutdown(&mut host, task).await;
}

#[tokio::test(start_paused = true)]
async fn unrelated_host_callback_does_not_pause_processing_budget() {
    let (mut host, task) = start_session(TestHandler::default()).await;
    send_call_with_timeout(
        &mut host,
        1,
        "callback",
        json!({}),
        vec![],
        Duration::from_millis(100),
    )
    .await;
    assert!(matches!(
        receive(&mut host).await.message,
        Message::Callback { parent_id: 1, .. }
    ));
    let Message::Error { id: 1, error } = receive(&mut host).await.message else {
        panic!("only managed next suspends processing budget")
    };
    assert_eq!(error.code, ErrorCode::Timeout);
    shutdown(&mut host, task).await;
}

#[tokio::test]
async fn typed_budget_helpers_preserve_wire_contract_and_propagate_failure_without_retry() {
    for failed in [false, true] {
        let (mut host, task) = start_session(TestHandler::default()).await;
        send_call(&mut host, 1, "reset_budget", json!({}), vec![]).await;
        let list = receive(&mut host).await;
        let Message::Callback {
            id,
            parent_id: 1,
            method,
            params,
        } = list.message
        else {
            panic!("expected Key list callback")
        };
        assert_eq!(method, "host.keys.list");
        assert_eq!(params, json!({"cursor":null,"limit":10}));
        assert!(list.payload.is_empty());
        send_control(&mut host, Message::Result { id, result: json!({"keys":[{"id":"key_1","name":"test","enabled":true}],"next_cursor":null}) }).await;
        let reset = receive(&mut host).await;
        let Message::Callback {
            id,
            parent_id: 1,
            method,
            params,
        } = reset.message
        else {
            panic!("expected budget reset callback")
        };
        assert_eq!(method, "host.keys.reset_budget");
        assert_eq!(params, json!({}));
        assert_eq!(
            serde_json::from_slice::<Value>(&reset.payload).unwrap(),
            json!({"client_key_id":"key_1","period":"weekly"})
        );
        if failed {
            send_control(
                &mut host,
                Message::Error {
                    id,
                    error: PluginFault::new(ErrorCode::Conflict, "stale instance"),
                },
            )
            .await;
            let Message::Error { id: 1, error } = receive(&mut host).await.message else {
                panic!("expected error propagated without another callback")
            };
            assert_eq!(error.code, ErrorCode::Conflict);
        } else {
            write_frame(
                &mut host.writer,
                &Frame {
                    message: Message::Result {
                        id,
                        result: json!({}),
                    },
                    payload: serde_json::to_vec(&json!({"client_key_id":"key_1"})).unwrap(),
                },
            )
            .await
            .unwrap();
            let Message::Result { id: 1, result } = receive(&mut host).await.message else {
                panic!("expected successful reset")
            };
            assert_eq!(result, json!({"client_key_id":"key_1"}));
        }
        shutdown(&mut host, task).await;
    }
}

#[tokio::test]
async fn host_callback_round_trips_while_parent_call_is_waiting() {
    let (mut host, task) = start_session(TestHandler::default()).await;
    send_call(&mut host, 1, "callback", json!({"event": "ready"}), vec![7]).await;

    let callback = receive(&mut host).await;
    let callback_id = match callback.message {
        Message::Callback {
            id,
            parent_id: 1,
            ref method,
            ref params,
        } if id.is_multiple_of(2)
            && method == "host.log"
            && params == &json!({"event": "ready"}) =>
        {
            id
        }
        message => panic!("unexpected callback frame: {message:?}"),
    };
    assert_eq!(callback.payload, [7]);
    write_frame(
        &mut host.writer,
        &Frame {
            message: Message::Result {
                id: callback_id,
                result: json!({"recorded": true}),
            },
            payload: vec![9],
        },
    )
    .await
    .unwrap();

    let result = receive(&mut host).await;
    assert!(matches!(
        result,
        Frame {
            message: Message::Result { id: 1, result },
            payload,
        } if result == json!({"recorded": true}) && payload == [9]
    ));

    shutdown(&mut host, task).await;
}

#[tokio::test]
async fn callbacks_from_concurrent_calls_remain_correlated() {
    let (mut host, task) = start_session(TestHandler::default()).await;
    send_call(&mut host, 1, "callback", json!({"parent": 1}), vec![1]).await;
    send_call(&mut host, 3, "callback", json!({"parent": 3}), vec![3]).await;

    let mut callbacks = Vec::new();
    for _ in 0..2 {
        let frame = receive(&mut host).await;
        let Message::Callback {
            id,
            parent_id,
            method,
            params,
        } = frame.message
        else {
            panic!("expected callback")
        };
        assert_eq!(method, "host.log");
        assert_eq!(params, json!({"parent": parent_id}));
        assert_eq!(frame.payload, [parent_id as u8]);
        callbacks.push((id, parent_id));
    }
    assert!(callbacks[0].0 < callbacks[1].0);
    assert!(callbacks.iter().all(|(id, _)| id.is_multiple_of(2)));

    for (callback_id, parent_id) in callbacks.into_iter().rev() {
        write_frame(
            &mut host.writer,
            &Frame {
                message: Message::Result {
                    id: callback_id,
                    result: json!({"parent": parent_id}),
                },
                payload: vec![parent_id as u8],
            },
        )
        .await
        .unwrap();
    }
    let mut completed = Vec::new();
    for _ in 0..2 {
        let frame = receive(&mut host).await;
        let Message::Result { id, result } = frame.message else {
            panic!("expected parent result")
        };
        assert_eq!(result, json!({"parent": id}));
        assert_eq!(frame.payload, [id as u8]);
        completed.push(id);
    }
    completed.sort_unstable();
    assert_eq!(completed, [1, 3]);

    shutdown(&mut host, task).await;
}

#[tokio::test]
async fn cancelling_parent_retires_callback_and_late_reply_does_not_close_session() {
    let (mut host, task) = start_session(TestHandler::default()).await;
    send_call(&mut host, 1, "callback", json!({}), Vec::new()).await;
    let callback = receive(&mut host).await;
    let Message::Callback {
        id: callback_id, ..
    } = callback.message
    else {
        panic!("expected callback")
    };

    send_control(&mut host, Message::Cancel { id: 1 }).await;
    assert!(matches!(
        receive(&mut host).await.message,
        Message::Cancelled { id: 1 }
    ));
    send_control(
        &mut host,
        Message::Result {
            id: callback_id,
            result: json!({}),
        },
    )
    .await;
    send_call(&mut host, 3, "echo", json!("still-ready"), Vec::new()).await;
    assert!(matches!(
        receive(&mut host).await.message,
        Message::Result { id: 3, result } if result == json!("still-ready")
    ));

    shutdown(&mut host, task).await;
}

#[tokio::test]
async fn typed_key_and_quota_calls_keep_payloads_and_do_not_retry_failures() {
    for (entry, method, request, response) in [
        (
            "key_facts",
            "host.data.keys.get",
            json!({"client_key_id":"key_1"}),
            json!({"schema_version":1,"client_key_id":"key_1","enabled":false,"group_ids":["grp_1"]}),
        ),
        (
            "get_budget",
            "host.keys.get_budget",
            json!({"client_key_id":"key_1"}),
            json!({
                "client_key_id":"key_1", "daily_limit_usd":"10", "weekly_limit_usd":"20",
                "daily_used_usd":"1.25", "weekly_used_usd":"2.5", "daily_resets_at_ms":null, "weekly_resets_at_ms":null,
            }),
        ),
        (
            "update_budget_limits",
            "host.keys.update_budget_limits",
            json!({"client_key_id":"key_1","weekly_limit_usd":"12.5"}),
            json!({"client_key_id":"key_1"}),
        ),
        (
            "refresh_quota",
            "host.quota_observations.refresh",
            json!({"account_id":"acct_1"}),
            json!({"schema_version":1,"account_id":"acct_1","observed_at_ms":null,"windows":[]}),
        ),
    ] {
        for failed in [false, true] {
            let (mut host, task) = start_session(TestHandler::default()).await;
            send_call(&mut host, 1, entry, json!({}), vec![]).await;
            let callback = receive(&mut host).await;
            let Message::Callback {
                id,
                parent_id: 1,
                method: actual,
                params,
            } = callback.message
            else {
                panic!("expected callback");
            };
            assert_eq!(actual, method);
            assert_eq!(params, json!({}));
            assert_eq!(
                serde_json::from_slice::<Value>(&callback.payload).unwrap(),
                request
            );
            if failed {
                send_control(
                    &mut host,
                    Message::Error {
                        id,
                        error: PluginFault::new(ErrorCode::Conflict, "changed"),
                    },
                )
                .await;
                let Message::Error { id: 1, error } = receive(&mut host).await.message else {
                    panic!("expected propagated error without retry");
                };
                assert_eq!(error.code, ErrorCode::Conflict);
            } else {
                let mut response_payload = response.clone();
                if matches!(entry, "key_facts" | "refresh_quota") {
                    response_payload["future_fact"] = json!({"value":1});
                }
                write_frame(
                    &mut host.writer,
                    &Frame {
                        message: Message::Result {
                            id,
                            result: json!({}),
                        },
                        payload: serde_json::to_vec(&response_payload).unwrap(),
                    },
                )
                .await
                .unwrap();
                let Message::Result { id: 1, result } = receive(&mut host).await.message else {
                    panic!("expected projected response");
                };
                assert_eq!(result, response);
            }
            shutdown(&mut host, task).await;
        }
    }
}
