use super::*;

#[tokio::test(start_paused = true)]
async fn idle_pump_sends_heartbeat_without_bursting_after_a_delay() {
    let PumpHarness {
        connection,
        incoming: _incoming,
        mut written,
        ..
    } = test_connection(false);
    tokio::task::yield_now().await;
    assert!(matches!(written.try_recv(), Err(TryRecvError::Empty)));

    tokio::time::advance(Duration::from_secs(25)).await;
    let first = written.recv().await.expect("first heartbeat");
    assert!(matches!(first, Message::Ping(_)));

    tokio::time::advance(Duration::from_secs(120)).await;
    let second = written.recv().await.expect("heartbeat after delay");
    assert!(matches!(&second, Message::Ping(_)));
    assert_ne!(first, second);
    tokio::task::yield_now().await;
    assert!(matches!(written.try_recv(), Err(TryRecvError::Empty)));
    drop(connection);
}

#[tokio::test(start_paused = true)]
async fn missing_pongs_do_not_cancel_the_connection_or_block_business_frames() {
    let PumpHarness {
        mut connection,
        incoming,
        mut written,
        ..
    } = test_connection(false);
    for _ in 0..13 {
        tokio::time::advance(Duration::from_secs(25)).await;
        assert!(matches!(written.recv().await, Some(Message::Ping(_))));
    }

    incoming
        .send(Ok(Message::Text("still connected".into())))
        .expect("send business frame after 325 seconds without Pong");
    assert!(matches!(
        connection.next_event().await,
        Some(ConnectionEvent::Text(text)) if text == "still connected"
    ));
    incoming.send(Ok(Message::Close(None))).expect("close peer");
    assert_eq!(
        connection.wait_for_exit().await,
        PumpExitReason::ClientClose
    );
    tokio::time::advance(Duration::from_secs(25)).await;
    assert!(written.recv().await.is_none());
}

#[tokio::test(start_paused = true)]
async fn stalled_heartbeat_can_be_cancelled_without_waiting_for_write_timeout() {
    let PumpHarness {
        mut connection,
        incoming: _incoming,
        written: _written,
        dropped,
        cancellation,
    } = test_connection(true);
    tokio::time::advance(Duration::from_secs(25)).await;
    tokio::task::yield_now().await;
    cancellation.cancel();
    let reason = tokio::time::timeout(Duration::from_secs(1), connection.wait_for_exit())
        .await
        .expect("cancel a blocked heartbeat promptly");
    assert_eq!(reason, PumpExitReason::LifecycleShutdown);
    assert!(dropped.load(Ordering::Acquire));
}

#[tokio::test(start_paused = true)]
async fn stalled_heartbeat_obeys_the_existing_write_timeout() {
    let PumpHarness {
        mut connection,
        incoming: _incoming,
        written: _written,
        dropped,
        ..
    } = test_connection(true);
    let started = tokio::time::Instant::now();
    assert_eq!(
        connection.wait_for_exit().await,
        PumpExitReason::WriteTimeout
    );
    assert_eq!(started.elapsed(), Duration::from_secs(25 + 300));
    assert!(dropped.load(Ordering::Acquire));
}

#[tokio::test]
async fn active_response_receives_heartbeats_during_two_hundred_seconds_of_silence() {
    let (trace, mut socket, server) = start_active_response().await;
    for _ in 0..8 {
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(25)).await;
        tokio::task::yield_now().await;
        tokio::time::resume();
        let message = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .expect("heartbeat while the upstream response is pending")
            .expect("connection remains open")
            .expect("valid heartbeat frame");
        let ClientMessage::Ping(payload) = message else {
            panic!("expected heartbeat while business stream is silent: {message:?}");
        };
        // 与官方 WsStream 一样，在业务响应未完成时回复同 payload 的 Pong。
        socket
            .send(ClientMessage::Pong(payload))
            .await
            .expect("reply to heartbeat");
    }
    assert_eq!(trace.starts.load(Ordering::Acquire), 1);
    assert!(!trace.cancelled.load(Ordering::Acquire));

    trace.release_terminal.notify_one();
    let message = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .expect("terminal response after heartbeats")
        .expect("connection remains open")
        .expect("valid terminal frame");
    let ClientMessage::Text(text) = message else {
        panic!("expected completed response: {message:?}");
    };
    let value: Value = serde_json::from_str(&text).expect("terminal JSON");
    assert_eq!(value["type"], "response.completed");
    assert_eq!(trace.starts.load(Ordering::Acquire), 1);
    socket.close(None).await.expect("close WebSocket");
    server.abort();
}
