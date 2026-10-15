use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::future::BoxFuture;
use gateway_core::lifecycle::CancellationToken;
use gateway_core::lifecycle::ConnectionLifecycle as _;
use gateway_core::task::{
    DaemonRestartPolicy, DaemonTask, WorkerContribution, WorkerId, WorkerKind, WorkerRegistration,
    WorkerRunnable, WorkerTaskError,
};
use gateway_host::HostBundle;
use gateway_host::serve::{ConnectionTracker, bind_listener};
use tokio::sync::{Mutex, Notify, mpsc, oneshot};

struct DrainWriter {
    entered: Arc<Notify>,
    stopped: Arc<AtomicBool>,
    receiver: Mutex<Option<mpsc::Receiver<oneshot::Sender<()>>>>,
}

impl DaemonTask for DrainWriter {
    fn run(&self, cancellation: CancellationToken) -> BoxFuture<'_, Result<(), WorkerTaskError>> {
        Box::pin(async move {
            let mut receiver = self.receiver.lock().await.take().unwrap();
            self.entered.notify_one();
            loop {
                tokio::select! { biased;
                    () = cancellation.cancelled() => {
                        self.stopped.store(true, Ordering::SeqCst);
                        return Ok(());
                    },
                    item = receiver.recv() => {
                        let Some(acknowledge) = item else {
                            return Err(WorkerTaskError::safe("test writer closed"));
                        };
                        let _ = acknowledge.send(());
                    },
                }
            }
        })
    }
}

#[test]
fn host_bundle_serve_is_a_consuming_process_entrypoint() {
    let _serve =
        |host: HostBundle, router: axum::Router| host.serve(router, std::future::ready(()));

    assert_eq!(std::mem::size_of_val(&_serve), 0);
}

#[test]
fn command_terminal_preserves_results_cancels_and_drains_without_binding_http() {
    const CASE: &str = "CPR_TEST_COMMAND_TERMINAL_CASE";
    let Some(case) = std::env::var_os(CASE) else {
        for case in [
            "ok",
            "cancel",
            "drain_timeout",
            "operation_timeout",
            "workers",
            "serve_workers",
            "extension_workers",
            "serve_extension_workers",
            "serve_error_extension_workers",
            "startup_error_extension_workers",
        ] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "serve::command_terminal_preserves_results_cancels_and_drains_without_binding_http"])
                .env(CASE, case).env("RUST_LOG", "info").output().unwrap();
            assert!(
                output.status.success(),
                "{case}: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        #[cfg(unix)]
        {
            for signal_case in ["sigterm", "sigterm_extension_workers"] {
                let directory = tempfile::tempdir().unwrap();
                let ready = directory.path().join("ready");
                let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "serve::command_terminal_preserves_results_cancels_and_drains_without_binding_http"])
                .env(CASE, signal_case).env("CPR_TEST_COMMAND_TERMINAL_READY", &ready)
                .env("RUST_LOG", "info").stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).spawn().unwrap();
                let until = std::time::Instant::now() + Duration::from_secs(5);
                while !ready.exists() && std::time::Instant::now() < until {
                    assert!(
                        child.try_wait().unwrap().is_none(),
                        "signal child exited before ready"
                    );
                    std::thread::sleep(Duration::from_millis(5));
                }
                if !ready.exists() {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("signal child did not become ready");
                }
                assert!(
                    std::process::Command::new("kill")
                        .args(["-TERM", &child.id().to_string()])
                        .status()
                        .unwrap()
                        .success()
                );
                let output = child.wait_with_output().unwrap();
                assert!(
                    output.status.success(),
                    "{} {}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        }
        return;
    };
    let directory = tempfile::tempdir().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::time::pause();
        let mut listener = Some(std::net::TcpListener::bind("127.0.0.1:0").unwrap());
        let mut config = crate::logging::logging_config(directory.path().into(), false);
        config.listen.host = "127.0.0.1".into();
        config.listen.port = listener.as_ref().unwrap().local_addr().unwrap().port();
        config.drain_timeout_seconds = 1;
        config.worker_shutdown_timeout_seconds = 1;
        config.logging.level = "info".into();
        let host = gateway_host::initialize(config).await.unwrap();
        let cancellation = host.cancellation();
        let graph = gateway_core::engine::nested::ExtensionCallScope::default();
        match case.to_str().unwrap() {
            "ok" => {
                let child = graph.fork_http("host-command".into()).unwrap();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    drop(child);
                });
                let completion = host
                    .command(async { "returned" }, graph.clone(), async {})
                    .await;
                assert_eq!(completion.result, Some("returned"));
                assert!(completion.graph_drained);
            }
            "cancel" => {
                let stop = cancellation.clone();
                tokio::spawn(async move {
                    tokio::task::yield_now().await;
                    stop.cancel();
                });
                let completion = host
                    .command(
                        async {
                            cancellation.cancelled().await;
                            "cancelled"
                        },
                        graph.clone(),
                        async {},
                    )
                    .await;
                assert_eq!(completion.result, Some("cancelled"));
                assert!(completion.graph_drained);
            }
            #[cfg(unix)]
            "sigterm" => {
                let ready = std::env::var_os("CPR_TEST_COMMAND_TERMINAL_READY").unwrap();
                let operation = async {
                    // This task runs after the host select registers its native
                    // signal listener; readiness is not based on a sleep.
                    tokio::spawn(async move {
                        std::fs::write(ready, b"ready").unwrap();
                    });
                    cancellation.cancelled().await;
                    "signal cancelled"
                };
                let completion = host.command(operation, graph.clone(), async {}).await;
                assert_eq!(completion.result, Some("signal cancelled"));
                assert!(completion.graph_drained);
            }
            "drain_timeout" => {
                let held = graph.fork_http("host-command".into()).unwrap();
                let completion = host
                    .command(async { "confirmed effects" }, graph.clone(), async {})
                    .await;
                assert_eq!(completion.result, Some("confirmed effects"));
                assert!(!completion.graph_drained);
                drop(held);
                graph.wait_until_idle().await;
            }
            "operation_timeout" => {
                let held = graph.fork_http("host-command".into()).unwrap();
                cancellation.cancel();
                let operation = async move {
                    let _held = held;
                    std::future::pending::<String>().await
                };
                let completion = host.command(operation, graph.clone(), async {}).await;
                assert!(completion.result.is_none());
                assert!(
                    completion.graph_drained,
                    "dropping the operation must close its original guards before drain"
                );
                graph.wait_until_idle().await;
            }
            "workers"
            | "serve_workers"
            | "extension_workers"
            | "serve_extension_workers"
            | "serve_error_extension_workers"
            | "sigterm_extension_workers"
            | "startup_error_extension_workers" => {
                let entered = Arc::new(Notify::new());
                let stopped = Arc::new(AtomicBool::new(false));
                let (sender, receiver) = mpsc::channel(1);
                let writer = DrainWriter {
                    entered: entered.clone(),
                    stopped: stopped.clone(),
                    receiver: Mutex::new(Some(receiver)),
                };
                let mut plan = crate::workers::complete_plan(Vec::new());
                plan.retain(|item| item.kind() != WorkerKind::OpsFlush);
                plan.push(WorkerContribution::Registration(
                    WorkerRegistration::try_new(
                        WorkerId::try_new(WorkerKind::OpsFlush, "drain_test").unwrap(),
                        WorkerRunnable::Daemon {
                            restart: DaemonRestartPolicy::try_new(
                                Duration::from_secs(1),
                                Duration::from_secs(2),
                            )
                            .unwrap(),
                            task: Box::new(writer),
                        },
                    )
                    .unwrap(),
                ));
                host.start_workers(plan, Arc::new(crate::workers::FakeLeasePort::default()))
                    .unwrap();
                entered.notified().await;
                let extension = case.to_str().unwrap().contains("extension");
                let child = (!extension).then(|| graph.fork_http("host-command".into()).unwrap());
                let connection = (case == "serve_workers")
                    .then(|| host.connection_lifecycle().try_register().unwrap());
                let observed = cancellation.clone();
                let queued_sender = sender.clone();
                let cleanup = tokio::spawn(async move {
                    observed.cancelled().await;
                    tokio::task::yield_now().await;
                    let (acknowledge, acknowledged) = oneshot::channel();
                    queued_sender
                        .send(acknowledge)
                        .await
                        .expect("writer must stay live during drain");
                    acknowledged
                        .await
                        .expect("cleanup write after cancellation must finish");
                    drop(child);
                    drop(connection);
                });
                let finalize = async {
                    cleanup
                        .await
                        .expect("late cleanup finishes before worker shutdown");
                    assert!(!stopped.load(Ordering::SeqCst));
                    tracing::info!("extension cleanup finished before writer shutdown");
                };
                if case == "startup_error_extension_workers" {
                    host.shutdown(finalize).await;
                } else if !case.to_str().unwrap().starts_with("serve_") {
                    let operation = async {
                        if case == "sigterm_extension_workers" {
                            let ready =
                                std::env::var_os("CPR_TEST_COMMAND_TERMINAL_READY").unwrap();
                            tokio::spawn(async move {
                                std::fs::write(ready, b"ready").unwrap();
                            });
                            cancellation.cancelled().await;
                        }
                        "confirmed"
                    };
                    let completion = host.command(operation, graph.clone(), finalize).await;
                    assert_eq!(completion.result, Some("confirmed"));
                    assert!(completion.graph_drained);
                } else {
                    let bind_error = case == "serve_error_extension_workers";
                    if !bind_error {
                        drop(listener.take());
                        let stop = cancellation.clone();
                        tokio::spawn(async move {
                            tokio::task::yield_now().await;
                            stop.cancel();
                        });
                    }
                    assert_eq!(
                        host.serve(axum::Router::new(), finalize).await.is_err(),
                        bind_error
                    );
                }
                assert!(
                    std::fs::read_dir(directory.path())
                        .unwrap()
                        .filter_map(Result::ok)
                        .any(|entry| std::fs::read_to_string(entry.path())
                            .unwrap_or_default()
                            .contains("extension cleanup finished before writer shutdown")),
                    "logging must remain live until extension finalization completes"
                );
                assert!(
                    stopped.load(Ordering::SeqCst),
                    "worker must join after drain"
                );
                drop(sender);
            }
            _ => panic!("unknown test case"),
        }
        assert!(cancellation.is_cancelled());
        drop(listener);
    });
}

#[tokio::test(start_paused = true)]
async fn wait_until_idle_should_return_immediately_without_active_connections() {
    let tracker = ConnectionTracker::new(CancellationToken::new());
    let started = tokio::time::Instant::now();

    tracker.wait_until_idle(Duration::from_secs(30)).await;

    assert_eq!(started.elapsed(), Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn wait_until_idle_should_give_up_at_timeout_while_connections_remain() {
    let tracker = ConnectionTracker::new(CancellationToken::new());
    let _guard = tracker.try_register().expect("register connection");
    let started = tokio::time::Instant::now();

    tracker.wait_until_idle(Duration::from_secs(30)).await;

    assert_eq!(started.elapsed(), Duration::from_secs(30));
}

#[tokio::test]
async fn wait_until_idle_should_wake_when_last_guard_drops_after_first_poll() {
    let tracker = ConnectionTracker::new(CancellationToken::new());
    let guard = tracker.try_register().expect("register connection");
    let mut wait = Box::pin(tracker.wait_until_idle(Duration::from_secs(30)));
    assert!(futures::poll!(wait.as_mut()).is_pending());

    drop(guard);

    tokio::time::timeout(Duration::from_secs(2), wait)
        .await
        .expect("woken by last guard drop instead of waiting out the timeout");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_until_idle_should_observe_guard_drop_racing_the_idle_check() {
    // 回归约束：notified 必须在读取活跃计数前完成注册（enable），否则最后
    // 一个 guard 在计数检查与首次 poll 之间 drop 时唤醒丢失，只能等满整个
    // 超时。多线程反复交错，任何一次丢唤醒都会撞上 2s 超时并触发断言。
    for _ in 0..256 {
        let tracker = Arc::new(ConnectionTracker::new(CancellationToken::new()));
        let guard = tracker.try_register().expect("register connection");
        let waiter = tokio::spawn({
            let tracker = Arc::clone(&tracker);
            async move { tracker.wait_until_idle(Duration::from_secs(2)).await }
        });
        let dropper = std::thread::spawn(move || drop(guard));
        let started = std::time::Instant::now();
        waiter.await.expect("waiter completes");
        dropper.join().expect("dropper completes");
        assert!(
            started.elapsed() < Duration::from_millis(1_900),
            "idle wakeup was lost and the waiter slept until the drain timeout"
        );
    }
}

#[tokio::test]
async fn bind_listener_should_retry_until_previous_listener_releases_port() {
    let holder = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("holder listener");
    let address = holder.local_addr().expect("holder address").to_string();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        drop(holder);
    });

    let listener = tokio::time::timeout(Duration::from_secs(8), bind_listener(&address))
        .await
        .expect("bound within the retry window")
        .expect("bound after the previous listener released the port");

    assert_eq!(
        listener.local_addr().expect("bound address").to_string(),
        address
    );
}
