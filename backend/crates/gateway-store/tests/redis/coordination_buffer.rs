use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use gateway_core::engine::ModelRequestId;
use gateway_core::engine::admission::{
    ClientAdmissionDecision, ClientAdmissionError, ClientAdmissionPort, ClientAdmissionRecovery,
    ClientAdmissionRequest, ClientAdmissionRestoreResult,
};
use gateway_core::lifecycle::CancellationToken;
use gateway_core::policy::ClientApiKeyId;
use gateway_store::redis::BufferedClientAdmissionPort;

#[derive(Default)]
struct RecordingCoordination {
    operations: Mutex<Vec<&'static str>>,
}

impl RecordingCoordination {
    fn record(&self, operation: &'static str) {
        self.operations
            .lock()
            .expect("coordination operations lock")
            .push(operation);
    }

    fn operations(&self) -> Vec<&'static str> {
        self.operations
            .lock()
            .expect("coordination operations lock")
            .clone()
    }
}

impl ClientAdmissionPort for RecordingCoordination {
    fn maintain(
        &self,
        _: &gateway_core::policy::ClientApiKeyId,
        _: &gateway_core::engine::ModelRequestId,
        _: std::time::Duration,
        _: gateway_core::lifecycle::CancellationToken,
    ) -> Box<dyn gateway_core::engine::admission::ClientAdmissionLeaseGuard> {
        self.record("maintain");
        Box::new(TestAdmissionLease)
    }

    fn admit(
        &self,
        _: ClientAdmissionRequest,
    ) -> BoxFuture<'_, Result<ClientAdmissionDecision, ClientAdmissionError>> {
        Box::pin(async { Ok(ClientAdmissionDecision::Granted) })
    }

    fn release<'a>(
        &'a self,
        _: &'a ClientApiKeyId,
        _: &'a ModelRequestId,
    ) -> BoxFuture<'a, Result<bool, ClientAdmissionError>> {
        Box::pin(async move {
            self.record("admission");
            Ok(true)
        })
    }

    fn restore(
        &self,
        _: ClientAdmissionRecovery,
    ) -> BoxFuture<'_, Result<ClientAdmissionRestoreResult, ClientAdmissionError>> {
        Box::pin(async { Ok(ClientAdmissionRestoreResult::default()) })
    }
}

#[tokio::test]
async fn full_recoverable_coordination_queues_should_drop_writes_without_waiting() {
    let inner = Arc::new(RecordingCoordination::default());
    let (admissions, _admission_writer) = BufferedClientAdmissionPort::with_capacity(
        inner.clone(),
        NonZeroUsize::new(1).expect("capacity"),
    );
    let client = ClientApiKeyId::new("key_buffer_test").expect("client key");
    let request = ModelRequestId::new("req_buffer_test").expect("request ID");

    tokio::time::timeout(Duration::from_millis(50), async {
        admissions
            .release(&client, &request)
            .await
            .expect("first admission enqueue");
        admissions
            .release(&client, &request)
            .await
            .expect("full admission queue remains fail-open");
    })
    .await
    .expect("coordination enqueue must never wait for Redis");

    assert!(inner.operations().is_empty());
}

#[tokio::test]
async fn redis_coordination_writers_should_flush_each_side_effect() {
    let inner = Arc::new(RecordingCoordination::default());
    let (admissions, admission_writer) = BufferedClientAdmissionPort::with_capacity(
        inner.clone(),
        NonZeroUsize::new(8).expect("capacity"),
    );
    let client = ClientApiKeyId::new("key_writer_test").expect("client key");
    let request = ModelRequestId::new("req_writer_test").expect("request ID");
    admissions
        .release(&client, &request)
        .await
        .expect("enqueue admission release");
    let cancellation = CancellationToken::new();
    let tasks = [spawn_writer(
        Arc::new(admission_writer),
        cancellation.clone(),
    )];
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if inner.operations().len() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("writers should drain queued coordination writes");
    cancellation.cancel();
    for task in tasks {
        task.await
            .expect("coordination writer task")
            .expect("coordination writer cancellation");
    }

    let mut operations = inner.operations();
    operations.sort_unstable();
    assert_eq!(operations, ["admission"]);
}

fn spawn_writer<T>(
    writer: Arc<T>,
    cancellation: CancellationToken,
) -> tokio::task::JoinHandle<Result<(), gateway_core::task::WorkerTaskError>>
where
    T: gateway_core::task::DaemonTask + 'static,
{
    tokio::spawn(async move { writer.run(cancellation).await })
}

struct TestAdmissionLease;
impl gateway_core::engine::admission::ClientAdmissionLeaseGuard for TestAdmissionLease {}

#[tokio::test]
async fn renewal_bypasses_release_queue_and_delegates_to_the_authoritative_port() {
    let inner = Arc::new(RecordingCoordination::default());
    let (admissions, writer) = BufferedClientAdmissionPort::new(inner.clone());
    drop(writer);
    let guard = gateway_core::engine::admission::ClientAdmissionPort::maintain(
        &admissions,
        &ClientApiKeyId::new("key_renewal").unwrap(),
        &ModelRequestId::new("req_renewal").unwrap(),
        Duration::from_secs(30),
        CancellationToken::new(),
    );
    assert_eq!(inner.operations(), vec!["maintain"]);
    drop(guard);
}
