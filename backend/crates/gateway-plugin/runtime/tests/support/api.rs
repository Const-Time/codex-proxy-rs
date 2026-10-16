//! Shared native API and Host connection ownership; no worker/signal-supervisor claim.
use crate::services::upstream_sdk::environment::Environment;
use gateway_admin::plugins::PluginManager;
use std::sync::Arc;

pub(crate) struct ApiServer {
    pub(crate) address: std::net::SocketAddr,
    pub(crate) client: reqwest::Client,
    pub(crate) dispatcher: Arc<dyn gateway_core::engine::middleware::http::Dispatcher>,
    cancellation: gateway_core::lifecycle::CancellationToken,
    task: tokio::task::JoinHandle<()>,
    _workers: gateway_host::workers::WorkerSupervisor,
}
impl ApiServer {
    pub(crate) async fn new(env: &Environment, manager: Arc<PluginManager>) -> Self {
        Self::with_timeout(env, manager, None).await
    }
    pub(crate) async fn with_timeout(
        env: &Environment,
        manager: Arc<PluginManager>,
        request_timeout_seconds: Option<u64>,
    ) -> Self {
        let cancellation = gateway_core::lifecycle::CancellationToken::new();
        let connections = Arc::new(gateway_host::serve::ConnectionTracker::new(
            cancellation.clone(),
        ));
        let workers = gateway_host::workers::WorkerSupervisor::new(cancellation.clone());
        let app = gateway_api::initialize(
            gateway_api::ApiConfig {
                ingress: Default::default(),
                asset_directory: env.cache.path().into(),
                cors_allowed_origins: vec![],
                request_timeout_seconds,
                request_id_header: "x-request-id".into(),
            },
            env.core.execution_service(),
            env.admin.services().with_plugins(manager),
            vec![],
            workers.health_source(),
            connections,
        )
        .unwrap();
        let dispatcher = app.dispatcher();
        // Real API routes and Host connection guards; this fixture does not
        // claim to run the full Host worker/signal supervisor.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = cancellation.clone();
        let task = tokio::spawn(async move {
            axum::serve(listener, app.router())
                .with_graceful_shutdown(async move { shutdown.cancelled().await })
                .await
                .unwrap();
        });
        Self {
            address,
            dispatcher,
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(std::time::Duration::from_secs(20))
                .build()
                .unwrap(),
            cancellation,
            task,
            _workers: workers,
        }
    }
    pub(crate) async fn close(&mut self) {
        self.cancellation.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(5), &mut self.task)
            .await
            .expect("API connection drain")
            .unwrap();
    }
}
impl Drop for ApiServer {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.task.abort();
    }
}
