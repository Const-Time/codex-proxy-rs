//! Authenticate before buffering; account for aggregate upload memory and bound upload time.
use super::{
    auth::{authenticate_client, client_access_error_response},
    error::openai_error_response,
};
use crate::ApiState;
use axum::{
    body::Bytes,
    extract::{FromRequest, Request},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use futures::StreamExt as _;
use gateway_core::engine::execution::AuthenticatedClient;
use serde::Deserialize;
use std::{sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct IngressConfig {
    pub memory_budget_mib: u32,
    pub maximum_uploads: u32,
    pub upload_timeout_seconds: u64,
}
impl Default for IngressConfig {
    fn default() -> Self {
        Self {
            memory_budget_mib: 128,
            maximum_uploads: 32,
            upload_timeout_seconds: 60,
        }
    }
}
impl IngressConfig {
    pub(crate) fn valid(&self) -> bool {
        (1..=2048).contains(&self.memory_budget_mib)
            && (1..=4096).contains(&self.maximum_uploads)
            && self.upload_timeout_seconds > 0
    }
}
pub(crate) struct IngressBudget {
    bytes: Arc<Semaphore>,
    uploads: Arc<Semaphore>,
    timeout: Duration,
}
impl IngressBudget {
    pub(crate) fn new(config: &IngressConfig) -> Self {
        Self {
            bytes: Arc::new(Semaphore::new(
                config.memory_budget_mib as usize * 1024 * 1024,
            )),
            uploads: Arc::new(Semaphore::new(config.maximum_uploads as usize)),
            timeout: Duration::from_secs(config.upload_timeout_seconds),
        }
    }
}
pub(crate) struct AuthenticatedBody {
    pub client: AuthenticatedClient,
    pub body: Bytes,
    // Held through decoding/dispatch, so parallel handlers cannot immediately reuse body memory.
    pub memory: IngressMemory,
}
pub(crate) struct IngressMemory {
    permits: OwnedSemaphorePermit,
    capacity: Arc<Semaphore>,
}
impl IngressMemory {
    pub(crate) fn reserve(&mut self, bytes: usize) -> bool {
        let Ok(bytes) = u32::try_from(bytes) else {
            return false;
        };
        match self.capacity.clone().try_acquire_many_owned(bytes) {
            Ok(permit) => {
                self.permits.merge(permit);
                true
            }
            Err(_) => false,
        }
    }
}
impl FromRequest<ApiState> for AuthenticatedBody {
    type Rejection = Response;
    async fn from_request(request: Request, state: &ApiState) -> Result<Self, Response> {
        let budget = &state.openai().ingress;
        let _upload = budget
            .uploads
            .clone()
            .try_acquire_owned()
            .map_err(|_| exhausted())?;
        // The deadline includes plugin authentication, but never upstream generation/SSE delivery.
        tokio::time::timeout(budget.timeout, async {
            let client = authenticate_client(state.openai(), request.headers())
                .await
                .map_err(client_access_error_response)?;
            let mut memory = budget
                .bytes
                .clone()
                .try_acquire_many_owned(0)
                .map_err(|_| exhausted())?;
            let mut chunks = Vec::new();
            let mut length = 0usize;
            let mut body = request.into_body().into_data_stream();
            while let Some(chunk) = body.next().await {
                let chunk = chunk.map_err(|_| {
                    openai_error_response(
                        StatusCode::BAD_REQUEST,
                        "Could not read request body",
                        "invalid_request_error",
                        "invalid_request_body",
                    )
                    .into_response()
                })?;
                // Include the final contiguous allocation plus per-frame bookkeeping.
                let cost = chunk
                    .len()
                    .checked_mul(2)
                    .and_then(|n| n.checked_add(128))
                    .and_then(|n| u32::try_from(n).ok())
                    .ok_or_else(exhausted)?;
                memory.merge(
                    budget
                        .bytes
                        .clone()
                        .try_acquire_many_owned(cost)
                        .map_err(|_| exhausted())?,
                );
                length += chunk.len();
                chunks.push(chunk);
            }
            let mut bytes = Vec::with_capacity(length);
            for chunk in chunks {
                bytes.extend_from_slice(&chunk);
            }
            Ok(Self {
                client,
                body: Bytes::from(bytes),
                memory: IngressMemory {
                    permits: memory,
                    capacity: budget.bytes.clone(),
                },
            })
        })
        .await
        .map_err(|_| {
            openai_error_response(
                StatusCode::REQUEST_TIMEOUT,
                "Request body upload timed out",
                "invalid_request_error",
                "request_body_timeout",
            )
            .into_response()
        })?
    }
}
pub(crate) fn exhausted() -> Response {
    let mut response = openai_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "Gateway upload capacity is exhausted; retry later",
        "server_error",
        "ingress_capacity_exhausted",
    )
    .into_response();
    response.headers_mut().insert(
        axum::http::header::RETRY_AFTER,
        axum::http::HeaderValue::from_static("1"),
    );
    response
}
