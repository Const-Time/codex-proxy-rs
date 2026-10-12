//! A projection of the original response body; commits and cleanup delegate inward.
use futures::future::BoxFuture;
use gateway_core::{
    engine::{
        CommitRequirement, CoordinatedEvent, EngineError, execution::ExecutionSession,
        middleware::*,
    },
    error::{GatewayError, GatewayErrorKind},
    event::{ProviderEvent, ProviderResponseHeader},
    lifecycle::CancellationToken,
};

pub(super) fn invalid() -> GatewayError {
    GatewayError::new(
        GatewayErrorKind::InvalidRequest,
        "native model middleware request is invalid",
    )
}
pub(super) fn gateway_error(error: MiddlewareError) -> GatewayError {
    let error = match error {
        MiddlewareError::Gateway(error) => error,
        MiddlewareError::Engine(EngineError::Middleware) => GatewayError::new(
            GatewayErrorKind::Internal,
            "native model attempt middleware failed",
        ),
        MiddlewareError::Engine(error) => {
            gateway_core::engine::execution::gateway_error_from_engine(&error)
        }
        MiddlewareError::InvalidState => GatewayError::new(
            GatewayErrorKind::InvalidRequest,
            "native model middleware state is invalid",
        ),
        MiddlewareError::Remote { rejected: true, .. } | MiddlewareError::Rejected => {
            GatewayError::new(
                GatewayErrorKind::PolicyDenied,
                "native model middleware rejected the request",
            )
        }
        MiddlewareError::Remote { .. } => {
            GatewayError::new(GatewayErrorKind::Internal, "native model plugin RPC failed")
        }
        MiddlewareError::Provider(error) => GatewayError::from_provider(&error),
        _ => GatewayError::new(GatewayErrorKind::Internal, "native model middleware failed"),
    };
    if error.upstream_failure().is_some() {
        error
    } else {
        error.with_upstream_failure(gateway_core::upstream::UpstreamSendState::Ambiguous, None)
    }
}
fn engine_error(error: MiddlewareError) -> EngineError {
    match error {
        MiddlewareError::Engine(error) => error,
        _ => EngineError::Middleware,
    }
}
pub(super) struct NativeSession {
    delivery: super::super::delivery::Delivery,
    headers: Vec<ProviderResponseHeader>,
    stop: CancellationToken,
    raw_json: bool,
    pending: Option<(bool, CommitRequirement)>,
}
impl NativeSession {
    pub(super) fn new(
        response: MiddlewareResponse,
        format: super::super::WireFormat,
        stop: CancellationToken,
    ) -> Result<Self, GatewayError> {
        let (delivery, headers) = super::super::delivery::Delivery::new(response, format, None);
        delivery.validate().map_err(gateway_error)?;
        Ok(Self {
            delivery,
            headers: headers
                .into_iter()
                .map(|h| {
                    let (n, v) = h.into_parts();
                    ProviderResponseHeader::new(n, v)
                })
                .collect(),
            stop,
            raw_json: format.raw_json(),
            pending: None,
        })
    }
}
impl ExecutionSession for NativeSession {
    fn next_event(&mut self) -> BoxFuture<'_, Result<Option<CoordinatedEvent>, EngineError>> {
        Box::pin(async move {
            if self.pending.is_some() {
                return Err(EngineError::InvalidDeliveryState);
            }
            let Some((event, terminal)) = self
                .delivery
                .model_batch(self.raw_json)
                .await
                .map_err(engine_error)?
            else {
                return Ok(None);
            };
            if event.commit_requirement() == CommitRequirement::CommitBeforeDelivery {
                self.pending = Some((terminal, event.commit_requirement()));
            }
            Ok(Some(event))
        })
    }
    fn collect_uncommitted(&mut self) -> BoxFuture<'_, Result<Vec<ProviderEvent>, EngineError>> {
        Box::pin(async move {
            let event = self
                .next_event()
                .await?
                .ok_or(EngineError::InvalidDeliveryState)?;
            if self.pending.is_some_and(|(terminal, _)| !terminal) {
                return Err(EngineError::InvalidDeliveryState);
            }
            Ok(event.into_provider_events())
        })
    }
    fn response_headers(&self) -> &[ProviderResponseHeader] {
        &self.headers
    }
    fn response_status_code(&self) -> Option<u16> {
        Some(self.delivery.status())
    }
    fn commit_downstream(&mut self, _: Option<u16>) -> BoxFuture<'_, Result<(), EngineError>> {
        Box::pin(async move {
            if let Some((terminal, requirement)) = self.pending {
                self.delivery
                    .model_commit(terminal, requirement)
                    .await
                    .map_err(engine_error)?;
                self.pending = None;
            }
            Ok(())
        })
    }
    fn record_client_status(&mut self, status: u16) -> BoxFuture<'_, Result<(), EngineError>> {
        Box::pin(async move {
            self.delivery.record_client_status(status).await;
            Ok(())
        })
    }
    fn is_finalized(&self) -> bool {
        self.pending.is_none() && self.delivery.model_finalized()
    }
    fn cancel(&self) {
        self.stop.cancel();
    }
    fn detach_finalize(mut self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            self.cancel();
            self.delivery.close().await;
        })
    }
}
