//! Per-attempt raw JSON bridge; Core observes original receipts before plugins.
use super::*;
pub(super) struct RawAttempt {
    pub registry: Arc<PluginMiddlewareRegistry>,
    pub context: MiddlewareContext,
}
impl AttemptMiddleware for RawAttempt {
    fn invoke(
        &self,
        original: ProviderRequest,
        attempt: AttemptContext,
        next: AttemptNext,
    ) -> BoxFuture<'static, Result<AttemptOutput, MiddlewareError>> {
        let registry = self.registry.clone();
        let mut context = self.context.clone();
        Box::pin(async move {
            context.attempt_index = Some(attempt.attempt_index().get());
            context.provider = Some(original.candidate().provider().clone());
            context.model = None;
            context.operation = original.operation().kind();
            context.cancellation = attempt.cancellation().clone();
            context.deadline_at = attempt.deadline();
            let plan = registry
                .freeze(context)
                .map_err(|_| MiddlewareError::Fault)?;
            let payload = super::super::raw::payload(original.operation())?;
            let headers = super::super::raw::context_headers(payload)?;
            let request = super::super::raw::request(original.operation(), &headers)?;
            let terminal =
                gateway_core::middleware::compose(vec![], move |request: MiddlewareRequest| {
                    Box::pin(async move {
                        let operation = super::super::raw::rewrite(original.operation(), &request)?;
                        body::SourceBody::response(
                            next.run(operation).await?,
                            super::super::WireFormat::RawJson,
                        )
                        .await
                    })
                });
            output::decode(
                plan.run(request, terminal).await?,
                super::super::WireFormat::RawJson,
            )
        })
    }
}
