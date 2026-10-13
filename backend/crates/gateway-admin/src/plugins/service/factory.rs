//! Bind the original settings owner to the request's frozen publication.
use super::{Origin, PluginServiceAuthority, settings::Service};
use crate::plugins::middleware::PluginMiddlewareRegistry;
use gateway_core::engine::middleware::service as core;
use std::sync::Arc;

pub(super) struct Factory {
    pub(super) service: Service,
    pub(super) registry: PluginMiddlewareRegistry,
}
impl core::Factory for Factory {
    fn bind(
        &self,
        context: &core::Context,
        caller: core::Caller,
    ) -> Result<Arc<dyn core::Dispatcher>, core::Error> {
        let authority = PluginServiceAuthority::from_caller(caller).map_err(super::encode_error)?;
        Ok(self.service.plugin_dispatcher(
            Origin {
                registry: self.registry.clone(),
                request_id: context.request_id.clone(),
                call_id: context.call_id.clone(),
                cancellation: gateway_core::lifecycle::CancellationToken::linked([
                    context.cancellation.clone(),
                    self.registry.1.clone(),
                ]),
                deadline_at: context.deadline_at,
                call_scope: context.call_scope.clone(),
                dispatcher: None,
            },
            authority,
        ))
    }
}
