//! A frozen host policy may choose a public model and narrow its Provider.
//! Core retains Key scope, capability validation, continuation and admission.
use super::{ModelRequestId, nested::NestedExecutionContext};
use crate::{
    identity::ProviderKind,
    lifecycle::{CancellationToken, Deadline},
    operation::Operation,
    policy::ClientApiKeyId,
    routing::{AccountGroupId, PublicModelId},
};
use futures::future::BoxFuture;
use std::collections::BTreeSet;

#[derive(Clone)]
pub struct ModelRouteInput {
    pub(super) parent: NestedExecutionContext,
    pub(super) operation: Operation,
    pub(super) requested_model: PublicModelId,
    pub(super) available_providers: BTreeSet<ProviderKind>,
}
impl ModelRouteInput {
    pub fn request_id(&self) -> &ModelRequestId {
        &self.parent.request_id
    }
    pub fn client_key_id(&self) -> &ClientApiKeyId {
        self.parent.client.policy().key_id()
    }
    pub fn account_group_ids(&self) -> Vec<AccountGroupId> {
        self.parent
            .client
            .policy()
            .account_scope()
            .routing_snapshot()
            .groups_snapshot()
            .iter()
            .map(|g| g.id().clone())
            .collect()
    }
    pub fn excluded_instances(&self) -> &BTreeSet<String> {
        self.parent.scope.excluded_instances()
    }
    pub fn nested_context(&self) -> NestedExecutionContext {
        self.parent.clone()
    }
    pub fn operation(&self) -> &Operation {
        &self.operation
    }
    pub fn requested_model(&self) -> &PublicModelId {
        &self.requested_model
    }
    pub fn available_providers(&self) -> &BTreeSet<ProviderKind> {
        &self.available_providers
    }
    pub fn cancellation(&self) -> CancellationToken {
        self.parent.cancellation()
    }
    pub fn deadline(&self) -> Deadline {
        self.parent.deadline
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelRouteDecision {
    Unhandled,
    Route {
        provider: Option<ProviderKind>,
        model: Option<PublicModelId>,
    },
    Reject,
}
pub struct ModelRouteResult {
    pub decision: ModelRouteDecision,
    /// Only successfully consulted generations contribute revocation.
    pub revocation: CancellationToken,
}
#[derive(Debug, Clone, Copy)]
pub struct ModelRoutingFault;
pub trait ModelRouter: Send + Sync {
    /// Publication revocation is frozen before the first request middleware.
    fn cancellation(&self) -> CancellationToken;
    fn route_model(
        &self,
        input: ModelRouteInput,
    ) -> BoxFuture<'static, Result<ModelRouteResult, ModelRoutingFault>>;
}
