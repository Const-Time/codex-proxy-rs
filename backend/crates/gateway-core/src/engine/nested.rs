//! Host-issued model ancestry, immutable authorization and bounded execution graphs.
use super::{ModelRequestId, execution::AuthenticatedClient};
use crate::{
    error::{GatewayError, GatewayErrorKind},
    lifecycle::{CancellationToken, Deadline},
    routing::RequestSettings,
};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

const MAX_DEPTH: usize = 8;
const MAX_CHILDREN: usize = 64;
const MAX_ACTIVE: usize = 16;

/// Optional host model target narrows Key authorization; it never grants an account.
#[derive(Debug, Clone, Default)]
pub struct ModelExecutionTarget {
    pub provider: Option<crate::identity::ProviderKind>,
    pub account: Option<crate::account::ProviderAccountId>,
    /// A parent attempt may already own this account's lease while awaiting its child.
    pub parent_account: Option<crate::account::ProviderAccountId>,
}

#[derive(Debug, Default)]
struct GraphState {
    total: usize,
    active: usize,
}
#[derive(Debug, Default)]
struct Graph(Mutex<GraphState>);
#[derive(Debug)]
struct ActiveChild {
    graph: Arc<Graph>,
    released: AtomicBool,
}
impl ActiveChild {
    fn release(&self) {
        if !self.released.swap(true, Ordering::AcqRel) {
            self.graph
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .active -= 1;
        }
    }
}
impl Drop for ActiveChild {
    fn drop(&mut self) {
        self.release();
    }
}

/// One execution owns its own effects marker; descendants notify only their ancestors.
#[derive(Debug, Clone, Copy)]
enum PermitLifetime {
    Execution,
    Http,
}

#[derive(Debug, Clone)]
pub struct ExtensionCallScope {
    graph: Arc<Graph>,
    ancestors: Arc<BTreeSet<String>>,
    parent_request_id: Option<ModelRequestId>,
    initiating_plugin_instance_id: Option<String>,
    permit: Option<Arc<ActiveChild>>,
    permit_lifetime: PermitLifetime,
    effects: Arc<AtomicBool>,
    ancestor_effects: Arc<[Arc<AtomicBool>]>,
}
impl Default for ExtensionCallScope {
    fn default() -> Self {
        Self {
            graph: Arc::default(),
            ancestors: Arc::default(),
            parent_request_id: None,
            initiating_plugin_instance_id: None,
            permit: None,
            permit_lifetime: PermitLifetime::Execution,
            effects: Arc::default(),
            ancestor_effects: Arc::from([]),
        }
    }
}
impl ExtensionCallScope {
    /// HTTP callbacks use the same bounded graph as model callbacks. This scope
    /// describes placement and effects only; it does not authenticate a client.
    pub fn fork_http(&self, instance: String) -> Result<Self, GatewayError> {
        let mut scope = self.fork_call(self.parent_request_id.clone(), instance)?;
        scope.permit_lifetime = PermitLifetime::Http;
        Ok(scope)
    }
    pub fn is_direct_child_of(&self, parent: &Self) -> bool {
        Arc::ptr_eq(&self.graph, &parent.graph)
            && self
                .permit
                .as_ref()
                .is_some_and(|permit| !permit.released.load(Ordering::Acquire))
            && self.ancestors.len() == parent.ancestors.len() + 1
            && parent.ancestors.is_subset(&self.ancestors)
            && self
                .ancestor_effects
                .last()
                .is_some_and(|effects| Arc::ptr_eq(effects, &parent.effects))
    }
    /// A Router dispatch can mutate host state before an upstream send. Its
    /// initiating execution and ancestors must not replay those unknown effects.
    pub fn observe_http_dispatch(&self) {
        self.effects.store(true, Ordering::Release);
        self.observe_upstream_effect();
    }
    pub fn excluded_instances(&self) -> &BTreeSet<String> {
        &self.ancestors
    }
    pub fn parent_request_id(&self) -> Option<&ModelRequestId> {
        self.parent_request_id.as_ref()
    }
    pub fn initiating_plugin_instance_id(&self) -> Option<&str> {
        self.initiating_plugin_instance_id.as_deref()
    }
    /// Reserved before admission; clones share one permit through settlement and cleanup.
    pub(crate) fn fork(
        &self,
        parent: ModelRequestId,
        instance: String,
    ) -> Result<Self, GatewayError> {
        self.fork_call(Some(parent), instance)
    }
    pub(crate) fn fork_call(
        &self,
        parent: Option<ModelRequestId>,
        instance: String,
    ) -> Result<Self, GatewayError> {
        if instance.is_empty() || instance.len() > 128 || instance.chars().any(char::is_control) {
            return Err(GatewayError::new(
                GatewayErrorKind::InvalidRequest,
                "invalid initiating plugin identity",
            ));
        }
        if self.ancestors.contains(&instance) || self.ancestors.len() >= MAX_DEPTH {
            return Err(GatewayError::new(
                GatewayErrorKind::Unsupported,
                "nested model recursion limit reached",
            ));
        }
        {
            let mut graph = self
                .graph
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if graph.total >= MAX_CHILDREN || graph.active >= MAX_ACTIVE {
                return Err(GatewayError::new(
                    GatewayErrorKind::RateLimited,
                    "nested model execution capacity exhausted",
                ));
            }
            graph.total += 1;
            graph.active += 1;
        }
        let mut ancestors = self.ancestors.as_ref().clone();
        ancestors.insert(instance.clone());
        let mut effects = self.ancestor_effects.to_vec();
        effects.push(self.effects.clone());
        Ok(Self {
            graph: self.graph.clone(),
            ancestors: Arc::new(ancestors),
            initiating_plugin_instance_id: parent.as_ref().map(|_| instance),
            parent_request_id: parent,
            permit: Some(Arc::new(ActiveChild {
                graph: self.graph.clone(),
                released: AtomicBool::new(false),
            })),
            permit_lifetime: PermitLifetime::Execution,
            effects: Arc::default(),
            ancestor_effects: Arc::from(effects),
        })
    }
    pub(crate) fn release(&self) {
        if matches!(self.permit_lifetime, PermitLifetime::Http) {
            return;
        }
        if let Some(permit) = &self.permit {
            permit.release();
        }
    }
    pub(crate) fn has_descendant_effects(&self) -> bool {
        self.effects.load(Ordering::Acquire)
    }
    pub(crate) fn observe_upstream_effect(&self) {
        for effect in self.ancestor_effects.iter() {
            effect.store(true, Ordering::Release);
        }
    }
}

// End revokes descendant authority; it must not cancel the parent's buffered delivery.
pub(crate) struct ExecutionLifetime {
    cancellation: CancellationToken,
    permit: Mutex<Option<Arc<ActiveChild>>>,
    parent: Mutex<std::sync::Weak<Self>>,
    // The current attempt owns the strong reference alongside its Provider lease.
    // This projection cannot retain a completed attempt or a parent execution.
    account: Mutex<AccountProjection>,
}
#[derive(Default)]
struct AccountProjection {
    account: std::sync::Weak<crate::account::ProviderAccountId>,
    changed: CancellationToken,
}
impl ExecutionLifetime {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            cancellation: CancellationToken::new(),
            permit: Mutex::default(),
            parent: Mutex::default(),
            account: Mutex::default(),
        })
    }
    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }
    pub(crate) fn bind_scope(&self, scope: &ExtensionCallScope) {
        *self
            .permit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = scope.permit.clone();
    }
    pub(crate) fn bind_parent(&self, parent: std::sync::Weak<Self>) {
        *self
            .parent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = parent;
    }
    fn bind_account(
        &self,
        account: &crate::account::ProviderAccountId,
    ) -> Arc<crate::account::ProviderAccountId> {
        let account = Arc::new(account.clone());
        let mut projection = self
            .account
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        projection.account = Arc::downgrade(&account);
        std::mem::take(&mut projection.changed).cancel();
        account
    }
    pub(crate) fn ancestor_accounts(&self) -> BTreeSet<crate::account::ProviderAccountId> {
        self.ancestor_account_snapshot().0
    }
    fn ancestor_account_snapshot(
        &self,
    ) -> (
        BTreeSet<crate::account::ProviderAccountId>,
        Option<CancellationToken>,
    ) {
        let mut accounts = BTreeSet::new();
        let mut changes = vec![];
        let mut parent = self
            .parent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .upgrade();
        while let Some(lifetime) = parent {
            {
                let projection = lifetime
                    .account
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                changes.push(projection.changed.clone());
                if let Some(account) = projection.account.upgrade() {
                    accounts.insert(account.as_ref().clone());
                }
            }
            parent = lifetime
                .parent
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .upgrade();
        }
        let changed = (!changes.is_empty()).then(|| CancellationToken::linked(changes));
        (accounts, changed)
    }
}
impl Drop for ExecutionLifetime {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(permit) = self
            .permit
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            permit.release();
        }
    }
}

/// Only Core issues this authority. Wire facts and plugin-supplied IDs cannot construct it.
#[derive(Clone)]
pub struct NestedExecutionContext {
    pub(crate) lifetime: std::sync::Weak<ExecutionLifetime>,
    pub(crate) client: AuthenticatedClient,
    pub(crate) settings: RequestSettings,
    pub(crate) request_id: ModelRequestId,
    pub(crate) deadline: Deadline,
    pub(crate) started_at: std::time::SystemTime,
    pub(crate) parent_deadline: Deadline,
    pub(crate) cancellation: CancellationToken,
    pub(crate) scope: ExtensionCallScope,
}
impl std::fmt::Debug for NestedExecutionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NestedExecutionContext")
            .field("request_id", &self.request_id)
            .field("client_key_id", self.client.policy().key_id())
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}
impl NestedExecutionContext {
    pub(crate) fn bind_account(
        &self,
        account: &crate::account::ProviderAccountId,
    ) -> Option<Arc<crate::account::ProviderAccountId>> {
        self.lifetime
            .upgrade()
            .map(|lifetime| lifetime.bind_account(account))
    }
    pub(crate) fn ancestor_accounts(&self) -> BTreeSet<crate::account::ProviderAccountId> {
        self.lifetime
            .upgrade()
            .map_or_else(BTreeSet::new, |lifetime| lifetime.ancestor_accounts())
    }
    pub(crate) fn ancestor_account_snapshot(
        &self,
    ) -> (
        BTreeSet<crate::account::ProviderAccountId>,
        Option<CancellationToken>,
    ) {
        self.lifetime.upgrade().map_or_else(
            || (BTreeSet::new(), None),
            |lifetime| lifetime.ancestor_account_snapshot(),
        )
    }
    pub fn request_id(&self) -> &ModelRequestId {
        &self.request_id
    }
    pub fn client(&self) -> &AuthenticatedClient {
        &self.client
    }
    pub fn settings(&self) -> &RequestSettings {
        &self.settings
    }
    pub fn scope(&self) -> &ExtensionCallScope {
        &self.scope
    }
    pub fn deadline(&self) -> Deadline {
        self.deadline
    }
    pub fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }
    pub fn with_revocation(mut self, revocation: CancellationToken) -> Self {
        self.cancellation = CancellationToken::linked([self.cancellation, revocation]);
        self
    }
    pub fn with_settings(mut self, settings: RequestSettings) -> Result<Self, GatewayError> {
        self.validate()?;
        if !settings.has_baseline(self.client.snapshot())
            || !settings.is_for(self.client.policy().key_id())
        {
            return Err(GatewayError::new(
                GatewayErrorKind::InvalidRequest,
                "nested settings do not match their execution authority",
            ));
        }
        let timeout = settings
            .execution_values()
            .ok_or_else(|| {
                GatewayError::new(
                    GatewayErrorKind::InvalidRequest,
                    "nested execution settings unavailable",
                )
            })?
            .timeout_ms;
        self.deadline = Deadline::from_timeout(
            self.started_at,
            timeout.map(std::time::Duration::from_millis),
        )
        .ok_or_else(|| {
            GatewayError::new(
                GatewayErrorKind::InvalidRequest,
                "nested timeout outside supported range",
            )
        })?;
        if let Some(parent) = self.parent_deadline.at() {
            self.deadline = self.deadline.cap(parent);
        }
        self.settings = settings;
        self.validate()?;
        Ok(self)
    }
    pub(crate) fn validate(&self) -> Result<(), GatewayError> {
        if self
            .client
            .bound_access()
            .is_none_or(|access| access.cancellation().is_cancelled())
        {
            return Err(GatewayError::new(
                GatewayErrorKind::Unauthorized,
                "parent model authorization was revoked",
            ));
        }
        if self.lifetime.upgrade().is_none() || self.cancellation.is_cancelled() {
            return Err(GatewayError::new(
                GatewayErrorKind::Cancelled,
                "parent model execution was cancelled",
            ));
        }
        if self.deadline.is_elapsed() {
            return Err(GatewayError::new(
                GatewayErrorKind::Timeout,
                "parent model deadline elapsed",
            ));
        }
        Ok(())
    }
}
