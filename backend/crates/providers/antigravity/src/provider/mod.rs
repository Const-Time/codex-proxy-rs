use std::{
    borrow::Cow,
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use gateway_core::{
    account::{
        AccountCandidate, AccountCapacitySnapshot, AccountEligibilityPolicy, AccountFeedbackStats,
        AccountSelectionContext, AccountSelector, AccountStatus, ProviderAccount,
        ProviderAccountId, ProviderAccountStore,
    },
    engine::{
        AttemptContext, ContinuationAttempt,
        account_scheduling::{AccountPolicyError, AccountScheduleOwnership},
        continuation::ContinuationBinding,
        provider::{
            EventStream, Provider, ProviderCallMetadata, ProviderRequest,
            ProviderSelectionObservation, ProviderStream,
        },
    },
    error::{ProviderError, ProviderErrorKind as Kind},
    event::{GatewayEvent, ProviderEvent},
    metering::ModelPriceOverride,
    operation::Operation,
    provider_ports::{
        ProviderCooldownPort, ProviderCooldownScope, ProviderLeaseAcquisition, ProviderLeaseGuard,
        ProviderLeasePort, ProviderLeaseRequest, ProviderSchedulingLeaseRequest,
        ProviderScopedCooldown, ProviderSessionAffinityKey, ProviderSessionAffinityPort,
        ProviderStorePorts,
    },
    routing::{
        ProviderCatalogGeneration, ProviderKind, ProviderModelCapabilities, UpstreamModelId,
    },
    upstream::{UpstreamSendState, UpstreamTransport},
};
use gateway_protocol::{chat_completions::CHAT_PROTOCOL, genai::GENAI_PROTOCOL};
use serde_json::Value;

use crate::{
    chat::ChatProjection,
    credential::{
        CredentialService,
        catalog::{CatalogService, presentation},
        oauth::Material,
        quota::QuotaService,
    },
    profile::ProfileService,
    transport::{
        CloudCodeClient, CloudCodeResponseContext, GenerateMode, decode_cloud_code_response,
        decode_count_response, unsent,
    },
};

mod affinity;

pub use affinity::{SessionAffinity, apply_upstream_session_id, session_key, upstream_session_id};

pub(crate) struct AntigravityProvider {
    kind: ProviderKind,
    catalog: Arc<CatalogService>,
    credentials: Arc<CredentialService>,
    quota: Arc<QuotaService>,
    accounts: Arc<dyn ProviderAccountStore>,
    leases: Arc<dyn ProviderLeasePort>,
    cooldowns: Arc<dyn ProviderCooldownPort>,
    session_affinity: Arc<dyn ProviderSessionAffinityPort>,
    feedback: Arc<AccountFeedbackStats>,
    profile: Option<Arc<ProfileService>>,
}

struct SelectedAccount {
    account: ProviderAccount,
    material: Material,
    project: String,
    lease: Box<dyn ProviderLeaseGuard>,
    capacity: Option<AccountCapacitySnapshot>,
    update_affinity: bool,
}

#[derive(Clone, Copy)]
enum RequestKind {
    CountTokens,
    Generate(GenerateMode),
}

struct PreparedRequest {
    kind: RequestKind,
    body: Bytes,
    max_output_tokens: Option<u64>,
    pricing: Option<ModelPriceOverride>,
    projection: Option<ChatProjection>,
    affinity_key: Option<ProviderSessionAffinityKey>,
}

impl PreparedRequest {
    fn requires_web_search(&self) -> bool {
        matches!(self.kind, RequestKind::Generate(GenerateMode::WebSearch))
    }
}

impl AntigravityProvider {
    pub(crate) fn new(
        kind: ProviderKind,
        ports: &ProviderStorePorts,
        credentials: Arc<CredentialService>,
        catalog: Arc<CatalogService>,
        quota: Arc<QuotaService>,
    ) -> Self {
        Self {
            kind,
            credentials,
            catalog,
            quota,
            accounts: ports.accounts(),
            leases: ports.leases(),
            cooldowns: ports.cooldowns(),
            session_affinity: ports.session_affinity(),
            feedback: ports.account_feedback(),
            profile: None,
        }
    }

    pub(crate) fn with_profile(mut self, profile: Arc<ProfileService>) -> Self {
        self.profile = Some(profile);
        self
    }

    fn prepare_request(
        &self,
        operation: &Operation,
        model: &UpstreamModelId,
        ctx: &AttemptContext,
    ) -> Result<PreparedRequest, ProviderError> {
        let generate = match operation {
            Operation::CountTokens(count) if count.payload().protocol() == GENAI_PROTOCOL => {
                return Ok(PreparedRequest {
                    kind: RequestKind::CountTokens,
                    body: count.payload().body().clone(),
                    max_output_tokens: None,
                    pricing: None,
                    projection: None,
                    affinity_key: None,
                });
            }
            Operation::Generate(generate) => generate,
            _ => return Err(unsent(Kind::Unsupported)),
        };
        let payload = generate.protocol_payload();
        let search = payload.protocol() == CHAT_PROTOCOL
            && payload
                .body()
                .get("web_search_options")
                .is_some_and(Value::is_object);
        let streaming = payload
            .context()
            .get("stream")
            .or_else(|| {
                (payload.protocol() == CHAT_PROTOCOL)
                    .then(|| payload.body().get("stream"))
                    .flatten()
            })
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let (mut body, projection) = match payload.protocol() {
            GENAI_PROTOCOL => (Cow::Borrowed(payload.body()), None),
            CHAT_PROTOCOL => {
                let (body, projection) = ChatProjection::prepare(payload.body(), ctx)?;
                (Cow::Owned(body), Some(projection))
            }
            _ => return Err(unsent(Kind::Unsupported)),
        };
        let affinity_key = session_key(payload, ctx.client_api_key_ref(), model);
        if affinity_key.is_some()
            || body.contains_key("sessionId")
            || body.contains_key("session_id")
        {
            apply_upstream_session_id(body.to_mut(), affinity_key.as_ref());
        }
        let pricing = crate::billing::request_is_priceable(&body)
            .then(|| {
                crate::billing::effective_pricing(
                    model.as_str(),
                    ctx.pricing()
                        .get(self.kind.as_str())
                        .and_then(|models| models.get(model.as_str())),
                )
            })
            .flatten();
        let body = serde_json::to_vec(body.as_ref()).map_err(|_| unsent(Kind::InvalidRequest))?;
        let mode = if search {
            GenerateMode::WebSearch
        } else {
            GenerateMode::from(streaming)
        };
        Ok(PreparedRequest {
            kind: RequestKind::Generate(mode),
            body: body.into(),
            max_output_tokens: generate.max_output_tokens(),
            pricing,
            projection,
            affinity_key,
        })
    }

    async fn select(
        &self,
        request: &ProviderRequest,
        model: &UpstreamModelId,
        prepared: &PreparedRequest,
        ctx: &AttemptContext,
        pin: Option<&ProviderAccountId>,
        mut affinity: Option<&mut SessionAffinity>,
    ) -> Result<SelectedAccount, ProviderError> {
        let diagnostic = ctx.is_diagnostic_required_account();
        let search = prepared.requires_web_search();
        if diagnostic && search {
            return Err(unsent(Kind::InvalidRequest));
        }
        let diagnostic_account = if diagnostic {
            Some(pin.ok_or_else(|| unsent(Kind::NoEligibleAccount))?)
        } else {
            None
        };
        let scope = request.candidate().account_scope();
        let mut accounts = if let Some(id) = pin {
            self.accounts
                .get_account(id)
                .await
                .map_err(infrastructure)?
                .into_iter()
                .collect::<Vec<_>>()
        } else {
            self.accounts
                .list_for_provider_accounts(&self.kind, &scope.account_ids(&self.kind))
                .await
                .map_err(infrastructure)?
        };
        accounts.retain(|account| {
            account.provider() == &self.kind
                && account.authentication_kind() == "oauth"
                && !ctx.excluded_accounts().contains(account.id())
                && !ctx.excludes_proxy(account.outbound_proxy())
                && (diagnostic
                    || scope.allows_model(account.id(), model.as_str())
                        && ctx
                            .account_scope()
                            .is_none_or(|scope| scope.allows_model(account.id(), model.as_str()))
                        && account.model_access().allows(model.as_str()))
        });
        if accounts.is_empty() {
            return Err(unsent(Kind::NoEligibleAccount));
        }
        let mut projects = self
            .catalog
            .projects(
                model,
                prepared.max_output_tokens,
                diagnostic_account,
                search,
            )
            .await?;
        accounts.retain(|account| {
            projects
                .get(account.id())
                .is_some_and(|(revision, _)| *revision == account.revision())
        });
        if accounts.is_empty() {
            return Err(unsent(Kind::NoEligibleAccount));
        }
        let ids: Vec<_> = accounts.iter().map(|a| a.id().clone()).collect();
        let scheduling = self
            .leases
            .load_state(ctx.client_api_key_ref(), &self.kind, &ids)
            .await
            .map_err(infrastructure)?;
        let mut model_quota = if diagnostic {
            Default::default()
        } else {
            self.quota
                .scheduling(&accounts, model.as_str())
                .await
                .map_err(infrastructure)?
        };
        let now = SystemTime::now();
        let mut candidates = Vec::new();
        let model_scope = ProviderCooldownScope::upstream_model(model.clone());
        let cooldowns = if diagnostic {
            Vec::new()
        } else {
            self.cooldowns.read_many(&ids).await
        };
        let mut cooldowns = cooldowns.into_iter();
        for account in accounts {
            let account_cooldown = cooldowns
                .next()
                .transpose()
                .map_err(infrastructure)?
                .flatten();
            let scoped = if diagnostic {
                None
            } else {
                self.cooldowns
                    .read_scoped(account.id(), &model_scope)
                    .await
                    .map_err(infrastructure)?
            };
            let (quota_exhausted, quota) = model_quota.remove(account.id()).unwrap_or_default();
            if quota_exhausted {
                continue;
            }
            let cooldown_until = [
                account_cooldown
                    .filter(|lease| lease.credential_revision() == account.revision())
                    .map(|lease| lease.until()),
                scoped
                    .filter(|lease| lease.credential_revision() == account.revision())
                    .map(|lease| lease.until()),
            ]
            .into_iter()
            .flatten()
            .filter(|until| *until > now)
            .max();
            let health = self.feedback.scheduling_signals(&self.kind, account.id());
            let signals = scheduling
                .signals()
                .get(account.id())
                .cloned()
                .ok_or_else(|| unsent(Kind::ProviderInfrastructureUnavailable))?;
            let rate_limit = signals.rate_limited_until.max(cooldown_until);
            let signals = signals
                .with_provider_quota(quota)
                .with_rate_limit(rate_limit)
                .with_runtime_health(health.0, health.1);
            candidates.push(AccountCandidate { account, signals });
        }
        let mut selection = AccountSelectionContext {
            policy: ctx.account_selection_policy(),
            now,
            excluded_accounts: ctx.excluded_accounts().clone(),
            preferred_account: pin
                .or_else(|| affinity.as_ref().and_then(|session| session.bound()))
                .cloned(),
            preferred_account_overrides_weight: true,
            round_robin_cursor: scheduling.round_robin_cursor(),
            eligibility: if diagnostic {
                AccountEligibilityPolicy::BypassForDiagnostic
            } else {
                AccountEligibilityPolicy::Enforce
            },
            account_scope: (!diagnostic).then(|| Arc::clone(scope)),
        };
        let capacity = AccountSelector.capacity_snapshot(&candidates, &selection);
        let mut capacity_denied = capacity.is_some();
        let mut retry_after = AccountSelector.request_interval_retry_after(&candidates, &selection);
        let mut stale_policy = false;
        while !ctx.deadline().is_elapsed() && !ctx.cancellation().is_cancelled() {
            selection.now = SystemTime::now();
            let choice = match ctx
                .select_account(
                    &self.kind,
                    Some(model.as_str()),
                    &candidates,
                    &mut selection,
                    AccountScheduleOwnership {
                        bound_account: affinity.as_ref().and_then(|session| session.bound()),
                        pinned_account: pin,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(choice) => choice,
                Err(AccountPolicyError::StaleCandidate) if !stale_policy => {
                    stale_policy = true;
                    continue;
                }
                Err(error) => return Err(error.into_provider_error()),
            };
            let Some(choice) = choice else {
                break;
            };
            let account = &choice.candidate().account;
            let id = account.id().clone();
            let lease = self
                .leases
                .try_acquire(ProviderLeaseRequest::Scheduling(
                    ProviderSchedulingLeaseRequest::new(
                        self.kind.clone(),
                        id.clone(),
                        account.revision(),
                        account
                            .effective_concurrency(selection.policy.max_concurrent_per_account()),
                        selection.policy.request_interval(),
                        ctx.deadline(),
                        ctx.cancellation().clone(),
                    ),
                ))
                .await
                .map_err(infrastructure)?;
            if let ProviderLeaseAcquisition::Acquired(lease) = lease {
                let current = self
                    .accounts
                    .load_credential(&id, account.revision())
                    .await
                    .map_err(infrastructure)?;
                let account = &current.account;
                let now = SystemTime::now();
                if account.provider() == &self.kind
                    && !ctx.excludes_proxy(account.outbound_proxy())
                    && account.dispatch_hours().allows_at(now)
                    && (diagnostic
                        || (account.model_access().allows(model.as_str())
                            && account.status_projection(now, None).status
                                == AccountStatus::Normal))
                {
                    let material = self.credentials.material(&current, !diagnostic)?;
                    if let Some(session) = &mut affinity
                        && session.bound().is_none()
                        && let Some(winner) = session
                            .claim(self.session_affinity.as_ref(), &self.kind, &id)
                            .await
                    {
                        selection.preferred_account = Some(winner);
                        drop(lease);
                        continue;
                    }
                    let update_affinity = affinity
                        .as_ref()
                        .and_then(|session| session.bound())
                        .is_some_and(|bound| {
                            bound == &id
                                || !candidates.iter().any(|candidate| {
                                    candidate.account.id() == bound
                                        && candidate.account.dispatch_hours().allows_at(now)
                                        && candidate
                                            .account
                                            .status_projection(
                                                now,
                                                candidate.signals.rate_limited_until,
                                            )
                                            .status
                                            == AccountStatus::Normal
                                })
                        });
                    let (_, project) = projects
                        .remove(&id)
                        .ok_or_else(|| unsent(Kind::NoEligibleAccount))?;
                    return Ok(SelectedAccount {
                        account: current.account,
                        material,
                        project,
                        lease,
                        capacity: capacity.map(AccountCapacitySnapshot::with_acquired_request),
                        update_affinity,
                    });
                }
            } else if let ProviderLeaseAcquisition::Busy { retry_after: retry } = lease {
                capacity_denied = true;
                if let Some(retry) = retry {
                    retry_after = Some(retry_after.map_or(retry, |first| first.min(retry)));
                }
            }
            selection.excluded_accounts.insert(id);
        }
        if ctx.cancellation().is_cancelled() {
            return Err(unsent(Kind::Cancelled));
        }
        if ctx.deadline().is_elapsed() {
            return Err(unsent(Kind::Timeout));
        }
        let error = unsent(if capacity_denied {
            Kind::AccountCapacityUnavailable
        } else {
            Kind::NoEligibleAccount
        });
        Err(retry_after.map_or(error.clone(), |duration| error.with_retry_after(duration)))
    }
}

/// Determine an account constraint from a diagnostic or a stateful continuation.
pub fn locked_account<'a>(
    provider: &ProviderKind,
    operation: &Operation,
    ctx: &'a AttemptContext,
) -> Result<Option<&'a ProviderAccountId>, ProviderError> {
    let required = ctx.required_account();
    let native_pin = (ctx.continuation_attempt() == ContinuationAttempt::Native)
        .then(|| ctx.continuation().and_then(ContinuationBinding::pinned))
        .flatten();
    let owner = if let Some(pin) = native_pin {
        Some((pin.provider(), pin.account()))
    } else if ctx.continuation_attempt() == ContinuationAttempt::ReplayOwner
        || operation
            .provider_session_state(provider.as_str())
            .is_some()
    {
        let owner = ctx
            .account_state_owner()
            .ok_or_else(|| unsent(Kind::NoEligibleAccount).with_retry_prohibited())?;
        Some((owner.provider(), owner.account()))
    } else {
        // The coordinator also records the first account of ordinary requests for
        // state isolation. Its presence alone does not constrain account rotation.
        None
    };
    match owner {
        Some((kind, account)) if kind != provider || required.is_some_and(|id| id != account) => {
            Err(unsent(Kind::NoEligibleAccount).with_retry_prohibited())
        }
        Some((_, account)) => Ok(Some(account)),
        None => Ok(required),
    }
}

fn infrastructure<T>(_: T) -> ProviderError {
    unsent(Kind::ProviderInfrastructureUnavailable)
}

async fn record_failure(
    error: &ProviderError,
    account: &ProviderAccount,
    model: &UpstreamModelId,
    accounts: &dyn ProviderAccountStore,
    cooldowns: &dyn ProviderCooldownPort,
) {
    if error.send_state() != UpstreamSendState::Sent {
        return;
    }
    match error.kind() {
        Kind::Unauthorized => {
            if accounts
                .expire_access_token(account, SystemTime::now())
                .await
                .is_err()
            {
                tracing::warn!("Antigravity credential expiry could not be updated");
            }
        }
        Kind::RateLimited | Kind::QuotaExhausted => {
            let delay = error
                .retry_after()
                .unwrap_or(Duration::from_secs(30))
                .min(Duration::from_secs(86_400));
            if let Some(until) = SystemTime::now().checked_add(delay) {
                let cooldown = ProviderScopedCooldown::new(
                    account.id().clone(),
                    account.revision(),
                    ProviderCooldownScope::upstream_model(model.clone()),
                    until,
                );
                if cooldowns.put_scoped_if_later(cooldown).await.is_err() {
                    tracing::warn!("Antigravity model cooldown could not be updated");
                }
            }
        }
        _ => {}
    }
}

#[async_trait]
impl Provider for AntigravityProvider {
    fn name(&self) -> &'static str {
        crate::NAME
    }

    fn catalog_generation(&self) -> ProviderCatalogGeneration {
        ProviderCatalogGeneration::new(self.catalog.generation.load(Ordering::Relaxed))
    }

    async fn query_model_capabilities(
        &self,
    ) -> Result<Vec<ProviderModelCapabilities>, ProviderError> {
        self.catalog.capabilities().await
    }

    async fn query_diagnostic_model_catalog(
        &self,
        account: &ProviderAccountId,
    ) -> Result<Option<Vec<ProviderModelCapabilities>>, ProviderError> {
        self.catalog
            .diagnostic_capabilities(account)
            .await
            .map(Some)
    }

    async fn query_client_model_catalog(
        &self,
        scope: &gateway_core::account::scope::FrozenAccountScope,
        protocol: &str,
        _: &str,
    ) -> Result<
        Option<Vec<gateway_core::routing::ProviderModelDescriptor>>,
        gateway_core::routing::ProviderCatalogUnavailable,
    > {
        use gateway_core::routing::{
            ProviderCatalogUnavailable, ProviderModelContent, ProviderModelDescriptor,
        };
        if protocol != GENAI_PROTOCOL {
            return Ok(None);
        }
        let mut descriptors = std::collections::BTreeMap::new();
        for (owners, catalog) in self
            .catalog
            .catalogs()
            .await
            .map_err(|_| ProviderCatalogUnavailable)?
        {
            for (model, data) in catalog.available.models {
                if !owners
                    .iter()
                    .any(|owner| scope.allows_model(&owner.account_id, &model))
                {
                    continue;
                }
                let model = UpstreamModelId::new(model).map_err(|_| ProviderCatalogUnavailable)?;
                let content = ProviderModelContent::Adapted(presentation(&model, data));
                descriptors
                    .entry(model.clone())
                    .or_insert(ProviderModelDescriptor { model, content });
            }
        }
        Ok(Some(descriptors.into_values().collect()))
    }

    async fn execute(
        &self,
        request: ProviderRequest,
        ctx: AttemptContext,
    ) -> Result<ProviderStream, ProviderError> {
        let model = request
            .candidate()
            .upstream_model()
            .cloned()
            .ok_or_else(|| unsent(Kind::InvalidRequest))?;
        let pin = locked_account(&self.kind, request.operation(), &ctx)?;
        let prohibit_retry = pin.is_some();
        let respect_pin = move |error: ProviderError| {
            if prohibit_retry {
                error.with_retry_prohibited()
            } else {
                error
            }
        };
        let mut prepared = self
            .prepare_request(request.operation(), &model, &ctx)
            .map_err(respect_pin)?;
        let profile = match &self.profile {
            Some(service) => Some(
                service
                    .current()
                    .await
                    .map_err(infrastructure)
                    .map_err(respect_pin)?,
            ),
            None => None,
        };
        let started = Instant::now();
        let mut affinity = match prepared.affinity_key.take().filter(|_| pin.is_none()) {
            Some(key) => {
                SessionAffinity::load(self.session_affinity.as_ref(), &self.kind, key).await
            }
            None => None,
        };
        let selected = self
            .select(&request, &model, &prepared, &ctx, pin, affinity.as_mut())
            .await
            .map_err(respect_pin)?;
        let account = selected.account;
        let token = selected.material.access_token;
        let client = CloudCodeClient::with_profile(
            &self.catalog.origin,
            account.outbound_proxy(),
            account.auto_use_quota_credits(),
            profile.as_ref(),
        )
        .map_err(respect_pin)?;
        let transport = UpstreamTransport::new("http").map_err(infrastructure)?;
        let metadata = ProviderCallMetadata::new(
            self.kind.clone(),
            model.clone(),
            account.id().clone(),
            transport,
        )
        .with_selection_observation(ProviderSelectionObservation::new(
            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            selected.capacity,
        ));
        let records_feedback = !ctx.is_diagnostic_required_account() || account.enabled();
        let accounts = Arc::clone(&self.accounts);
        let cooldowns = Arc::clone(&self.cooldowns);
        let affinity_store = Arc::clone(&self.session_affinity);
        let provider = self.kind.clone();
        let stats = Arc::clone(&self.feedback);
        let deadline = ctx.deadline();
        let cancel = ctx.cancellation().clone();
        let started_at = ctx.timing_started_at();
        let PreparedRequest {
            kind,
            body,
            pricing,
            mut projection,
            ..
        } = prepared;
        let event_stream = async_stream::try_stream! {
            if cancel.is_cancelled() {
                Err(unsent(Kind::Cancelled))?;
            }
            if deadline.is_elapsed() {
                Err(unsent(Kind::Timeout))?;
            }
            let response = tokio::select! {
                biased;
                _ = cancel.cancelled() => Err(ProviderError::new(Kind::Cancelled, UpstreamSendState::Ambiguous)),
                _ = deadline.wait() => Err(ProviderError::new(Kind::Timeout, UpstreamSendState::Ambiguous)),
                result = async {
                    match kind {
                        RequestKind::CountTokens => client.count_tokens(&token, &body).await,
                        RequestKind::Generate(mode) => {
                            client.generate(&selected.project, model.as_str(), &token, &body, mode).await
                        }
                    }
                } => result,
            };
            let record = async |error: ProviderError| {
                if records_feedback {
                    record_failure(&error, &account, &model, accounts.as_ref(), cooldowns.as_ref()).await;
                }
                respect_pin(error)
            };
            let response = match response {
                Ok(response) => response,
                Err(error) => Err(record(error).await)?,
            };
            let mut upstream: EventStream = match kind {
                RequestKind::CountTokens => Box::pin(decode_count_response(
                    response, model.clone(), deadline, cancel, started_at,
                )),
                RequestKind::Generate(mode) => Box::pin(decode_cloud_code_response(
                    response,
                    CloudCodeResponseContext {
                        stream: mode == GenerateMode::Stream,
                        model: model.clone(),
                        deadline,
                        cancel,
                        token,
                        pricing,
                        started_at,
                    },
                )),
            };
            while let Some(event) = upstream.next().await {
                let event = match event {
                    Ok(event) => event,
                    Err(error) => Err(record(error).await)?,
                };
                if event.canonical_facts().iter().any(|fact| matches!(fact, GatewayEvent::Completed(_))) {
                    if let Some(projection) = &mut projection {
                        for wire in projection.finish()? {
                            yield ProviderEvent::wire(wire);
                        }
                    }
                    if let Some(affinity) = affinity.as_ref().filter(|_| selected.update_affinity) {
                        affinity.record_success(affinity_store.as_ref(), &provider, account.id()).await;
                    }
                }
                if let Some(projection) = &mut projection
                    && let Some(wire) = event.wire_event()
                {
                    let mut frames = projection
                        .translate(wire)
                        .map_err(|error| {
                            let metering = event
                                .canonical_facts()
                                .iter()
                                .filter(|fact| matches!(
                                    fact,
                                    GatewayEvent::Usage(_)
                                        | GatewayEvent::CalculatedCost(_)
                                        | GatewayEvent::CalculatedCostWithdrawn
                                ))
                                .cloned()
                                .map(ProviderEvent::canonical)
                                .collect();
                            error.with_atomic_client_events(metering)
                        })?
                        .into_iter();
                    yield event.with_wire_projection(frames.next());
                    for wire in frames {
                        yield ProviderEvent::wire(wire);
                    }
                    continue;
                }
                yield event;
            }
        };
        let stream = ProviderStream::new(metadata, Box::pin(event_stream), selected.lease);
        Ok(if records_feedback {
            stream.with_account_feedback(stats)
        } else {
            stream
        })
    }
}
