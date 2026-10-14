//! xAI 已选 OAuth 会话的受管连接；插件不读取令牌或重新选号

use super::*;
use gateway_core::engine::middleware::MiddlewareHeader;
use gateway_core::{
    account::{CredentialRevision, OutboundProxy},
    engine::upstream_adapter::{
        UpstreamAccountConnection, UpstreamAdapter, UpstreamAdapterInvocation,
        UpstreamCredentialUpdate,
    },
    metering::{CalculatedCost, Usage},
};

struct CurrentCredential {
    session: Arc<SelectedGrokSession>,
    usable: bool,
}

struct SelectedConnection {
    selector: Arc<dyn GrokSessionSelector>,
    recovery: Arc<dyn GrokCredentialRecovery>,
    account_id: ProviderAccountId,
    outbound_proxy: Option<OutboundProxy>,
    current: std::sync::RwLock<CurrentCredential>,
    context: AttemptContext,
    model: UpstreamModelId,
}

impl GrokBuildProvider {
    pub(super) async fn execute_upstream_adapter(
        &self,
        generate: &GenerateRequest,
        candidate: &ProviderCandidate,
        context: AttemptContext,
        adapter: Arc<dyn UpstreamAdapter>,
    ) -> Result<ProviderStream, ProviderError> {
        let model = candidate_upstream_model(candidate)?.clone();
        let started = Instant::now();
        let account = match (context.continuation_attempt(), context.continuation()) {
            (
                ContinuationAttempt::Native | ContinuationAttempt::ReplayOwner,
                Some(ContinuationBinding::Pinned(pin)),
            ) => {
                if pin.provider().as_str() != XAI_PROVIDER_NAME {
                    return Err(protocol_not_sent());
                }
                Some(pin.account().clone())
            }
            _ => None,
        };
        let session = Arc::new(
            select_grok_session(
                self.selector.as_ref(),
                candidate,
                &model,
                &context,
                account,
                GrokResponsesRequest::adapter_affinity(
                    generate,
                    model.as_str(),
                    context.client_api_key_ref(),
                ),
            )
            .await?,
        );
        let account_selection_wait_ms =
            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let metadata = ProviderCallMetadata::new(
            ProviderKind::new(XAI_PROVIDER_NAME).map_err(|_| protocol_not_sent())?,
            model.clone(),
            session.account_id().clone(),
            UpstreamTransport::new(adapter.transport()).map_err(|_| protocol_not_sent())?,
        )
        .with_selection_observation(ProviderSelectionObservation::new(
            account_selection_wait_ms,
            session.capacity_snapshot(),
        ));
        let account = Arc::new(SelectedConnection {
            selector: Arc::clone(&self.selector),
            recovery: Arc::clone(&self.credential_recovery),
            account_id: session.account_id().clone(),
            outbound_proxy: session.binding().outbound_proxy().cloned(),
            current: std::sync::RwLock::new(CurrentCredential {
                session: Arc::clone(&session),
                usable: true,
            }),
            context: context.clone(),
            model,
        });
        let events = UpstreamAdapterInvocation {
            operation: Operation::Generate(generate.clone()),
            context,
            metadata: metadata.clone(),
            account,
        }
        .into_stream(adapter);
        let mutation = session.allows_account_state_mutation();
        let stream = ProviderStream::new(metadata, events, session);
        Ok(if mutation {
            // 插件协议或 RPC 故障不能降低原生账号评分；只接纳已解析的上游拒绝事实
            stream.with_filtered_account_feedback(Arc::clone(&self.account_feedback), |error| {
                error.client_visible_upstream_error().is_some()
            })
        } else {
            stream
        })
    }
}

impl UpstreamAccountConnection for SelectedConnection {
    fn account_id(&self) -> &ProviderAccountId {
        &self.account_id
    }
    fn credential_revision(&self) -> CredentialRevision {
        self.current
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .session
            .credential_revision()
    }
    fn authentication_kind(&self) -> &str {
        crate::credential::XAI_AUTHENTICATION_KIND_OAUTH
    }
    fn outbound_proxy(&self) -> Option<&OutboundProxy> {
        self.outbound_proxy.as_ref()
    }

    fn authorization(&self) -> Result<Vec<MiddlewareHeader>, ProviderError> {
        let session = self.current_session().ok_or_else(|| {
            ProviderError::new(ProviderErrorKind::Unavailable, UpstreamSendState::NotSent)
        })?;
        Ok(vec![
            MiddlewareHeader::new(
                "authorization",
                bytes::Bytes::from(format!("Bearer {}", session.access_token().expose())),
            ),
            MiddlewareHeader::new(
                "x-grok-user-id",
                bytes::Bytes::copy_from_slice(session.user_id().expose().as_bytes()),
            ),
            MiddlewareHeader::new(
                "x-xai-token-auth",
                bytes::Bytes::from_static(b"xai-grok-cli"),
            ),
        ])
    }

    fn credential_updates(&self) -> Option<&dyn UpstreamCredentialUpdate> {
        self.selector
            .credential_updates()
            .map(|_| self as &dyn UpstreamCredentialUpdate)
    }

    fn calculate_cost(&self, tier: Option<&str>, usage: &Usage) -> Option<CalculatedCost> {
        crate::transport::canonical::grok_billing_breakdown_with_override(
            self.model.as_str(),
            usage.input_tokens?,
            usage.output_tokens?,
            usage.cached_tokens.unwrap_or(0),
            usage.cache_write_tokens.unwrap_or(0),
            tier,
            self.context
                .pricing()
                .get(XAI_PROVIDER_NAME)
                .and_then(|models| models.get(self.model.as_str())),
        )
        .map(|cost| cost.calculated_cost())
    }

    fn record_failure(&self, mut error: ProviderError) -> BoxFuture<'_, ProviderError> {
        Box::pin(async move {
            let Some(session) = self.current_session() else {
                return error;
            };
            if error.send_state() == UpstreamSendState::NotSent {
                return error;
            }
            if error.kind() == ProviderErrorKind::Unauthorized {
                error = error.with_credential_recovery();
            }
            let failure = stream_credential_failure(&error, &self.model);
            recover_or_record_failure(
                self.selector.as_ref(),
                self.recovery.as_ref(),
                &session,
                error,
                failure,
                self.context.credential_recovery_attempted(),
            )
            .await
        })
    }
}

impl SelectedConnection {
    fn current_session(&self) -> Option<Arc<SelectedGrokSession>> {
        let current = self
            .current
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        current.usable.then(|| current.session.clone())
    }
}
impl UpstreamCredentialUpdate for SelectedConnection {
    fn adopt(
        &self,
        expected: CredentialRevision,
        committed: CredentialRevision,
    ) -> BoxFuture<'_, Result<(), ProviderError>> {
        Box::pin(async move {
            let owner = self
                .selector
                .credential_updates()
                .ok_or_else(protocol_not_sent)?;
            let session = {
                let mut current = self
                    .current
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if !current.usable
                    || current.session.credential_revision() != expected
                    || committed <= expected
                {
                    return Err(protocol_not_sent());
                }
                current.usable = false;
                current.session.clone()
            };
            let session = owner.reload(&session, committed).await.map_err(|_| {
                ProviderError::new(ProviderErrorKind::Unavailable, UpstreamSendState::NotSent)
            })?;
            let mut current = self
                .current
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            current.session = Arc::new(session);
            current.usable = true;
            Ok(())
        })
    }
}
