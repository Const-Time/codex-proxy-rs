//! Synthetic, separately audited requests. A header alone never validates a candidate.
use super::*;
use crate::transport::{CodexBackendClient, CodexClientError, CodexRequestContext};
use futures::StreamExt as _;
use gateway_core::{
    account::{CredentialState, OutboundProxy},
    lifecycle::CancellationToken,
    provider_ports::{
        ProviderLeaseAcquisition, ProviderLeaseRequest, ProviderSchedulingLeaseRequest,
    },
};
use gateway_protocol::openai::sse::SseEventDecoder;
use secrecy::ExposeSecret as _;
use std::{
    num::NonZeroU32,
    time::{Duration, Instant, SystemTime},
};

pub(super) struct Probe<'a> {
    pub manager: &'a StateManager,
    pub target: &'a Target,
    pub pool: &'a Pool,
    pub account: &'a ProviderAccount,
    pub policy: &'a TurnStatePolicy,
    pub generation: u64,
    pub binding: [u8; 32],
    /// System profile, frozen across collection and verification, never borrowed from a user.
    pub profile: CodexWireProfileState,
    pub cancellation: &'a CancellationToken,
}

pub(super) struct ProbeFailure {
    pub message: String,
    pub pause: bool,
    pub cooldown_until: i64,
    pub reason: &'static str,
}

impl ProbeFailure {
    fn retry(message: impl Into<String>, reason: &'static str) -> Self {
        Self {
            message: message.into(),
            pause: false,
            cooldown_until: 0,
            reason,
        }
    }
    fn cancelled() -> Self {
        Self::retry("配置或任务已取消", "cancelled")
    }
}

impl Probe<'_> {
    async fn valid(&self) -> Result<(), ProbeFailure> {
        if self.cancellation.is_cancelled() {
            return Err(ProbeFailure::cancelled());
        }
        let mut runtime = self.manager.runtime.lock().await;
        self.manager
            .synchronize(&mut runtime)
            .await
            .map_err(|_| ProbeFailure::retry("设置读取失败，未发送探测", "storage_unavailable"))?;
        if runtime.document.generation != self.generation
            || !runtime.document.policy.enabled
            || runtime.document.budgets.iter().any(|b| {
                b.account_id == self.target.input.account_id
                    && b.cooldown_until > Utc::now().timestamp()
            })
            || !runtime.document.targets.iter().any(|t| {
                t.input.account_id == self.target.input.account_id
                    && t.input.model == self.target.input.model
                    && t.input.enabled
            })
        {
            return Err(ProbeFailure::cancelled());
        }
        Ok(())
    }

    async fn gap(&self) -> Result<(), ProbeFailure> {
        let mut changes = self.manager.changes.subscribe();
        tokio::select! {
            _ = self.cancellation.cancelled() => Err(ProbeFailure::cancelled()),
            _ = changes.changed() => Err(ProbeFailure::cancelled()),
            _ = tokio::time::sleep(Duration::from_secs(2)) => self.valid().await,
        }
    }

    pub(super) async fn run(&self) -> Result<Current, ProbeFailure> {
        let attempts = if self.pool.mode == "fixed" {
            1
        } else {
            self.policy.max_attempts
        };
        let mut failure = ProbeFailure::retry("无符合结构、长度和剩余时间要求的新 state", "retry");
        for attempt in 0..attempts {
            self.valid().await?;
            if attempt > 0 {
                self.gap().await?;
            }
            let mut changes = self.manager.changes.subscribe();
            let proxy = tokio::select! {
                _ = self.cancellation.cancelled() => return Err(ProbeFailure::cancelled()),
                _ = changes.changed() => return Err(ProbeFailure::cancelled()),
                proxy = network::acquire(self.pool) => proxy
                    .map_err(|e| ProbeFailure::retry(e.message(), "proxy_unavailable"))?,
            };
            let token = match self.call(Some(&proxy), None).await {
                Ok(Some(token)) => token,
                Ok(None) => continue,
                Err(error)
                    if !error.pause && error.cooldown_until == 0 && error.reason == "network" =>
                {
                    failure = error;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let Some(shape) = FernetShape::parse(&token) else {
                continue;
            };
            let Some(expires_at) = shape.candidate_expiry(self.policy, Utc::now().timestamp())
            else {
                continue;
            };
            if self
                .target
                .current
                .iter()
                .chain(&self.target.alternatives)
                .any(|s| s.token == token)
            {
                continue;
            }
            self.gap().await?;
            self.call(self.account.outbound_proxy(), Some(&token))
                .await?;
            return Ok(Current {
                fingerprint: fingerprint(&token),
                token,
                issued_at: shape.issued_at,
                expires_at,
                account_revision: self.account.revision().get(),
                binding: Some(self.binding),
            });
        }
        Err(failure)
    }

    async fn call(
        &self,
        proxy: Option<&OutboundProxy>,
        token: Option<&str>,
    ) -> Result<Option<String>, ProbeFailure> {
        self.valid().await?;
        let (account, binding) = self
            .manager
            .current_identity(self.account)
            .await
            .ok_or_else(|| ProbeFailure::retry("无法读取账号鉴权", "account_unavailable"))?;
        if binding != self.binding
            || !account.enabled()
            || account.credential_state() != CredentialState::Ready
            || !account.model_access().allows(&self.target.input.model)
        {
            return Err(ProbeFailure::cancelled());
        }
        let duration = Duration::from_secs(self.policy.timeout_seconds);
        let lease = ProviderSchedulingLeaseRequest::new(
            account.provider().clone(),
            account.id().clone(),
            account.revision(),
            NonZeroU32::new(1).expect("one"),
            Duration::from_secs(2),
            SystemTime::now() + duration,
        );
        let _guard = match self
            .manager
            .leases
            .try_acquire(ProviderLeaseRequest::Scheduling(lease))
            .await
        {
            Ok(ProviderLeaseAcquisition::Acquired(guard)) => guard,
            _ => return Err(ProbeFailure::retry("账号正忙，延后探测", "account_busy")),
        };
        let credential = self
            .manager
            .repository
            .load_runtime_credential(&account)
            .await
            .map_err(|_| ProbeFailure::retry("凭据读取失败", "account_unavailable"))?;
        let auth = credential
            .authentication
            .authorization_header()
            .map_err(|_| ProbeFailure::retry("凭据不可用", "account_unavailable"))?;
        let proxy = if token.is_some() {
            account.outbound_proxy()
        } else {
            proxy
        };
        let http = network::client(proxy, duration)
            .map_err(|e| ProbeFailure::retry(e.message(), "proxy_unavailable"))?;
        let client = CodexBackendClient::new(http, &self.manager.base_url, self.profile.clone());
        let mut request = CodexResponsesRequest::from_body(
            serde_json::json!({
                "model": self.target.input.model, "instructions": "Reply with OK only.",
                "input": [{"role":"user","content":[{"type":"input_text","text":"Reply OK."}]}],
                "store": false, "stream": true, "tools": []
            })
            .as_object()
            .expect("object")
            .clone(),
        );
        let id = uuid::Uuid::new_v4().to_string();
        request.turn_state = token.map(str::to_owned);
        request.client_session_id = Some(id.clone());
        request.client_thread_id = Some(id.clone());
        crate::transport::request::scope_request_to_account(
            &mut request,
            &credential.installation_id,
            crate::transport::request::RequestAccountScope::Same,
        );
        self.manager
            .converge(&mut request, account.id().as_str(), "state-probe");
        let mut context = CodexRequestContext::auxiliary(
            auth.expose_secret(),
            account.upstream_account_id(),
            &id,
            Some(&credential.installation_id),
        );
        context.turn_state = token;
        context.session_id = request.client_session_id.as_deref();
        context.thread_id = request.client_thread_id.as_deref();
        context.client_request_id = request.client_request_id.as_deref();
        self.manager
            .reserve_probe(account.id().as_str(), self.generation)
            .await
            .map_err(|m| ProbeFailure::retry(m, "budget"))?;
        let start = TurnStateProbeStart {
            id: format!("probe_{id}"),
            account_id: account.id().as_str().to_owned(),
            model: self.target.input.model.clone(),
            phase: if token.is_some() { "verify" } else { "collect" }.to_owned(),
            started_at: Utc::now(),
            timeout_seconds: self.policy.timeout_seconds,
        };
        tokio::time::timeout(
            Duration::from_secs(5),
            self.manager.store.begin_probe(&start),
        )
        .await
        .map_err(|_| ProbeFailure::retry("探测记账超时，未发送请求", "storage_unavailable"))?
        .map_err(|_| ProbeFailure::retry("探测记账失败，未发送请求", "storage_unavailable"))?;
        let began = Instant::now();
        let mut observed = TurnStateProbeResult {
            sent_state: token.map(str::to_owned),
            ..Default::default()
        };
        let mut changes = self.manager.changes.subscribe();
        let operation = async {
            self.valid().await?;
            let mut response = client
                .create_response_stream_http_sse(&request, context)
                .await
                .map_err(|e| classify(e, &mut observed))?;
            observed.status = response.diagnostics.status_code;
            observed.request_id = response.diagnostics.request_id;
            observed.returned_state = response.turn_state.filter(|v| v.len() <= 4096);
            let mut decoder = SseEventDecoder::default();
            let mut bytes = 0;
            while let Some(chunk) = response.body.next().await {
                let chunk = chunk.map_err(|e| classify(e, &mut observed))?;
                bytes += chunk.len();
                if bytes > 256 * 1024 {
                    return Err(ProbeFailure::retry("探测响应超限", "protocol"));
                }
                for frame in decoder.push_frames(&chunk) {
                    for event in frame.events() {
                        let Ok(value) = serde_json::from_str::<serde_json::Value>(&event.data)
                        else {
                            continue;
                        };
                        match value.get("type").and_then(serde_json::Value::as_str) {
                            Some("response.completed")
                                if value
                                    .pointer("/response/status")
                                    .and_then(serde_json::Value::as_str)
                                    == Some("completed") =>
                            {
                                observe_usage(&mut observed, &value);
                                observed.succeeded = true;
                                return Ok(());
                            }
                            Some("error" | "response.failed" | "response.incomplete") => {
                                observe_usage(&mut observed, &value);
                                return Err(classify_terminal(&value));
                            }
                            _ => {}
                        }
                    }
                }
            }
            Err(ProbeFailure::retry("探测流未正常完成", "protocol"))
        };
        let result = tokio::select! {
            _ = self.cancellation.cancelled() => Err(ProbeFailure::cancelled()),
            _ = changes.changed() => Err(ProbeFailure::cancelled()),
            result = tokio::time::timeout(duration, operation) =>
                result.unwrap_or_else(|_| Err(ProbeFailure::retry("探测请求超时", "network"))),
        };
        observed.latency_ms = began.elapsed().as_millis().try_into().unwrap_or(u64::MAX);
        observed.message = result.as_ref().err().map(|e| e.message.clone());
        if let Err(error) = &result
            && error.cooldown_until > 0
        {
            self.manager
                .cooldown(account.id().as_str(), error.cooldown_until)
                .await
                .map_err(|_| {
                    ProbeFailure::retry("上游冷却保存失败；停止探测", "storage_unavailable")
                })?;
        }
        tokio::time::timeout(
            Duration::from_secs(5),
            self.manager.store.finish_probe(&start.id, &observed),
        )
        .await
        .map_err(|_| ProbeFailure::retry("探测结果记账超时", "storage_unavailable"))?
        .map_err(|_| ProbeFailure::retry("探测结果记账失败", "storage_unavailable"))?;
        result?;
        Ok(observed.returned_state)
    }
}

fn classify(error: CodexClientError, observed: &mut TurnStateProbeResult) -> ProbeFailure {
    match error {
        CodexClientError::Upstream {
            status,
            retry_after_seconds,
            diagnostics,
            ..
        } => {
            observed.status = Some(status.as_u16());
            observed.request_id = diagnostics.request_id;
            let cooldown_until = if status.as_u16() == 429 {
                Utc::now().timestamp().saturating_add(
                    retry_after_seconds
                        .unwrap_or(60)
                        .max(60)
                        .min(i64::MAX as u64) as i64,
                )
            } else {
                0
            };
            ProbeFailure {
                message: format!(
                    "上游 HTTP {}，停止本轮探测，不切换出口绕过",
                    status.as_u16()
                ),
                pause: matches!(status.as_u16(), 401 | 403 | 407),
                cooldown_until,
                reason: "upstream_failure",
            }
        }
        _ => ProbeFailure::retry("探测网络或协议错误", "network"),
    }
}

fn classify_terminal(value: &serde_json::Value) -> ProbeFailure {
    let error = value
        .pointer("/response/error")
        .or_else(|| value.get("error"));
    let code = error
        .and_then(|e| e.get("code"))
        .and_then(serde_json::Value::as_str);
    let status = value
        .get("status")
        .or_else(|| value.get("status_code"))
        .and_then(serde_json::Value::as_u64);
    let rate_limited = status == Some(429)
        || matches!(
            code,
            Some("rate_limit_exceeded" | "usage_limit_reached" | "insufficient_quota")
        );
    let cooldown_until = if rate_limited {
        let retry = error
            .and_then(|e| e.get("retry_after_seconds"))
            .or_else(|| value.get("retry_after_seconds"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(60)
            .max(60);
        Utc::now()
            .timestamp()
            .saturating_add(retry.min(i64::MAX as u64) as i64)
    } else {
        0
    };
    ProbeFailure {
        message: if rate_limited {
            "探测流被限流；账号维护进入冷却，不切换出口绕过"
        } else {
            "探测流未成功完成；维护已暂停，请检查上游"
        }
        .to_owned(),
        pause: !rate_limited,
        cooldown_until,
        reason: "upstream_failure",
    }
}

fn observe_usage(observed: &mut TurnStateProbeResult, value: &serde_json::Value) {
    let number = |path: &str| {
        value
            .pointer(path)
            .and_then(serde_json::Value::as_u64)
            .filter(|v| *v <= i64::MAX as u64)
    };
    observed.response_id = value
        .pointer("/response/id")
        .and_then(serde_json::Value::as_str)
        .filter(|s| s.len() <= 512)
        .map(str::to_owned);
    observed.input_tokens = number("/response/usage/input_tokens");
    observed.output_tokens = number("/response/usage/output_tokens");
    observed.total_tokens = number("/response/usage/total_tokens");
    observed.cached_tokens = number("/response/usage/input_tokens_details/cached_tokens");
    observed.reasoning_tokens = number("/response/usage/output_tokens_details/reasoning_tokens");
}
