//! 有界、可取消的刷新循环。候选先经业务出口验证，随后以配置代际检查发布。
use super::*;
use crate::transport::{
    CodexBackendClient, CodexClientError, CodexRequestContext,
    protocol::responses::CodexResponsesRequest,
};
use futures::{StreamExt as _, future::BoxFuture};
use gateway_core::{
    account::{CredentialState, OutboundProxy},
    provider_ports::{
        ProviderLeaseAcquisition, ProviderLeaseRequest, ProviderSchedulingLeaseRequest,
    },
    task::{ScheduledTask, WorkerCycleContext, WorkerTaskError},
};
use gateway_protocol::openai::sse::SseEventDecoder;
use secrecy::ExposeSecret as _;
use std::{
    num::NonZeroU32,
    time::{Duration, SystemTime},
};

struct Outcome {
    current: Option<Current>,
    observations: Vec<TurnStateObservation>,
    message: String,
    stop: bool,
}

pub(crate) struct StateRefreshTask(pub(crate) Arc<StateManager>);

impl ScheduledTask for StateRefreshTask {
    fn run_cycle(&self, context: WorkerCycleContext) -> BoxFuture<'_, Result<(), WorkerTaskError>> {
        Box::pin(async move {
            let result = self.0.cycle(&context).await;
            // 取消后也释放登记；探测没有独立 spawn，不会留下孤儿任务。
            self.0.runtime.lock().await.running.clear();
            result.map_err(|_| WorkerTaskError::safe("State 刷新失败"))
        })
    }
}

impl StateManager {
    async fn cycle(&self, context: &WorkerCycleContext) -> Result<(), AdminError> {
        let accounts = self
            .repository
            .list_for_provider()
            .await
            .map_err(|_| AdminError::unavailable("账号目录不可用"))?;
        let (doc, jobs) = {
            let mut runtime = self.runtime.lock().await;
            self.synchronize(&mut runtime).await?;
            if runtime.dirty {
                let document = runtime.document.clone();
                self.commit(&mut runtime, document, None).await?;
            }
            if !runtime.document.policy.enabled {
                return Ok(());
            }
            let now = Utc::now().timestamp();
            // 凭据变化只触发一次立即补充，后续失败仍服从 retry_seconds。
            let mut invalidated = false;
            for target in &mut runtime.document.targets {
                if target.current.as_ref().is_some_and(|s| {
                    accounts
                        .iter()
                        .find(|a| a.id().as_str() == target.input.account_id)
                        .is_none_or(|a| s.account_revision != a.revision().get())
                }) {
                    target.current = None;
                    target.alternatives.clear();
                    target.next_probe_at = now;
                    target.last_message = "凭据已变化，旧 state 已撤下".to_owned();
                    invalidated = true;
                }
            }
            if invalidated {
                let document = runtime.document.clone();
                self.commit(&mut runtime, document, None).await?;
            }
            let doc = runtime.document.clone();
            let jobs: Vec<_> = doc
                .targets
                .iter()
                .filter_map(|target| {
                    let account = accounts
                        .iter()
                        .find(|a| a.id().as_str() == target.input.account_id)?;
                    if !target.input.enabled
                        || !self.account_policy(account.id().as_str()).takeover
                        || target.next_probe_at > now
                        || !account.enabled()
                        || account.credential_state() != CredentialState::Ready
                        || !account.model_access().allows(&target.input.model)
                    {
                        return None;
                    }
                    let pool = doc
                        .pools
                        .iter()
                        .find(|p| p.id == target.input.pool_id && p.enabled)?;
                    Some((target.clone(), pool.clone(), account.clone()))
                })
                .take(doc.policy.concurrency)
                .collect();
            for (t, _, _) in &jobs {
                runtime
                    .running
                    .insert((t.input.account_id.clone(), t.input.model.clone()));
            }
            (doc, jobs)
        };
        let mut jobs = futures::stream::iter(jobs)
            .map(|(target, pool, account)| {
                let policy = &doc.policy;
                async move {
                    // 一个 cycle 有总时间预算；不因排队、无限流或大量重试永久占用 worker。
                    let budget = Duration::from_secs(
                        (policy.timeout_seconds * 2 + 20) * policy.max_attempts as u64,
                    );
                    let outcome = tokio::time::timeout(
                        budget,
                        self.probe(&target, &pool, &account, policy, doc.generation),
                    )
                    .await
                    .unwrap_or_else(|_| Outcome {
                        current: None,
                        observations: vec![],
                        message: "探测周期超时".to_owned(),
                        stop: false,
                    });
                    (target, account, outcome)
                }
            })
            .buffer_unordered(doc.policy.concurrency);
        loop {
            let next = tokio::select! {
                _ = context.cancellation().cancelled() => return Ok(()),
                next = jobs.next() => next,
            };
            let Some((target, account, outcome)) = next else {
                break;
            };
            // 网络返回后重新确认凭据未变化，再检查配置代际。
            let latest = self
                .repository
                .list_for_provider()
                .await
                .map_err(|_| AdminError::unavailable("账号目录不可用"))?;
            let identity_matches = latest.iter().any(|a| {
                a.id() == account.id()
                    && a.revision() == account.revision()
                    && a.enabled()
                    && a.credential_state() == CredentialState::Ready
                    && a.model_access().allows(&target.input.model)
            });
            let mut runtime = self.runtime.lock().await;
            runtime
                .running
                .remove(&(target.input.account_id.clone(), target.input.model.clone()));
            if runtime.document.generation != doc.generation || !identity_matches {
                continue;
            }
            let mut updated = runtime.document.clone();
            let Some(t) = updated.targets.iter_mut().find(|t| {
                t.input.account_id == target.input.account_id && t.input.model == target.input.model
            }) else {
                continue;
            };
            let now = Utc::now().timestamp();
            // 认证/限流需要人工恢复；不切换地址继续尝试。其他错误进入有界冷却周期。
            if outcome.stop {
                t.input.enabled = false;
            }
            t.next_probe_at = now + doc.policy.retry_seconds;
            t.last_message = outcome.message;
            t.history.extend(outcome.observations);
            if t.history.len() > 20 {
                t.history.drain(..t.history.len() - 20);
            }
            if let Some(current) = outcome.current.filter(|s| s.expires_at > now) {
                t.next_probe_at = current.expires_at - doc.policy.refresh_before_seconds;
                publish_candidate(t, current, now);
            }
            if outcome.stop {
                updated.generation += 1;
            }
            self.commit(&mut runtime, updated, None).await?;
        }
        Ok(())
    }

    async fn probe(
        &self,
        target: &Target,
        pool: &Pool,
        account: &ProviderAccount,
        policy: &TurnStatePolicy,
        generation: u64,
    ) -> Outcome {
        let mut out = Outcome {
            current: None,
            observations: vec![],
            message: "没有合格的新 state".to_owned(),
            stop: false,
        };
        for attempt in 0..policy.max_attempts {
            if self.runtime.lock().await.document.generation != generation {
                return out;
            }
            if attempt > 0 {
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            let proxy = match network::acquire(pool).await {
                Ok(proxy) => proxy,
                Err(e) => {
                    out.message = e.message().to_owned();
                    break;
                }
            };
            let captured = match self
                .call(account, &target.input.model, Some(&proxy), None, policy)
                .await
            {
                Ok(state) => state,
                Err((message, stop)) => {
                    out.message = message;
                    out.stop = stop;
                    if stop {
                        break;
                    } else {
                        continue;
                    }
                }
            };
            let token = captured.unwrap_or_default();
            let shape = FernetShape::parse(&token);
            let expiry = shape
                .as_ref()
                .and_then(|s| s.candidate_expiry(policy, Utc::now().timestamp()));
            let newer = expiry.is_some()
                && !target
                    .current
                    .iter()
                    .chain(&target.alternatives)
                    .any(|old| old.token == token);
            out.observations.push(TurnStateObservation {
                at: Utc::now().timestamp(),
                direction: "candidate".to_owned(),
                fingerprint: if token.is_empty() {
                    String::new()
                } else {
                    fingerprint(&token)
                },
                shape: shape.clone(),
                message: if newer {
                    "符合筛选规则，等待业务出口验证"
                } else {
                    "无 state、结构/长度/时间不符，或并未续期"
                }
                .to_owned(),
            });
            if !newer {
                continue;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
            if self.runtime.lock().await.document.generation != generation {
                return out;
            }
            // 业务出口测试只用独立、无工具的新会话，绝不重放用户正文。
            match self
                .call(
                    account,
                    &target.input.model,
                    account.outbound_proxy(),
                    Some(&token),
                    policy,
                )
                .await
            {
                Ok(_) => {
                    let shape = shape.expect("validated candidate");
                    out.current = Some(Current {
                        fingerprint: fingerprint(&token),
                        token,
                        issued_at: shape.issued_at,
                        expires_at: expiry.expect("validated expiry"),
                        account_revision: account.revision().get(),
                    });
                    out.message = "新 state 已经业务出口验证并发布；不代表能力提升证明".to_owned();
                    break;
                }
                Err((message, stop)) => {
                    out.message = message;
                    out.stop = stop;
                    if stop {
                        break;
                    }
                }
            }
        }
        out
    }

    async fn call(
        &self,
        account: &ProviderAccount,
        model: &str,
        proxy: Option<&OutboundProxy>,
        token: Option<&str>,
        policy: &TurnStatePolicy,
    ) -> Result<Option<String>, (String, bool)> {
        let duration = Duration::from_secs(policy.timeout_seconds);
        let request = ProviderSchedulingLeaseRequest::new(
            account.provider().clone(),
            account.id().clone(),
            account.revision(),
            NonZeroU32::new(1).expect("one"),
            Duration::from_secs(2),
            SystemTime::now() + duration,
        );
        let _guard = match self
            .leases
            .try_acquire(ProviderLeaseRequest::Scheduling(request))
            .await
        {
            Ok(ProviderLeaseAcquisition::Acquired(guard)) => guard,
            _ => return Err(("账号正忙，延后探测".to_owned(), false)),
        };
        let operation = async {
            let credential = self
                .repository
                .load_runtime_credential(account)
                .await
                .map_err(|_| ("凭据不可用，已暂停".to_owned(), true))?;
            let auth = credential
                .authentication
                .authorization_header()
                .map_err(|_| ("凭据不合法，已暂停".to_owned(), true))?;
            let http =
                network::client(proxy, duration).map_err(|e| (e.message().to_owned(), false))?;
            let client = CodexBackendClient::new(http, &self.base_url, self.profile.clone());
            let mut request = CodexResponsesRequest::from_body(
                serde_json::json!({
                    "model": model, "instructions": "Reply with OK only.", "input": [{
                        "role":"user", "content":[{"type":"input_text","text":"Reply OK."}]
                    }], "store": false, "stream": true, "tools": []
                })
                .as_object()
                .expect("object")
                .clone(),
            );
            request.turn_state = token.map(str::to_owned);
            let id = uuid::Uuid::new_v4().to_string();
            request.client_session_id = Some(id.clone());
            request.client_thread_id = Some(id.clone());
            crate::transport::request::scope_request_to_account(
                &mut request,
                &credential.installation_id,
                crate::transport::request::RequestAccountScope::Same,
            );
            self.converge(&mut request, account.id().as_str(), "state-probe");
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
            let mut response = client
                .create_response_stream_http_sse(&request, context)
                .await
                .map_err(classify)?;
            let captured = response.turn_state.take();
            let mut decoder = SseEventDecoder::default();
            let mut bytes = 0usize;
            while let Some(chunk) = response.body.next().await {
                let chunk = chunk.map_err(classify)?;
                bytes += chunk.len();
                if bytes > 256 * 1024 {
                    return Err(("探测响应超限".to_owned(), false));
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
                                return Ok(captured);
                            }
                            Some("error" | "response.failed" | "response.incomplete") => {
                                return Err((
                                    "探测返回失败事件；已暂停，请检查账号和上游".to_owned(),
                                    true,
                                ));
                            }
                            _ => {}
                        }
                    }
                }
            }
            Err(("探测流未正常完成".to_owned(), false))
        };
        tokio::time::timeout(duration, operation)
            .await
            .unwrap_or_else(|_| Err(("探测请求超时".to_owned(), false)))
    }
}

fn classify(error: CodexClientError) -> (String, bool) {
    match error {
        CodexClientError::Upstream { status, .. } => {
            let stop = matches!(status.as_u16(), 401 | 403 | 407 | 429);
            (
                format!(
                    "上游返回 HTTP {}{}",
                    status.as_u16(),
                    if stop {
                        "，已暂停；不会通过换出口继续请求"
                    } else {
                        ""
                    }
                ),
                stop,
            )
        }
        _ => ("探测网络或协议错误".to_owned(), false),
    }
}
