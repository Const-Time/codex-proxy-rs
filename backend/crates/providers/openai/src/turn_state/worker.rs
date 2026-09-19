//! Fair, bounded refresh cycles. All futures belong to the leased worker.
use super::probe::{Probe, ProbeFailure};
use super::*;
use futures::{StreamExt as _, future::BoxFuture};
use gateway_core::{
    account::CredentialState,
    task::{ScheduledTask, WorkerCycleContext, WorkerTaskError},
};

pub(crate) struct StateRefreshTask(pub(crate) Arc<StateManager>);

impl ScheduledTask for StateRefreshTask {
    fn run_cycle(&self, context: WorkerCycleContext) -> BoxFuture<'_, Result<(), WorkerTaskError>> {
        Box::pin(async move {
            let result = self.0.cycle(&context).await;
            self.0.runtime.lock().await.running.clear();
            result.map_err(|_| WorkerTaskError::safe("State 刷新失败"))
        })
    }
}

impl StateManager {
    async fn cycle(&self, context: &WorkerCycleContext) -> Result<(), AdminError> {
        let account_ids: HashSet<_> = {
            let mut runtime = self.runtime.lock().await;
            self.synchronize(&mut runtime).await?;
            if runtime.dirty {
                let doc = runtime.document.clone();
                self.commit(&mut runtime, doc, None).await?;
            }
            if !runtime.document.policy.enabled {
                return Ok(());
            }
            runtime
                .document
                .targets
                .iter()
                .map(|t| t.input.account_id.clone())
                .collect()
        };
        let accounts = self
            .repository
            .list_for_provider()
            .await
            .map_err(|_| AdminError::unavailable("账号目录不可用"))?;
        let mut identities = HashMap::new();
        for account in accounts
            .iter()
            .filter(|a| account_ids.contains(a.id().as_str()))
        {
            if context.cancellation().is_cancelled() {
                return Ok(());
            }
            if let Some(identity) = self.current_identity(account).await {
                identities.insert(account.id().as_str().to_owned(), identity);
            }
        }
        let (doc, jobs) = {
            let mut runtime = self.runtime.lock().await;
            self.synchronize(&mut runtime).await?;
            let mut doc = runtime.document.clone();
            let now = Utc::now().timestamp();
            let mut changed = runtime.dirty;
            for target in &mut doc.targets {
                if let Some((account, binding)) = identities.get(&target.input.account_id) {
                    if !account.enabled()
                        || target
                            .current
                            .iter()
                            .chain(&target.alternatives)
                            .any(|s| !s.belongs_to(account, binding))
                    {
                        target.current = None;
                        target.alternatives.clear();
                        target.next_probe_at = now;
                        target.last_message =
                            "鉴权身份已变化或账号已停用，旧 state 已撤下".to_owned();
                        changed = true;
                    }
                } else if !accounts
                    .iter()
                    .any(|a| a.id().as_str() == target.input.account_id)
                    && (target.current.is_some() || !target.alternatives.is_empty())
                {
                    target.current = None;
                    target.alternatives.clear();
                    changed = true;
                }
            }
            let mut jobs = Vec::new();
            // Maintenance is independently limited to one model per account per cycle.
            // Do not encode this limit into the lease shared with real business requests.
            let mut probing_accounts = HashSet::new();
            let count = doc.targets.len();
            let start = doc
                .cursor
                .as_ref()
                .and_then(|(a, m)| {
                    doc.targets
                        .iter()
                        .position(|t| &t.input.account_id == a && &t.input.model == m)
                })
                .map_or(0, |i| i + 1);
            for offset in 0..count {
                let index = (start + offset) % count;
                let target = &doc.targets[index];
                let mut reason = scheduling::gate(&doc, target, now);
                let identity = identities.get(&target.input.account_id);
                if reason == "queued"
                    && identity.is_none_or(|(a, _)| {
                        !a.enabled()
                            || a.credential_state() != CredentialState::Ready
                            || !a.model_access().allows(&target.input.model)
                    })
                {
                    reason = "account_unavailable";
                }
                if reason != target.wait_reason {
                    doc.targets[index].wait_reason = reason.to_owned();
                    changed = true;
                }
                if reason != "queued" || jobs.len() >= doc.policy.concurrency {
                    continue;
                }
                let target = &doc.targets[index];
                if !probing_accounts.insert(target.input.account_id.clone()) {
                    continue;
                }
                let (account, binding) = identity.expect("eligible identity");
                let pool = doc
                    .pools
                    .iter()
                    .find(|p| p.id == target.input.pool_id)
                    .expect("eligible pool");
                jobs.push((target.clone(), pool.clone(), account.clone(), *binding));
                doc.cursor = Some((target.input.account_id.clone(), target.input.model.clone()));
                doc.targets[index].refresh_requested = false;
                changed = true;
            }
            if changed {
                self.commit(&mut runtime, doc.clone(), None).await?;
            }
            for (t, ..) in &jobs {
                runtime
                    .running
                    .insert((t.input.account_id.clone(), t.input.model.clone()));
            }
            (doc, jobs)
        };
        let mut jobs = futures::stream::iter(jobs)
            .map(|(target, pool, account, binding)| {
                let doc = &doc;
                async move {
                    let probe = Probe {
                        manager: self,
                        target: &target,
                        pool: &pool,
                        account: &account,
                        policy: &doc.policy,
                        generation: doc.generation,
                        binding,
                        profile: CodexWireProfileState::new(self.profile.snapshot()),
                        cancellation: context.cancellation(),
                    };
                    let outcome = probe.run().await;
                    (target, account, binding, outcome)
                }
            })
            .buffer_unordered(doc.policy.concurrency);
        // Each probe observes cancellation and finalizes its audit before returning.
        while let Some((target, account, binding, outcome)) = jobs.next().await {
            let latest = self.current_identity(&account).await;
            let mut runtime = self.runtime.lock().await;
            runtime
                .running
                .remove(&(target.input.account_id.clone(), target.input.model.clone()));
            self.synchronize(&mut runtime).await?;
            if context.cancellation().is_cancelled()
                || runtime.document.generation != doc.generation
                || latest.is_none_or(|(a, b)| {
                    b != binding
                        || !a.enabled()
                        || a.credential_state() != CredentialState::Ready
                        || !a.model_access().allows(&target.input.model)
                })
            {
                continue;
            }
            let mut updated = runtime.document.clone();
            let Some(t) = updated.targets.iter_mut().find(|t| {
                t.input.account_id == target.input.account_id && t.input.model == target.input.model
            }) else {
                continue;
            };
            let now = Utc::now().timestamp();
            t.next_probe_at = now + doc.policy.retry_seconds;
            match outcome {
                Ok(current) => {
                    t.next_probe_at = current.expires_at - doc.policy.refresh_before_seconds;
                    record(
                        t,
                        "candidate",
                        &current.token,
                        "已经业务出口验证；不代表能力提升证明",
                    );
                    t.last_message = "新 state 已经业务出口验证并发布".to_owned();
                    t.wait_reason = "fresh".to_owned();
                    publish_candidate(t, current, now);
                }
                Err(ProbeFailure {
                    message,
                    pause,
                    cooldown_until,
                    reason,
                }) => {
                    t.last_message = message;
                    t.wait_reason = reason.to_owned();
                    t.cooldown_until = t.cooldown_until.max(cooldown_until);
                    if pause {
                        t.input.enabled = false;
                    }
                }
            }
            self.commit(&mut runtime, updated, None).await?;
        }
        Ok(())
    }
}
