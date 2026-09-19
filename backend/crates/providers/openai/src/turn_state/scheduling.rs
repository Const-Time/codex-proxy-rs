//! Persisted request budgets and a fair cursor, shared by manual/background work.
use super::*;

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Budget {
    pub account_id: String,
    pub started_at: i64,
    pub used: u32,
    #[serde(default)]
    pub cooldown_until: i64,
}

pub(super) fn budget(doc: &Document, account: &str, now: i64) -> (u32, Option<i64>) {
    doc.budgets
        .iter()
        .find(|b| b.account_id == account && now < b.started_at + 3600)
        .map_or((0, None), |b| (b.used, Some(b.started_at + 3600)))
}

pub(super) fn maintenance(doc: &Document, account: &str) -> TurnStateMaintenance {
    doc.accounts
        .iter()
        .find(|p| p.account_id == account)
        .map(|p| p.maintenance.clone())
        .unwrap_or_default()
}

pub(super) fn gate(doc: &Document, target: &Target, now: i64) -> &'static str {
    let policy = maintenance(doc, &target.input.account_id);
    if !doc.policy.enabled
        || !target.input.enabled
        || !doc
            .accounts
            .iter()
            .any(|p| p.account_id == target.input.account_id && p.takeover)
        || (target.automatic && !policy.auto_models)
    {
        return "paused";
    }
    if !doc
        .pools
        .iter()
        .any(|p| p.id == target.input.pool_id && p.enabled)
    {
        return "proxy_unavailable";
    }
    if target.cooldown_until > now
        || doc
            .budgets
            .iter()
            .any(|b| b.account_id == target.input.account_id && b.cooldown_until > now)
    {
        return "upstream_cooldown";
    }
    if !target.refresh_requested
        && target
            .current
            .iter()
            .chain(&target.alternatives)
            .any(|c| c.expires_at > now + doc.policy.refresh_before_seconds)
    {
        return "fresh";
    }
    let idle_seconds = if target.automatic && policy.idle_seconds == 0 {
        3600
    } else {
        policy.idle_seconds
    };
    if !target.refresh_requested
        && idle_seconds > 0
        && target
            .last_traffic_at
            .is_none_or(|at| at + idle_seconds <= now)
    {
        return "idle";
    }
    if budget(doc, &target.input.account_id, now).0 >= policy.max_probes_per_hour {
        return "budget";
    }
    if target.next_probe_at > now {
        return "retry";
    }
    "queued"
}

impl StateManager {
    /// Traffic is recorded even if the upstream never issues a state. Probes never call this.
    pub(crate) async fn note_traffic(&self, account: &str, model: &str) {
        let Ok(mut runtime) = self.runtime.try_lock() else {
            return;
        };
        if runtime.document.policy.enabled && self.account_policy(account).takeover {
            Self::discover(&mut runtime, account, model, Utc::now().timestamp());
            self.changes.send_if_modified(|generation| {
                if *generation == runtime.document.generation {
                    false
                } else {
                    *generation = runtime.document.generation;
                    true
                }
            });
        }
        if let Some(target) = runtime
            .document
            .targets
            .iter_mut()
            .find(|t| t.input.account_id == account && t.input.model == model)
        {
            target.last_traffic_at = Some(Utc::now().timestamp());
            runtime.dirty = true;
        }
    }

    /// Reserve before sending. A crash/uncertain network error conservatively spends a slot.
    /// CAS makes this safe even if a stale worker temporarily overlaps the next lease holder.
    pub(super) async fn reserve_probe(&self, account: &str, generation: u64) -> Result<(), String> {
        let mut runtime = self.runtime.lock().await;
        self.synchronize(&mut runtime)
            .await
            .map_err(|_| "预算存储不可用，未发送探测")?;
        if runtime.document.generation != generation {
            return Err("设置已变化，已停止探测".to_owned());
        }
        let now = Utc::now().timestamp();
        if runtime
            .document
            .budgets
            .iter()
            .any(|b| b.account_id == account && b.cooldown_until > now)
        {
            return Err("账号处于上游冷却，未发送探测".to_owned());
        }
        let limit = maintenance(&runtime.document, account).max_probes_per_hour;
        if budget(&runtime.document, account, now).0 >= limit {
            return Err("账号本小时探测预算已用完".to_owned());
        }
        let mut doc = runtime.document.clone();
        doc.budgets
            .retain(|b| now < b.started_at + 3600 || now < b.cooldown_until);
        if let Some(b) = doc.budgets.iter_mut().find(|b| b.account_id == account) {
            b.used += 1;
        } else {
            doc.budgets.push(Budget {
                account_id: account.to_owned(),
                started_at: now,
                used: 1,
                cooldown_until: 0,
            });
        }
        self.commit(&mut runtime, doc, None)
            .await
            .map_err(|_| "预算保存失败，未发送探测".to_owned())
    }

    /// A real upstream 429 survives config edits/cancellation and blocks every model of this account.
    pub(super) async fn cooldown(&self, account: &str, until: i64) -> Result<(), AdminError> {
        for _ in 0..3 {
            let mut runtime = self.runtime.lock().await;
            self.synchronize(&mut runtime).await?;
            let mut doc = runtime.document.clone();
            if let Some(b) = doc.budgets.iter_mut().find(|b| b.account_id == account) {
                b.cooldown_until = b.cooldown_until.max(until);
            } else {
                doc.budgets.push(Budget {
                    account_id: account.to_owned(),
                    started_at: Utc::now().timestamp(),
                    used: 0,
                    cooldown_until: until,
                });
            }
            if self.commit(&mut runtime, doc, None).await.is_ok() {
                return Ok(());
            }
        }
        Err(AdminError::unavailable("上游冷却保存失败"))
    }

    pub(super) fn discover(runtime: &mut Runtime, account: &str, model: &str, now: i64) {
        let policy = maintenance(&runtime.document, account);
        if !policy.auto_models
            || model.starts_with("gpt-image-")
            || model.is_empty()
            || model.len() > 128
            || model
                .chars()
                .any(|c| c.is_whitespace() || c.is_control() || c == '*')
            || runtime
                .document
                .targets
                .iter()
                .any(|t| t.input.account_id == account && t.input.model == model)
        {
            return;
        }
        let Some(pool_id) = policy.pool_id.filter(|id| {
            runtime
                .document
                .pools
                .iter()
                .any(|p| &p.id == id && p.enabled)
        }) else {
            return;
        };
        // Reclaim only idle, expired discoveries. Manual/paused targets are never evicted.
        runtime.document.targets.retain(|t| {
            t.input.account_id != account
                || !t.automatic
                || !t.input.enabled
                || t.last_traffic_at
                    .is_some_and(|at| now < at + policy.idle_seconds.max(3600))
                || t.current
                    .iter()
                    .chain(&t.alternatives)
                    .any(|c| c.expires_at > now)
        });
        if runtime.document.targets.len() >= 128
            || runtime
                .document
                .targets
                .iter()
                .filter(|t| t.automatic && t.input.account_id == account)
                .count()
                >= 8
        {
            return;
        }
        runtime.document.targets.push(Target {
            input: TurnStateTargetInput {
                account_id: account.to_owned(),
                model: model.to_owned(),
                pool_id,
                enabled: true,
            },
            current: None,
            alternatives: vec![],
            next_probe_at: now,
            last_message: "由真实业务流量发现精确模型".to_owned(),
            history: vec![],
            automatic: true,
            last_traffic_at: Some(now),
            wait_reason: "queued".to_owned(),
            cooldown_until: 0,
            refresh_requested: false,
        });
        // Prevent a stale settings form from deleting newly discovered targets.
        runtime.document.generation += 1;
        runtime.dirty = true;
    }
}
