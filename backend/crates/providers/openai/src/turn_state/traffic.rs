//! Coalesced activity survives runtime lock contention and cross-instance CAS reloads.
use super::*;

#[derive(Clone, PartialEq, Eq)]
pub(super) struct Traffic {
    at: i64,
    generation: u64,
}

impl StateManager {
    pub(crate) async fn note_traffic(&self, account: &str, model: &str) {
        if model.is_empty() || model.len() > 128 {
            return;
        }
        let key = (account.to_owned(), model.to_owned());
        {
            // No network/await under this lock; one coalesced timestamp per account/model.
            let mut pending = self
                .traffic
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if pending.len() >= 4096
                && !pending.contains_key(&key)
                && let Some(oldest) = pending
                    .iter()
                    .min_by_key(|(_, event)| event.at)
                    .map(|(key, _)| key.clone())
            {
                pending.remove(&oldest);
            }
            pending
                .entry(key)
                .and_modify(|event| event.at = event.at.max(Utc::now().timestamp()))
                .or_insert_with(|| Traffic {
                    at: Utc::now().timestamp(),
                    generation: *self.changes.borrow(),
                });
            // Keep the original generation until persistence: an uncommitted
            // discovery increments the local generation but not the stored one.
        }
        if let Ok(mut runtime) = self.runtime.try_lock() {
            self.flush_traffic(&mut runtime);
        }
    }

    pub(super) fn traffic_snapshot(&self) -> HashMap<(String, String), Traffic> {
        self.traffic
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(super) fn apply_traffic(
        doc: &mut Document,
        pending: &HashMap<(String, String), Traffic>,
    ) -> bool {
        let generation = doc.generation;
        let mut changed = false;
        for ((account, model), event) in pending {
            // A newer administrative edit wins. Old activity must not resurrect a
            // removed discovery or undo a pause/revocation when replayed after CAS.
            if event.generation == generation
                && doc.policy.enabled
                && doc
                    .accounts
                    .iter()
                    .any(|policy| &policy.account_id == account && policy.takeover)
            {
                changed |= Self::discover(doc, account, model, event.at);
            }
            if let Some(target) = doc
                .targets
                .iter_mut()
                .find(|target| &target.input.account_id == account && &target.input.model == model)
                && target.last_traffic_at.is_none_or(|at| at < event.at)
            {
                target.last_traffic_at = Some(event.at);
                changed = true;
            }
        }
        changed
    }

    pub(super) fn flush_traffic(&self, runtime: &mut Runtime) {
        runtime.dirty |= Self::apply_traffic(&mut runtime.document, &self.traffic_snapshot());
        self.changes.send_if_modified(|generation| {
            if *generation == runtime.document.generation {
                false
            } else {
                *generation = runtime.document.generation;
                true
            }
        });
    }
}
