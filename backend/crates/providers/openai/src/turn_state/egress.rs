use super::*;
use gateway_core::account::OutboundProxy;
use std::time::Duration;

impl StateManager {
    pub(super) async fn egress(
        &self,
        proxy: Option<&OutboundProxy>,
        rotating: bool,
        force: bool,
    ) -> Option<TurnStateEgress> {
        let detector = self.egress_probe.as_ref()?;
        // Hash configuration, never store proxy credentials in diagnostic records.
        let key = fingerprint(proxy.map_or("direct", OutboundProxy::expose_url));
        let now = Utc::now().timestamp();
        // A rotating/API endpoint is not an exit identity. Never reuse its
        // independent detection across attempts (or let manual tests seed it).
        if !rotating
            && !force
            && let Some(snapshot) = self
                .egress_cache
                .lock()
                .await
                .get(&key)
                .filter(|s| now - s.detected_at < 60)
        {
            return Some(snapshot.clone());
        }
        // Telemetry is best effort and cannot turn a successful probe into a failure.
        let result = tokio::time::timeout(Duration::from_secs(6), detector.test_egress(proxy))
            .await
            .ok()
            .flatten();
        let snapshot = TurnStateEgress {
            ip: result
                .as_ref()
                .and_then(|r| r.exit_ip.map(|ip| ip.to_string())),
            location: result.and_then(|r| r.location),
            detected_at: now,
            source: "independent_proxy_test".to_owned(),
            rotating,
        };
        if !rotating {
            let mut cache = self.egress_cache.lock().await;
            cache.retain(|_, s| now - s.detected_at < 60);
            if cache.len() >= 256 {
                cache.clear();
            }
            cache.insert(key, snapshot.clone());
        }
        Some(snapshot)
    }
}
