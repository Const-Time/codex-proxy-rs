//! Observe the actual selected native connection and graph, then delegate to the published SDK adapter.
use super::fixture::Fixture;
use gateway_admin::ports::plugins::PluginService;
use gateway_core::{
    engine::{
        AttemptContext, nested::ExtensionCallScope, provider::EventStream, upstream_adapter::*,
    },
    error::ProviderError,
    lifecycle::CancellationToken,
    routing::{ProviderKind, UpstreamModelId},
};
use std::{
    fmt,
    sync::{Arc, Mutex},
};

pub(super) struct Captured {
    pub account: Arc<dyn UpstreamAccountConnection>,
    pub scope: ExtensionCallScope,
}
pub(super) type Selected = Arc<Mutex<Option<Captured>>>;
struct CapturePlan {
    inner: Arc<dyn UpstreamAdapterPlan>,
    selected: Selected,
}
struct CaptureAdapter {
    inner: Arc<dyn UpstreamAdapter>,
    selected: Selected,
}
impl fmt::Debug for CapturePlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NativeLeaseCapturePlan")
    }
}
impl fmt::Debug for CaptureAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NativeLeaseCaptureAdapter")
    }
}
impl UpstreamAdapterPlan for CapturePlan {
    fn cancellation(&self) -> CancellationToken {
        self.inner.cancellation()
    }
    fn select(
        &self,
        context: &AttemptContext,
        provider: &ProviderKind,
        model: &UpstreamModelId,
    ) -> Result<Option<Arc<dyn UpstreamAdapter>>, ProviderError> {
        self.inner.select(context, provider, model).map(|adapter| {
            adapter.map(|inner| {
                Arc::new(CaptureAdapter {
                    inner,
                    selected: self.selected.clone(),
                }) as Arc<dyn UpstreamAdapter>
            })
        })
    }
}
impl UpstreamAdapter for CaptureAdapter {
    fn transport(&self) -> &str {
        self.inner.transport()
    }
    fn execute(self: Arc<Self>, invocation: UpstreamAdapterInvocation) -> EventStream {
        let mut selected = self.selected.lock().unwrap();
        assert!(
            selected.is_none(),
            "only the actual native selection may be captured"
        );
        *selected = Some(Captured {
            account: invocation.account.clone(),
            scope: invocation
                .context
                .nested_context()
                .expect("real Core parent graph")
                .scope()
                .clone(),
        });
        drop(selected);
        self.inner.clone().execute(invocation)
    }
}
pub(super) fn plan(f: &Fixture) -> (Arc<dyn UpstreamAdapterPlan>, Selected) {
    let selected = Arc::new(Mutex::new(None));
    (
        Arc::new(CapturePlan {
            inner: Arc::new(f.manager.middleware_registry().unwrap()),
            selected: selected.clone(),
        }),
        selected,
    )
}
