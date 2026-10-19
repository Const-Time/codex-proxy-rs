//! Project host entry facts into the SDK, without granting authority from wire data.
use gateway_core::engine::nested::ExtensionCallScope;
use gateway_plugin_sdk::{RequestContext, call::middleware::MiddlewareHeader};
use std::sync::Arc;

pub(crate) fn wire(scope: &ExtensionCallScope) -> Option<Arc<RequestContext>> {
    let request = scope.request()?;
    Some(Arc::new(RequestContext {
        method: request.method.clone(),
        uri: request.uri.clone(),
        http_version: request.http_version.clone(),
        headers: request
            .headers
            .iter()
            .map(|header| MiddlewareHeader {
                name: header.name().to_owned(),
                value: header.value().to_vec(),
            })
            .collect(),
        peer_addr: request.peer_addr.map(|v| v.to_string()),
        client_ip: request.client_ip.map(|v| v.to_string()),
    }))
}
