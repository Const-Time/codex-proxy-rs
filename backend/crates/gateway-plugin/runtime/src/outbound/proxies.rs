//! Proxy selection stays within the existing invocation and managed network limits.
use super::{HttpScope, invalid};
use crate::RpcReply;
use gateway_admin::{
    model::{AdminError, AdminErrorKind, PageSize, proxies::ProxyListQuery},
    ports::proxy::PluginProxyAccess,
};
use gateway_host::outbound::{HttpClient, HttpRequest};
use gateway_plugin_sdk::{
    ErrorCode, PluginFault,
    call::{host as wire, upstream_adapter as adapter},
};

fn fault(error: AdminError) -> PluginFault {
    let code = match error.kind() {
        AdminErrorKind::Invalid => ErrorCode::InvalidInput,
        AdminErrorKind::NotFound => ErrorCode::Rejected,
        _ => ErrorCode::Fault,
    };
    PluginFault::new(code, "selected proxy is unavailable")
}

pub(super) async fn dispatch(
    scope: &HttpScope,
    client: &HttpClient,
    proxies: Option<&dyn PluginProxyAccess>,
    method: &str,
    params: serde_json::Value,
    payload: Vec<u8>,
) -> Result<RpcReply, PluginFault> {
    if !matches!(method, wire::PROXY_HTTP | wire::PROXIES_LIST) {
        return scope.dispatch(client, method, params, payload).await;
    }
    let access =
        proxies.ok_or_else(|| PluginFault::new(ErrorCode::Fault, "proxy access unavailable"))?;
    tokio::time::timeout(scope.timeout()?, async {
        if method == wire::PROXIES_LIST {
            if params != serde_json::json!({}) {
                return Err(invalid());
            }
            let query: wire::ProxyListRequest =
                serde_json::from_slice(&payload).map_err(|_| invalid())?;
            let page = access
                .list(ProxyListQuery {
                    page: query.page,
                    page_size: PageSize::new(query.page_size).map_err(|_| invalid())?,
                    search: query.search,
                })
                .await
                .map_err(fault)?;
            let result = wire::ProxyListResult {
                items: page
                    .items
                    .into_iter()
                    .map(|proxy| wire::ProxySummary {
                        id: proxy.id,
                        name: proxy.name,
                        enabled: true,
                    })
                    .collect(),
                total: page.total,
                page: page.page,
                page_size: page.page_size,
            };
            return Ok(RpcReply {
                result: serde_json::json!({}),
                payload: serde_json::to_vec(&result).map_err(|_| invalid())?,
            });
        }
        let request: wire::ProxyHttpRequest =
            serde_json::from_value(params).map_err(|_| invalid())?;
        let proxy = access.resolve(&request.proxy_id).await.map_err(fault)?;
        scope
            .send_via(
                client,
                HttpRequest {
                    method: request.request.method,
                    url: request.request.url,
                    headers: request.request.headers,
                    body: payload,
                },
                true,
                adapter::UpstreamPathPurpose::Auxiliary,
                Some(&proxy),
            )
            .await
    })
    .await
    .map_err(|_| scope.fault(ErrorCode::Timeout, "proxy callback deadline"))?
}
