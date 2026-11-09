use super::*;
use gateway_admin::{
    model::{
        AdminError,
        proxies::{ProxyListQuery, ProxyPage},
    },
    ports::proxy::PluginProxyAccess,
};
use gateway_core::account::OutboundProxy;
struct Proxies {
    url: String,
}
#[async_trait::async_trait]
impl PluginProxyAccess for Proxies {
    async fn list(&self, query: ProxyListQuery) -> Result<ProxyPage, AdminError> {
        Ok(ProxyPage {
            items: vec![],
            total: 0,
            page: query.page,
            page_size: query.page_size.get(),
        })
    }
    async fn resolve(&self, id: &str) -> Result<OutboundProxy, AdminError> {
        if id != "selected" {
            return Err(AdminError::invalid("unavailable"));
        }
        Ok(OutboundProxy::parse(&self.url).unwrap())
    }
}
#[tokio::test]
async fn sdk_proxy_http_uses_selected_proxy_and_never_falls_back_to_direct() {
    let proxy = Server::new().await;
    let target = Server::new().await;
    let f = Fixture::with_proxies(Some(Arc::new(Proxies {
        url: proxy.url.clone(),
    })))
    .await;
    assert_eq!(
        f.call(json!({"mode":"proxies"})).await,
        json!({"items":[],"total":0,"page":1,"page_size":20})
    );
    let reply = f
        .call(json!({"mode":"proxy","proxy_id":"selected","url":format!("{}/binary",target.url)}))
        .await;
    assert_eq!(reply, json!({"status":200,"bytes":[0,255,1,0]}));
    assert_eq!(proxy.requests.lock().unwrap().len(), 1);
    assert!(target.requests.lock().unwrap().is_empty());
    let failed = f
        .call(json!({"mode":"proxy","proxy_id":"missing","url":format!("{}/binary",target.url)}))
        .await;
    assert_eq!(failed["error"]["code"], "invalid_input");
    assert_eq!(failed["error"]["send_state"], "not_sent");
    assert!(target.requests.lock().unwrap().is_empty());
    f.manager.shutdown().await;
}
