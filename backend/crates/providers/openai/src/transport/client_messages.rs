use std::collections::BTreeSet;
use std::time::Instant;

use bytes::Bytes;
use futures::StreamExt;
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderValue};
use reqwest::{Method, Response};
use serde_json::Value;

use super::catalog::{
    CodexModelCatalogError, CodexModelCatalogSnapshot, MAX_CATALOG_MODELS, catalog_etag,
    parse_messages_model_catalog,
};
use super::client::*;
use super::{diagnostics::CodexUpstreamDiagnostics, endpoints::endpoint_url, response_meta};

pub(crate) struct MessagesHttpResponse {
    pub body: CodexBackendSseStream,
    pub streaming: bool,
    pub diagnostics: CodexUpstreamDiagnostics,
    pub response_metadata: response_meta::CodexResponseMetadata,
    pub transport_metrics: CodexTransportMetrics,
}

impl CodexBackendClient {
    pub(super) async fn fetch_messages_models(
        &self,
        context: CodexRequestContext<'_>,
        native_count: bool,
    ) -> CodexClientResult<CodexModelCatalogSnapshot> {
        let mut cursor = None::<String>;
        let mut seen = BTreeSet::new();
        let mut models = Vec::new();
        let mut etag = None;
        loop {
            let mut request = self
                .client
                .request(Method::GET, endpoint_url(&self.base_url, "models"))
                .headers(self.model_request_headers(&self.profile.snapshot(), context)?)
                .query(&[("limit", "1000")]);
            if let Some(cursor) = &cursor {
                request = request.query(&[("after_id", cursor)]);
            }
            let response = request.send().await.map_err(CodexClientError::HttpJson)?;
            if !response.status().is_success() {
                return Err(
                    messages_upstream_error(response, CodexTransportMetrics::default()).await,
                );
            }
            if cursor.is_none() {
                etag = catalog_etag(response.headers())?;
            }
            let mut page: Value = response.json().await.map_err(CodexClientError::HttpJson)?;
            let data = page
                .get_mut("data")
                .and_then(Value::as_array_mut)
                .ok_or(CodexModelCatalogError::InvalidWire)?;
            let empty = data.is_empty();
            models.append(data);
            if models.len() > MAX_CATALOG_MODELS {
                return Err(CodexModelCatalogError::TooManyModels.into());
            }
            if page.get("has_more").and_then(Value::as_bool) != Some(true) {
                break;
            }
            let next = page
                .get("last_id")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty() && value.len() <= 512)
                .ok_or(CodexModelCatalogError::InvalidWire)?;
            if empty || !seen.insert(next.to_owned()) {
                return Err(CodexModelCatalogError::InvalidWire.into());
            }
            cursor = Some(next.to_owned());
        }
        let body = serde_json::to_vec(&serde_json::json!({"data": models}))
            .map_err(|_| CodexModelCatalogError::InvalidWire)?;
        Ok(parse_messages_model_catalog(
            &body,
            etag.as_deref(),
            native_count,
        )?)
    }

    pub(crate) async fn post_messages(
        &self,
        endpoint: &'static str,
        body: Bytes,
        passthrough: &HeaderMap,
        context: CodexRequestContext<'_>,
    ) -> CodexClientResult<MessagesHttpResponse> {
        let mut headers = self.model_request_headers(&self.profile.snapshot(), context)?;
        if let Some(version) = passthrough.get("anthropic-version") {
            headers.insert("anthropic-version", version.clone());
        }
        for beta in passthrough.get_all("anthropic-beta") {
            headers.append("anthropic-beta", beta.clone());
        }
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/json, text/event-stream"),
        );

        let trace = context
            .trace
            .cloned()
            .unwrap_or_default()
            .exchange("messages_http");
        trace.headers(
            "upstream.request.headers",
            serde_json::json!({"method": "POST", "endpoint": endpoint}),
            headers
                .iter()
                .filter(|(_, value)| !value.is_sensitive())
                .map(|(name, value)| (name.as_str(), value.as_bytes())),
        );
        trace.capture("upstream.request.body", &body);
        let started = Instant::now();
        let response = self
            .http_opening_client()?
            .post(endpoint_url(&self.base_url, endpoint))
            .headers(headers)
            .body(body)
            .send()
            .await
            .map_err(CodexClientError::HttpJson)?;
        let transport_metrics = CodexTransportMetrics {
            upstream_headers_ms: Some(elapsed_duration_millis(started.elapsed())),
            http_version: Some(http_version_name(response.version()).to_owned()),
            ..Default::default()
        };
        trace.headers(
            "upstream.response.headers",
            serde_json::json!({"status": response.status().as_u16()}),
            response
                .headers()
                .iter()
                .filter(|_| !response.status().is_redirection())
                .map(|(name, value)| (name.as_str(), value.as_bytes())),
        );
        if !response.status().is_success() {
            return Err(messages_upstream_error(response, transport_metrics).await);
        }
        let streaming = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("text/event-stream"));
        let diagnostics =
            response_meta::diagnostics(Some(response.status().as_u16()), response.headers());
        let response_metadata = response_meta::response_metadata(response.headers());
        let body = Box::pin(
            response
                .bytes_stream()
                .map(|chunk| chunk.map_err(CodexClientError::HttpJson)),
        );
        Ok(MessagesHttpResponse {
            body,
            streaming,
            diagnostics,
            response_metadata,
            transport_metrics,
        })
    }
}

async fn messages_upstream_error(
    response: Response,
    metrics: CodexTransportMetrics,
) -> CodexClientError {
    let status = response.status();
    let (body, client_response, retry_after_seconds, diagnostics) = if status.is_redirection() {
        (
            String::new(),
            None,
            None,
            CodexUpstreamDiagnostics::with_status(status.as_u16()),
        )
    } else {
        let headers = response.headers();
        let content_type = headers
            .get(CONTENT_TYPE)
            .map(|value| value.as_bytes().to_vec());
        let client_headers = response_meta::client_headers(headers);
        let diagnostics = response_meta::diagnostics(Some(status.as_u16()), headers);
        let retry_after_seconds = retry_after_seconds(headers, None);
        let raw = match read_error_response_body(response).await {
            Ok(raw) => raw,
            Err(error) => return CodexClientError::HttpJson(error),
        };
        let body = String::from_utf8_lossy(&raw).into_owned();
        let client_response = Box::new(CodexClientVisibleUpstreamResponse::new(
            status,
            content_type,
            client_headers,
            raw,
        ));
        (
            body,
            Some(client_response),
            retry_after_seconds,
            diagnostics,
        )
    };
    CodexClientError::Upstream {
        status,
        body,
        client_response,
        retry_after_seconds,
        diagnostics: Box::new(diagnostics),
        set_cookie_headers: Vec::new(),
        rate_limit_headers: Vec::new(),
        transport: CodexBackendTransport::HttpJson,
        transport_metrics: Box::new(metrics),
        send_phase: super::diagnostics::CodexUpstreamSendPhase::AfterPayload,
    }
}
