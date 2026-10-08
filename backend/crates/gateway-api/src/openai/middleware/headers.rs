//! Keep plugin projections behind the same upstream header filter as ingress.
use crate::openai::responses::request::forwarded_request_headers;
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use gateway_core::engine::middleware::{MiddlewareError, MiddlewareHeader};

pub(super) fn filtered_headers(input: &[MiddlewareHeader]) -> Result<HeaderMap, MiddlewareError> {
    let mut headers = HeaderMap::new();
    for header in input {
        let name = HeaderName::from_bytes(header.name().as_bytes())
            .map_err(|_| MiddlewareError::InvalidState)?;
        let value =
            HeaderValue::from_bytes(header.value()).map_err(|_| MiddlewareError::InvalidState)?;
        headers.append(name, value);
    }
    Ok(forwarded_request_headers(&headers)
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect())
}

pub(super) fn from_context(
    context: &serde_json::Map<String, serde_json::Value>,
) -> Result<HeaderMap, MiddlewareError> {
    let mut headers = HeaderMap::new();
    if let Some(values) = context
        .get("opaque_request_headers")
        .and_then(|v| v.as_array())
    {
        for value in values {
            let fields = value
                .as_array()
                .filter(|a| a.len() == 2)
                .ok_or(MiddlewareError::InvalidState)?;
            let name = HeaderName::from_bytes(
                fields[0]
                    .as_str()
                    .ok_or(MiddlewareError::InvalidState)?
                    .as_bytes(),
            )
            .map_err(|_| MiddlewareError::InvalidState)?;
            let value = STANDARD
                .decode(fields[1].as_str().ok_or(MiddlewareError::InvalidState)?)
                .map_err(|_| MiddlewareError::InvalidState)?;
            headers.append(
                name,
                HeaderValue::from_bytes(&value).map_err(|_| MiddlewareError::InvalidState)?,
            );
        }
    }
    Ok(headers)
}
