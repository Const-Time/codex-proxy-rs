//! Native Google GenAI generation and model discovery.

mod error;
use crate::ApiState;
use crate::openai::{
    auth::{authenticate_client, client_access_error_response},
    error::protocol_error_response,
    ingress::{AuthenticatedBody, normalize_api_key},
    responses::{
        RequestDecodeError,
        http::{GenerationProtocol, respond_authenticated},
    },
};
use axum::{
    Json, Router,
    extract::{Extension, Path, Query, RawQuery, State, connect_info::ConnectInfo},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse as _, Response},
    routing::get,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
pub(super) use error::error_frame;
use gateway_core::{operation::OperationKind, routing::PublicModelProfile};
use gateway_protocol::genai::GENAI_PROTOCOL;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::net::SocketAddr;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelsQuery {
    #[serde(alias = "page_size")]
    page_size: Option<i64>,
    #[serde(alias = "page_token")]
    page_token: Option<String>,
}

#[derive(Deserialize, Serialize)]
struct ModelCursor {
    after: String,
    size: usize,
}

fn valid_model_name(model: &str) -> bool {
    !matches!(model, "" | "." | "..")
        && model
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-@".contains(&b))
}

fn catalog_error(status: StatusCode, message: &str) -> Response {
    (status, Json(error::payload(status, message))).into_response()
}

pub(super) fn router() -> Router<ApiState> {
    Router::new()
        .route("/v1beta/models", get(list))
        .route("/v1beta/models/{resource}", get(detail).post(generate))
        .layer(axum::middleware::map_response(error::project_response))
}

struct ModelEntry {
    profile: PublicModelProfile,
    count_tokens: bool,
}

async fn catalog(state: &ApiState, mut headers: HeaderMap) -> Result<Vec<ModelEntry>, Response> {
    normalize_api_key(&mut headers, "x-goog-api-key");
    let client = authenticate_client(state.openai(), &headers)
        .await
        .map_err(client_access_error_response)?;
    let snapshot = client.snapshot();
    let scope = client.policy().account_scope();
    Ok(snapshot
        .public_native_model_profiles(scope, GENAI_PROTOCOL)
        .into_iter()
        .filter(|profile| valid_model_name(profile.model().as_str()))
        .map(|profile| ModelEntry {
            count_tokens: snapshot.supports_public_native_operation(
                profile.model(),
                scope,
                GENAI_PROTOCOL,
                OperationKind::CountTokens,
            ),
            profile,
        })
        .collect())
}

async fn list(
    State(state): State<ApiState>,
    Query(query): Query<ModelsQuery>,
    headers: HeaderMap,
) -> Response {
    match catalog(&state, headers).await {
        Ok(models) => paginate(&models, query),
        Err(response) => response,
    }
}

fn paginate(models: &[ModelEntry], query: ModelsQuery) -> Response {
    let size = match query.page_size.unwrap_or(50) {
        n if n < 0 => {
            return catalog_error(StatusCode::BAD_REQUEST, "pageSize must not be negative.");
        }
        0 => 50,
        n => n.min(1000) as usize,
    };
    let start = if let Some(token) = query.page_token.filter(|token| !token.is_empty()) {
        let cursor = (token.len() <= 4096)
            .then(|| URL_SAFE_NO_PAD.decode(token).ok())
            .flatten()
            .and_then(|bytes| serde_json::from_slice::<ModelCursor>(&bytes).ok());
        let Some(cursor) = cursor.filter(|cursor| cursor.size == size) else {
            return catalog_error(
                StatusCode::BAD_REQUEST,
                "Invalid pageToken or changed pageSize.",
            );
        };
        let Some(index) = models
            .iter()
            .position(|model| model.profile.model().as_str() == cursor.after)
        else {
            return catalog_error(
                StatusCode::BAD_REQUEST,
                "pageToken is no longer in the model catalog.",
            );
        };
        index + 1
    } else {
        0
    };
    let end = (start + size).min(models.len());
    let mut body =
        json!({"models": models[start..end].iter().map(model_response).collect::<Vec<_>>()});
    if end < models.len() {
        let cursor = ModelCursor {
            after: models[end - 1].profile.model().as_str().to_owned(),
            size,
        };
        let Ok(bytes) = serde_json::to_vec(&cursor) else {
            return catalog_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Could not encode pageToken.",
            );
        };
        body["nextPageToken"] = json!(URL_SAFE_NO_PAD.encode(bytes));
    }
    Json(body).into_response()
}

async fn detail(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if id.contains(':') {
        return catalog_error(
            StatusCode::METHOD_NOT_ALLOWED,
            "This model action requires POST.",
        );
    }
    if !valid_model_name(&id) {
        return catalog_error(StatusCode::BAD_REQUEST, "Invalid model name.");
    }
    match catalog(&state, headers).await {
        Ok(models) => models
            .iter()
            .find(|entry| entry.profile.model().as_str() == id)
            .map(|profile| Json(model_response(profile)).into_response())
            .unwrap_or_else(|| catalog_error(StatusCode::NOT_FOUND, "Model not found.")),
        Err(response) => response,
    }
}

fn model_response(entry: &ModelEntry) -> Value {
    let id = entry.profile.model().as_str();
    let presentation = entry.profile.presentation();
    let mut methods = vec!["generateContent", "streamGenerateContent"];
    if entry.count_tokens {
        methods.push("countTokens");
    }
    json!({
        "name": format!("models/{id}"),
        "baseModelId": id,
        "displayName": presentation.display_name().unwrap_or(id),
        "description": presentation.description().unwrap_or(""),
        "supportedGenerationMethods": methods,
    })
}

pub(in crate::openai) async fn generate(
    state: State<ApiState>,
    Path(resource): Path<String>,
    RawQuery(query): RawQuery,
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    ingress_id: Option<Extension<tower_http::request_id::RequestId>>,
    headers: HeaderMap,
    input: AuthenticatedBody,
) -> Response {
    let bad_request = |field: &str| {
        protocol_error_response(
            StatusCode::BAD_REQUEST,
            RequestDecodeError::InvalidValue {
                field: field.to_owned(),
            }
            .protocol_body(),
        )
    };
    let Some((model, action)) = resource.split_once(':') else {
        return bad_request("model");
    };
    if !valid_model_name(model) {
        return bad_request("model");
    }
    let (streaming, endpoint) = match action {
        "generateContent" => (false, "/v1beta/models/{model}:generateContent"),
        "streamGenerateContent" => (true, "/v1beta/models/{model}:streamGenerateContent"),
        "countTokens" => (false, "/v1beta/models/{model}:countTokens"),
        _ => {
            return protocol_error_response(
                StatusCode::NOT_FOUND,
                RequestDecodeError::InvalidValue {
                    field: "action".into(),
                }
                .protocol_body(),
            );
        }
    };
    let query = query.unwrap_or_default();
    let mut formats = url::form_urlencoded::parse(query.as_bytes())
        .filter_map(|(name, value)| (name == "alt").then_some(value));
    let format = formats.next();
    if formats.next().is_some()
        || (streaming && format.as_deref() != Some("sse"))
        || (!streaming && format.is_some_and(|value| value != "json"))
    {
        return bad_request("alt");
    }
    respond_authenticated(
        state,
        connect_info,
        ingress_id,
        headers,
        input.body,
        endpoint,
        (input.client, Some(input.memory)),
        if action == "countTokens" {
            GenerationProtocol::GenAiCount {
                model: model.to_owned(),
            }
        } else {
            GenerationProtocol::GenAi {
                model: model.to_owned(),
                streaming,
            }
        },
    )
    .await
}
