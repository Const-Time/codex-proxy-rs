//! 固定 `/api/admin` 观测路由与 handler。

use super::*;

pub fn router<S>() -> Router<S>
where
    S: AdminSessionState + Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/api/admin/dashboard/summary", get(dashboard_summary::<S>))
        .route("/api/admin/dashboard/trend", get(dashboard_trend::<S>))
        .route("/api/admin/usage/records", get(usage_records::<S>))
        .route(
            "/api/admin/usage/records/detail",
            get(usage_record_detail::<S>),
        )
        .route(
            "/api/admin/usage/records/summary",
            get(usage_records_summary::<S>),
        )
        .route(
            "/api/admin/usage/insights/overview",
            get(usage_insights_overview::<S>),
        )
        .route(
            "/api/admin/usage/insights/diagnostics",
            get(usage_insights_diagnostics::<S>),
        )
        .route("/api/admin/operations/errors", get(ops_errors::<S>))
}

pub(crate) async fn dashboard_summary<S>(
    _auth: AdminAuth,
    State(state): State<S>,
    AdminQuery(query): AdminQuery<DashboardQuery>,
) -> Result<impl IntoResponse, AdminError>
where
    S: AdminSessionState + Send + Sync,
{
    let kind = query.trend_kind().map_err(map_wire_error)?;
    // 概览默认按中国时区当日统计，与单独趋势接口保持同一口径。
    let range = dashboard_today_range(query.start_time.as_deref(), query.end_time.as_deref())
        .map_err(map_wire_error)?;
    let result = state
        .admin_services()
        .observability()
        .dashboard_summary(range, domain_trend_kind(kind))
        .await
        .map_err(map_service_error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(dashboard_view(result, kind)),
    ))
}

pub(crate) async fn dashboard_trend<S>(
    _auth: AdminAuth,
    State(state): State<S>,
    AdminQuery(query): AdminQuery<DashboardQuery>,
) -> Result<impl IntoResponse, AdminError>
where
    S: AdminSessionState + Send + Sync,
{
    let kind = query.trend_kind().map_err(map_wire_error)?;
    let range = dashboard_today_range(query.start_time.as_deref(), query.end_time.as_deref())
        .map_err(map_wire_error)?;
    let result = state
        .admin_services()
        .observability()
        .dashboard_trend(range, domain_trend_kind(kind))
        .await
        .map_err(map_service_error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(trend_view(result, kind)),
    ))
}

pub(crate) async fn usage_records<S>(
    auth: super::super::auth::UserAuth,
    State(state): State<S>,
    AdminQuery(query): AdminQuery<UsageQuery>,
) -> Result<impl IntoResponse, AdminError>
where
    S: AdminSessionState + Send + Sync,
{
    let mut command = usage_command(&query).map_err(map_wire_error)?;
    command.filter.owner_user_id = personal_owner(&auth, query.personal);
    let result = state
        .admin_services()
        .observability()
        .usage_records(command)
        .await
        .map_err(map_service_error)?;
    let data = usage_page_view(result);
    let mut data = serde_json::to_value(data).map_err(|_| AdminError::internal())?;
    if personal_owner(&auth, query.personal).is_some()
        && let Some(items) = data
            .get_mut("items")
            .and_then(serde_json::Value::as_array_mut)
    {
        for item in items {
            retain_personal_fields(item);
        }
    }
    Ok(AdminResponse::new(StatusCode::OK, AdminEnvelope::ok(data)))
}

pub(crate) async fn usage_record_detail<S>(
    auth: super::super::auth::UserAuth,
    State(state): State<S>,
    AdminQuery(query): AdminQuery<DetailQuery>,
) -> Result<impl IntoResponse, AdminError>
where
    S: AdminSessionState + Send + Sync,
{
    query.validate().map_err(map_wire_error)?;
    let result = state
        .admin_services()
        .observability()
        .usage_record_detail(
            query.id.trim(),
            personal_owner(&auth, query.personal).as_deref(),
        )
        .await
        .map_err(map_service_error)?;
    let mut data =
        serde_json::to_value(usage_detail_view(result)).map_err(|_| AdminError::internal())?;
    if personal_owner(&auth, query.personal).is_some() {
        retain_personal_fields(&mut data);
    }
    Ok(AdminResponse::new(StatusCode::OK, AdminEnvelope::ok(data)))
}

pub(crate) async fn usage_records_summary<S>(
    auth: super::super::auth::UserAuth,
    State(state): State<S>,
    AdminQuery(query): AdminQuery<UsageQuery>,
) -> Result<impl IntoResponse, AdminError>
where
    S: AdminSessionState + Send + Sync,
{
    let range = usage_range(query.start_time.as_deref(), query.end_time.as_deref())
        .map_err(map_wire_error)?;
    let mut filter = usage_filter(&query).map_err(map_wire_error)?;
    filter.owner_user_id = personal_owner(&auth, query.personal);
    let result = state
        .admin_services()
        .observability()
        .usage_summary(range, filter)
        .await
        .map_err(map_service_error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(usage_summary_view(result)),
    ))
}

pub(crate) async fn usage_insights_overview<S>(
    auth: super::super::auth::UserAuth,
    State(state): State<S>,
    AdminQuery(query): AdminQuery<UsageQuery>,
) -> Result<impl IntoResponse, AdminError>
where
    S: AdminSessionState + Send + Sync,
{
    let range = usage_range(query.start_time.as_deref(), query.end_time.as_deref())
        .map_err(map_wire_error)?;
    let mut filter = usage_filter(&query).map_err(map_wire_error)?;
    filter.owner_user_id = personal_owner(&auth, query.personal);
    let result = state
        .admin_services()
        .observability()
        .usage_insights(range, filter)
        .await
        .map_err(map_service_error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(usage_insights_view(result)),
    ))
}

pub(crate) async fn usage_insights_diagnostics<S>(
    auth: super::super::auth::UserAuth,
    State(state): State<S>,
    AdminQuery(query): AdminQuery<DiagnosticsQuery>,
) -> Result<impl IntoResponse, AdminError>
where
    S: AdminSessionState + Send + Sync,
{
    let dimension = query.dimension().map_err(map_wire_error)?;
    let owner = personal_owner(&auth, query.personal);
    if owner.is_some()
        && matches!(
            dimension,
            DiagnosticDimension::Account | DiagnosticDimension::User
        )
    {
        return Err(AdminError::bad_request(
            "个人使用记录不提供上游账号或用户排行维度",
        ));
    }
    let range = usage_range(query.start_time.as_deref(), query.end_time.as_deref())
        .map_err(map_wire_error)?;
    let filter = domain::UsageFilter {
        owner_user_id: owner,
        user_id: non_empty(query.user_id),
        client_api_key_ref: non_empty(query.client_api_key_id),
        group_id: non_empty(query.group_id),
        provider_account_ref: non_empty(query.account_id),
        client_transport: non_empty(query.client_transport),
        provider_kind: non_empty(query.provider),
        model: non_empty(query.model),
        status_code: parse_status(query.status_code).map_err(map_wire_error)?,
        search: non_empty(query.search),
        ..domain::UsageFilter::default()
    };
    let result = state
        .admin_services()
        .observability()
        .diagnostics(range, filter, domain_diagnostic_dimension(dimension))
        .await
        .map_err(map_service_error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(diagnostics_view(result, dimension)),
    ))
}

pub(crate) async fn ops_errors<S>(
    auth: super::super::auth::UserAuth,
    State(state): State<S>,
    AdminQuery(query): AdminQuery<OpsQuery>,
) -> Result<impl IntoResponse, AdminError>
where
    S: AdminSessionState + Send + Sync,
{
    let mut command = ops_command(&query).map_err(map_wire_error)?;
    let owner = personal_owner(&auth, query.personal);
    if let Some(owner) = &owner {
        command.filter.user_id = Some(owner.clone());
    }
    let result = state
        .admin_services()
        .observability()
        .ops_errors(command)
        .await
        .map_err(map_service_error)?;
    let mut data = ops_page_view(result);
    if owner.is_some() {
        for item in &mut data.items {
            item.user_id = None;
            item.user_email = None;
            item.username = None;
            item.account_id = None;
            item.account_name = None;
            item.account_email = None;
            item.authentication_kind = None;
            item.raw_upstream_error = None;
            item.provider_error_code = None;
            item.upstream_request_id = None;
            item.message = format!("请求失败（{}）", item.failure_class);
            item.metadata.account_label = None;
            item.metadata.continuation_affinity_hash = None;
            item.metadata.continuation_previous_response_id_hash = None;
            item.metadata.upstream_connection_id = None;
            item.metadata.upstream_connection_exit_reason = None;
        }
    }
    Ok(AdminResponse::new(StatusCode::OK, AdminEnvelope::ok(data)))
}

fn personal_owner(auth: &super::super::auth::UserAuth, personal: bool) -> Option<String> {
    (personal || auth.user.role == gateway_admin::model::users::UserRole::User)
        .then(|| auth.user.id.clone())
}

/// An explicit public field contract: newly added administrator diagnostics do
/// not become visible to ordinary users automatically.
fn retain_personal_fields(value: &mut serde_json::Value) {
    const FIELDS: &[&str] = &[
        "id",
        "requestId",
        "clientApiKeyId",
        "routingScope",
        "routingGroupRefs",
        "routingGroupNamesSnapshot",
        "kind",
        "provider",
        "route",
        "model",
        "requestedModel",
        "serviceTier",
        "statusCode",
        "clientTransport",
        "imageGenerationRequested",
        "imageGenerationSucceeded",
        "responseId",
        "latencyMs",
        "firstTokenMs",
        "inputTokens",
        "outputTokens",
        "cachedTokens",
        "cacheWriteTokens",
        "reasoningTokens",
        "imageInputTokens",
        "imageOutputTokens",
        "createdAt",
        "createdAtDisplay",
        "clientIp",
        "userAgent",
        "reasoningEffort",
        "reasoningPreset",
        "compact",
        "requestKind",
        "subagentKind",
        "tokenDetails",
        "billing",
        "costs",
        "costCoverage",
        "firstTokenLatencyMs",
        "firstTokenLatencyMsDisplay",
        "latencyMsDisplay",
        "logicalOutcome",
    ];
    if let Some(object) = value.as_object_mut() {
        object.retain(|key, _| FIELDS.contains(&key.as_str()));
    }
}
