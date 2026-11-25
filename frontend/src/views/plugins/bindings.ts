import type {
  PluginAccountSchedulerBinding,
  PluginArtifact,
  PluginAuthenticationBinding,
  PluginInstance,
  PluginMiddlewareBinding,
  PluginModelRouterBinding,
  PluginObserverBinding,
  PluginRetryPolicyBinding,
  PluginUpstreamAdapterBinding,
} from '@/api/modules/plugins'

export type BindingScopeKind = 'all' | 'keys' | 'groups'
export type FailurePolicy = 'reject' | 'delegate'

export interface PluginFormState {
  name: string
  digest: string
  configuration: string
  secrets: Record<string, string>
  removeSecrets: Record<string, boolean>
  trust: boolean
  router: { enabled: boolean, order: string, scope: BindingScopeKind, ids: string, failurePolicy: FailurePolicy }
  scheduler: { enabled: boolean, order: string, scope: BindingScopeKind, ids: string, failurePolicy: FailurePolicy }
  upstream: { enabled: boolean, scope: BindingScopeKind, ids: string }
  retry: { enabled: boolean, order: string, scope: BindingScopeKind, ids: string }
  observer: {
    enabled: boolean
    order: string
    completed: boolean
    completedScope: BindingScopeKind
    completedIds: string
    webSocket: boolean
    webSocketScope: BindingScopeKind
    webSocketIds: string
  }
  middleware: {
    enabled: boolean
    order: string
    http: boolean
    webSocket: boolean
    service: boolean
    request: boolean
    attempt: boolean
    scope: BindingScopeKind
    ids: string
  }
  authentication: {
    enabled: boolean
    failurePolicy: FailurePolicy
    identities: string
  }
}

export const SCOPE_OPTIONS = [
  { label: '全部请求', value: 'all' },
  { label: '指定 Key', value: 'keys' },
  { label: '指定分组', value: 'groups' },
]

function scopeIdsText(scope?: PluginMiddlewareBinding['scope'] | null) {
  return scope && scope.kind !== 'all' ? scope.ids.join('\n') : ''
}

export function createPluginFormState(
  item?: PluginInstance,
  artifacts: PluginArtifact[] = [],
  pkg?: PluginArtifact,
): PluginFormState {
  return {
    name: item?.name ?? pkg?.displayName ?? '',
    digest: pkg?.sha256 ?? item?.artifact.sha256 ?? artifacts[0]?.sha256 ?? '',
    configuration: JSON.stringify(item?.configuration ?? {}, null, 2),
    secrets: {},
    removeSecrets: {},
    trust: false,
    router: {
      enabled: Boolean(item?.modelRouter),
      order: String(item?.modelRouter?.order ?? 0),
      scope: item?.modelRouter?.scope.kind ?? 'all',
      ids: scopeIdsText(item?.modelRouter?.scope),
      failurePolicy: item?.modelRouter?.failurePolicy ?? 'reject',
    },
    scheduler: {
      enabled: Boolean(item?.accountScheduler),
      order: String(item?.accountScheduler?.order ?? 0),
      scope: item?.accountScheduler?.scope.kind ?? 'all',
      ids: scopeIdsText(item?.accountScheduler?.scope),
      failurePolicy: item?.accountScheduler?.failurePolicy ?? 'reject',
    },
    upstream: {
      enabled: Boolean(item?.upstreamAdapter),
      scope: item?.upstreamAdapter?.scope.kind ?? 'all',
      ids: scopeIdsText(item?.upstreamAdapter?.scope),
    },
    retry: {
      enabled: Boolean(item?.retryPolicy),
      order: String(item?.retryPolicy?.order ?? 0),
      scope: item?.retryPolicy?.scope.kind ?? 'all',
      ids: scopeIdsText(item?.retryPolicy?.scope),
    },
    observer: {
      enabled: Boolean(item?.observer),
      order: String(item?.observer?.order ?? 0),
      completed: item?.observer ? Boolean(item.observer.requestCompleted) : true,
      completedScope: item?.observer?.requestCompleted?.kind ?? 'all',
      completedIds: scopeIdsText(item?.observer?.requestCompleted),
      webSocket: Boolean(item?.observer?.websocketResponse),
      webSocketScope: item?.observer?.websocketResponse?.kind ?? 'all',
      webSocketIds: scopeIdsText(item?.observer?.websocketResponse),
    },
    middleware: {
      enabled: Boolean(item?.middleware),
      order: String(item?.middleware?.order ?? 0),
      http: item?.middleware?.mounts.includes('http') ?? false,
      webSocket: item?.middleware?.mounts.includes('websocket') ?? false,
      service: item?.middleware?.mounts.includes('service') ?? false,
      request: item?.middleware?.mounts.includes('request') ?? true,
      attempt: item?.middleware?.mounts.includes('attempt') ?? false,
      scope: item?.middleware?.scope.kind ?? 'all',
      ids: scopeIdsText(item?.middleware?.scope),
    },
    authentication: {
      enabled: Boolean(item?.frontendAuthentication),
      failurePolicy: item?.frontendAuthentication?.failurePolicy ?? 'reject',
      identities: JSON.stringify(item?.frontendAuthentication?.identities ?? {}, null, 2),
    },
  }
}

export function parseBindingOrder(raw: string, label: string): number {
  const order = Number(raw)
  if (!Number.isInteger(order) || order < -10_000 || order > 10_000)
    throw new Error(`${label}必须是 -10000 到 10000 的整数`)
  return order
}

export function parseBindingScope(
  kind: BindingScopeKind,
  rawIds: string,
  label: string,
): PluginMiddlewareBinding['scope'] {
  if (kind === 'all')
    return { kind: 'all' }
  const ids = rawIds.split(/\s+/u).filter(Boolean)
  if (!ids.length || ids.length > 256 || new Set(ids).size !== ids.length)
    throw new Error(`请填写不重复的${label}，1～256 个`)
  return { kind, ids }
}

export function parseJsonObject(raw: string, label: string): Record<string, unknown> {
  let parsed: unknown
  try {
    parsed = JSON.parse(raw)
  }
  catch {
    throw new Error(`${label}不是有效的 JSON 格式`)
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed))
    throw new Error(`${label}必须是 JSON 对象`)
  return parsed as Record<string, unknown>
}

export function parsePluginPayload(
  form: PluginFormState,
  secretFields: string[],
  revision: number,
  enabled: boolean,
) {
  const configuration = parseJsonObject(form.configuration, '配置（JSON）')

  const modelRouter: PluginModelRouterBinding | null = form.router.enabled
    ? {
        order: parseBindingOrder(form.router.order, '模型路由顺序'),
        failurePolicy: form.router.failurePolicy,
        scope: parseBindingScope(form.router.scope, form.router.ids, '路由作用范围 ID'),
      }
    : null

  const accountScheduler: PluginAccountSchedulerBinding | null = form.scheduler.enabled
    ? {
        order: parseBindingOrder(form.scheduler.order, '账号调度顺序'),
        failurePolicy: form.scheduler.failurePolicy,
        scope: parseBindingScope(form.scheduler.scope, form.scheduler.ids, '调度作用范围 ID'),
      }
    : null

  const upstreamAdapter: PluginUpstreamAdapterBinding | null = form.upstream.enabled
    ? {
        scope: parseBindingScope(form.upstream.scope, form.upstream.ids, '适配器作用范围 ID'),
      }
    : null

  const retryPolicy: PluginRetryPolicyBinding | null = form.retry.enabled
    ? {
        order: parseBindingOrder(form.retry.order, '重试策略顺序'),
        scope: parseBindingScope(form.retry.scope, form.retry.ids, '重试作用范围 ID'),
      }
    : null

  let observer: PluginObserverBinding | null = null
  if (form.observer.enabled) {
    const order = parseBindingOrder(form.observer.order, '观察顺序')
    if (!form.observer.completed && !form.observer.webSocket)
      throw new Error('请至少选择一种观察事件')
    observer = {
      order,
      requestCompleted: form.observer.completed
        ? parseBindingScope(form.observer.completedScope, form.observer.completedIds, '观察作用范围 ID')
        : null,
      websocketResponse: form.observer.webSocket
        ? parseBindingScope(form.observer.webSocketScope, form.observer.webSocketIds, '观察作用范围 ID')
        : null,
    }
  }

  let middleware: PluginMiddlewareBinding | null = null
  if (form.middleware.enabled) {
    const order = parseBindingOrder(form.middleware.order, '中间件顺序')
    const mounts: PluginMiddlewareBinding['mounts'] = []
    if (form.middleware.http)
      mounts.push('http')
    if (form.middleware.webSocket)
      mounts.push('websocket')
    if (form.middleware.service)
      mounts.push('service')
    if (form.middleware.request)
      mounts.push('request')
    if (form.middleware.attempt)
      mounts.push('attempt')
    if (!mounts.length)
      throw new Error('请选择中间件挂载位置')
    if ((form.middleware.http || form.middleware.webSocket || form.middleware.service) && form.middleware.scope !== 'all')
      throw new Error('HTTP、WebSocket 会话与宿主服务中间件需选择全部请求')
    middleware = {
      order,
      mounts,
      scope: parseBindingScope(form.middleware.scope, form.middleware.ids, '中间件作用范围 ID'),
    }
  }

  let frontendAuthentication: PluginAuthenticationBinding | null = null
  if (form.authentication.enabled) {
    const identities = parseJsonObject(form.authentication.identities, '外部身份映射（JSON）') as Record<string, string>
    frontendAuthentication = {
      failurePolicy: form.authentication.failurePolicy,
      identities,
    }
  }

  const secrets: Record<string, string | null> = {}
  for (const key of secretFields) {
    if (form.removeSecrets[key])
      secrets[key] = null
    else if (form.secrets[key])
      secrets[key] = form.secrets[key]!
  }

  return {
    observer,
    upstreamAdapter,
    retryPolicy,
    accountScheduler,
    modelRouter,
    middleware,
    frontendAuthentication,
    revision,
    name: form.name,
    artifactSha256: form.digest,
    configuration,
    secrets,
    enabled,
    trustedProcess: form.trust,
  }
}
