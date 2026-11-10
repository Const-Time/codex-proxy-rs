import type { OpsError, UsageErrorOrigin } from '@/api'

const failureClassLabels: Readonly<Record<string, string>> = {
  user_concurrency_limited: '用户并发超限',
  user_rate_limited: '用户请求频率超限',
  client_key_concurrency_limited: 'Key 并发超限',
  client_key_rate_limited: 'Key 请求频率超限',
  group_daily_budget_exceeded: '分组日额度不足',
  group_weekly_budget_exceeded: '分组周额度不足',
  continuation_recovery_required: '会话续接需要重建',
}

export function failureClassText(value: string | null | undefined) {
  if (!value)
    return '未记录'
  return failureClassLabels[value] ?? value
}

export function opsErrorSummary(record: OpsError) {
  const failureLabel = failureClassLabels[record.failureClass]
  return failureLabel ?? record.providerErrorCode ?? record.failureClass
}

export function entryRejected(record: OpsError) {
  return record.metadata.component === 'request_entry' && record.upstreamSendState === 'not_sent'
}

export function errorOriginText(origin: UsageErrorOrigin | undefined) {
  const labels: Record<UsageErrorOrigin, string> = {
    proxy: '代理内部处理',
    upstream: '上游响应',
    connection: '上游连接',
    unknown: '来源未确定',
  }
  return labels[origin ?? 'unknown']
}

export function responseStatusText(status: number | null, transport: string | null) {
  if (transport === 'websocket')
    return status === 101 ? 'WebSocket · 握手 101' : status == null ? 'WebSocket · HTTP 状态未记录' : `WebSocket · HTTP ${status}`
  return status == null ? '未记录' : `HTTP ${status}`
}

export function usageDisplayOutcome(outcome: string, code: string | null, kind: string | null) {
  if (outcome === 'incomplete' && kind !== 'process_interrupted' && (code || kind === 'timeout'))
    return 'failed'
  return outcome
}

export function deliveryStateText(committed: boolean | null) {
  return committed === null ? '未记录' : committed ? '已开始交付' : '未开始交付'
}
