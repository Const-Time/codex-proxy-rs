import type { OpsError } from '@/api'

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
