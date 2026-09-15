import type { AccountQuotaWindow } from '@/api'

export function forecastNumber(value: unknown, currency = false, empty = '待估算') {
  if (value == null || value === '' || (typeof value !== 'number' && typeof value !== 'string'))
    return empty
  const amount = Number(value)
  if (!Number.isFinite(amount) || amount < 0)
    return empty
  return currency
    ? new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 2 }).format(amount)
    : new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 2 }).format(amount)
}

export function forecastCapacityRows(window: AccountQuotaWindow) {
  const estimate = window.estimatedQuota
  const local = window.localUsage && typeof window.localUsage === 'object' && 'totalTokens' in window.localUsage
    ? window.localUsage.totalTokens
    : null
  return [
    {
      label: 'Token 容量',
      basis: '本周期已记录',
      used: forecastNumber(local, false, '—'),
      total: forecastNumber(estimate?.estimatedTokens),
      remaining: forecastNumber(estimate?.remainingTokens, false, '—'),
    },
    {
      label: '原始费用 · USD',
      basis: estimate?.cycleBased ? '周期累计费用' : '采样区间费用',
      used: forecastNumber(estimate?.usedUsd, true, '待补算'),
      total: forecastNumber(estimate?.totalUsd, true),
      remaining: forecastNumber(estimate?.remainingUsd, true, '—'),
    },
    {
      label: '倍率后计费 · USD',
      basis: estimate?.cycleBased ? '周期累计计费' : '采样区间计费',
      used: forecastNumber(estimate?.billedUsedUsd, true, '待补算'),
      total: forecastNumber(estimate?.billedTotalUsd, true),
      remaining: forecastNumber(estimate?.remainingBilledUsd, true, '—'),
    },
  ]
}
