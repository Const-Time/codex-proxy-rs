import type { AccountQuotaWindow } from '@/api'
import { estimateCostCoverage, estimateCostDisplay } from '../AccountQuotaPanel/estimate.ts'

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
  const coverage = estimateCostCoverage(estimate)
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
      basis: coverage.raw ? '本周期已知费用（不完整）' : '本周期累计费用',
      used: forecastNumber(estimate?.usedUsd, true, '待补算'),
      total: estimateCostDisplay(estimate?.totalUsd, coverage.raw),
      remaining: estimate?.remainingUsd == null ? '—' : estimateCostDisplay(estimate.remainingUsd, coverage.raw),
    },
    {
      label: '倍率后计费 · USD',
      basis: coverage.billed ? '本周期已知计费（不完整）' : '本周期累计计费',
      used: forecastNumber(estimate?.billedUsedUsd, true, '待补算'),
      total: estimateCostDisplay(estimate?.billedTotalUsd, coverage.billed),
      remaining: estimate?.remainingBilledUsd == null ? '—' : estimateCostDisplay(estimate.remainingBilledUsd, coverage.billed),
    },
  ]
}
