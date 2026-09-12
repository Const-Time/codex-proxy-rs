export interface QuotaUsageBudget {
  dailyLimitUsd: string
  weeklyLimitUsd: string
  dailyUsedUsd: string
  weeklyUsedUsd: string
  dailyResetsAt: string | null
  weeklyResetsAt: string | null
}

export function quotaUsagePeriods(budget: QuotaUsageBudget) {
  return (['daily', 'weekly'] as const)
    .filter(period => period !== 'daily' || Number(budget.dailyLimitUsd) > 0)
    .map((period) => {
      const limit = Number(budget[`${period}LimitUsd`])
      const used = Number(budget[`${period}UsedUsd`])
      const percent = limit > 0 ? Math.min(100, Math.max(0, used / limit * 100)) : null
      return {
        key: period,
        label: period === 'daily' ? '每日' : '每周',
        used: budget[`${period}UsedUsd`],
        limit: budget[`${period}LimitUsd`],
        resetsAt: budget[`${period}ResetsAt`],
        percent,
        tone: percent !== null && percent >= 100 ? 'danger' : percent !== null && percent >= 80 ? 'warning' : 'success',
      }
    })
}

export function quotaResetLabel(value: string | null, now: number): string {
  if (!value)
    return '额度周期尚未开始'
  const target = Date.parse(value)
  if (!Number.isFinite(target))
    return '重置时间暂不可用'
  const remaining = target - now
  if (remaining <= 0)
    return '已到重置时间，请刷新'
  const minutes = Math.ceil(remaining / 60_000)
  const days = Math.floor(minutes / 1440)
  const hours = Math.floor(minutes % 1440 / 60)
  if (days > 0)
    return `${days} 天${hours ? ` ${hours} 小时` : ''}后重置`
  if (hours > 0)
    return `${hours} 小时${minutes % 60 ? ` ${minutes % 60} 分钟` : ''}后重置`
  return `${minutes} 分钟后重置`
}
