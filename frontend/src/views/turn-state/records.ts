import type { TurnStateEgress } from '@/api/modules/turn-state'

export function egressRegion(egress?: TurnStateEgress | null): string {
  const location = egress?.location
  if (!location)
    return '归属地区未知'
  return [...new Set([location.country, location.region, location.city].filter(Boolean))].join(' · ') || '归属地区未知'
}

export function egressLabel(egress?: TurnStateEgress | null): string {
  if (!egress)
    return '出口未记录'
  if (!egress.ip)
    return '检测未确认'
  return egress.rotating ? '轮换出口参考' : '独立检测快照'
}

export function decisionLabel(value: string): string {
  const labels: Record<string, string> = {
    accepted: '候选入选',
    verified: '验证通过',
    rejected: '候选未入选',
    failed: '探测失败',
    running: '探测中',
    interrupted: '结果未确认',
    legacy: '历史结果未记录',
  }
  return labels[value] ?? '结果未知'
}

export function ratio(numerator: number, denominator: number): string {
  return denominator ? `${(numerator / denominator * 100).toFixed(1)}%` : '—'
}

export function recordRange(hours = 24, now = Date.now()) {
  return { from: new Date(now - hours * 3600000).toISOString(), to: new Date(now).toISOString() }
}
