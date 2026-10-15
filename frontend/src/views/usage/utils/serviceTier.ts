/** Preserve request intent separately from the actual upstream response. */
export function serviceTierLabel(tier: string | null | undefined): string {
  switch (tier) {
    case 'priority': return 'Fast'
    case 'default':
    case 'standard': return 'Standard'
    case 'auto': return 'Auto（未确认）'
    case 'flex': return 'Flex'
    default: return tier || '未记录'
  }
}

export function serviceTierSummary(record: { requestedServiceTier?: string | null, upstreamServiceTier?: string | null }): string | null {
  if (!record.requestedServiceTier && !record.upstreamServiceTier)
    return null
  return `请求 ${serviceTierLabel(record.requestedServiceTier)} · 上游 ${serviceTierLabel(record.upstreamServiceTier)}`
}
