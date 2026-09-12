export interface TokenUsagePoint {
  label: string
  inputTokens: number
  outputTokens: number
  cachedTokens: number
  cacheWriteTokens: number
  totalTokens: number
  requests: number
  cost?: string | null
}

export function tokenCacheHitRate(input: number, read: number): number | null {
  return input > 0 ? Math.min(1, Math.max(0, read / input)) : null
}

export function tokenUsageTotals(points: TokenUsagePoint[]) {
  const totals = points.reduce((sum, point) => ({
    input: sum.input + point.inputTokens,
    output: sum.output + point.outputTokens,
    read: sum.read + point.cachedTokens,
    write: sum.write + point.cacheWriteTokens,
  }), { input: 0, output: 0, read: 0, write: 0 })
  return { ...totals, hitRate: tokenCacheHitRate(totals.input, totals.read) }
}
