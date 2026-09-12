import type { TokenUsagePoint } from '../src/components/charts/tokenUsage.ts'
import assert from 'node:assert/strict'
// eslint-disable-next-line test/no-import-node-test -- Node 24 runs these pure TypeScript checks without another test framework.
import test from 'node:test'
import { tokenCacheHitRate, tokenUsageTotals } from '../src/components/charts/tokenUsage.ts'

test('cache hit rate is weighted by input tokens and write tokens remain independent', () => {
  const points: TokenUsagePoint[] = [
    { label: '10:00', inputTokens: 100, outputTokens: 10, cachedTokens: 90, cacheWriteTokens: 20, totalTokens: 110, requests: 1 },
    { label: '10:15', inputTokens: 900, outputTokens: 20, cachedTokens: 0, cacheWriteTokens: 150, totalTokens: 920, requests: 2 },
  ]
  assert.deepEqual(tokenUsageTotals(points), { input: 1000, output: 30, read: 90, write: 170, hitRate: 0.09 })
  assert.equal(points.reduce((sum, point) => sum + point.totalTokens, 0), 1030)
})

test('empty input has no measurable hit rate, while measured misses are zero', () => {
  assert.equal(tokenCacheHitRate(0, 0), null)
  assert.equal(tokenCacheHitRate(100, 0), 0)
  assert.equal(tokenCacheHitRate(100, 100), 1)
  assert.equal(tokenCacheHitRate(100, 120), 1)
  assert.equal(tokenUsageTotals([]).hitRate, null)
})
