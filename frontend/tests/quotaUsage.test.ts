import type { QuotaUsageBudget } from '../src/components/quota/usage.ts'
import assert from 'node:assert/strict'
// eslint-disable-next-line test/no-import-node-test -- Use the project's existing native Node test runner.
import test from 'node:test'
import { quotaResetLabel, quotaUsagePeriods } from '../src/components/quota/usage.ts'

const budget: QuotaUsageBudget = {
  dailyLimitUsd: '0',
  dailyUsedUsd: '12',
  dailyResetsAt: null,
  weeklyLimitUsd: '600',
  weeklyUsedUsd: '150',
  weeklyResetsAt: null,
}

test('unlimited daily quota is hidden and weekly quota remains visible', () => {
  const rows = quotaUsagePeriods(budget)
  assert.equal(rows.length, 1)
  assert.equal(rows[0]?.key, 'weekly')
  assert.equal(rows[0]?.percent, 25)
  assert.equal(quotaUsagePeriods({ ...budget, dailyLimitUsd: '30' }).length, 2)
  assert.equal(quotaUsagePeriods({ ...budget, weeklyLimitUsd: '0' })[0]?.percent, null)
  const exceeded = quotaUsagePeriods({ ...budget, weeklyUsedUsd: '650' })[0]
  assert.equal(exceeded?.percent, 100)
  assert.equal(exceeded?.used, '650')
  assert.equal(exceeded?.tone, 'danger')
})

test('reset countdown handles missing, future and elapsed windows without inventing a reset', () => {
  const now = Date.parse('2026-09-12T12:00:00Z')
  assert.equal(quotaResetLabel(null, now), '额度周期尚未开始')
  assert.equal(quotaResetLabel('invalid', now), '重置时间暂不可用')
  assert.equal(quotaResetLabel('2026-09-12T12:55:00Z', now), '55 分钟后重置')
  assert.equal(quotaResetLabel('2026-09-19T05:00:00Z', now), '6 天 17 小时后重置')
  assert.equal(quotaResetLabel('2026-09-12T11:00:00Z', now), '已到周期边界，等待确认重置')
})
