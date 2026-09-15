import type { AccountQuotaWindow } from '../src/api/modules/accounts.ts'
import assert from 'node:assert/strict'
// eslint-disable-next-line test/no-import-node-test -- Use the existing native Node test runner.
import test from 'node:test'
import { forecastCapacityRows, forecastNumber } from '../src/views/accounts/components/AccountQuotaForecastModal/presentation.ts'

const window: AccountQuotaWindow = {
  key: 'week',
  group: 'shortTerm',
  limitId: null,
  limitName: null,
  role: 'secondary',
  windowSeconds: 604800,
  labelDisplay: '周额度',
  windowLabelDisplay: '周额度',
  usedPercent: 46,
  usedPercentDisplay: '46%',
  limitReached: false,
  resetAtDisplay: '2026-09-19 16:10:17',
  localUsage: { totalTokens: 368000000 },
  estimatedQuota: {
    usedUsd: '120',
    totalUsd: '800',
    billedUsedUsd: '240',
    billedTotalUsd: '1600',
    estimatedTokens: 800000000,
    remainingTokens: 432000000,
    remainingUsd: '432',
    remainingBilledUsd: '864',
    percentDelta: 15,
    sampleStart: '2026-09-15T09:00:00+08:00',
    sampleEnd: '2026-09-15T12:00:00+08:00',
  },
}

test('forecast keeps window usage, sample costs, raw and billed estimates distinct', () => {
  const rows = forecastCapacityRows(window)
  assert.equal(rows[0]!.used, '368M')
  assert.equal(rows[0]!.remaining, '432M')
  assert.equal(rows[1]!.basis, '采样区间费用')
  assert.equal(rows[1]!.used, '$120.00')
  assert.equal(rows[1]!.total, '$800.00')
  assert.equal(rows[2]!.total, '$1,600.00')
  assert.equal(rows[2]!.remaining, '$864.00')
})

test('missing costs do not become zero or hide available token forecasts', () => {
  const rows = forecastCapacityRows({
    ...window,
    estimatedQuota: { ...window.estimatedQuota!, totalUsd: null, billedTotalUsd: null, usedUsd: null, billedUsedUsd: null },
  })
  assert.equal(rows[0]!.total, '800M')
  assert.equal(rows[1]!.total, '待估算')
  assert.equal(rows[1]!.used, '待补算')
  assert.equal(rows[2]!.total, '待估算')
  assert.equal(forecastNumber('0', true), '$0.00')
  for (const value of [null, undefined, '', Number.NaN, Infinity, -1, {}, 'bad'])
    assert.equal(forecastNumber(value), '待估算')
})

test('empty observations and cycle fallback show their actual basis', () => {
  const empty = forecastCapacityRows({ ...window, estimatedQuota: null, localUsage: null })
  assert.equal(empty[0]!.used, '—')
  assert.equal(empty[0]!.total, '待估算')
  const cycle = forecastCapacityRows({ ...window, estimatedQuota: { ...window.estimatedQuota!, cycleBased: true } })
  assert.equal(cycle[1]!.basis, '周期累计费用')
  assert.equal(cycle[2]!.basis, '周期累计计费')
})
