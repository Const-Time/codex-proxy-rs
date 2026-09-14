import assert from 'node:assert/strict'
// eslint-disable-next-line test/no-import-node-test -- Use the existing native Node test runner.
import test from 'node:test'
import { quotaEstimatePresentation } from '../src/views/accounts/components/AccountQuotaPanel/estimate.ts'

const estimate = { usedUsd: '2.37', billedUsedUsd: '4.74', percentDelta: 1, sampleStart: '2026-09-14T10:00:00+08:00', sampleEnd: '2026-09-14T11:00:00+08:00' }

test('a one-point interval cannot look like a full-cycle reliable estimate', () => {
  const view = quotaEstimatePresentation(estimate)
  assert.equal(view.label, '区间估算 · 初步估算')
  assert.match(view.basis, /上涨 1 个百分点/)
  assert.match(view.warning, /样本较少/)
  const cycle = quotaEstimatePresentation({ ...estimate, cycleBased: true, percentDelta: 35 })
  assert.equal(cycle.label, '周期估算 · 参考估算')
  assert.match(cycle.basis, /已用 35%/)
})

test('partial and retained estimates disclose missing facts and the old timestamp', () => {
  const partial = quotaEstimatePresentation({ ...estimate, missingCostCount: 2, requestCount: 10 })
  assert.match(partial.label, /记录不完整/)
  assert.match(partial.warning, /2 \/ 10 笔费用缺失/)
  const retained = quotaEstimatePresentation(estimate, '保留上次估算：新采样暂无可用费用')
  assert.match(retained.label, /上次结果/)
  assert.match(retained.warning, /2026-09-14T11:00:00/)
  assert.equal(quotaEstimatePresentation(null, '无有效快照').basis, '无有效快照')
  assert.match(quotaEstimatePresentation({ ...estimate, billedUsedUsd: '0.00' }).basis, /计费 \$0.00/)
})
