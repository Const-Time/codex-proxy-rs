import assert from 'node:assert/strict'
// eslint-disable-next-line test/no-import-node-test -- Use the existing native Node test runner.
import test from 'node:test'
import { estimateCostCoverage, estimateCostDisplay, quotaEstimatePresentation } from '../src/views/accounts/components/AccountQuotaPanel/estimate.ts'

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
  assert.match(partial.label, /部分费用估算/)
  assert.match(partial.warning, /2 \/ 10 笔费用缺失/)
  const retained = quotaEstimatePresentation(estimate, '保留上次估算：新采样暂无可用费用')
  assert.match(retained.label, /上次结果/)
  assert.match(retained.warning, /2026-09-14T11:00:00/)
  assert.match(retained.warning, /样本较少/)
  assert.match(partial.warning, /样本较少/)
  assert.equal(quotaEstimatePresentation(null, '无有效快照').basis, '无有效快照')
  assert.match(quotaEstimatePresentation({ ...estimate, billedUsedUsd: '0.00' }).basis, /计费 \$0.00/)
})

test('paired blocks describe nullable costs without making missing prices look free', () => {
  const view = quotaEstimatePresentation({
    ...estimate,
    usedUsd: null,
    billedUsedUsd: null,
    percentDelta: 15,
    blockCount: 3,
    lowSample: false,
    missingCostCount: 2,
    requestCount: 50,
  })
  assert.match(view.basis, /原始 待补算 · 计费 待补算/)
  assert.match(view.basis, /3 个完整分段/)
  assert.match(view.warning, /仅按已知金额外推/)
  assert.doesNotMatch(view.warning, /样本较少/)
  assert.doesNotMatch(view.basis, /\$0/)
})

test('cycle holes stay visible even when the recent sample is fully priced', () => {
  const view = quotaEstimatePresentation({ ...estimate, percentDelta: 20, costIncomplete: true, cycleMissingCostCount: 24, missingCostCount: 0, sampleUsd: '1.00', sampleBilledUsd: null })
  assert.match(view.label, /部分费用估算/)
  assert.match(view.warning, /本周期累计 24 笔/)
  assert.match(view.warning, /不修改历史账单/)
  assert.match(view.basis, /原始 \$1.00 · 计费 待补算/)
  assert.equal(estimateCostDisplay('0.00', true), '部分估算 $0.00')
  assert.equal(estimateCostDisplay(null, true), '待估算')
  assert.deepEqual(estimateCostCoverage({ costIncomplete: false, billedCostIncomplete: true }), { raw: false, billed: true })
})
