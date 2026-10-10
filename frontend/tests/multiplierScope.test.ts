import assert from 'node:assert/strict'
// eslint-disable-next-line test/no-import-node-test -- Node runs these pure TypeScript checks without another test framework.
import test from 'node:test'
import { costComponents, scopeIncludes, scopeLabel, validScope } from '../src/utils/multiplierScope.ts'
import { effectivePrice, pricingRows } from '../src/views/settings/pricing/model.ts'

test('fee scopes support all, individual and combined components but reject empty selections', () => {
  assert.equal(validScope('all'), true)
  assert.equal(validScope({ components: [] }), false)
  assert.equal(validScope({ components: ['input', 'input'] }), false)
  for (const { value } of costComponents) {
    assert.equal(scopeIncludes(undefined, value), true)
    assert.equal(validScope({ components: [value] }), true)
    assert.equal(effectivePrice('2.5', 20000, { components: [value] }, value), '5')
  }
  const scope = { components: ['cacheRead', 'cacheWrite'] as const }
  const selected = { components: [...scope.components] }
  assert.equal(effectivePrice('2.5', 20000, selected, 'input'), '2.5')
  assert.equal(effectivePrice('2.5', 0, selected, 'cacheRead'), '0')
  assert.equal(scopeLabel(selected), '缓存读、缓存写')
  assert.equal(scopeLabel('all'), '全部费用')
})

test('pricing rows preserve a manual component selection without changing synced prices', () => {
  const [row] = pricingRows({ defaults: {}, synced: { openai: { model: { multiplierBps: 10000, bands: {} } } }, overrides: { openai: { model: { multiplierBps: 20000, multiplierScope: { components: ['input', 'output'] }, bands: {} } } }, syncedAt: null, sourceHash: null, syncPolicy: { enabled: true, updateExisting: false } }, 'openai')
  assert.deepEqual(row?.effective.multiplierScope, { components: ['input', 'output'] })
  assert.equal(row?.effective.multiplierBps, 20000)
  assert.equal(row?.base.multiplierBps, 10000)
})
