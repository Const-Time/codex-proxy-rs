import type { PricingCatalog, TokenPrices } from '../src/api/modules/pricing.ts'
import assert from 'node:assert/strict'
// eslint-disable-next-line test/no-import-node-test -- Node runs these pure TypeScript checks without another test framework.
import test from 'node:test'
import { effectivePrice, parseMultiplier, pricingRows, validPrice } from '../src/views/settings/pricing/model.ts'

function rates(input: string): TokenPrices {
  return { input, output: '10', cacheRead: '0.2', cacheWrite: '2.5' }
}

test('pricing layers merge per band and preserve only the manual multiplier', () => {
  const catalog: PricingCatalog = {
    defaults: { openai: { model: { multiplierBps: 10_000, bands: { standard: rates('2'), fast: rates('4') } } } },
    synced: { openai: { model: { multiplierBps: 10_000, bands: { standard: rates('3'), long_standard: rates('6') } } } },
    overrides: { openai: { model: { multiplierBps: 15_000, bands: { standard: rates('5') } } } },
    syncedAt: null,
    syncPolicy: { enabled: true, updateExisting: false },
    sourceHash: null,
  }
  const original = structuredClone(catalog)
  const [row] = pricingRows(catalog, 'openai')
  assert.equal(row?.source, 'custom')
  assert.equal(row?.base.bands.standard?.input, '3')
  assert.equal(row?.effective.bands.standard?.input, '5')
  assert.equal(row?.effective.bands.fast?.input, '4')
  assert.equal(row?.effective.bands.long_standard?.input, '6')
  assert.equal(row?.effective.multiplierBps, 15_000)
  assert.equal(row?.effective.bands.flex, undefined)
  assert.deepEqual(catalog, original)
})

test('pricing rows include new models without mixing providers or inventing bands', () => {
  const catalog: PricingCatalog = {
    defaults: {},
    synced: { openai: { 'new-model': { multiplierBps: 10_000, bands: { standard: rates('2') } } } },
    overrides: { xai: { other: { multiplierBps: 10_000, bands: { standard: rates('1') } } } },
    syncedAt: null,
    syncPolicy: { enabled: true, updateExisting: false },
    sourceHash: null,
  }
  const rows = pricingRows(catalog, 'openai')
  assert.equal(rows.length, 1)
  assert.equal(rows[0]?.model, 'new-model')
  assert.equal(rows[0]?.source, 'synced')
  assert.equal(rows[0]?.effective.bands.fast, undefined)
  assert.deepEqual(pricingRows(catalog, 'missing'), [])
})

test('multiplier parsing is exact in basis points and rejects invalid ranges', () => {
  assert.equal(parseMultiplier('1.2345'), 12_345)
  assert.equal(parseMultiplier('0'), 0)
  assert.equal(parseMultiplier('100'), 1_000_000)
  for (const input of ['100.0001', '-1', '1e2', '1.00001', '', 'NaN'])
    assert.equal(parseMultiplier(input), undefined)
})

test('price validation distinguishes explicit zero from absent or invalid prices', () => {
  for (const input of ['0', '0.0001', '1000000'])
    assert.equal(validPrice(input), true)
  for (const input of ['', '-1', '1e2', '0.00001', '1000000.0001', 'NaN'])
    assert.equal(validPrice(input), false)
  assert.equal(effectivePrice(undefined), '未配置')
  assert.equal(effectivePrice('0'), '0')
})

test('effective price uses exact decimal arithmetic even at precision boundaries', () => {
  assert.equal(effectivePrice('0.1', 12_345), '0.12345')
  assert.equal(effectivePrice('0.0001', 1), '0.00000001')
  assert.equal(effectivePrice('1000000', 1_000_000), '100000000')
  assert.equal(effectivePrice('2', 0), '0')
})
