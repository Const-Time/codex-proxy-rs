import type { CostComponent, ModelPricing, MultiplierScope, PriceBand, PricingCatalog, TokenPrices } from '@/api'
import { scopeIncludes } from '../../../utils/multiplierScope.ts'

export const bands: { value: PriceBand, label: string }[] = [
  { value: 'standard', label: '标准' },
  { value: 'fast', label: 'Priority / Fast' },
  { value: 'ultrafast', label: 'Ultrafast' },
  { value: 'flex', label: 'Flex' },
  { value: 'long_standard', label: '长上下文 · 标准' },
  { value: 'long_fast', label: '长上下文 · Priority' },
  { value: 'long_ultrafast', label: '长上下文 · Ultrafast' },
  { value: 'long_flex', label: '长上下文 · Flex' },
  { value: 'image', label: '图像 Token' },
]
export const priceFields: { key: keyof TokenPrices, label: string }[] = [
  { key: 'input', label: '输入' },
  { key: 'output', label: '输出' },
  { key: 'cacheRead', label: '缓存读取' },
  { key: 'cacheWrite', label: '缓存写入' },
]
export interface PricingRow {
  model: string
  provider: string
  source: 'builtin' | 'synced' | 'custom'
  base: ModelPricing
  custom?: ModelPricing
  effective: ModelPricing
}
export function fixedSpeedMultiplier(provider: string, band: PriceBand): number | undefined {
  if (provider !== 'openai')
    return undefined
  if (band === 'fast' || band === 'long_fast')
    return 2
  if (band === 'ultrafast' || band === 'long_ultrafast')
    return 6
  return undefined
}
// Display projection of the OpenAI billing policy. Stored source prices remain
// intact; only Standard prices determine our fixed speed premiums.
export function billingBands(provider: string, source: ModelPricing['bands']): ModelPricing['bands'] {
  const result = { ...source }
  if (provider !== 'openai' || source.image)
    return result
  for (const band of ['fast', 'ultrafast', 'long_fast', 'long_ultrafast'] as const) {
    const standard = source[band.startsWith('long_') ? 'long_standard' : 'standard']
    delete result[band]
    if (standard) {
      const bps = fixedSpeedMultiplier(provider, band)! * 10_000
      result[band] = {
        input: effectivePrice(standard.input, bps),
        output: effectivePrice(standard.output, bps),
        cacheRead: effectivePrice(standard.cacheRead, bps),
        cacheWrite: effectivePrice(standard.cacheWrite, bps),
      }
    }
  }
  return result
}
export function pricingRows(catalog: PricingCatalog, provider: string): PricingRow[] {
  const defaults = catalog.defaults[provider] ?? {}
  const synced = catalog.synced[provider] ?? {}
  const custom = catalog.overrides[provider] ?? {}
  return [...new Set([...Object.keys(defaults), ...Object.keys(synced), ...Object.keys(custom)])].sort().map((model) => {
    const base: ModelPricing = { multiplierBps: 10_000, bands: billingBands(provider, { ...defaults[model]?.bands, ...synced[model]?.bands }) }
    const override = custom[model]
    return {
      model,
      provider,
      base,
      custom: override,
      source: override ? 'custom' : synced[model] ? 'synced' : 'builtin',
      effective: { multiplierBps: override?.multiplierBps ?? 10_000, multiplierScope: override?.multiplierScope ?? 'all', bands: billingBands(provider, { ...base.bands, ...override?.bands }) },
    }
  })
}
export const sourceLabels = { builtin: '内置价目', synced: '同步价目', custom: '人工覆盖' }
export function parseMultiplier(value: string): number | undefined {
  if (!/^\d{1,3}(?:\.\d{1,4})?$/.test(value))
    return undefined
  const [whole = '0', fraction = ''] = value.split('.')
  const bps = Number(whole) * 10_000 + Number(fraction.padEnd(4, '0'))
  return bps <= 1_000_000 ? bps : undefined
}
export function validPrice(value: string) {
  return /^\d{1,7}(?:\.\d{1,4})?$/.test(value) && Number(value) <= 1_000_000
}
export function multiplierText(bps: number) {
  return `${bps / 10_000}×`
}
export function effectivePrice(value: string | undefined, bps = 10_000, scope?: MultiplierScope, component?: CostComponent): string {
  if (value === undefined || !/^\d{1,7}(?:\.\d{1,4})?$/.test(value) || Number(value) > 6_000_000)
    return '未配置'
  if (component && !scopeIncludes(scope, component))
    bps = 10_000
  const [whole = '0', fraction = ''] = value.split('.')
  const scaled = (BigInt(whole) * 10_000n + BigInt(fraction.padEnd(4, '0'))) * BigInt(bps)
  const decimal = (scaled % 100_000_000n).toString().padStart(8, '0').replace(/0+$/, '')
  return `${scaled / 100_000_000n}${decimal ? `.${decimal}` : ''}`
}
