import type { AxiosRequestConfig as RequestOptions } from 'axios'
import request from '../request'

export type CostComponent = 'input' | 'output' | 'cacheRead' | 'cacheWrite'
export type MultiplierScope = 'all' | { components: CostComponent[] }

export type PriceBand = 'standard' | 'fast' | 'flex' | 'long_standard' | 'long_fast' | 'long_flex' | 'image'

export interface TokenPrices {
  input: string
  output: string
  cacheRead: string
  cacheWrite: string
}

export interface ModelPricing {
  multiplierBps: number
  multiplierScope?: MultiplierScope
  bands: Partial<Record<PriceBand, TokenPrices>>
}

export type PricingMap = Record<string, Record<string, ModelPricing>>

export interface PricingCatalog {
  defaults: PricingMap
  synced: PricingMap
  overrides: PricingMap
  syncedAt: string | null
  syncPolicy: PricingSyncPolicy
  sourceHash: string | null
}

export interface PricingSyncPolicy {
  enabled: boolean
  updateExisting: boolean
}

export type PricingChange
  = | { action: 'replace', pricing: ModelPricing }
    | { action: 'multiplier', multiplierBps: number, multiplierScope?: MultiplierScope }
    | { action: 'reset' }

export interface PricingSyncPreview {
  prices: PricingMap
  skipped: string[]
  sourceHash: string
}

interface UpdatePricingParam {
  provider: string
  models: string[]
  change: PricingChange
}

interface PricingMutationResponse {
  saved: boolean
}

export function getPricing(options: RequestOptions = {}) {
  return request<PricingCatalog>({
    url: '/api/admin/settings/pricing',
    method: 'GET',
    ...options,
  })
}

export function updatePricing(data: UpdatePricingParam) {
  return request<PricingMutationResponse>({
    url: '/api/admin/settings/pricing/update',
    method: 'POST',
    data,
  })
}

export function previewPricingSync() {
  return request<PricingSyncPreview>({
    url: '/api/admin/settings/pricing/sync/preview',
    method: 'POST',
    timeout: 40_000,
  })
}

export function syncPricing(data: PricingSyncPreview) {
  return request<PricingMutationResponse>({
    url: '/api/admin/settings/pricing/sync',
    method: 'POST',
    data,
    timeout: 45_000,
  })
}

export function updatePricingSyncPolicy(data: PricingSyncPolicy) {
  return request<PricingMutationResponse>({
    url: '/api/admin/settings/pricing/sync/policy',
    method: 'POST',
    data,
  })
}
