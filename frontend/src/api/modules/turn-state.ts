import request from '../request'

export interface TurnStatePolicy {
  enabled: boolean
  headerLength: number
  cipherBlocks: number
  ttlSeconds: number
  refreshBeforeSeconds: number
  minimumRemainingSeconds: number
  concurrency: number
  maxAttempts: number
  timeoutSeconds: number
  retrySeconds: number
}

export interface TurnStatePool {
  id: string
  name: string
  enabled: boolean
  mode: 'gateway' | 'api'
  endpoint: string
  hasSecret: boolean
  jsonPointer: string
}

export interface TurnStateShape {
  headerLength: number
  issuedAt: number
  totalBytes: number
  cipherBytes: number
  blocks: number
}

export interface TurnStateObservation {
  at: number
  direction: string
  fingerprint: string
  shape: TurnStateShape | null
  message: string
}

export interface TurnStateTargetInput {
  accountId: string
  model: string
  poolId: string
  enabled: boolean
}

export interface TurnStateTarget extends TurnStateTargetInput {
  status: 'paused' | 'probing' | 'ready' | 'expired' | 'empty'
  fingerprint: string | null
  issuedAt: number | null
  expiresAt: number | null
  nextProbeAt: number
  lastMessage: string
  history: TurnStateObservation[]
  candidateCount: number
  takeover: boolean
}

export interface TurnStateAccountPolicy {
  accountId: string
  fingerprintConvergence: boolean
  takeover: boolean
}

export interface TurnStateView {
  revision: number
  policy: TurnStatePolicy
  pools: TurnStatePool[]
  targets: TurnStateTarget[]
  accounts: TurnStateAccountPolicy[]
}

export interface TurnStateSettings {
  revision: number
  policy: TurnStatePolicy
  pools: (Omit<TurnStatePool, 'endpoint' | 'hasSecret'> & { endpoint?: string, bearer?: string })[]
  targets: TurnStateTargetInput[]
}

export function getTurnState() {
  return request<TurnStateView>({ url: '/api/admin/turn-state', method: 'GET' })
}
export function saveAccountTurnState(accountId: string, data: { revision: number, fingerprintConvergence: boolean, takeover: boolean }) {
  return request<TurnStateView>({ url: '/api/admin/turn-state/account', method: 'POST', data: { ...data, accountId } })
}
export function saveTurnState(data: TurnStateSettings) {
  return request<TurnStateView>({ url: '/api/admin/turn-state', method: 'POST', data })
}
export function turnStateAction(data: { accountId: string, model: string, action: 'refresh' | 'pause' | 'resume' | 'revoke' }) {
  return request<TurnStateView>({ url: '/api/admin/turn-state/action', method: 'POST', data })
}
export function testTurnStatePool(id: string) {
  return request<{ success: boolean, ipv6: boolean, exitIp: string | null, message: string }>({
    url: '/api/admin/turn-state/test-pool',
    method: 'POST',
    data: { id },
    timeout: 40000,
  })
}
