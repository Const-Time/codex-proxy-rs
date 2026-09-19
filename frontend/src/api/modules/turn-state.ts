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
  mode: 'gateway' | 'fixed' | 'rotating' | 'api'
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
  waitReason: string
  hourlyUsed: number
  hourlyLimit: number
  budgetResetsAt: number | null
  lastTrafficAt: number | null
  automatic: boolean
}

export interface TurnStateMaintenance {
  maxProbesPerHour: number
  idleSeconds: number
  autoModels: boolean
  poolId: string | null
}

export interface TurnStateAccountPolicy {
  accountId: string
  fingerprintConvergence: boolean
  takeover: boolean
  maintenance: TurnStateMaintenance
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
export function saveAccountTurnState(accountId: string, data: { revision: number, fingerprintConvergence: boolean, takeover: boolean, maintenance?: TurnStateMaintenance }) {
  return request<TurnStateView>({ url: '/api/admin/turn-state/account', method: 'POST', data: { ...data, accountId } })
}
export function saveTurnState(data: TurnStateSettings) {
  return request<TurnStateView>({ url: '/api/admin/turn-state', method: 'POST', data })
}
export function turnStateAction(data: { accountId: string, model: string, action: 'refresh' | 'pause' | 'resume' | 'revoke' }) {
  return request<TurnStateView>({ url: '/api/admin/turn-state/action', method: 'POST', data })
}
export function testTurnStatePool(id: string) {
  return request<{ success: boolean, ipv6: boolean, exitIp: string | null, ipv4Address: string | null, ipv6Address: string | null, message: string, egress: TurnStateEgress | null }>({
    url: '/api/admin/turn-state/test-pool',
    method: 'POST',
    data: { id },
    timeout: 40000,
  })
}

export interface TurnStateEgress {
  ip: string | null
  location: { country: string, region: string, city: string, timezone: string } | null
  detectedAt: number
  source: string
  rotating: boolean
}

export interface TurnStateProbeRecord {
  id: string
  cycleId: string
  accountId: string
  accountName: string
  model: string
  phase: 'collect' | 'verify'
  poolId: string | null
  routeName: string
  startedAt: string
  finishedAt: string | null
  facts: {
    completed: boolean
    status: number | null
    decision: string
    reason: string | null
    message: string | null
    latencyMs: number | null
    inputTokens: number | null
    outputTokens: number | null
    cachedTokens: number | null
    reasoningTokens: number | null
    totalTokens: number | null
    stateLength: number | null
    shape: TurnStateShape | null
    fingerprint: string | null
    expiresAt: number | null
    egress: TurnStateEgress | null
  }
}

export interface TurnStateProbeStats {
  total: number
  completed: number
  collections: number
  collectionResults: number
  accepted: number
  verifications: number
  verificationResults: number
  verified: number
  rejected: number
  failed: number
  unknownTokens: number
  knownTokens: number
  averageLatencyMs: number | null
}

export interface TurnStateRecordQuery {
  from: string
  to: string
  page: number
  pageSize: number
  accountId?: string
  model?: string
  phase?: string
  decision?: string
  cycleId?: string
}

export interface TurnStateRecordPage {
  items: TurnStateProbeRecord[]
  stats: TurnStateProbeStats
}

export function getTurnStateRecords(data: TurnStateRecordQuery) {
  return request<TurnStateRecordPage>({ url: '/api/admin/turn-state/records', method: 'POST', data })
}
