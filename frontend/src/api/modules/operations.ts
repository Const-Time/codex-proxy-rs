import request from '../request'

export interface OperationLog {
  id: string
  occurredAt: string
  actorUserId: string | null
  email: string | null
  username: string | null
  authMethod: string
  method: string
  path: string
  status: number
  durationMs: number
  clientIp: string | null
  forwardedIp: string | null
  requestId: string
  changes: { action: string, entityKind: string, entityRef: string, changedFields: string[] }[]
}
export const getOperationLogs = (params: Record<string, string | number | undefined>) => request<{ items: OperationLog[], total: number }>({ url: '/api/admin/operation-logs', method: 'GET', params })
export const getUsageKeyOptions = (personal: boolean) => request<{ id: string, name: string, userId: string | null }[]>({ url: '/api/admin/usage/key-options', method: 'GET', params: { personal } })
