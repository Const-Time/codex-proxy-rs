import request from '../request'

export interface Subscription {
  userId: string
  email: string
  username: string
  enabled: boolean
  groupId: string
  groupName: string
  quotaMultiplier: string
  dailyLimitUsd: string
  weeklyLimitUsd: string
  dailyUsedUsd: string
  weeklyUsedUsd: string
  dailyResetsAt: string | null
  weeklyResetsAt: string | null
  lastResetAt: string | null
  lastResetReason: string | null
}
export const getSubscriptions = () => request<Subscription[]>({ url: '/api/admin/subscriptions', method: 'GET' })
export type SubscriptionTarget = Pick<Subscription, 'userId' | 'groupId'>
export const resetSubscriptions = (requestId: string, targets: SubscriptionTarget[]) => request<number>({ url: '/api/admin/subscriptions/reset', method: 'POST', data: { requestId, targets } })
