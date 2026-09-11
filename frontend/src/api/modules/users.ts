import type { AccountGroupRef } from './account-groups'
import request from '../request'

export interface User {
  id: string
  username: string
  role: 'admin' | 'user'
  enabled: boolean
  maxConcurrency: number
  requestsPerMinute: number
  quotaMultipliers: Record<string, string>
  groupIds: string[]
  createdAt: string
  updatedAt: string
}

export interface UserGroup extends AccountGroupRef {
  dailyLimitUsd: string
  weeklyLimitUsd: string
  dailyUsedUsd: string
  weeklyUsedUsd: string
  dailyResetsAt: string | null
  weeklyResetsAt: string | null
}

export const getUsers = () => request<User[]>({ url: '/api/admin/users', method: 'GET' })
export const createUser = (data: { username: string, password: string, groupIds: string[], maxConcurrency: number, requestsPerMinute: number }) => request<User>({ url: '/api/admin/users/create', method: 'POST', data })
export const updateUser = (data: { id: string, quotaMultipliers?: Record<string, string>, enabled: boolean, groupIds: string[], maxConcurrency: number, requestsPerMinute: number }) => request<User>({ url: '/api/admin/users/update', method: 'POST', data })
export const changePassword = (data: { currentPassword: string, newPassword: string }) => request<void>({ url: '/api/profile/password', method: 'POST', data })
export const getUserGroups = () => request<UserGroup[]>({ url: '/api/profile/groups', method: 'GET' })
