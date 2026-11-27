import type { Account, AccountQuotaWindow, AccountUsage } from '@/api'
import { formatInteger } from '../../../utils/number.ts'
import { isRecord } from '../../../utils/object.ts'
import { isSupportedProvider } from '../../../utils/providers.ts'

const quotaLimitLabels = new Map<string, string>([
  ['gemini models', 'Gemini 模型'],
  ['claude and gpt models', 'Claude 和 GPT 模型'],
])

const antigravityPlanLabels = new Map<string, string>([
  ['free-tier', 'FREE'],
  ['g1-pro-tier', 'PRO'],
  ['g1-ultra-tier', 'ULTRA'],
])

const usageWindowKindLabels: Record<NonNullable<AccountUsage['windowKind']>, string> = {
  general: '累计用量',
  daily: '日用量',
  recent_week: '近一周',
  weekly: '周用量',
  monthly: '月用量',
  quota: '周期用量',
}

export function quotaLimitLabel(window: Pick<AccountQuotaWindow, 'limitId' | 'limitName'>): string | null {
  if (window.limitId === 'codex')
    return '通用额度'

  const normalized = window.limitName?.trim().toLowerCase().replace(/\s+/g, ' ')
  return (normalized ? quotaLimitLabels.get(normalized) : undefined) ?? window.limitName
}

export function quotaWindowDisplayLabel(
  window: Pick<AccountQuotaWindow, 'limitId' | 'limitName' | 'labelDisplay' | 'windowLabelDisplay'>,
  grouped: boolean,
): string {
  if (grouped)
    return window.windowLabelDisplay
  if (window.limitId !== 'codex' && window.limitName) {
    const limitLabel = quotaLimitLabel(window)
    if (limitLabel)
      return `${limitLabel} · ${window.windowLabelDisplay}`
  }
  return window.labelDisplay
}

/**
 * 区分已确认的 0 Token 消耗与未知/未确认的本地归因（upstream a6bdeb17f）。
 */
export function quotaWindowConsumptionDisplay(localUsage: unknown): string {
  if (!isRecord(localUsage))
    return '—'
  const total = localUsage.totalTokens
  if (typeof total !== 'number' || !Number.isFinite(total) || total < 0)
    return '—'
  if (total === 0)
    return '0 Tokens'
  const display = typeof localUsage.totalTokensDisplay === 'string' && localUsage.totalTokensDisplay.trim()
    ? localUsage.totalTokensDisplay.trim()
    : formatInteger(total)
  return `${display} Tokens`
}

export function resolveAccountPlanDisplay(input: {
  authenticationKind?: string
  planType?: string | null
  planTypeDisplay: string
}) {
  if (input.authenticationKind === 'api_key') {
    return {
      rawPlanType: 'api',
      displayText: 'API',
      paletteKey: 'api',
    }
  }
  const rawPlanType = input.planType?.trim() || ''
  const rawDisplay = input.planTypeDisplay.trim()
  const mappedTier = antigravityPlanLabels.get(rawDisplay.toLowerCase())
    ?? (rawDisplay === rawPlanType ? antigravityPlanLabels.get(rawPlanType.toLowerCase()) : undefined)
  const displayText = mappedTier ?? input.planTypeDisplay
  const paletteKey = (displayText.trim() || rawPlanType).toLowerCase()
  return {
    rawPlanType,
    displayText,
    paletteKey,
  }
}

export function resolveAccountIdentityDisplay(
  account: Pick<Account, 'id' | 'email'> & Partial<Pick<Account, 'accountId' | 'userId' | 'name' | 'authenticationKind'>>,
  titleMode: 'local-part' | 'email' = 'local-part',
) {
  if (account.authenticationKind === 'api_key') {
    const label = account.name?.trim() || account.email?.trim() || account.accountId?.trim() || String(account.id)
    return {
      displayTitle: label,
      secondaryText: null as string | null,
      identityTitle: label,
    }
  }

  const email = account.email?.trim()
  if (email) {
    const displayTitle = titleMode === 'email' ? email : email.split('@')[0] || email
    const secondaryText = titleMode === 'email' ? null : email
    const identityTitle = secondaryText && secondaryText !== displayTitle
      ? `${displayTitle}\n${secondaryText}`
      : displayTitle
    return { displayTitle, secondaryText, identityTitle }
  }

  const accountId = typeof account.accountId === 'string' ? account.accountId.trim() : ''
  const name = typeof account.name === 'string' ? account.name.trim() : ''
  const userId = typeof account.userId === 'string' ? account.userId.trim() : ''
  const fallback = accountId || name || userId || String(account.id)
  const secondaryText = titleMode === 'email'
    ? null
    : userId && userId !== fallback
      ? userId
      : null
  const identityTitle = secondaryText ? `${fallback}\n${secondaryText}` : fallback
  return {
    displayTitle: fallback,
    secondaryText,
    identityTitle,
  }
}

export function shouldShowWindowEstimate(
  window: Pick<AccountQuotaWindow, 'estimatedQuota' | 'estimateHint'>,
): boolean {
  return Boolean(window.estimatedQuota || window.estimateHint)
}

export function supportsAccountQuotaForecast(
  account: Pick<Account, 'provider' | 'authenticationKind'>,
): boolean {
  return account.authenticationKind !== 'api_key' && account.provider !== 'antigravity'
}

export function supportsAccountReauthorization(
  account: Pick<Account, 'provider' | 'authenticationKind'>,
): boolean {
  return account.authenticationKind === 'oauth' && isSupportedProvider(account.provider)
}

export function accountQuotaSubtitle(provider: string): string {
  if (provider === 'xai')
    return 'xAI 用量窗口'
  if (provider === 'antigravity')
    return 'Antigravity 额度'
  return 'Codex 额度'
}

export function accountUsageWindowLabel(usage: Pick<AccountUsage, 'windowKind' | 'windowLabelDisplay'>): string {
  if (usage.windowKind === 'recent_week')
    return usageWindowKindLabels.recent_week
  return usage.windowLabelDisplay
}
