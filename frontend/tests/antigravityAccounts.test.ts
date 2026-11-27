import assert from 'node:assert/strict'
// eslint-disable-next-line test/no-import-node-test -- Use the existing native Node test runner.
import test from 'node:test'
import { formatProviderLabel, isSupportedProvider, PROVIDER_DISPLAY_NAMES } from '../src/utils/providers.ts'
import { codexAuthFiles } from '../src/views/accounts/utils/codexAuthExport.ts'
import {
  accountQuotaSubtitle,
  accountUsageWindowLabel,
  quotaLimitLabel,
  quotaWindowConsumptionDisplay,
  quotaWindowDisplayLabel,
  resolveAccountIdentityDisplay,
  resolveAccountPlanDisplay,
  shouldShowWindowEstimate,
  supportsAccountQuotaForecast,
  supportsAccountReauthorization,
} from '../src/views/accounts/utils/quotaPresentation.ts'

test('provider catalog exposes antigravity alongside openai and xai', () => {
  assert.equal(PROVIDER_DISPLAY_NAMES.antigravity, 'Antigravity')
  assert.equal(isSupportedProvider('antigravity'), true)
  assert.equal(isSupportedProvider('openai'), true)
  assert.equal(isSupportedProvider('xai'), true)
  assert.equal(isSupportedProvider('unknown'), false)
  assert.equal(formatProviderLabel('antigravity'), 'Antigravity')
  assert.equal(formatProviderLabel('ANTIGRAVITY'), 'Antigravity')
})

test('Antigravity quota model groups use upstream established labels and attribution display', () => {
  assert.equal(quotaLimitLabel({ limitId: 'codex', limitName: null }), '通用额度')
  assert.equal(quotaLimitLabel({ limitId: 'gemini-group', limitName: 'Gemini Models' }), 'Gemini 模型')
  assert.equal(quotaLimitLabel({ limitId: 'claude-gpt', limitName: 'Claude and GPT Models' }), 'Claude 和 GPT 模型')
  assert.equal(quotaLimitLabel({ limitId: 'custom', limitName: 'Custom Bucket' }), 'Custom Bucket')

  assert.equal(
    quotaWindowDisplayLabel({
      limitId: 'gemini-group',
      limitName: 'Gemini Models',
      labelDisplay: 'Gemini Models · 5 小时额度',
      windowLabelDisplay: '5 小时额度',
    }, false),
    'Gemini 模型 · 5 小时额度',
  )
  assert.equal(
    quotaWindowDisplayLabel({
      limitId: 'gemini-group',
      limitName: 'Gemini Models',
      labelDisplay: 'Gemini Models · 5 小时额度',
      windowLabelDisplay: '5 小时额度',
    }, true),
    '5 小时额度',
  )
  assert.equal(
    quotaWindowDisplayLabel({
      limitId: 'codex',
      limitName: null,
      labelDisplay: '5 小时额度',
      windowLabelDisplay: '5 小时额度',
    }, false),
    '5 小时额度',
  )

  assert.equal(quotaWindowConsumptionDisplay(null), '—')
  assert.equal(quotaWindowConsumptionDisplay(undefined), '—')
  assert.equal(quotaWindowConsumptionDisplay({ totalTokens: 0, totalTokensDisplay: '0' }), '0 Tokens')
  assert.equal(quotaWindowConsumptionDisplay({ totalTokens: 12_340, totalTokensDisplay: '12.3K' }), '12.3K Tokens')
  assert.equal(quotaWindowConsumptionDisplay({ totalTokens: 1200 }), '1,200 Tokens')
})

test('Antigravity plan display maps tier labels without inventing free for missing plans', () => {
  assert.deepEqual(
    resolveAccountPlanDisplay({ planType: 'free-tier', planTypeDisplay: 'FREE' }),
    { rawPlanType: 'free-tier', displayText: 'FREE', paletteKey: 'free' },
  )
  assert.deepEqual(
    resolveAccountPlanDisplay({ planType: 'g1-pro-tier', planTypeDisplay: 'PRO' }),
    { rawPlanType: 'g1-pro-tier', displayText: 'PRO', paletteKey: 'pro' },
  )
  assert.deepEqual(
    resolveAccountPlanDisplay({ planType: 'g1-ultra-tier', planTypeDisplay: 'ULTRA' }),
    { rawPlanType: 'g1-ultra-tier', displayText: 'ULTRA', paletteKey: 'ultra' },
  )
  assert.deepEqual(
    resolveAccountPlanDisplay({ planType: 'g1-pro-tier', planTypeDisplay: 'g1-pro-tier' }),
    { rawPlanType: 'g1-pro-tier', displayText: 'PRO', paletteKey: 'pro' },
  )
  assert.deepEqual(
    resolveAccountPlanDisplay({ planType: null, planTypeDisplay: '未知套餐' }),
    { rawPlanType: '', displayText: '未知套餐', paletteKey: '未知套餐' },
  )
  assert.deepEqual(
    resolveAccountPlanDisplay({ authenticationKind: 'api_key', planType: null, planTypeDisplay: '未知套餐' }),
    { rawPlanType: 'api', displayText: 'API', paletteKey: 'api' },
  )
})

test('account identity handles verified Google email and email-less OAuth fallback cleanly', () => {
  assert.deepEqual(
    resolveAccountIdentityDisplay({ id: 'acct_1', email: 'dev@gmail.com', name: 'Antigravity OAuth', userId: '108234' }),
    { displayTitle: 'dev', secondaryText: 'dev@gmail.com', identityTitle: 'dev\ndev@gmail.com' },
  )
  assert.deepEqual(
    resolveAccountIdentityDisplay({ id: 'acct_2', email: null, name: 'Work AGY', userId: '108234', accountId: null }),
    { displayTitle: 'Work AGY', secondaryText: '108234', identityTitle: 'Work AGY\n108234' },
  )
  assert.deepEqual(
    resolveAccountIdentityDisplay({ id: 'acct_3', email: null, name: '', userId: '108234', accountId: null }),
    { displayTitle: '108234', secondaryText: null, identityTitle: '108234' },
  )
  assert.deepEqual(
    resolveAccountIdentityDisplay({ id: 'acct_4', email: null, name: '密钥账号', authenticationKind: 'api_key' }),
    { displayTitle: '密钥账号', secondaryText: null, identityTitle: '密钥账号' },
  )
})

test('Antigravity capability guards hide unsupported forecast/estimates and allow OAuth reauthorization', () => {
  assert.equal(accountQuotaSubtitle('antigravity'), 'Antigravity 额度')
  assert.equal(accountQuotaSubtitle('xai'), 'xAI 用量窗口')
  assert.equal(accountQuotaSubtitle('openai'), 'Codex 额度')

  assert.equal(supportsAccountQuotaForecast({ provider: 'antigravity', authenticationKind: 'oauth' }), false)
  assert.equal(supportsAccountQuotaForecast({ provider: 'openai', authenticationKind: 'oauth' }), true)
  assert.equal(supportsAccountQuotaForecast({ provider: 'xai', authenticationKind: 'oauth' }), true)
  assert.equal(supportsAccountQuotaForecast({ provider: 'openai', authenticationKind: 'api_key' }), false)

  assert.equal(shouldShowWindowEstimate({ estimatedQuota: null, estimateHint: null }), false)
  assert.equal(shouldShowWindowEstimate({ estimatedQuota: null, estimateHint: '等待本周期的有效额度快照' }), true)

  assert.equal(supportsAccountReauthorization({ provider: 'antigravity', authenticationKind: 'oauth' }), true)
  assert.equal(supportsAccountReauthorization({ provider: 'openai', authenticationKind: 'oauth' }), true)
  assert.equal(supportsAccountReauthorization({ provider: 'xai', authenticationKind: 'oauth' }), true)
  assert.equal(supportsAccountReauthorization({ provider: 'openai', authenticationKind: 'api_key' }), false)
  assert.equal(supportsAccountReauthorization({ provider: 'unknown', authenticationKind: 'oauth' }), false)

  assert.equal(accountUsageWindowLabel({ windowKind: 'recent_week', windowLabelDisplay: '周/月额度窗口' }), '近一周')
  assert.equal(accountUsageWindowLabel({ windowLabelDisplay: '周额度窗口' }), '周额度窗口')

  assert.throws(
    () => codexAuthFiles({
      documents: [{ provider: 'antigravity', document: { provider: 'antigravity', version: 1, accounts: [{ id: 'a' }] } }],
    }, ['a']),
    /仅支持 OpenAI OAuth/,
  )
})
