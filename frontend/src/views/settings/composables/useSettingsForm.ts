import type { rotationOptions } from '../constants'
import { computed, reactive, ref, shallowRef } from 'vue'

import { getSettings, updateSettings } from '@/api'
import { toast } from '@/components/base/BaseToast'
import { errorMessage } from '@/utils/async'

type RotationStrategy = (typeof rotationOptions)[number]['value']

export function useSettingsForm() {
  const loading = shallowRef(true)
  const saving = shallowRef(false)
  const error = shallowRef('')
  const saved = shallowRef<Awaited<ReturnType<typeof getSettings>>>()
  const mappings = ref<Array<{ requestedModel: string, upstreamModel: string }>>([])
  const form = reactive({
    maxRetryAfterSeconds: 240 as number | null,
    resetCreditLeadMinutes: 10 as number | null,
    resetCreditMaxWeeklyRemainingPercent: null as number | null,
    subscriptionAutoResetEnabled: true,
    refreshMarginSeconds: null as number | null,
    refreshConcurrency: null as number | null,
    maxConcurrentPerAccount: null as number | null,
    requestIntervalMs: null as number | null,
    rotationStrategy: '' as RotationStrategy | '',
    minCodexDesktopVersion: '',
    minCodexCliVersion: '',
    usageRetentionDays: 31,
    opsEventRetentionDays: 30,
    auditRetentionDays: 90,
  })

  function numericModel(key: 'maxRetryAfterSeconds' | 'resetCreditLeadMinutes' | 'resetCreditMaxWeeklyRemainingPercent' | 'refreshMarginSeconds' | 'refreshConcurrency' | 'maxConcurrentPerAccount' | 'requestIntervalMs') {
    return computed({
      get: () => (form[key] === null ? '' : String(form[key])),
      set: (value: string) => {
        if (!value.trim()) {
          form[key] = null
          return
        }
        const parsed = Number(value)
        form[key] = Number.isFinite(parsed) ? parsed : null
      },
    })
  }

  const retentionModel = (key: 'usageRetentionDays' | 'opsEventRetentionDays' | 'auditRetentionDays') => computed({
    get: () => Number.isNaN(form[key]) ? '' : String(form[key]),
    set: (value: string) => { form[key] = value.trim() ? Number(value) : Number.NaN },
  })
  const usageRetentionDaysValue = retentionModel('usageRetentionDays')
  const opsEventRetentionDaysValue = retentionModel('opsEventRetentionDays')
  const auditRetentionDaysValue = retentionModel('auditRetentionDays')

  const maxRetryAfterSecondsValue = numericModel('maxRetryAfterSeconds')
  const resetCreditLeadMinutesValue = numericModel('resetCreditLeadMinutes')
  const resetCreditMaxWeeklyRemainingPercentValue = numericModel('resetCreditMaxWeeklyRemainingPercent')
  const refreshMarginSecondsValue = numericModel('refreshMarginSeconds')
  const refreshConcurrencyValue = numericModel('refreshConcurrency')
  const maxConcurrentPerAccountValue = numericModel('maxConcurrentPerAccount')
  const requestIntervalMsValue = numericModel('requestIntervalMs')
  const minCodexDesktopVersionError = computed(() => versionError(form.minCodexDesktopVersion))
  const minCodexCliVersionError = computed(() => versionError(form.minCodexCliVersion))

  function versionError(value: string): string {
    const normalized = value.trim()
    return normalized && !isSemver(normalized) ? '请输入标准 SemVer，例如 0.152.0' : ''
  }

  function applySettings(data: Awaited<ReturnType<typeof getSettings>>) {
    form.maxRetryAfterSeconds = data.maxRetryAfterSeconds ?? 240
    form.resetCreditLeadMinutes = data.resetCreditLeadMinutes ?? 10
    form.resetCreditMaxWeeklyRemainingPercent = data.resetCreditMaxWeeklyRemainingPercent ?? null
    form.subscriptionAutoResetEnabled = data.subscriptionAutoResetEnabled ?? true
    form.refreshMarginSeconds = data.refreshMarginSeconds
    form.refreshConcurrency = data.refreshConcurrency
    form.maxConcurrentPerAccount = data.maxConcurrentPerAccount
    form.requestIntervalMs = data.requestIntervalMs
    form.rotationStrategy = data.rotationStrategy
    form.minCodexDesktopVersion = data.minCodexDesktopVersion ?? ''
    form.minCodexCliVersion = data.minCodexCliVersion ?? ''
    form.usageRetentionDays = data.usageRetentionDays
    form.opsEventRetentionDays = data.opsEventRetentionDays
    form.auditRetentionDays = data.auditRetentionDays
    mappings.value = Object.entries(data.modelMappings || {}).map(([requestedModel, upstreamModel]) => ({
      requestedModel,
      upstreamModel: String(upstreamModel),
    }))
  }

  const currentDraft = () => JSON.stringify([form, mappings.value])
  const savedDraft = shallowRef('')
  const dirty = computed(() => Boolean(saved.value && currentDraft() !== savedDraft.value))

  function rememberSettings(data: Awaited<ReturnType<typeof getSettings>>) {
    applySettings(data)
    saved.value = data
    savedDraft.value = currentDraft()
  }

  function resetSettings() {
    if (saved.value)
      rememberSettings(saved.value)
  }

  async function loadSettings() {
    if (saved.value)
      return
    loading.value = true
    error.value = ''
    try {
      rememberSettings(await getSettings())
    }
    catch (cause: unknown) {
      error.value = errorMessage(cause, '设置加载失败')
      toast.error(error.value)
    }
    finally {
      loading.value = false
    }
  }

  function addMapping() {
    mappings.value = [...mappings.value, { requestedModel: '', upstreamModel: '' }]
  }

  function updateMapping(index: number, key: 'requestedModel' | 'upstreamModel', value: string) {
    const rows = [...mappings.value]
    if (!rows[index])
      return
    rows[index] = { ...rows[index], [key]: value }
    mappings.value = rows
  }

  function removeMapping(index: number) {
    const rows = [...mappings.value]
    rows.splice(index, 1)
    mappings.value = rows
  }

  function mappingPayload() {
    const entries: Record<string, string> = {}
    for (const row of mappings.value) {
      const requested = row.requestedModel.trim()
      const upstream = row.upstreamModel.trim()
      if (!requested || !upstream)
        throw new Error('请完整填写模型映射')
      if (entries[requested])
        throw new Error(`存在重复的客户端模型：${requested}`)
      entries[requested] = upstream
    }
    return entries
  }

  async function saveSettings() {
    if (saving.value || loading.value || !dirty.value)
      return
    const { refreshMarginSeconds, refreshConcurrency, maxConcurrentPerAccount, requestIntervalMs, rotationStrategy } = form
    if (refreshMarginSeconds === null || refreshConcurrency === null || maxConcurrentPerAccount === null || requestIntervalMs === null || !rotationStrategy) {
      toast.warning('请完整填写运行参数和调度策略')
      return
    }
    const { maxRetryAfterSeconds, resetCreditLeadMinutes, resetCreditMaxWeeklyRemainingPercent } = form
    if (maxRetryAfterSeconds === null || !Number.isInteger(maxRetryAfterSeconds) || maxRetryAfterSeconds < 0 || maxRetryAfterSeconds > 4294967295) {
      toast.warning('重试等待上限必须是 0～4294967295 的整数')
      return
    }
    if (resetCreditLeadMinutes === null || !Number.isInteger(resetCreditLeadMinutes) || resetCreditLeadMinutes < 1 || resetCreditLeadMinutes > 1440) {
      toast.warning('提前使用时间必须是 1～1440 的整数')
      return
    }
    if (resetCreditMaxWeeklyRemainingPercent !== null && (!Number.isInteger(resetCreditMaxWeeklyRemainingPercent) || resetCreditMaxWeeklyRemainingPercent < 0 || resetCreditMaxWeeklyRemainingPercent > 100)) {
      toast.warning('剩余周额度上限必须是 0～100 的整数，或留空不限制')
      return
    }
    if (minCodexDesktopVersionError.value || minCodexCliVersionError.value) {
      toast.warning('请修正客户端最低版本格式')
      return
    }
    if (!Number.isInteger(form.usageRetentionDays) || form.usageRetentionDays < 31 || form.usageRetentionDays > 4294967295
      || [form.opsEventRetentionDays, form.auditRetentionDays].some(value => !Number.isInteger(value) || value < 1 || value > 4294967295)) {
      toast.warning('请求记录至少保留 31 天，运维事件和操作日志至少保留 1 天')
      return
    }
    try {
      saving.value = true
      error.value = ''
      const result = await updateSettings({
        maxRetryAfterSeconds,
        resetCreditLeadMinutes,
        resetCreditMaxWeeklyRemainingPercent,
        modelMappings: mappingPayload(),
        refreshMarginSeconds,
        refreshConcurrency,
        maxConcurrentPerAccount,
        requestIntervalMs,
        rotationStrategy,
        minCodexDesktopVersion: form.minCodexDesktopVersion.trim() || null,
        minCodexCliVersion: form.minCodexCliVersion.trim() || null,
        usageRetentionDays: form.usageRetentionDays,
        opsEventRetentionDays: form.opsEventRetentionDays,
        auditRetentionDays: form.auditRetentionDays,
        subscriptionAutoResetEnabled: form.subscriptionAutoResetEnabled,
      })
      rememberSettings(result)
      toast.success('设置已保存')
    }
    catch (cause: unknown) {
      error.value = errorMessage(cause, '保存失败')
      toast.error(error.value)
    }
    finally {
      saving.value = false
    }
  }

  return {
    loading,
    saving,
    error,
    form,
    mappings,
    dirty,
    resetSettings,
    usageRetentionDaysValue,
    opsEventRetentionDaysValue,
    auditRetentionDaysValue,
    addMapping,
    updateMapping,
    removeMapping,
    maxRetryAfterSecondsValue,
    resetCreditLeadMinutesValue,
    resetCreditMaxWeeklyRemainingPercentValue,
    refreshMarginSecondsValue,
    refreshConcurrencyValue,
    maxConcurrentPerAccountValue,
    requestIntervalMsValue,
    minCodexDesktopVersionError,
    minCodexCliVersionError,
    saveSettings,
    loadSettings,
  }
}

function isSemver(value: string): boolean {
  if (value.length > 64 || value.startsWith('v'))
    return false

  const buildParts = value.split('+')
  if (buildParts.length > 2)
    return false
  const [versionAndPrerelease = '', build] = buildParts
  if (build !== undefined && !validIdentifiers(build, false))
    return false

  const prereleaseSeparator = versionAndPrerelease.indexOf('-')
  const core = prereleaseSeparator < 0
    ? versionAndPrerelease
    : versionAndPrerelease.slice(0, prereleaseSeparator)
  const prerelease = prereleaseSeparator < 0
    ? undefined
    : versionAndPrerelease.slice(prereleaseSeparator + 1)
  if (prerelease !== undefined && !validIdentifiers(prerelease, true))
    return false

  const coreParts = core.split('.')
  return coreParts.length === 3 && coreParts.every(validCoreNumericIdentifier)
}

function validIdentifiers(value: string, rejectNumericLeadingZeros: boolean): boolean {
  return Boolean(value) && value.split('.').every((identifier) => {
    if (!identifier || !/^[\da-z-]+$/i.test(identifier))
      return false
    return !rejectNumericLeadingZeros || !/^\d+$/.test(identifier) || validNumericIdentifier(identifier)
  })
}

function validNumericIdentifier(value: string): boolean {
  return /^(?:0|[1-9]\d*)$/.test(value)
}

function validCoreNumericIdentifier(value: string): boolean {
  return validNumericIdentifier(value) && BigInt(value) <= 18_446_744_073_709_551_615n
}
