<script setup lang="ts">
import { Save, Undo2 } from '@lucide/vue'
import { useEventListener } from '@vueuse/core'
import { computed, nextTick, onScopeDispose, reactive, ref, useTemplateRef, watch } from 'vue'
import { onBeforeRouteLeave, useRoute, useRouter } from 'vue-router'

import BaseButton from '@/components/base/BaseButton.vue'
import BaseCard from '@/components/base/BaseCard.vue'
import BaseConfirmModal from '@/components/base/BaseConfirmModal.vue'
import BaseFormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseIconButton from '@/components/base/BaseIconButton.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BasePageHeader from '@/components/base/BasePageHeader.vue'
import BaseSegmented from '@/components/base/BaseSegmented.vue'
import { useAuthStore } from '@/stores/modules/auth'

import AdminApiKeyCard from './components/AdminApiKeyCard.vue'
import SettingsBackupSection from './components/backup/SettingsBackupSection.vue'
import ClientVersionSettings from './components/client-version/index.vue'
import ModelAliasesCard from './components/ModelAliasesCard.vue'
import ResetCreditCard from './components/ResetCreditCard.vue'
import RotationStrategyCard from './components/RotationStrategyCard.vue'
import RuntimeSettingsCard from './components/RuntimeSettingsCard.vue'
import { useAdminApiKey } from './composables/useAdminApiKey'
import { useSettingsForm } from './composables/useSettingsForm'
import { rotationOptions } from './constants'
import PricingSection from './pricing/index.vue'

const route = useRoute()
const router = useRouter()
const auth = useAuthStore()
const sectionOptions = [
  { label: '运行设置', value: 'runtime' },
  { label: '上游设置', value: 'upstream' },
  { label: '接入设置', value: 'access' },
  { label: '模型定价', value: 'pricing' },
  { label: '备份', value: 'backup' },
]
const section = computed(() => sectionOptions.find(item => route.name === `settings-${item.value}`)?.value ?? 'runtime')
const visited = reactive(new Set<string>())
const settingsStarted = ref(false)
const basicSection = computed(() => ['runtime', 'upstream', 'access'].includes(section.value))

function switchSection(value: string): void {
  void router.push(value === 'runtime' ? '/settings' : `/settings/${value}`)
}

const {
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
} = useSettingsForm()

const {
  loading: adminKeyLoading,
  regenerating: adminKeyRegenerating,
  deleting: adminKeyDeleting,
  showDeleteModal: showDeleteAdminKeyModal,
  generatedKey: generatedAdminApiKey,
  status: adminApiKeyStatus,
  regenerate: handleRegenerateAdminApiKey,
  remove: handleDeleteAdminApiKey,
  copyGeneratedKey: copyAdminApiKey,
  loadStatus: loadAdminApiKeyStatus,
} = useAdminApiKey()

const backupSettings = useTemplateRef<InstanceType<typeof SettingsBackupSection>>('backupSettings')
const hasUnsavedChanges = computed(() => dirty.value || !!backupSettings.value?.dirty)
const confirmLeave = ref(false)
let pendingLeave: ((allowed: boolean) => void) | undefined

function finishLeave(allowed: boolean) {
  const resolve = pendingLeave
  pendingLeave = undefined
  if (allowed)
    resetSettings()
  confirmLeave.value = false
  resolve?.(allowed)
}

onBeforeRouteLeave((to) => {
  if (!auth.isAuthenticated || !hasUnsavedChanges.value || to.path === '/settings' || to.path.startsWith('/settings/'))
    return true
  pendingLeave?.(false)
  confirmLeave.value = true
  return new Promise<boolean>((resolve) => {
    pendingLeave = resolve
  })
})
watch(confirmLeave, (open) => {
  if (!open)
    finishLeave(false)
})
onScopeDispose(() => pendingLeave?.(false))
useEventListener(window, 'beforeunload', (event) => {
  if (hasUnsavedChanges.value)
    event.preventDefault()
})

watch(section, (value) => {
  visited.add(value)
  if (basicSection.value && !settingsStarted.value) {
    settingsStarted.value = true
    void loadSettings()
  }
  if (value === 'access')
    void loadAdminApiKeyStatus()
}, { immediate: true })
watch([() => route.hash, loading, section], async ([hash, isLoading, currentSection]) => {
  if (hash === '#reset-credits' && !isLoading && currentSection === 'runtime') {
    await nextTick()
    document.getElementById('reset-credits')?.scrollIntoView({ behavior: 'smooth', block: 'center' })
  }
}, { immediate: true })
</script>

<template>
  <div class="w-full" :class="section === 'pricing' ? 'flex h-[calc(100dvh-2rem)] flex-none! flex-col min-[961px]:h-[calc(100dvh-3rem)]' : undefined">
    <BasePageHeader title="系统设置" description="管理运行参数、模型定价、管理员凭据与备份配置" />

    <div class="mt-4 flex min-h-cp-control shrink-0 flex-wrap items-center justify-between gap-3">
      <BaseSegmented
        :model-value="section"
        label="设置分区"
        class="w-full sm:w-auto [&>button]:px-1.5 sm:[&>button]:px-3"
        :options="sectionOptions"
        @update:model-value="switchSection"
      />
      <div v-if="basicSection || hasUnsavedChanges" class="ml-auto flex shrink-0 items-center gap-2">
        <span v-if="hasUnsavedChanges" class="text-cp-xs text-cp-text-secondary" role="status">有未保存更改</span>
        <BaseIconButton v-if="dirty" label="撤销设置更改" :disabled="saving" @click="resetSettings">
          <Undo2 class="size-4" />
        </BaseIconButton>
        <BaseButton v-if="basicSection || dirty" variant="primary" :loading="saving" :disabled="loading || !dirty" @click="saveSettings">
          <template #icon>
            <Save class="size-4" />
          </template>
          保存更改
        </BaseButton>
      </div>
    </div>

    <div v-if="basicSection && error" class="mt-4 flex flex-wrap items-center gap-2 rounded-cp bg-cp-error-container px-3 py-2 text-cp-sm text-cp-error-on-container" role="alert">
      {{ error }}
      <BaseButton v-if="!dirty" size="sm" variant="ghost" @click="loadSettings">
        重新加载
      </BaseButton>
    </div>

    <div v-if="section === 'runtime'" class="mt-5 grid min-w-0 gap-5">
      <RotationStrategyCard v-model="form.rotationStrategy" :options="rotationOptions" :disabled="loading || saving" />
      <RuntimeSettingsCard
        v-model:subscription-auto-reset-enabled="form.subscriptionAutoResetEnabled"
        v-model:max-retry-after-seconds="maxRetryAfterSecondsValue"
        v-model:max-concurrent-per-account="maxConcurrentPerAccountValue"
        v-model:request-interval-ms="requestIntervalMsValue"
        :disabled="loading || saving"
      />

      <ResetCreditCard v-model="resetCreditLeadMinutesValue" v-model:max-weekly-remaining-percent="resetCreditMaxWeeklyRemainingPercentValue" :disabled="loading || saving" />

      <BaseCard title="历史保留">
        <div class="grid min-w-0 gap-4 sm:grid-cols-3">
          <BaseFormItem label="请求记录（天）">
            <BaseInput v-model="usageRetentionDaysValue" type="number" min="31" aria-label="请求记录保留天数" :disabled="loading || saving" />
          </BaseFormItem>
          <BaseFormItem label="运维事件（天）">
            <BaseInput v-model="opsEventRetentionDaysValue" type="number" min="1" aria-label="运维事件保留天数" :disabled="loading || saving" />
          </BaseFormItem>
          <BaseFormItem label="操作日志（天）">
            <BaseInput v-model="auditRetentionDaysValue" type="number" min="1" aria-label="操作日志保留天数" :disabled="loading || saving" />
          </BaseFormItem>
        </div>
      </BaseCard>
    </div>

    <div v-else-if="section === 'upstream'" class="mt-5 grid min-w-0 gap-5">
      <BaseCard title="凭据刷新">
        <div class="grid min-w-0 gap-4 sm:grid-cols-2">
          <BaseFormItem label="提前刷新（秒）" description="Token 到期前开始刷新">
            <BaseInput v-model="refreshMarginSecondsValue" type="number" aria-label="提前刷新秒数" :disabled="loading || saving" />
          </BaseFormItem>
          <BaseFormItem label="刷新并发" description="同时刷新凭据的最大请求数">
            <BaseInput v-model="refreshConcurrencyValue" type="number" aria-label="刷新并发数" :disabled="loading || saving" />
          </BaseFormItem>
        </div>
      </BaseCard>
      <ModelAliasesCard
        :mappings="mappings"
        :loading="loading"
        :disabled="saving"
        @add-mapping="addMapping"
        @update-mapping="updateMapping"
        @remove-mapping="removeMapping"
      />
    </div>

    <div v-else-if="section === 'access'" class="mt-5 grid min-w-0 gap-5">
      <ClientVersionSettings
        v-model:min-codex-desktop-version="form.minCodexDesktopVersion"
        v-model:min-codex-cli-version="form.minCodexCliVersion"
        :loading="loading || saving"
        :desktop-error="minCodexDesktopVersionError"
        :cli-error="minCodexCliVersionError"
      />

      <AdminApiKeyCard
        :status="adminApiKeyStatus"
        :loading="adminKeyLoading"
        :regenerating="adminKeyRegenerating"
        :deleting="adminKeyDeleting"
        :generated-key="generatedAdminApiKey"
        @regenerate="handleRegenerateAdminApiKey"
        @request-delete="showDeleteAdminKeyModal = true"
        @copy="copyAdminApiKey"
      />

      <BaseConfirmModal
        v-model="showDeleteAdminKeyModal"
        title="删除管理员 API Key"
        description="删除后外部系统将无法继续使用该 Key 调用管理接口"
        destructive
        confirm-text="确认删除"
        :loading="adminKeyDeleting"
        @confirm="handleDeleteAdminApiKey"
      >
        <p class="m-0">
          确定要删除当前管理员 API Key 吗？此操作会立即生效
        </p>
      </BaseConfirmModal>
    </div>

    <SettingsBackupSection v-if="visited.has('backup')" v-show="section === 'backup'" ref="backupSettings" :active="section === 'backup'" class="mt-5" />
    <PricingSection v-if="visited.has('pricing')" v-show="section === 'pricing'" class="mt-5 min-h-0 flex-1" />
    <BaseConfirmModal v-model="confirmLeave" title="有未保存的设置" description="离开此页会放弃这些更改。" confirm-text="放弃并离开" cancel-text="继续编辑" @confirm="finishLeave(true)" />
  </div>
</template>
