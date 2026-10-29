<script setup lang="ts">
import { Save } from '@lucide/vue'
import { computed, nextTick, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'

import BaseButton from '@/components/base/BaseButton.vue'
import BaseConfirmModal from '@/components/base/BaseConfirmModal.vue'
import BasePageHeader from '@/components/base/BasePageHeader.vue'
import BaseSegmented from '@/components/base/BaseSegmented.vue'

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
const pricingVisited = ref(false)
const section = computed(() => route.name === 'settings-pricing' ? 'pricing' : route.name === 'settings-backup' ? 'backup' : 'runtime')

function switchSection(value: string): void {
  void router.push(value === 'runtime' ? '/settings' : `/settings/${value}`)
}

const {
  loading,
  saving,
  error,
  form,
  mappings,
  addMapping,
  updateMapping,
  removeMapping,
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

// 只在切到对应分区时才加载各自接口，避免打开备份页也请求运行设置数据。
watch(
  section,
  (value) => {
    if (value === 'pricing')
      pricingVisited.value = true
    if (value === 'runtime') {
      void loadSettings()
      void loadAdminApiKeyStatus()
    }
  },
  { immediate: true },
)
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
        class="bg-(--cp-input-bg)!"
        :options="[
          { label: '运行设置', value: 'runtime' },
          { label: '模型定价', value: 'pricing' },
          { label: '备份', value: 'backup' },
        ]"
        @update:model-value="switchSection"
      />
      <BaseButton
        v-if="section === 'runtime'"
        variant="primary"
        :loading="saving"
        :disabled="loading"
        @click="saveSettings"
      >
        <template #icon>
          <Save class="size-4" />
        </template>
        {{ saving ? '保存中...' : '保存' }}
      </BaseButton>
    </div>

    <div v-if="section === 'runtime'" class="mt-5 grid w-full gap-5">
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

      <RuntimeSettingsCard
        v-model:subscription-auto-reset-enabled="form.subscriptionAutoResetEnabled"
        v-model:max-concurrent-per-account="maxConcurrentPerAccountValue"
        v-model:refresh-margin-seconds="refreshMarginSecondsValue"
        v-model:refresh-concurrency="refreshConcurrencyValue"
        v-model:request-interval-ms="requestIntervalMsValue"
        :disabled="loading || saving"
      />

      <ResetCreditCard v-model="resetCreditLeadMinutesValue" v-model:max-weekly-remaining-percent="resetCreditMaxWeeklyRemainingPercentValue" :disabled="loading || saving" />

      <ClientVersionSettings
        v-model:min-codex-desktop-version="form.minCodexDesktopVersion"
        v-model:min-codex-cli-version="form.minCodexCliVersion"
        :loading="loading"
        :desktop-error="minCodexDesktopVersionError"
        :cli-error="minCodexCliVersionError"
      />

      <ModelAliasesCard
        :mappings="mappings"
        :loading="loading"
        :error="error"
        @add-mapping="addMapping"
        @update-mapping="updateMapping"
        @remove-mapping="removeMapping"
      />

      <RotationStrategyCard v-model="form.rotationStrategy" :options="rotationOptions" />

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

    <div v-else-if="section === 'backup'" class="mt-5">
      <SettingsBackupSection :active="section === 'backup'" />
    </div>
    <PricingSection v-if="pricingVisited" v-show="section === 'pricing'" class="mt-5 min-h-0 flex-1" />
  </div>
</template>
