<script setup lang="ts">
import type { AntigravityProfile, AntigravityProfileConfiguration } from '@/api'
import { computed, onMounted, ref, shallowRef } from 'vue'

import { getAntigravityProfile, getAntigravityProfileOptions, updateAntigravityProfile } from '@/api'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseCard from '@/components/base/BaseCard.vue'
import BaseFormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseSelect from '@/components/base/BaseSelect.vue'
import { errorMessage } from '@/utils/async'

const platformNames = { macos: 'macOS', linux: 'Linux', windows: 'Windows' }
const archOptions = [
  { label: '平台默认', value: '' },
  { label: 'ARM64', value: 'arm64' },
  { label: 'AMD64 / x64', value: 'amd64' },
]
const versionOptions = [
  { label: '跟随官方版本', value: 'latest' },
  { label: '固定版本', value: 'fixed' },
]

const saved = shallowRef<AntigravityProfile>()
const draft = ref<AntigravityProfileConfiguration>()
const preview = shallowRef<AntigravityProfile>()
const options = shallowRef<Awaited<ReturnType<typeof getAntigravityProfileOptions>>>()
const pending = ref<'load' | 'preview' | 'save'>()
const error = ref('')
const savedFeedback = ref(false)

function targetKey(config: AntigravityProfileConfiguration) {
  return `${config.client}-${config.platform}`
}

const targets = computed(() => options.value?.presets.map(({ configuration }) => ({
  label: `${platformNames[configuration.platform]} · ${configuration.client === 'desktop' ? 'Desktop' : 'CLI'}`,
  value: targetKey(configuration),
})) ?? [])
const dirty = computed(() => JSON.stringify(draft.value) !== JSON.stringify(saved.value?.configuration))
const result = computed(() => {
  if (JSON.stringify(draft.value) === JSON.stringify(preview.value?.configuration))
    return preview.value
  return dirty.value ? undefined : saved.value
})
const versionError = computed(() => draft.value?.versionMode === 'fixed'
  && !/^\d{1,9}\.\d{1,9}\.\d{1,9}$/.test(draft.value.clientVersion?.trim() ?? ''))

function changeTarget(value: string) {
  const target = options.value?.presets.find(({ configuration }) => targetKey(configuration) === value)?.configuration
  if (draft.value && target) {
    Object.assign(draft.value, { client: target.client, platform: target.platform, arch: target.arch })
    savedFeedback.value = false
  }
}

function changeArch(value: string) {
  if (draft.value && (value === 'arm64' || value === 'amd64' || value === ''))
    draft.value.arch = value || null
}

function changeVersionMode(value: string) {
  if (draft.value && (value === 'latest' || value === 'fixed')) {
    draft.value.versionMode = value
    if (value === 'latest')
      draft.value.clientVersion = null
  }
}

function reset() {
  draft.value = saved.value && { ...saved.value.configuration }
  preview.value = undefined
  error.value = ''
  savedFeedback.value = false
}

async function load() {
  if (pending.value)
    return
  pending.value = 'load'
  error.value = ''
  try {
    const [profile, profileOptions] = await Promise.all([getAntigravityProfile(), getAntigravityProfileOptions()])
    saved.value = profile
    options.value = profileOptions
    reset()
  }
  catch (cause) {
    error.value = errorMessage(cause, 'Antigravity 配置加载失败')
  }
  finally {
    pending.value = undefined
  }
}

async function submit(action: 'preview' | 'save') {
  if (!draft.value || pending.value || versionError.value)
    return
  pending.value = action
  error.value = ''
  savedFeedback.value = false
  try {
    const configuration = { ...draft.value, clientVersion: draft.value.clientVersion?.trim() || null }
    const profile = await updateAntigravityProfile(configuration, action === 'preview')
    if (action === 'preview') {
      draft.value = { ...profile.configuration }
      preview.value = profile
    }
    else {
      saved.value = profile
      reset()
      savedFeedback.value = true
    }
  }
  catch (cause) {
    error.value = errorMessage(cause, action === 'preview' ? '预览失败' : '保存失败')
  }
  finally {
    pending.value = undefined
  }
}

onMounted(load)
defineExpose({ dirty, reset })
</script>

<template>
  <BaseCard title="Antigravity 客户端配置" description="选择网关请求使用的客户端、系统和版本">
    <div v-if="error" class="mb-4 text-cp-sm text-cp-error-on-container" role="alert">
      {{ error }}
      <BaseButton v-if="!draft" size="sm" variant="ghost" :disabled="!!pending" @click="load">
        重新加载
      </BaseButton>
    </div>
    <p v-if="pending === 'load'" class="text-cp-sm text-cp-text-secondary" role="status">
      加载中…
    </p>
    <template v-if="draft">
      <div class="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <BaseFormItem label="客户端">
          <BaseSelect :model-value="targetKey(draft)" :options="targets" :disabled="!!pending" @update:model-value="changeTarget" />
        </BaseFormItem>
        <BaseFormItem label="架构">
          <BaseSelect :model-value="draft.arch ?? ''" :options="archOptions" :disabled="!!pending" @update:model-value="changeArch" />
        </BaseFormItem>
        <BaseFormItem label="版本策略">
          <BaseSelect :model-value="draft.versionMode" :options="versionOptions" :disabled="!!pending" @update:model-value="changeVersionMode" />
        </BaseFormItem>
        <BaseFormItem v-if="draft.versionMode === 'fixed'" label="固定版本">
          <BaseInput
            :model-value="draft.clientVersion ?? ''"
            :disabled="!!pending"
            placeholder="1.2.3"
            :aria-invalid="versionError"
            @update:model-value="draft.clientVersion = String($event)"
          />
        </BaseFormItem>
      </div>
      <div v-if="result" class="mt-4 grid gap-2 rounded-cp bg-cp-surface-container px-4 py-3 text-cp-sm">
        <p class="m-0">
          <span class="mr-2 text-cp-text-secondary">User-Agent</span>
          <code class="break-all">{{ result.userAgent }}</code>
        </p>
        <a v-if="result.release" class="w-fit font-medium text-cp-primary hover:underline" :href="result.release.downloadUrl" target="_blank" rel="noopener noreferrer">
          下载官方安装包 {{ result.release.version }} ↗
        </a>
        <p v-else class="m-0 text-cp-text-secondary">
          当前选项暂无官方安装包链接
        </p>
      </div>
      <div class="mt-4 flex flex-wrap items-center justify-end gap-2">
        <span v-if="savedFeedback && !dirty" class="text-cp-sm text-cp-text-secondary" role="status">已保存</span>
        <BaseButton variant="ghost" :disabled="!dirty || !!pending" @click="reset">
          撤销
        </BaseButton>
        <BaseButton :loading="pending === 'preview'" :disabled="!!pending || versionError" @click="submit('preview')">
          预览
        </BaseButton>
        <BaseButton variant="primary" :loading="pending === 'save'" :disabled="!dirty || !!pending || versionError" @click="submit('save')">
          保存配置
        </BaseButton>
      </div>
    </template>
  </BaseCard>
</template>
