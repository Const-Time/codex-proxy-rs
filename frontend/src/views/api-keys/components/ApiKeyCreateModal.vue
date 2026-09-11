<script setup lang="ts">
import type { ApiKeyFormValue } from '../composables/useApiKeyMutations'
import type { UserGroup } from '@/api/modules/users'
import { Copy, KeyRound, Upload } from '@lucide/vue'
import { computed } from 'vue'

import BaseButton from '@/components/base/BaseButton.vue'
import BaseFormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseForm from '@/components/base/BaseForm/index.vue'
import BaseIconButton from '@/components/base/BaseIconButton.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import { useAuthStore } from '@/stores/modules/auth'

const props = defineProps<{
  groups: UserGroup[]
  groupLoading: boolean
  editing: boolean
  createdKey: string
  saving: boolean
}>()

const emit = defineEmits<{
  save: []
  copy: [text: string]
  importCcs: []
}>()

const authStore = useAuthStore()

const open = defineModel<boolean>({ default: false })
const createdOpen = defineModel<boolean>('createdOpen', { default: false })
const form = defineModel<ApiKeyFormValue>('form', { required: true })
const title = computed(() => props.editing ? '编辑 API Key' : '创建 API Key')
const selectedGroupId = computed({ get: () => form.value.groupIds[0] ?? '', set: (id: string) => {
  form.value.groupIds = id ? [id] : []
} })
const selectedGroup = computed(() => props.groups.find(group => group.id === selectedGroupId.value))
</script>

<template>
  <BaseModal
    v-model="open"
    :title="title"
    tone="info"
    size="md"
    :dismissible="!saving"
  >
    <template #icon>
      <KeyRound class="text-cp-text" :size="20" aria-hidden="true" />
    </template>

    <BaseForm class="grid gap-6">
      <BaseFormItem label="名称" required>
        <BaseInput
          v-model="form.name"
          aria-label="名称"
          placeholder="例如：生产环境"
          :disabled="saving"
        />
      </BaseFormItem>

      <BaseFormItem label="标签（可选）">
        <BaseInput
          v-model="form.label"
          aria-label="标签（可选）"
          placeholder="例如：后端服务"
          :disabled="saving"
        />
      </BaseFormItem>

      <BaseFormItem label="分组" required>
        <select v-model="selectedGroupId" aria-label="分组" :disabled="saving || groupLoading" class="w-full rounded-cp border border-cp-border bg-cp-bg px-3 py-2 text-cp-text">
          <option value="">
            请选择一个授权分组
          </option>
          <option v-for="group in groups" :key="group.id" :value="group.id" :disabled="!group.enabled">
            {{ group.name }}{{ group.enabled ? '' : '（已禁用）' }}
          </option>
        </select>
        <p v-if="!groupLoading && groups.length === 0" class="text-cp-sm text-cp-text-secondary">
          暂无可用分组，请联系管理员分配。
        </p>
        <p v-if="selectedGroup" class="text-cp-sm text-cp-text-secondary">
          日限额：{{ selectedGroup.dailyLimitUsd }} USD，周限额：{{ selectedGroup.weeklyLimitUsd }} USD（0 表示不限）。同一分组内的所有个人密钥共用额度。
        </p>
      </BaseFormItem>
      <div v-if="authStore.isAdmin" class="grid gap-6 sm:grid-cols-2">
        <BaseFormItem label="最大并发">
          <BaseInput
            v-model="form.maxConcurrency"
            type="number"
            aria-label="最大并发"
            min="0"
            step="1"
            placeholder="不限制"
            :disabled="saving"
          />
        </BaseFormItem>
        <BaseFormItem label="每分钟请求数（RPM）">
          <BaseInput
            v-model="form.requestsPerMinute"
            type="number"
            aria-label="每分钟请求数（RPM）"
            min="0"
            step="1"
            placeholder="不限制"
            :disabled="saving"
          />
        </BaseFormItem>
      </div>
      <p v-if="!authStore.isAdmin" class="text-cp-sm text-cp-text-secondary">
        并发和 RPM 由管理员统一设置，同一用户的所有密钥共用限制。
      </p>
    </BaseForm>

    <template #footer>
      <BaseButton variant="secondary" :disabled="saving" @click="open = false">
        取消
      </BaseButton>
      <BaseButton
        variant="primary"
        :loading="saving"
        :disabled="!form.name.trim()"
        @click="emit('save')"
      >
        {{ editing ? '保存更改' : '创建' }}
      </BaseButton>
    </template>
  </BaseModal>

  <BaseModal
    v-model="createdOpen"
    title="API Key 已创建"
    description="复制密钥，或直接导入 CCSwitch"
    tone="success"
    size="md"
  >
    <div class="flex flex-col gap-4">
      <div class="rounded-cp border border-cp-warning-border bg-cp-warning-container px-4 py-3">
        <p class="m-0 text-cp font-semibold text-cp-warning-on-container">
          该密钥具有网关访问权限，请仅发送给可信调用方
        </p>
      </div>
      <div>
        <p class="mb-2 text-cp font-medium text-cp-text-secondary">
          API Key
        </p>
        <div class="flex items-center gap-2">
          <code class="flex-1 rounded-cp bg-cp-fill-quaternary px-3 py-2.5 font-mono text-cp break-all text-cp-text">
            {{ createdKey }}
          </code>
          <BaseIconButton size="md" label="复制" @click="emit('copy', createdKey)">
            <Copy class="size-4" />
          </BaseIconButton>
        </div>
      </div>
    </div>

    <template #footer>
      <BaseButton variant="secondary" @click="emit('copy', createdKey)">
        <template #icon>
          <Copy class="size-4" />
        </template>
        复制密钥
      </BaseButton>
      <BaseButton variant="secondary" @click="emit('importCcs')">
        <template #icon>
          <Upload class="size-4" />
        </template>
        导入 CCSwitch
      </BaseButton>
      <BaseButton variant="primary" @click="createdOpen = false">
        我已保存
      </BaseButton>
    </template>
  </BaseModal>
</template>
