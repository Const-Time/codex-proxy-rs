<script setup lang="ts">
import type { AccountGroupRef, AccountModelAccess } from '@/api'

import { computed } from 'vue'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseCheckbox from '@/components/base/BaseCheckbox.vue'
import BaseFormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import BaseSegmented from '@/components/base/BaseSegmented.vue'
import AccountSettingsFields from './AccountSettingsFields.vue'

defineProps<{
  selectedCount: number
  catalogAccountId?: string
  groups: AccountGroupRef[]
  groupsLoading: boolean
  saving: boolean
}>()

const emit = defineEmits<{
  save: []
}>()

const open = defineModel<boolean>({ required: true })
const enabled = defineModel<boolean | undefined>('enabled', { required: true })
const concurrencyLimit = defineModel<string>('concurrencyLimit', { required: true })
const modelAccess = defineModel<AccountModelAccess | undefined>('modelAccess', { required: true })
const updateScheduling = defineModel<boolean>('updateScheduling', { required: true })
const weight = defineModel<string>('weight', { required: true })
const proxyMode = defineModel<string>('proxyMode', { required: true })
const proxyId = defineModel<string>('proxyId', { required: true })
const selectedGroupIds = defineModel<string[]>('selectedGroupIds', { required: true })
const schedulingModes = [
  { value: 'preserve', label: '保持不变' },
  { value: 'enabled', label: '启用调度' },
  { value: 'disabled', label: '停止调度' },
]
const schedulingMode = computed({
  get: () => enabled.value === undefined ? 'preserve' : enabled.value ? 'enabled' : 'disabled',
  set: (value: string) => { enabled.value = value === 'preserve' ? undefined : value === 'enabled' },
})
</script>

<template>
  <BaseModal
    v-model="open"
    title="批量编辑账号"
    :description="`编辑 ${selectedCount} 个账号`"
    size="lg"
    :dismissible="!saving"
  >
    <div class="grid gap-5">
      <BaseCheckbox v-model="updateScheduling" label="同时更新调度、分组和代理设置" show-label :disabled="saving" />
      <AccountSettingsFields
        v-model:concurrency-limit="concurrencyLimit"
        v-model:weight="weight"
        v-model:model-access="modelAccess"
        v-model:selected-group-ids="selectedGroupIds"
        v-model:proxy-mode="proxyMode"
        v-model:proxy-id="proxyId"
        preserve-model-access
        :model-only="!updateScheduling"
        :account-id="catalogAccountId"
        :groups="groups"
        :groups-loading="groupsLoading"
        :disabled="saving"
      >
        <template #scheduling>
          <BaseFormItem label="调度">
            <BaseSegmented v-model="schedulingMode" label="批量调度状态" :options="schedulingModes" :disabled="saving" />
          </BaseFormItem>
        </template>
      </AccountSettingsFields>
    </div>

    <template #footer>
      <BaseButton variant="secondary" :disabled="saving" @click="open = false">
        取消
      </BaseButton>
      <BaseButton
        variant="primary"
        :loading="saving"
        :disabled="selectedCount === 0 || (updateScheduling && groupsLoading) || (!updateScheduling && !modelAccess)"
        @click="emit('save')"
      >
        保存更改
      </BaseButton>
    </template>
  </BaseModal>
</template>
