<script setup lang="ts">
import type { ApiKey } from '@/api'

import type { UserGroup } from '@/api/modules/users'
import { computed, ref } from 'vue'
import { updateApiKey } from '@/api'
import BaseSelect from '@/components/base/BaseSelect.vue'
import { toast } from '@/components/base/BaseToast'
import { useAuthStore } from '@/stores/modules/auth'
import { errorMessage } from '@/utils/async'

const props = defineProps<{
  apiKey: ApiKey
  groups: UserGroup[]
  loading: boolean
}>()
const emit = defineEmits<{ updated: [] }>()
const auth = useAuthStore()
const saving = ref(false)
const current = computed(() => props.apiKey.groups[0])
const options = computed(() => {
  const rows = props.groups.map(group => ({ value: group.id, label: group.name, disabled: !group.enabled }))
  if (current.value && !rows.some(row => row.value === current.value?.id))
    rows.unshift({ value: current.value.id, label: `${current.value.name}（未授权）`, disabled: true })
  return rows
})
async function changeGroup(id: string) {
  if (saving.value || id === current.value?.id || !props.groups.some(group => group.id === id && group.enabled))
    return
  saving.value = true
  try {
    await updateApiKey({ id: props.apiKey.id, name: props.apiKey.name, label: props.apiKey.label, groupIds: [id], maxConcurrency: auth.isAdmin ? props.apiKey.maxConcurrency : 0, requestsPerMinute: auth.isAdmin ? props.apiKey.requestsPerMinute : 0 })
    toast.success('密钥分组已更新')
    emit('updated')
  }
  catch (cause) { toast.error(errorMessage(cause, '分组切换失败')) }
  finally { saving.value = false }
}
</script>

<template>
  <div class="flex min-w-36 items-center gap-2">
    <span class="size-3 shrink-0 rounded" :style="{ backgroundColor: current?.color || 'var(--cp-text-tertiary)' }" aria-hidden="true" />
    <BaseSelect :model-value="current?.id ?? ''" :options="options" :aria-label="`${apiKey.name}的分组`" placeholder="未绑定分组" size="sm" class="min-w-0 flex-1" :disabled="loading || saving" @update:model-value="changeGroup" />
  </div>
</template>
