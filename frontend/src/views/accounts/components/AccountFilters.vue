<script setup lang="ts">
import type { AccountGroupRef } from '@/api'
import { Search } from '@lucide/vue'
import { computed } from 'vue'

import BaseInput from '@/components/base/BaseInput.vue'
import BaseSelect from '@/components/base/BaseSelect.vue'
import ProviderFilterSegmented from '@/components/ProviderFilterSegmented.vue'
import { accountStatusFilterOptions } from '../constants'

const props = defineProps<{
  groups: AccountGroupRef[]
  groupsLoading: boolean
}>()

const search = defineModel<string>('search', { required: true })
const status = defineModel<string>('status', { required: true })
const provider = defineModel<string>('provider', { required: true })
const group = defineModel<string>('group', { required: true })
const groupOptions = computed(() => [
  { label: '全部分组', value: '' },
  { label: '未分组账号', value: 'ungrouped' },
  ...props.groups.map(group => ({
    label: group.enabled ? group.name : `${group.name}（已禁用）`,
    value: group.id,
  })),
])
</script>

<template>
  <div
    class="flex w-full flex-col gap-3 xl:flex-row xl:flex-wrap xl:items-center"
    role="group"
    aria-label="账号筛选"
  >
    <div
      class="grid w-full min-w-0 grid-cols-2 items-center gap-2 sm:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_7.75rem] xl:flex xl:w-full xl:flex-wrap xl:gap-3"
    >
      <BaseInput
        v-model="search"
        placeholder="搜索账号"
        class="col-span-2 min-w-0 sm:col-span-3 xl:w-80 xl:flex-none"
      >
        <template #prefix>
          <Search class="size-4.5 text-cp-text-tertiary" />
        </template>
      </BaseInput>

      <BaseSelect
        v-model="status"
        :options="accountStatusFilterOptions"
        aria-label="按账号状态筛选"
        class="w-full min-w-0 xl:w-40 xl:shrink-0"
      />

      <BaseSelect
        v-model="group"
        :options="groupOptions"
        :disabled="groupsLoading"
        aria-label="按账号分组筛选"
        class="w-full min-w-0 xl:w-40 xl:shrink-0"
      />

      <ProviderFilterSegmented
        v-model="provider"
        class="col-span-2 w-full sm:col-span-1 xl:w-31 xl:shrink-0"
      />
    </div>
  </div>
</template>
