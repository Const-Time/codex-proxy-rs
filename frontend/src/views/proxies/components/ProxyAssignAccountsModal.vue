<script setup lang="ts">
import type { Account, OutboundProxyRecord } from '@/api'
import { Search } from '@lucide/vue'
import { watchDebounced } from '@vueuse/core'
import { computed, shallowRef, watch } from 'vue'
import { batchUpdateAccounts, getAccounts } from '@/api'
import AccountGroupMarks from '@/components/AccountGroupMarks.vue'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseCheckbox from '@/components/base/BaseCheckbox.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import BaseTablePagination from '@/components/base/BaseTable/BaseTablePagination.vue'
import { defineTableColumns } from '@/components/base/BaseTable/columns'
import BaseTable from '@/components/base/BaseTable/index.vue'
import { toast } from '@/components/base/BaseToast'
import ProviderIconGroup from '@/components/ProviderIconGroup.vue'
import { useAsyncAction } from '@/composables/useAsyncAction'
import { usePagedQuery } from '@/composables/usePagedQuery'
import AccountPlanBadge from '@/views/accounts/components/AccountPlanBadge.vue'

const props = defineProps<{ proxy: OutboundProxyRecord | null }>()
const emit = defineEmits<{ assigned: [] }>()
const open = defineModel<boolean>({ required: true })
const search = shallowRef('')
const selected = shallowRef(new Set<string>())
const limit = 1000
const action = useAsyncAction()
const saving = action.loading
const query = usePagedQuery({
  initialPageSize: 20,
  load: page => getAccounts({ ...page, search: search.value.trim() || undefined }),
})
const { items, loading, error, page, pageSize, total } = query
const pagination = computed(() => ({ currentPage: page.value, pageSize: pageSize.value, total: total.value }))
const columns = defineTableColumns<Account>([
  { key: 'selection', kind: 'selection' },
  { key: 'name', label: '账号', kind: 'identity', size: '2xl' },
  { key: 'provider', label: '平台', kind: 'custom', size: 'sm' },
  { key: 'planType', label: '套餐', kind: 'custom', size: 'sm' },
  { key: 'groups', label: '分组', kind: 'custom', size: 'lg' },
  { key: 'outboundProxyEndpoint', label: '当前出口', kind: 'mono', size: 'xl', emptyText: '直连' },
])

function alreadyAssigned(account: Account) {
  return account.outboundProxyId === props.proxy?.id
}

function toggle(id: string, checked: boolean) {
  const next = new Set(selected.value)
  if (!checked)
    next.delete(id)
  else if (next.size < limit)
    next.add(id)
  selected.value = next
}

function setPage(nextPage: number) {
  page.value = nextPage
  void query.execute()
}

function setPageSize(size: number) {
  pageSize.value = size
  setPage(1)
}

async function assign() {
  const proxy = props.proxy
  if (!proxy || !selected.value.size)
    return
  const ids = [...selected.value]
  await action.run(async () => {
    const result = await batchUpdateAccounts({ accountIds: ids, outboundProxyId: proxy.id })
    toast.success(`已分配 ${result.accountIds.length} 个账号`)
    emit('assigned')
    if (props.proxy?.id === proxy.id)
      open.value = false
  }, { errorText: '分配账号失败' })
}

watch([open, () => props.proxy?.id], ([visible]) => {
  query.invalidate()
  selected.value = new Set()
  search.value = ''
  if (visible)
    void query.reloadFromStart()
}, { immediate: true })
watchDebounced(search, () => {
  if (open.value)
    void query.reloadFromStart()
}, { debounce: 250 })
</script>

<template>
  <BaseModal v-model="open" title="分配账号" :description="proxy?.name" size="lg" :dismissible="!saving">
    <div class="grid min-w-0 gap-3">
      <div class="flex flex-wrap items-center gap-2">
        <BaseInput v-model="search" clearable class="min-w-40 flex-1" :disabled="saving" aria-label="搜索要分配的账号" placeholder="账号名称或邮箱">
          <template #prefix>
            <Search class="size-4" aria-hidden="true" />
          </template>
        </BaseInput>
        <BaseButton :disabled="!selected.size || saving" @click="selected = new Set()">
          重置
        </BaseButton>
        <BaseButton variant="primary" :disabled="!selected.size" :loading="saving" @click="assign">
          {{ selected.size ? `分配 ${selected.size} 个账号` : '分配账号' }}
        </BaseButton>
      </div>
      <BaseTable class="min-h-64 sm:h-[min(52dvh,28rem)]" density="compact" :columns="columns" :rows="items" :loading="loading" :empty-text="error || '没有匹配的账号'">
        <template #selection="{ row }">
          <BaseCheckbox :label="`选择 ${row.name}`" :model-value="alreadyAssigned(row) || selected.has(row.id)" :disabled="saving || alreadyAssigned(row) || (selected.size >= limit && !selected.has(row.id))" @update:model-value="value => toggle(row.id, value)" />
        </template>
        <template #name="{ row }">
          <div class="grid min-w-0 gap-0.5">
            <span class="truncate text-cp-sm font-bold" :title="row.name">{{ row.name }}</span>
            <span v-if="row.email && row.email !== row.name" class="truncate text-cp-xs text-cp-text-secondary" :title="row.email">{{ row.email }}</span>
          </div>
        </template>
        <template #provider="{ row }">
          <ProviderIconGroup :provider="row.provider" :authentication-kind="row.authenticationKind" />
        </template>
        <template #planType="{ row }">
          <AccountPlanBadge :plan-type="row.planType" :plan-type-display="row.planTypeDisplay" :authentication-kind="row.authenticationKind" size="sm" />
        </template>
        <template #groups="{ row }">
          <AccountGroupMarks :groups="row.groups" />
        </template>
      </BaseTable>
      <BaseTablePagination :pagination="pagination" :loading="loading || saving" @page-change="setPage" @page-size-change="setPageSize" />
    </div>
  </BaseModal>
</template>
