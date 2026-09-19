<script setup lang="ts">
import type { UsageDiagnosticItem } from '@/api/modules/usage'
import type { User } from '@/api/modules/users'
import { ArrowDown } from '@lucide/vue'
import { computed, onMounted, onScopeDispose, ref, watch } from 'vue'
import { getUsageRecordInsightsDiagnostics } from '@/api/modules/usage'
import { getUsers } from '@/api/modules/users'
import { defineTableColumns } from '@/components/base/BaseTable/columns'
import BaseTable from '@/components/base/BaseTable/index.vue'
import { errorMessage } from '@/utils/async'
import { formatCompactNumber } from '@/utils/number'
import { UNOWNED_USAGE_USER_LABEL } from '../utils/user'

const props = defineProps<{ range: { startTime: string, endTime: string }, provider?: string, filters?: Record<string, string | undefined> }>()
const items = ref<UsageDiagnosticItem[]>([])
const users = ref<User[]>([])
const loading = ref(false)
const error = ref('')
const columns = defineTableColumns<UsageDiagnosticItem>([
  { key: 'rank', label: '排名', kind: 'index', size: 'xs' },
  { key: 'name', label: '用户 / 邮箱', kind: 'custom', size: 'xl' },
  { key: 'requestCount', label: '请求数', kind: 'numeric', size: 'sm' },
  { key: 'totalTokens', label: 'Token', kind: 'numeric', size: 'sm' },
  { key: 'estimatedCost', label: '消费', kind: 'numeric', size: 'sm' },
])
const userIndex = computed(() => new Map(users.value.map(user => [user.id, user])))
const user = (id: string) => userIndex.value.get(id)
// unknown 是聚合桶，可能同时包含维护探测和其他无归属请求，不能整体标为系统维护。
const displayName = (item: UsageDiagnosticItem) => user(item.key)?.username || user(item.key)?.email || (item.key === 'unknown' ? UNOWNED_USAGE_USER_LABEL : item.name !== item.key ? item.name : `用户 ${item.key}`)
const money = (value: string | null) => value === null ? '—' : `$${Number(value).toLocaleString('en-US', { maximumFractionDigits: 4 })}`
let revision = 0
async function load() {
  const current = ++revision
  loading.value = true
  error.value = ''
  try {
    const [result, catalog] = await Promise.all([
      getUsageRecordInsightsDiagnostics({ ...props.range, ...props.filters, provider: props.provider || undefined, dimension: 'user' }),
      getUsers(),
    ])
    if (current !== revision)
      return
    items.value = result.items
    users.value = catalog
  }
  catch (cause) {
    if (current === revision) {
      items.value = []
      error.value = errorMessage(cause, '用户排行加载失败')
    }
  }
  finally {
    if (current === revision)
      loading.value = false
  }
}
onMounted(load)
watch([() => props.range, () => props.provider, () => props.filters], load)
onScopeDispose(() => {
  revision++
})
</script>

<template>
  <section class="flex min-h-0 flex-1 flex-col gap-2" aria-label="用户排行" :aria-busy="loading">
    <p v-if="error" role="alert" class="text-cp-error">
      {{ error }}
    </p>
    <BaseTable
      v-else
      class="min-h-0 w-full xl:contain-[size]"
      :columns="columns"
      :rows="loading ? [] : items"
      :loading="loading"
      :sort="{ key: 'totalTokens', direction: 'desc' }"
      density="compact"
      row-key="key"
      empty-text="当前筛选范围暂无用户用量"
    >
      <template #header-totalTokens>
        <span class="inline-flex items-center gap-0.5 text-cp-primary-text">Token<ArrowDown class="size-3" aria-hidden="true" /></span>
      </template>
      <template #rank="{ index }">
        <strong class="font-mono font-bold tabular-nums" :class="index < 3 ? 'text-cp-primary-text' : 'text-cp-text-tertiary'">
          {{ index + 1 }}
        </strong>
      </template>
      <template #name="{ row }">
        <div class="grid max-w-full min-w-0 gap-1">
          <span class="truncate font-emphasis text-cp-text" :title="displayName(row)">
            {{ displayName(row) }}
          </span>
          <span v-if="user(row.key)?.username" class="truncate text-cp-xs text-cp-text-secondary" :title="user(row.key)?.email">
            {{ user(row.key)?.email }}
          </span>
        </div>
      </template>
      <template #requestCount="{ row }">
        <strong class="font-mono font-bold text-cp-text tabular-nums" :title="`${row.requestCount.toLocaleString()} 次请求`">
          {{ row.requestCount.toLocaleString() }}
        </strong>
      </template>
      <template #totalTokens="{ row }">
        <span class="font-mono tabular-nums" :title="row.totalTokens.toLocaleString()">
          {{ formatCompactNumber(row.totalTokens) }}
        </span>
      </template>
      <template #estimatedCost="{ row }">
        <strong class="font-mono font-bold text-cp-success-text tabular-nums" :title="money(row.estimatedCost)">
          {{ money(row.estimatedCost) }}
        </strong>
      </template>
    </BaseTable>
  </section>
</template>
