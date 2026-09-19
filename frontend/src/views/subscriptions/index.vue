<script setup lang="ts">
import type { Subscription, SubscriptionTarget } from '@/api/modules/subscriptions'
import { Search } from '@lucide/vue'
import { computed, onMounted, ref, watch } from 'vue'
import { getSubscriptions, resetSubscriptions } from '@/api/modules/subscriptions'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseCard from '@/components/base/BaseCard.vue'
import BaseCheckbox from '@/components/base/BaseCheckbox.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import BasePageHeader from '@/components/base/BasePageHeader.vue'
import BaseSelect from '@/components/base/BaseSelect.vue'
import BaseTablePagination from '@/components/base/BaseTable/BaseTablePagination.vue'
import { defineTableColumns } from '@/components/base/BaseTable/columns'
import BaseTable from '@/components/base/BaseTable/index.vue'
import { toast } from '@/components/base/BaseToast'
import QuotaUsage from '@/components/quota/QuotaUsage.vue'
import { errorMessage } from '@/utils/async'
import { formatDateTime } from '@/utils/date'
import { generateRequestId } from '@/utils/requestId'

const records = ref<Subscription[]>([])
const columns = defineTableColumns<Subscription>([
  { key: 'selection', kind: 'selection' },
  { key: 'identity', label: '用户 / 邮箱', kind: 'identity' },
  { key: 'group', label: '分组', kind: 'custom', size: 'lg' },
  { key: 'multiplier', label: '额度倍率', kind: 'numeric' },
  { key: 'quota', label: '分组额度 · 已用 / 限额', kind: 'custom', size: '4xl', mobileFullWidth: true },
  { key: 'reset', label: '最近重置', kind: 'datetime' },
])
const loading = ref(false)
const resetting = ref(false)
const error = ref('')
const search = ref('')
const groupId = ref('')
const page = ref(1)
const confirmOpen = ref(false)
const resetRequestId = ref('')
const selected = ref<string[]>([])
const resetTargets = ref<SubscriptionTarget[]>([])
const collator = new Intl.Collator('zh-CN', { numeric: true, sensitivity: 'base' })
const rowKey = (item: SubscriptionTarget) => JSON.stringify([item.userId, item.groupId])
const groups = computed(() => [...new Map(records.value.map(item => [item.groupId, item.groupName])).entries()])
const filtered = computed(() => records.value.filter(item => (!groupId.value || item.groupId === groupId.value)
  && `${item.email} ${item.username || item.email}`.toLowerCase().includes(search.value.toLowerCase())))
const pageCount = computed(() => Math.max(1, Math.ceil(filtered.value.length / 20)))
const visible = computed(() => filtered.value.slice((page.value - 1) * 20, page.value * 20))
const pageSelected = computed(() => visible.value.length > 0 && visible.value.every(item => selected.value.includes(rowKey(item))))
function togglePage(checked: boolean) {
  const keys = visible.value.map(rowKey)
  selected.value = checked ? [...new Set([...selected.value, ...keys])] : selected.value.filter(key => !keys.includes(key))
}
watch([search, groupId], () => {
  page.value = 1
  selected.value = []
})
const date = (value: string | null) => value ? formatDateTime(value) : '尚未开始'
const reason = (value: string | null) => ({ group_period: '分组周周期重置', daily_period: '每日周期重置', manual: '管理员重置', upstream_manual: '账号主动重置', upstream_window: '上游窗口重置', upstream_recovery: '上游额度恢复' }[value ?? ''] ?? '—')
async function load() {
  loading.value = true
  error.value = ''
  try {
    records.value = (await getSubscriptions()).sort((a, b) =>
      collator.compare(a.groupName, b.groupName)
      || a.groupId.localeCompare(b.groupId)
      || collator.compare(a.email, b.email)
      || a.userId.localeCompare(b.userId),
    )
    const keys = new Set(records.value.map(rowKey))
    selected.value = selected.value.filter(key => keys.has(key))
    page.value = Math.min(page.value, pageCount.value)
  }
  catch (cause) { error.value = errorMessage(cause, '订阅加载失败') }
  finally { loading.value = false }
}
function openReset() {
  resetTargets.value = records.value.filter(item => selected.value.includes(rowKey(item))).map(({ userId, groupId }) => ({ userId, groupId }))
  if (!resetTargets.value.length)
    return
  resetRequestId.value = generateRequestId()
  confirmOpen.value = true
}
async function resetSelected() {
  if (resetting.value)
    return
  resetting.value = true
  try {
    const count = await resetSubscriptions(resetRequestId.value, resetTargets.value)
    confirmOpen.value = false
    selected.value = []
    toast.success(`已重置 ${count} 个用户分组配额`)
    await load()
  }
  catch (cause) { toast.error(errorMessage(cause, '重置失败，可重试')) }
  finally { resetting.value = false }
}
onMounted(load)
</script>

<template>
  <div class="flex flex-col gap-5">
    <BasePageHeader title="订阅管理" description="查看用户的分组额度与用量。额度倍率由用户管理配置，历史消费在使用统计中保留。" />
    <div class="flex flex-wrap items-center gap-3">
      <BaseInput v-model="search" aria-label="搜索邮箱或用户名" placeholder="搜索邮箱或用户名" class="w-full sm:w-80">
        <template #prefix>
          <Search class="size-4" />
        </template>
      </BaseInput>
      <BaseSelect v-model="groupId" aria-label="筛选分组" class="w-full sm:w-40" :options="[{ label: '全部分组', value: '' }, ...groups.map(([value, label]) => ({ value, label }))]" />
      <BaseButton variant="secondary" :loading="loading" @click="load">
        刷新
      </BaseButton>
      <BaseButton variant="destructive" :disabled="loading || !selected.length || selected.length > 500" @click="openReset">
        重置选中订阅（{{ selected.length }}）
      </BaseButton>
    </div>
    <p v-if="error" role="alert" class="text-cp-error">
      {{ error }}
    </p>
    <BaseCard class="cp-mobile-table-host min-w-0">
      <BaseTable class="h-auto! min-h-48" :columns="columns" :loading="loading" :rows="visible" :row-key="rowKey" :selected-row-keys="selected" empty-text="没有匹配的订阅">
        <template #header-selection>
          <BaseCheckbox label="选择当前页订阅" :model-value="pageSelected" :indeterminate="!pageSelected && visible.some(item => selected.includes(rowKey(item)))" :disabled="loading || !visible.length" @update:model-value="togglePage" />
        </template>
        <template #selection="{ row: item }">
          <div class="min-w-0 py-2">
            <BaseCheckbox :label="`选择 ${item.username || item.email} / ${item.groupName}`" :model-value="selected.includes(rowKey(item))" @update:model-value="checked => selected = checked ? [...selected, rowKey(item)] : selected.filter(key => key !== rowKey(item))" />
          </div>
        </template>
        <template #identity="{ row: item }">
          <div class="min-w-0 py-2">
            <div class="truncate font-emphasis" :title="item.username || item.email">
              {{ item.username || item.email }}
            </div>
            <div v-if="item.username" class="mt-1 truncate text-cp-xs text-cp-text-secondary" :title="item.email">
              {{ item.email }}
            </div>
          </div>
        </template>
        <template #group="{ row: item }">
          <div class="min-w-0 py-2">
            <div class="truncate" :title="item.groupName">
              {{ item.groupName }}
            </div>
            <div v-if="!item.enabled" class="mt-1 text-cp-xs text-cp-text-tertiary">
              已停用
            </div>
          </div>
        </template>
        <template #multiplier="{ row: item }">
          <div class="min-w-0 py-2">
            {{ Number(item.quotaMultiplier) }}x
          </div>
        </template>
        <template #quota="{ row: item }">
          <div class="min-w-0 py-3">
            <QuotaUsage class="w-full" :budget="item" :label="`${item.username || item.email} / ${item.groupName}`" />
          </div>
        </template>
        <template #reset="{ row: item }">
          <div class="min-w-0 py-2">
            <div>
              <div>{{ item.lastResetAt ? date(item.lastResetAt) : '—' }}</div><div class="mt-1 font-sans text-cp-xs text-cp-text-tertiary">
                {{ reason(item.lastResetReason) }}
              </div>
            </div>
          </div>
        </template>
      </BaseTable>
      <BaseTablePagination :pagination="{ currentPage: page, pageSize: 20, total: filtered.length, pageSizes: [20] }" :loading="loading" @page-change="page = $event" />
    </BaseCard>
    <BaseModal v-model="confirmOpen" title="重置选中订阅" :dismissible="!resetting">
      <p class="text-cp-sm leading-relaxed">
        将重置选中的 {{ resetTargets.length }} 个用户分组订阅（包含跨页选择）的日、周额度，其他订阅不受影响。日额度在下一个北京时间零点刷新，周额度从本次重置起重新计算 7 天。历史请求和消费记录保留。
      </p>
      <template #footer>
        <BaseButton variant="secondary" :disabled="resetting" @click="confirmOpen = false">
          取消
        </BaseButton><BaseButton variant="destructive" :loading="resetting" @click="resetSelected">
          确认重置选中订阅
        </BaseButton>
      </template>
    </BaseModal>
  </div>
</template>
