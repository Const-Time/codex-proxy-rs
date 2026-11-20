<script setup lang="ts">
import type { OperationLog } from '@/api/modules/operations'
import { Eye, RefreshCw, Search } from '@lucide/vue'
import { computed, onMounted, reactive, ref } from 'vue'
import { getOperationLogs } from '@/api/modules/operations'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseCard from '@/components/base/BaseCard.vue'
import BaseFilterPanel from '@/components/base/BaseFilterPanel.vue'
import FormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseIconButton from '@/components/base/BaseIconButton.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BasePageHeader from '@/components/base/BasePageHeader.vue'
import BaseSelect from '@/components/base/BaseSelect.vue'
import BaseStatusBadge from '@/components/base/BaseStatusBadge.vue'
import BaseTablePagination from '@/components/base/BaseTable/BaseTablePagination.vue'
import { defineTableColumns } from '@/components/base/BaseTable/columns'
import BaseTable from '@/components/base/BaseTable/index.vue'
import { errorMessage } from '@/utils/async'
import { formatDateTime as date } from '@/utils/date'
import OperationLogDetailModal from './OperationLogDetailModal.vue'

const defaultFilters = { email: '', action: '', ip: '', method: '', authMethod: '', result: '', days: '7' }
const filters = reactive({ ...defaultFilters })
const filterCount = computed(() => Object.entries(filters).filter(([key, value]) => value.trim() !== defaultFilters[key as keyof typeof defaultFilters]).length)
const items = ref<OperationLog[]>([])
const columns = defineTableColumns<OperationLog>([
  { key: 'time', label: '时间', kind: 'datetime' },
  { key: 'actor', label: '操作者', kind: 'identity', size: 'xl', mobileFullWidth: true },
  { key: 'action', label: '路径', kind: 'mono', size: '2xl', mobileFullWidth: true },
  { key: 'result', label: '结果', kind: 'status', size: 'sm' },
  { key: 'duration', label: '耗时', kind: 'numeric', size: 'sm' },
  { key: 'source', label: '来源 IP', kind: 'custom', size: 'xl' },
  { key: 'actions', label: '操作', kind: 'actions', size: 'sm' },
])
const total = ref(0)
const page = ref(1)
const loading = ref(false)
const error = ref('')
const detail = ref<OperationLog | null>(null)
const detailOpen = ref(false)
type AuditQuery = Parameters<typeof getOperationLogs>[0]
let applied: AuditQuery = {}
let revision = 0
const authName = (method: string) => ({ session: '登录会话', api_key: '管理密钥', anonymous: '未认证' }[method] ?? method)
let appliedDays = Number(defaultFilters.days)

function timeRange(days: number) {
  const now = Date.now()
  return {
    startTime: new Date(now - days * 86400000).toISOString(),
    endTime: new Date(now).toISOString(),
  }
}

async function load(targetPage: number, query = applied, days = appliedDays) {
  const current = ++revision
  loading.value = true
  error.value = ''
  try {
    const data = await getOperationLogs({ ...query, page: targetPage })
    if (current !== revision)
      return
    applied = query
    appliedDays = days
    page.value = targetPage
    items.value = data.items
    total.value = data.total
  }
  catch (cause) {
    if (current === revision)
      error.value = errorMessage(cause, '操作日志加载失败')
  }
  finally {
    if (current === revision)
      loading.value = false
  }
}

function search() {
  const { days, ...fields } = filters
  const query = Object.fromEntries(Object.entries(fields).map(([key, value]) => [key, value.trim() || undefined]))
  void load(1, { ...query, ...timeRange(Number(days)) }, Number(days))
}
function refresh() {
  void load(1, { ...applied, ...timeRange(appliedDays) })
}
function reset() {
  Object.assign(filters, defaultFilters)
  search()
}
function changePage(value: number) {
  void load(value)
}
onMounted(search)
</script>

<template>
  <div class="cp-mobile-auto-height flex h-full min-h-0 w-full flex-col gap-5">
    <BasePageHeader title="操作日志" description="查看登录、管理变更和鉴权失败记录" />
    <BaseCard class="cp-mobile-auto-height cp-mobile-table-host @container flex h-[calc(100dvh-136px)] min-h-125 min-w-0 flex-col">
      <BaseFilterPanel :active-count="filterCount" class="mb-3 shrink-0" collapsible>
        <form class="grid items-end gap-3 rounded-cp bg-cp-fill-quaternary p-3 @[480px]:grid-cols-2 @[760px]:grid-cols-4" @submit.prevent="search">
          <FormItem label="操作者邮箱">
            <BaseInput v-model="filters.email" aria-label="操作者邮箱" placeholder="按邮箱筛选" clearable>
              <template #prefix>
                <Search class="size-4" />
              </template>
            </BaseInput>
          </FormItem>
          <FormItem label="动作 / 路径">
            <BaseInput v-model="filters.action" aria-label="动作或路径" placeholder="路径或动作" clearable />
          </FormItem>
          <FormItem label="IP">
            <BaseInput v-model="filters.ip" aria-label="IP" placeholder="连接 / 上报 IP" clearable />
          </FormItem>
          <FormItem label="请求方法">
            <BaseSelect v-model="filters.method" class="w-full" aria-label="请求方法" :options="[{ label: '全部方法', value: '' }, ...['GET', 'POST', 'PUT', 'PATCH', 'DELETE'].map(value => ({ label: value, value }))]" />
          </FormItem>
          <FormItem label="认证方式">
            <BaseSelect v-model="filters.authMethod" class="w-full" aria-label="认证方式" :options="[{ label: '全部方式', value: '' }, ...['session', 'api_key', 'anonymous'].map(value => ({ label: authName(value), value }))]" />
          </FormItem>
          <FormItem label="结果">
            <BaseSelect v-model="filters.result" class="w-full" aria-label="结果" :options="[{ label: '全部结果', value: '' }, { label: '成功', value: 'success' }, { label: '失败', value: 'failure' }]" />
          </FormItem>
          <FormItem label="时间范围">
            <BaseSelect v-model="filters.days" class="w-full" aria-label="时间范围" :options="[{ label: '最近 24 小时', value: '1' }, { label: '最近 7 天', value: '7' }, { label: '最近 30 天', value: '30' }, { label: '最近 90 天', value: '90' }]" />
          </FormItem>
          <div class="flex items-end gap-2 whitespace-nowrap">
            <BaseButton type="submit" variant="primary" :loading="loading">
              查询
            </BaseButton>
            <BaseButton type="button" variant="ghost" @click="reset">
              重置
            </BaseButton>
          </div>
        </form>
      </BaseFilterPanel>
      <p v-if="error" role="alert" class="mb-3 rounded-cp bg-cp-error-container px-3 py-2 text-cp-sm text-cp-error-on-container">
        {{ error }}<span v-if="items.length">。当前显示上次查询结果。</span>
      </p>
      <BaseTable table-id="operation-logs" class="min-h-48 flex-1" density="compact" :columns="columns" :loading="loading" :rows="items" empty-text="暂无匹配日志">
        <template #toolbar>
          <span class="block pb-1.5 text-cp-sm text-cp-text-secondary">共 {{ total }} 条日志</span>
        </template>
        <template #toolbar-actions>
          <BaseIconButton :loading="loading" label="刷新操作日志" @click="refresh">
            <RefreshCw class="size-4" />
          </BaseIconButton>
        </template>
        <template #time="{ row: item }">
          <span>{{ date(item.occurredAt) }}</span>
        </template>
        <template #actor="{ row: item }">
          <div class="grid min-w-0 gap-1 py-1">
            <strong class="truncate text-cp-text" :title="item.username || item.email || authName(item.authMethod)">
              {{ item.username || item.email || authName(item.authMethod) }}
            </strong>
            <span v-if="item.username || item.email" class="truncate text-cp-xs text-cp-text-secondary" :title="item.email || undefined">
              {{ item.username && item.email ? item.email : authName(item.authMethod) }}
            </span>
          </div>
        </template>
        <template #action="{ row: item }">
          <div class="grid min-w-0 gap-1 py-1" :title="`${item.method} ${item.path}`">
            <span class="w-fit rounded-cp-sm bg-cp-fill-tertiary px-1.5 py-0.5 text-cp-xs leading-tight text-cp-text-secondary">{{ item.method }}</span>
            <span class="truncate text-cp-sm max-sm:whitespace-normal max-sm:break-all">{{ item.path }}</span>
          </div>
        </template>
        <template #result="{ row: item }">
          <BaseStatusBadge :tone="item.status < 400 ? 'success' : 'danger'">
            {{ item.status }}
          </BaseStatusBadge>
        </template>
        <template #duration="{ row: item }">
          <span class="whitespace-nowrap">{{ item.durationMs }} ms</span>
        </template>
        <template #source="{ row: item }">
          <div class="grid min-w-0 gap-1 py-1">
            <span class="break-all font-mono text-cp-sm" :title="item.forwardedIp ? '代理请求头上报的地址，未经可信代理验证' : '应用直接连接的对端地址，可能是 Docker 网桥或反向代理'">{{ item.forwardedIp || item.clientIp || '未记录' }}</span>
            <span class="text-cp-xs text-cp-text-tertiary">{{ item.forwardedIp ? '代理上报 · 未验证' : '连接 IP' }}</span>
          </div>
        </template>
        <template #actions="{ row: item }">
          <div class="flex min-w-0 max-sm:justify-end">
            <BaseIconButton size="sm" label="查看操作详情" @click="detail = item; detailOpen = true">
              <Eye class="size-3.5 text-cp-link" />
            </BaseIconButton>
          </div>
        </template>
      </BaseTable>
      <BaseTablePagination :pagination="{ currentPage: page, pageSize: 50, total, pageSizes: [50] }" :loading="loading" @page-change="changePage" />
    </BaseCard>
    <OperationLogDetailModal v-model="detailOpen" :detail="detail" :authentication-label="detail ? authName(detail.authMethod) : ''" />
  </div>
</template>
