<script setup lang="ts">
import type { OperationLog } from '@/api/modules/operations'
import { Eye, Search, ShieldCheck } from '@lucide/vue'
import { onMounted, reactive, ref } from 'vue'
import { getOperationLogs } from '@/api/modules/operations'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseCard from '@/components/base/BaseCard.vue'
import BaseFilterPanel from '@/components/base/BaseFilterPanel.vue'
import FormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseIconButton from '@/components/base/BaseIconButton.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import BasePageHeader from '@/components/base/BasePageHeader.vue'
import BaseSelect from '@/components/base/BaseSelect.vue'
import BaseTablePagination from '@/components/base/BaseTable/BaseTablePagination.vue'
import { defineTableColumns } from '@/components/base/BaseTable/columns'
import BaseTable from '@/components/base/BaseTable/index.vue'
import { errorMessage } from '@/utils/async'
import { formatDateTime as date } from '@/utils/date'

const filters = reactive({ email: '', action: '', ip: '', method: '', authMethod: '', result: '', days: '7' })
const items = ref<OperationLog[]>([])
const columns = defineTableColumns<OperationLog>([
  { key: 'time', label: '时间', kind: 'datetime' },
  { key: 'actor', label: '操作者', kind: 'identity', size: 'xl' },
  { key: 'action', label: '动作', kind: 'custom', size: '3xl' },
  { key: 'result', label: '结果', kind: 'status', size: 'sm' },
  { key: 'duration', label: '耗时', kind: 'numeric', size: 'sm' },
  { key: 'source', label: '来源 IP', kind: 'custom', size: '2xl' },
  { key: 'actions', label: '操作', kind: 'actions', size: 'sm' },
])
const total = ref(0)
const page = ref(1)
const loading = ref(false)
const error = ref('')
const detail = ref<OperationLog | null>(null)
const detailOpen = ref(false)
let applied: Record<string, string | number | undefined> = {}
let revision = 0
const authName = (method: string) => ({ session: '登录会话', api_key: '管理密钥', anonymous: '未认证' }[method] ?? method)
async function load() {
  const current = ++revision
  loading.value = true
  error.value = ''
  try {
    const data = await getOperationLogs({ ...applied, page: page.value })
    if (current !== revision)
      return
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
  page.value = 1
  const { days, ...fields } = filters
  const now = Date.now()
  applied = { ...Object.fromEntries(Object.entries(fields).map(([key, value]) => [key, value.trim() || undefined])), startTime: new Date(now - Number(days) * 86400000).toISOString(), endTime: new Date(now).toISOString() }
  void load()
}
function reset() {
  Object.assign(filters, { email: '', action: '', ip: '', method: '', authMethod: '', result: '', days: '7' })
  search()
}
function changePage(value: number) {
  page.value = value
  void load()
}
onMounted(search)
</script>

<template>
  <div class="grid gap-5">
    <BasePageHeader title="操作日志" description="记录登录、管理修改、敏感操作与鉴权失败，按系统日志保留天数保存。" />
    <BaseCard class="@container">
      <BaseFilterPanel :active-count="[filters.email, filters.action, filters.ip, filters.method, filters.authMethod, filters.result].filter(Boolean).length">
        <form class="grid items-end gap-3 sm:grid-cols-2 @[760px]:grid-cols-4 @[1120px]:grid-cols-[minmax(130px,1.2fr)_minmax(150px,1.4fr)_minmax(130px,1fr)_104px_104px_104px_120px_auto]" @submit.prevent="search">
          <FormItem label="操作者邮箱">
            <BaseInput v-model="filters.email" aria-label="操作者邮箱" placeholder="按邮箱筛选">
              <template #prefix>
                <Search class="size-4" />
              </template>
            </BaseInput>
          </FormItem>
          <FormItem label="动作 / 路径">
            <BaseInput v-model="filters.action" aria-label="动作或路径" placeholder="路径或动作" />
          </FormItem>
          <FormItem label="IP">
            <BaseInput v-model="filters.ip" aria-label="IP" placeholder="连接 / 上报 IP" />
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
              查询 / 刷新
            </BaseButton><BaseButton type="button" @click="reset">
              重置
            </BaseButton>
          </div>
        </form>
      </BaseFilterPanel>
    </BaseCard>
    <p v-if="error" role="alert" class="text-cp-error">
      {{ error }}
    </p>
    <BaseCard class="cp-mobile-table-host min-w-0">
      <BaseTable class="h-auto! min-h-48" :columns="columns" :loading="loading" :rows="items" empty-text="暂无匹配日志">
        <template #time="{ row: item }">
          <div class="min-w-0 py-2">
            {{ date(item.occurredAt) }}
          </div>
        </template>
        <template #actor="{ row: item }">
          <div class="min-w-0 py-2">
            <div>
              <div class="truncate font-emphasis" :title="item.username || item.email || authName(item.authMethod)">
                {{ item.username || item.email || authName(item.authMethod) }}
              </div><div class="mt-1 truncate text-cp-xs text-cp-text-secondary" :title="item.email || undefined">
                {{ item.username ? item.email : authName(item.authMethod) }}
              </div>
            </div>
          </div>
        </template>
        <template #action="{ row: item }">
          <div class="min-w-0 py-2">
            <div class="min-w-0" :title="`${item.method} ${item.path}`">
              <div class="truncate font-mono text-cp-sm">
                {{ item.path }}
              </div><div class="mt-1 font-mono text-cp-xs text-cp-text-tertiary">
                {{ item.method }}
              </div>
            </div>
          </div>
        </template>
        <template #result="{ row: item }">
          <div class="min-w-0 py-2">
            <span class="rounded-full px-2 py-1 text-cp-xs" :class="item.status < 400 ? 'bg-cp-success-container text-cp-success' : 'bg-cp-error-container text-cp-error'">{{ item.status }}</span>
          </div>
        </template>
        <template #duration="{ row: item }">
          <div class="min-w-0 py-2">
            {{ item.durationMs }} ms
          </div>
        </template>
        <template #source="{ row: item }">
          <div class="min-w-0 py-2">
            <div class="break-all font-mono text-cp-sm">
              <div :title="item.forwardedIp ? '代理请求头上报的地址，未经可信代理验证' : '应用直接连接的对端地址，可能是 Docker 网桥或反向代理'">
                {{ item.forwardedIp || item.clientIp || '未记录' }}
              </div>
              <div v-if="item.forwardedIp" class="mt-1 font-sans text-cp-xs text-cp-text-tertiary">
                代理上报 · 未验证
              </div>
              <div class="mt-1 text-cp-xs text-cp-text-secondary">
                {{ item.forwardedIp ? `连接 ${item.clientIp || '未记录'}` : '直连 · 未上报代理来源' }}
              </div>
            </div>
          </div>
        </template>
        <template #actions="{ row: item }">
          <div class="min-w-0 py-2">
            <BaseIconButton size="sm" label="查看操作详情" @click="detail = item; detailOpen = true">
              <Eye class="size-3.5 text-cp-link" />
            </BaseIconButton>
          </div>
        </template>
      </BaseTable>
      <BaseTablePagination :pagination="{ currentPage: page, pageSize: 50, total, pageSizes: [50] }" :loading="loading" @page-change="changePage" />
    </BaseCard>
    <BaseModal v-model="detailOpen" title="操作详情" size="md">
      <div v-if="detail" class="grid gap-4 text-cp-sm">
        <p class="flex items-center gap-2 text-cp-text-secondary">
          <ShieldCheck class="size-4" />仅记录安全元数据，不记录密码、密钥原文或正文。
        </p><dl class="grid grid-cols-[auto_minmax(0,1fr)] gap-x-4 gap-y-3">
          <dt>请求 ID</dt><dd class="break-all font-mono">
            {{ detail.requestId }}
          </dd><dt>操作者</dt><dd>{{ detail.email || authName(detail.authMethod) }}</dd><dt>路径</dt><dd class="break-all">
            {{ detail.method }} {{ detail.path }}
          </dd><dt>连接 IP</dt><dd>
            {{ detail.clientIp || '未记录' }}<p class="mt-1 text-cp-xs text-cp-text-tertiary">
              应用直接连接的对端地址，经过 Docker 或反向代理后可能始终相同。
            </p>
          </dd><dt>代理上报 IP</dt><dd>
            {{ detail.forwardedIp || '未上报' }}<p class="mt-1 text-cp-xs text-cp-text-tertiary">
              来自 X-Forwarded-For、X-Real-IP 或 CF-Connecting-IP，仅供排查，未经可信代理验证。未上报时无法从连接地址还原访客 IP。
            </p>
          </dd><dt>认证 / 结果</dt><dd>{{ authName(detail.authMethod) }} / {{ detail.status }}</dd>
        </dl><section v-for="(change, index) in detail.changes" :key="index" class="rounded-cp bg-cp-fill-quaternary p-3">
          <p>{{ change.action }} · {{ change.entityKind }}</p><p class="mt-1 break-all font-mono">
            {{ change.entityRef }}
          </p><p class="mt-2 text-cp-text-secondary">
            变更字段：{{ change.changedFields.join('、') || '无' }}
          </p>
        </section>
      </div>
    </BaseModal>
  </div>
</template>
