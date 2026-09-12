<script setup lang="ts">
import type { OperationLog } from '@/api/modules/operations'
import { Search, ShieldCheck } from '@lucide/vue'
import { onMounted, reactive, ref } from 'vue'
import { getOperationLogs } from '@/api/modules/operations'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseCard from '@/components/base/BaseCard.vue'
import FormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import BasePageHeader from '@/components/base/BasePageHeader.vue'
import BaseSelect from '@/components/base/BaseSelect.vue'
import { errorMessage } from '@/utils/async'

const filters = reactive({ email: '', action: '', ip: '', method: '', authMethod: '', result: '', days: '7' })
const items = ref<OperationLog[]>([])
const total = ref(0)
const page = ref(1)
const loading = ref(false)
const error = ref('')
const detail = ref<OperationLog | null>(null)
const detailOpen = ref(false)
let applied: Record<string, string | number | undefined> = {}
let revision = 0
const date = (value: string) => new Date(value).toLocaleString('zh-CN', { hour12: false })
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
    </BaseCard>
    <p v-if="error" role="alert" class="text-cp-error">
      {{ error }}
    </p>
    <BaseCard>
      <div class="overflow-x-auto">
        <table class="w-full whitespace-nowrap text-left text-cp-sm">
          <thead class="bg-cp-fill-quaternary text-cp-text-secondary">
            <tr>
              <th class="p-3">
                时间
              </th><th class="p-3">
                操作者
              </th><th class="p-3">
                动作
              </th><th class="p-3">
                结果
              </th><th class="p-3">
                耗时
              </th><th class="p-3">
                连接 IP
              </th><th class="p-3">
                操作
              </th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="item in items" :key="item.id" class="border-b border-cp-border">
              <td class="p-3">
                {{ date(item.occurredAt) }}
              </td><td class="p-3">
                <div>{{ item.username || item.email || authName(item.authMethod) }}</div><div class="mt-1 text-cp-xs text-cp-text-secondary">
                  {{ item.username ? item.email : authName(item.authMethod) }}
                </div>
              </td><td class="max-w-md truncate p-3 font-mono" :title="`${item.method} ${item.path}`">
                {{ item.method }} {{ item.path }}
              </td><td class="p-3">
                <span class="rounded-full px-2 py-1 text-cp-xs" :class="item.status < 400 ? 'bg-cp-success-container text-cp-success' : 'bg-cp-error-container text-cp-error'">{{ item.status }}</span>
              </td><td class="p-3 font-mono">
                {{ item.durationMs }} ms
              </td><td class="p-3 font-mono">
                {{ item.clientIp || '未记录' }}
              </td><td class="p-3">
                <BaseButton size="sm" @click="detail = item; detailOpen = true">
                  详情
                </BaseButton>
              </td>
            </tr><tr v-if="!items.length">
              <td colspan="7" class="p-6 text-center text-cp-text-tertiary">
                {{ loading ? '加载中…' : '暂无匹配日志' }}
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <div class="mt-4 flex items-center justify-between text-cp-sm">
        <span>共 {{ total }} 条 · 每页 50 条</span><div class="flex items-center gap-3">
          <BaseButton :disabled="loading || page <= 1" @click="changePage(page - 1)">
            上一页
          </BaseButton><span>{{ page }} / {{ Math.max(1, Math.ceil(total / 50)) }}</span><BaseButton :disabled="loading || page * 50 >= total" @click="changePage(page + 1)">
            下一页
          </BaseButton>
        </div>
      </div>
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
          </dd><dt>连接 IP</dt><dd>{{ detail.clientIp || '未记录' }}</dd><dt>代理上报 IP</dt><dd>
            {{ detail.forwardedIp || '未上报' }}<p class="mt-1 text-cp-xs text-cp-text-tertiary">
              来自 X-Forwarded-For，仅供排查，未经可信代理验证。
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
