<script setup lang="ts">
import type { Subscription } from '@/api/modules/subscriptions'
import { computed, onMounted, ref, watch } from 'vue'
import { getSubscriptions, resetSubscriptions } from '@/api/modules/subscriptions'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseCard from '@/components/base/BaseCard.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import BasePageHeader from '@/components/base/BasePageHeader.vue'
import { toast } from '@/components/base/BaseToast'
import { errorMessage } from '@/utils/async'

const records = ref<Subscription[]>([])
const loading = ref(false)
const resetting = ref(false)
const error = ref('')
const search = ref('')
const groupId = ref('')
const page = ref(1)
const confirmOpen = ref(false)
const resetRequestId = ref('')
const groups = computed(() => [...new Map(records.value.map(item => [item.groupId, item.groupName])).entries()])
const filtered = computed(() => records.value.filter(item => (!groupId.value || item.groupId === groupId.value)
  && item.username.toLowerCase().includes(search.value.toLowerCase())))
const pageCount = computed(() => Math.max(1, Math.ceil(filtered.value.length / 20)))
const visible = computed(() => filtered.value.slice((page.value - 1) * 20, page.value * 20))
watch([search, groupId], () => {
  page.value = 1
})
const money = (value: string) => `$${Number(value).toLocaleString('en-US', { maximumFractionDigits: 4 })}`
const limit = (value: string) => Number(value) === 0 ? '不限' : money(value)
const date = (value: string | null) => value ? new Date(value).toLocaleString('zh-CN', { hour12: false }) : '尚未开始'
const reason = (value: string | null) => ({ manual: '管理员重置', upstream_manual: '账号主动重置', upstream_window: '上游窗口重置', upstream_recovery: '上游额度恢复' }[value ?? ''] ?? '—')
async function load() {
  loading.value = true
  error.value = ''
  try {
    records.value = await getSubscriptions()
    page.value = Math.min(page.value, pageCount.value)
  }
  catch (cause) { error.value = errorMessage(cause, '订阅加载失败') }
  finally { loading.value = false }
}
function openReset() {
  resetRequestId.value = crypto.randomUUID()
  confirmOpen.value = true
}
async function resetAll() {
  if (resetting.value)
    return
  resetting.value = true
  try {
    const count = await resetSubscriptions(resetRequestId.value)
    confirmOpen.value = false
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
      <BaseInput v-model="search" aria-label="搜索用户名" placeholder="搜索用户名" class="max-w-sm" />
      <select v-model="groupId" aria-label="筛选分组" class="rounded-cp border border-cp-border bg-cp-fill-tertiary p-2.5 text-cp-sm">
        <option value="">
          全部分组
        </option>
        <option v-for="[id, name] in groups" :key="id" :value="id">
          {{ name }}
        </option>
      </select>
      <BaseButton variant="secondary" :loading="loading" @click="load">
        刷新
      </BaseButton>
      <BaseButton variant="destructive" :disabled="loading || !records.length" @click="openReset">
        重置所有用户配额
      </BaseButton>
    </div>
    <p v-if="error" role="alert" class="text-cp-error">
      {{ error }}
    </p>
    <BaseCard>
      <div class="overflow-x-auto">
        <table class="w-full whitespace-nowrap text-left text-cp-sm">
          <thead class="text-cp-text-secondary">
            <tr>
              <th class="p-3">
                用户 / 分组
              </th><th class="p-3">
                额度倍率
              </th><th class="p-3">
                日用量 / 日限
              </th><th class="p-3">
                周用量 / 周限
              </th><th class="p-3">
                下次刷新（日 / 周）
              </th><th class="p-3">
                最近重置
              </th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="item in visible" :key="`${item.userId}:${item.groupId}`" class="border-t border-cp-border">
              <td class="p-3">
                <div class="font-medium">
                  {{ item.username }}
                </div><div class="mt-1 text-cp-text-secondary">
                  {{ item.groupName }} <span v-if="!item.enabled">· 已停用</span>
                </div>
              </td>
              <td class="p-3 font-mono">
                {{ Number(item.quotaMultiplier) }}x
              </td>
              <td class="p-3 font-mono">
                {{ money(item.dailyUsedUsd) }} / {{ limit(item.dailyLimitUsd) }}
              </td>
              <td class="p-3 font-mono">
                {{ money(item.weeklyUsedUsd) }} / {{ limit(item.weeklyLimitUsd) }}
              </td>
              <td class="p-3">
                <div>{{ date(item.dailyResetsAt) }}</div><div class="mt-1 text-cp-text-secondary">
                  {{ date(item.weeklyResetsAt) }}
                </div>
              </td>
              <td class="p-3">
                <div>{{ item.lastResetAt ? date(item.lastResetAt) : '—' }}</div><div class="mt-1 text-cp-text-secondary">
                  {{ reason(item.lastResetReason) }}
                </div>
              </td>
            </tr>
            <tr v-if="!visible.length">
              <td colspan="6" class="p-6 text-center text-cp-text-secondary">
                {{ loading ? '加载中…' : '没有匹配的订阅' }}
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <div class="mt-4 flex items-center justify-between gap-3 text-cp-sm">
        <span>共 {{ filtered.length }} 条 · 每页 20 条</span>
        <div class="flex items-center gap-3">
          <BaseButton variant="secondary" :disabled="page <= 1" @click="page--">
            上一页
          </BaseButton><span>{{ page }} / {{ pageCount }}</span><BaseButton variant="secondary" :disabled="page >= pageCount" @click="page++">
            下一页
          </BaseButton>
        </div>
      </div>
    </BaseCard>
    <BaseModal v-model="confirmOpen" title="重置所有用户配额">
      <p class="text-cp-sm leading-relaxed">
        将清零所有用户在所有分组下的日、周已用额度，不受当前筛选条件影响。日额度在下一个北京时间零点刷新，周额度从本次重置起重新计算 7 天。历史请求和消费记录保留。
      </p>
      <template #footer>
        <BaseButton variant="secondary" :disabled="resetting" @click="confirmOpen = false">
          取消
        </BaseButton><BaseButton variant="destructive" :loading="resetting" @click="resetAll">
          确认重置全部
        </BaseButton>
      </template>
    </BaseModal>
  </div>
</template>
