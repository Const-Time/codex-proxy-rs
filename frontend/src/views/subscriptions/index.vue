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
import { toast } from '@/components/base/BaseToast'
import { errorMessage } from '@/utils/async'
import { generateRequestId } from '@/utils/requestId'

const records = ref<Subscription[]>([])
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
const money = (value: string) => `$${Number(value).toLocaleString('en-US', { maximumFractionDigits: 4 })}`
const limit = (value: string) => Number(value) === 0 ? '不限' : money(value)
const date = (value: string | null) => value ? new Date(value).toLocaleString('zh-CN', { hour12: false }) : '尚未开始'
const reason = (value: string | null) => ({ manual: '管理员重置', upstream_manual: '账号主动重置', upstream_window: '上游窗口重置', upstream_recovery: '上游额度恢复' }[value ?? ''] ?? '—')
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
    <BaseCard>
      <div class="overflow-x-auto">
        <table class="w-full whitespace-nowrap text-left text-cp-sm">
          <thead class="text-cp-text-secondary">
            <tr>
              <th class="p-3">
                <BaseCheckbox label="选择当前页订阅" :model-value="pageSelected" :indeterminate="!pageSelected && visible.some(item => selected.includes(rowKey(item)))" :disabled="loading || !visible.length" @update:model-value="togglePage" />
              </th>
              <th class="p-3">
                用户
              </th><th class="p-3">
                分组
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
                <BaseCheckbox :label="`选择 ${item.username || item.email} / ${item.groupName}`" :model-value="selected.includes(rowKey(item))" @update:model-value="checked => selected = checked ? [...selected, rowKey(item)] : selected.filter(key => key !== rowKey(item))" />
              </td>
              <td class="p-3">
                <div class="font-medium">
                  {{ item.username || item.email }}
                  <div v-if="item.username" class="mt-1 text-cp-xs text-cp-text-secondary">
                    {{ item.email }}
                  </div>
                </div>
              </td>
              <td class="p-3">
                {{ item.groupName }} <span v-if="!item.enabled">· 已停用</span>
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
              <td colspan="8" class="p-6 text-center text-cp-text-secondary">
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
