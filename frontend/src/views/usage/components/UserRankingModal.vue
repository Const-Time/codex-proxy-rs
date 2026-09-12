<script setup lang="ts">
import type { UsageDiagnosticItem } from '@/api/modules/usage'
import type { User } from '@/api/modules/users'
import { ref, watch } from 'vue'
import { getUsageRecordInsightsDiagnostics } from '@/api/modules/usage'
import { getUsers } from '@/api/modules/users'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import { errorMessage } from '@/utils/async'

const props = defineProps<{ range: { startTime: string, endTime: string }, provider?: string }>()
const open = defineModel<boolean>({ default: false })
const items = ref<UsageDiagnosticItem[]>([])
const users = ref<User[]>([])
const loading = ref(false)
const error = ref('')
const user = (id: string) => users.value.find(user => user.id === id)
let revision = 0
async function load() {
  const current = ++revision
  loading.value = true
  error.value = ''
  try {
    const [result, catalog] = await Promise.all([getUsageRecordInsightsDiagnostics({ ...props.range, provider: props.provider || undefined, dimension: 'user' }), getUsers()])
    if (current !== revision)
      return
    items.value = result.items
    users.value = catalog
  }
  catch (cause) {
    if (current === revision)
      error.value = errorMessage(cause, '用户排行加载失败')
  }
  finally {
    if (current === revision)
      loading.value = false
  }
}
watch([open, () => props.range, () => props.provider], () => open.value && void load())
</script>

<template>
  <BaseModal v-model="open" title="用户排行" size="lg">
    <p class="mb-4 text-cp-sm text-cp-text-secondary">
      按当前时间范围和平台统计，按请求量展示前 100 位用户。消费按请求发生时的计费倍率汇总。
    </p>
    <p v-if="error" role="alert" class="text-cp-error">
      {{ error }}
    </p>
    <div class="overflow-x-auto">
      <table class="w-full whitespace-nowrap text-left text-cp-sm">
        <thead class="bg-cp-fill-quaternary text-cp-text-secondary">
          <tr>
            <th class="p-3">
              排名
            </th><th class="p-3">
              用户 / 邮箱
            </th><th class="p-3">
              请求数
            </th><th class="p-3">
              Token
            </th><th class="p-3">
              消费
            </th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(item, index) in items" :key="item.key" class="border-b border-cp-border">
            <td class="p-3 font-mono">
              {{ index + 1 }}
            </td><td class="p-3">
              <div>{{ user(item.key)?.username || user(item.key)?.email || '未关联用户' }}</div><div v-if="user(item.key)?.username" class="text-cp-xs text-cp-text-secondary">
                {{ user(item.key)?.email }}
              </div>
            </td><td class="p-3 font-mono">
              {{ item.requestCount.toLocaleString() }}
            </td><td class="p-3 font-mono">
              {{ item.totalTokens.toLocaleString() }}
            </td><td class="p-3 font-mono text-cp-success">
              {{ item.estimatedCost === null ? '—' : `$${Number(item.estimatedCost).toLocaleString('en-US', { maximumFractionDigits: 4 })}` }}
            </td>
          </tr><tr v-if="!items.length">
            <td colspan="5" class="p-6 text-center text-cp-text-tertiary">
              {{ loading ? '加载中…' : '此范围暂无记录' }}
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    <template #footer>
      <BaseButton :loading="loading" @click="load">
        刷新
      </BaseButton><BaseButton @click="open = false">
        关闭
      </BaseButton>
    </template>
  </BaseModal>
</template>
