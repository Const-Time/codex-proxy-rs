<script setup lang="ts">
import type { Ref } from 'vue'
import type { UsageRecordFilters } from '@/api'
import type { SelectOption } from '@/components/base/BaseSelect.vue'
import { computed, inject, onMounted, shallowRef } from 'vue'
import { getAccountGroups, getAccounts } from '@/api'
import { getUserGroups, getUsers } from '@/api/modules/users'
import BaseButton from '@/components/base/BaseButton.vue'
import FormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseSelect from '@/components/base/BaseSelect.vue'

const personal = inject<Readonly<Ref<boolean>>>('personalUsage')
const filters = defineModel<UsageRecordFilters>({ required: true })
const groups = shallowRef<SelectOption[]>([])
const accounts = shallowRef<SelectOption[]>([])
const users = shallowRef<SelectOption[]>([])
const loading = shallowRef(false)
const error = shallowRef('')
const active = computed(() => Object.values(filters.value).some(value => value.trim()))
const transports = [
  { label: '全部接入类型', value: '' },
  { label: 'HTTP', value: 'http_json' },
  { label: 'SSE', value: 'http_sse' },
  { label: 'WebSocket', value: 'websocket' },
]

async function loadCatalog<T>(load: (page: number) => Promise<{ items: T[], page: { totalPages: number } }>, label: (row: T) => SelectOption) {
  const items: SelectOption[] = []
  let page = 1
  while (true) {
    const result = await load(page)
    items.push(...result.items.map(label))
    if (page >= result.page.totalPages || result.items.length === 0)
      return items
    page += 1
  }
}

async function load() {
  if (loading.value)
    return
  loading.value = true
  error.value = ''
  if (personal?.value) {
    try {
      groups.value = (await getUserGroups()).map(group => ({ label: group.name, value: group.id }))
    }
    catch { error.value = '分组选项加载失败，请重试。' }
    finally { loading.value = false }
    return
  }
  const results = await Promise.allSettled([
    loadCatalog(page => getAccountGroups({ page, pageSize: 200 }), row => ({ label: row.name, value: row.id })),
    loadCatalog(page => getAccounts({ page, pageSize: 200 }), row => ({ label: row.email || row.name || row.id, value: row.id })),
    getUsers().then(rows => rows.map(row => ({ label: row.username, value: row.id }))),
  ])
  const targets = [groups, accounts, users]
  results.forEach((result, index) => {
    if (result.status === 'fulfilled')
      targets[index]!.value = result.value
    else
      error.value = '部分筛选选项加载失败，请重试。'
  })
  loading.value = false
}

function reset() {
  filters.value = { groupId: '', accountId: '', userId: '', model: '', clientTransport: '' }
}
onMounted(() => void load())
</script>

<template>
  <div class="space-y-2" role="group" aria-label="请求明细筛选">
    <div class="grid grid-cols-1 items-end gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-[repeat(5,minmax(0,1fr))_auto]">
      <FormItem label="分组">
        <BaseSelect v-model="filters.groupId" class="w-full" aria-label="筛选分组" :disabled="loading" :options="[{ label: '全部分组', value: '' }, ...groups]" />
      </FormItem>
      <FormItem v-if="!personal" label="账号">
        <BaseSelect v-model="filters.accountId" class="w-full" aria-label="筛选账号" :disabled="loading" :options="[{ label: '全部账号', value: '' }, ...accounts]" />
      </FormItem>
      <FormItem v-if="!personal" label="用户">
        <BaseSelect v-model="filters.userId" class="w-full" aria-label="筛选用户" :disabled="loading" :options="[{ label: '全部用户', value: '' }, ...users]" />
      </FormItem>
      <FormItem label="模型">
        <BaseInput v-model="filters.model" class="w-full" aria-label="筛选模型" placeholder="模型名称（精确匹配）" />
      </FormItem>
      <FormItem label="接入类型">
        <BaseSelect v-model="filters.clientTransport" class="w-full" aria-label="筛选接入类型" :options="transports" />
      </FormItem>
      <BaseButton :disabled="!active" @click="reset">
        重置筛选
      </BaseButton>
    </div>
    <p v-if="error" role="alert" class="m-0 flex items-center gap-2 text-cp-xs text-cp-error">
      {{ error }} <BaseButton size="sm" :loading="loading" @click="load">
        重新加载
      </BaseButton>
    </p>
  </div>
</template>
