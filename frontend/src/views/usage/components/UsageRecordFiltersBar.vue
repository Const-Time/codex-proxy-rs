<script setup lang="ts">
import type { Ref } from 'vue'
import type { UsageRecordFilters } from '@/api'
import type { SelectOption } from '@/components/base/BaseSelect.vue'
import { computed, inject, onMounted, reactive, shallowRef } from 'vue'
import { getUsageCatalogOptions, getUsageKeyOptions } from '@/api/modules/operations'
import { getUsers } from '@/api/modules/users'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseFilterPanel from '@/components/base/BaseFilterPanel.vue'
import FormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseSelect from '@/components/base/BaseSelect.vue'
import { errorMessage } from '@/utils/async'

const personal = inject<Readonly<Ref<boolean>>>('personalUsage')
const filters = defineModel<UsageRecordFilters>({ required: true })
const groups = shallowRef<SelectOption[]>([])
const accounts = shallowRef<SelectOption[]>([])
const users = shallowRef<SelectOption[]>([])
const usernames = shallowRef<SelectOption[]>([])
const keys = shallowRef<SelectOption[]>([])
const pending = reactive({ keys: false, groups: false, accounts: false, users: false })
const errors = reactive({ keys: '', groups: '', accounts: '', users: '' })
const loading = computed(() => Object.values(pending).some(Boolean))
const error = computed(() => Object.values(errors).filter(Boolean).join('；'))
const active = computed(() => Object.values(filters.value).some(value => value.trim()))
const transports = computed(() => [
  { label: '全部接入类型', value: '' },
  { label: 'HTTP', value: 'http_json' },
  { label: 'SSE', value: 'http_sse' },
  { label: 'WebSocket', value: 'websocket' },
])

async function loadOption(
  kind: keyof typeof pending,
  label: string,
  target: Ref<SelectOption[]>,
  fetch: () => Promise<SelectOption[]>,
) {
  pending[kind] = true
  errors[kind] = ''
  try {
    target.value = await fetch()
  }
  catch (reason) {
    errors[kind] = `${label}选项加载失败：${errorMessage(reason, '请重试')}`
  }
  finally {
    pending[kind] = false
  }
}

async function load() {
  if (loading.value)
    return
  const isPersonal = Boolean(personal?.value)
  const catalogs = [
    loadOption('keys', '密钥', keys, () => getUsageKeyOptions(isPersonal).then(rows => rows.map(key => ({ value: key.id, label: `${key.name} · ${key.id.slice(-8)}` })))),
    loadOption('groups', '分组', groups, () => getUsageCatalogOptions('groups', isPersonal).then(rows => rows.map(row => ({ label: row.name, value: row.id })))),
  ]
  if (!isPersonal) {
    catalogs.push(
      loadOption('accounts', '账号', accounts, () => getUsageCatalogOptions('accounts', false).then(rows => rows.map(row => ({ label: row.name, value: row.id })))),
      loadOption('users', '邮箱 / 用户名', users, () => getUsers().then((rows) => {
        usernames.value = rows.map(row => ({ label: row.username ? `${row.username} · ${row.email}` : `${row.email}（未设用户名）`, value: row.id }))
        return rows.map(row => ({ label: row.email, value: row.id }))
      })),
    )
  }
  await Promise.all(catalogs)
}

function reset() {
  filters.value = { clientApiKeyId: '', groupId: '', accountId: '', userId: '', model: '', clientTransport: '' }
}
onMounted(() => void load())
</script>

<template>
  <BaseFilterPanel :active-count="Object.values(filters).filter(value => value.trim()).length">
    <div class="space-y-2" role="group" aria-label="请求明细筛选">
      <div class="grid grid-cols-1 items-end gap-3 sm:grid-cols-2 lg:grid-cols-3 2xl:grid-cols-[repeat(7,minmax(0,1fr))_auto]">
        <FormItem label="分组">
          <BaseSelect v-model="filters.groupId" class="w-full" aria-label="筛选分组" :disabled="pending.groups" :options="[{ label: '全部分组', value: '' }, ...groups]" />
        </FormItem>
        <FormItem v-if="!personal" label="账号">
          <BaseSelect v-model="filters.accountId" class="w-full" aria-label="筛选账号" :disabled="pending.accounts" :options="[{ label: '全部账号', value: '' }, ...accounts]" />
        </FormItem>
        <FormItem v-if="!personal" label="邮箱">
          <BaseSelect v-model="filters.userId" class="w-full" aria-label="按邮箱筛选" :disabled="pending.users" :options="[{ label: '全部邮箱', value: '' }, ...users]" />
        </FormItem>
        <FormItem v-if="!personal" label="用户名">
          <BaseSelect v-model="filters.userId" class="w-full" aria-label="按用户名筛选" :disabled="pending.users" :options="[{ label: '全部用户名', value: '' }, ...usernames]" />
        </FormItem>
        <FormItem label="密钥">
          <BaseSelect v-model="filters.clientApiKeyId" class="w-full" aria-label="按密钥筛选" :disabled="pending.keys" :options="[{ label: '全部密钥', value: '' }, ...keys]" />
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
  </BaseFilterPanel>
</template>
