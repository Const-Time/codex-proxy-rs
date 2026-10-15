<script setup lang="ts">
import { RefreshCw } from '@lucide/vue'
import { ref } from 'vue'
import BaseButton from '../../src/components/base/BaseButton.vue'
import BaseCard from '../../src/components/base/BaseCard.vue'
import BaseIconButton from '../../src/components/base/BaseIconButton.vue'
import BaseInput from '../../src/components/base/BaseInput.vue'
import BaseTable from '../../src/components/base/BaseTable/index.vue'
import AccountFilters from '../../src/views/accounts/components/AccountFilters.vue'
import AccountToolbarActions from '../../src/views/accounts/components/AccountToolbarActions.vue'
import ApiKeyFilters from '../../src/views/api-keys/components/ApiKeyFilters.vue'
import UsageFilters from '../../src/views/usage/components/UsageFilters.vue'

const refreshCount = ref(0)
const search = ref('')
const status = ref('')
const provider = ref('')
const group = ref('')
const selected = ref(false)
const columns = [{ key: 'name', label: '名称' }, { key: 'status', label: '状态' }]
const rows = [{ id: 'fixture', name: '布局测试（无服务器请求）', status: '可用' }]
</script>

<template>
  <main class="grid gap-4 p-4">
    <h1>表格工具栏回归检查</h1>
    <p>刷新次数：{{ refreshCount }}</p>
    <BaseButton @click="selected = !selected">
      切换批量选中
    </BaseButton>
    <BaseCard title="请求明细">
      <BaseTable table-id="fixture-usage" class="h-auto! min-h-40" :columns="columns" :rows="rows">
        <template #toolbar-actions>
          <UsageFilters :loading="false" :refreshing="false" @refresh="refreshCount++" />
        </template>
      </BaseTable>
    </BaseCard>
    <BaseCard title="账号管理">
      <BaseTable table-id="fixture-accounts" class="h-auto! min-h-40" :columns="columns" :rows="rows">
        <template #toolbar-actions>
          <AccountToolbarActions :selected-count="selected ? 25 : 0" :batch-deleting="false" :exporting-accounts="false" />
        </template>
        <template #toolbar>
          <AccountFilters v-model:search="search" v-model:status="status" v-model:provider="provider" v-model:group="group" :groups="[]" :groups-loading="false" />
        </template>
      </BaseTable>
    </BaseCard>
    <BaseCard title="密钥管理">
      <BaseTable table-id="fixture-keys" class="h-auto! min-h-40" :columns="columns" :rows="rows">
        <template #toolbar-actions>
          <ApiKeyFilters :selected-count="selected ? 25 : 0" :batch-deleting="false" />
        </template>
      </BaseTable>
    </BaseCard>
    <BaseCard title="代理 / 订阅管理">
      <BaseTable table-id="fixture-search" class="h-auto! min-h-40" :columns="columns" :rows="rows">
        <template #toolbar>
          <BaseInput v-model="search" aria-label="搜索" placeholder="搜索" class="w-full sm:w-80" />
        </template>
        <template #toolbar-actions>
          <BaseIconButton label="刷新列表" @click="refreshCount++">
            <RefreshCw class="size-4" />
          </BaseIconButton>
          <BaseButton>新增 / 重置选中</BaseButton>
        </template>
      </BaseTable>
    </BaseCard>
    <BaseCard title="无列设置 / 空表">
      <BaseTable class="h-auto! min-h-40" :columns="columns" :rows="[]" empty-text="空表仍可刷新">
        <template #toolbar-actions>
          <BaseIconButton label="刷新空表" @click="refreshCount++">
            <RefreshCw class="size-4" />
          </BaseIconButton>
        </template>
      </BaseTable>
    </BaseCard>
  </main>
</template>
