<script setup lang="ts">
import type { UsageDisplayRecord } from '../../src/views/usage/utils/records'
import { ref } from 'vue'
import BaseButton from '../../src/components/base/BaseButton.vue'
import BaseSelect from '../../src/components/base/BaseSelect.vue'
import { useThemeStore } from '../../src/stores/modules/theme'
import UsageRecordsTable from '../../src/views/usage/components/UsageRecordsTable.vue'
import { usageRecordColumns } from '../../src/views/usage/constants'

const theme = useThemeStore()
const sample = ref('all')
const columns = usageRecordColumns.filter(column => ['model', 'latency', 'generationRate'].includes(column.key))
const rows = [
  { id: 'one', model: '示例模型 A', latencyMs: 13000, firstTokenLatencyMs: 9030, tokenDetails: { outputTokens: 147 } },
  { id: 'two', model: '示例模型 B', latencyMs: 7490, firstTokenLatencyMs: 3640, tokenDetails: { outputTokens: 169 } },
  { id: 'missing', model: '无首字采样', latencyMs: 12000, firstTokenLatencyMs: null, latencyDetails: { firstEventMs: 300 }, tokenDetails: { outputTokens: 100 } },
] as UsageDisplayRecord[]
</script>

<template>
  <main class="min-h-screen bg-cp-bg-layout p-6 text-cp-text">
    <div class="mx-auto max-w-3xl rounded-cp-lg bg-cp-bg-container p-5">
      <h1 class="mb-2 text-xl font-heavy">
        请求记录 · 独立速率列
      </h1>
      <p class="mb-4 text-cp-text-secondary">
        合成测试数据，不连接生产服务。
      </p>
      <BaseButton @click="theme.toggleTheme()">
        切换明暗
      </BaseButton>
      <BaseSelect v-model="sample" aria-label="样本筛选测试" class="ml-3" :options="[{ label: '全部样本', value: 'all' }, { label: '有效样本', value: 'valid' }]" />
      <UsageRecordsTable class="mt-4" :columns="columns" :rows="rows" />
    </div>
  </main>
</template>
