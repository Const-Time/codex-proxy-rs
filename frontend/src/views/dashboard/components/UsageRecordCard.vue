<script setup lang="ts">
import type { dashboardSnapshotView } from '../composables/useDashboard'

import BaseCard from '@/components/base/BaseCard.vue'
import UsageRecordsTable from '@/views/usage/components/UsageRecordsTable.vue'
import { usageRecordColumns } from '@/views/usage/constants'

type DashboardSnapshot = ReturnType<typeof dashboardSnapshotView>

defineProps<{
  rows: DashboardSnapshot['usageRecords']
}>()

const dashboardUsageRecordColumns = usageRecordColumns.filter(column => column.key !== 'actions')
</script>

<template>
  <BaseCard
    as="article"
    title="使用记录"
    description="最近 10 条成功请求"
    class="min-w-0 w-full"
  >
    <template #body>
      <UsageRecordsTable
        class="min-w-0"
        :columns="dashboardUsageRecordColumns"
        :rows="rows"
        empty-text="暂无成功记录"
      />
    </template>
  </BaseCard>
</template>
