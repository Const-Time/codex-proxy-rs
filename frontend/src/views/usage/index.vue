<script setup lang="ts">
import type { UsageRecordFilters } from '@/api'
import { Eye } from '@lucide/vue'
import { computed, provide, ref, shallowRef, watch } from 'vue'

import BaseCard from '@/components/base/BaseCard.vue'
import BaseIconButton from '@/components/base/BaseIconButton.vue'
import BasePageHeader from '@/components/base/BasePageHeader.vue'
import BaseSegmented from '@/components/base/BaseSegmented.vue'
import BaseSelect from '@/components/base/BaseSelect.vue'
import BaseTablePagination from '@/components/base/BaseTable/BaseTablePagination.vue'
import ProviderFilterSegmented from '@/components/ProviderFilterSegmented.vue'
import OpsErrorPanel from './components/OpsErrorPanel.vue'
import UsageFilters from './components/UsageFilters.vue'
import UsageInsightsGrid from './components/UsageInsightsGrid.vue'
import UsageRecordDetailModal from './components/UsageRecordDetailModal.vue'
import UsageRecordFiltersBar from './components/UsageRecordFiltersBar.vue'
import UsageRecordsTable from './components/UsageRecordsTable.vue'
import UsageSummaryCards from './components/UsageSummaryCards.vue'
import UserRankingPanel from './components/UserRankingPanel.vue'
import { useUsageRecordDetail } from './composables/useUsageRecordDetail'
import { useUsageRecordsTable } from './composables/useUsageRecordsTable'
import { useUsageTimeRange } from './composables/useUsageTimeRange'
import { usageRecordColumns, usageTimeRangeOptions } from './constants'

const props = defineProps<{ personal?: boolean }>()
provide('personalUsage', computed(() => Boolean(props.personal)))
const visibleColumns = computed(() => props.personal ? usageRecordColumns.filter(column => !['userEmail', 'accountEmail', 'upstreamTransport'].includes(column.key)) : usageRecordColumns)

const recordFilters = ref<UsageRecordFilters>({ clientApiKeyId: '', groupId: '', accountId: '', userId: '', model: '', clientTransport: '' })
const recordFilterParams = computed(() => Object.fromEntries(Object.entries(recordFilters.value).map(([key, value]) => [key, value.trim() || undefined])))
const recordView = shallowRef('success')
const recordViewOptions = [
  { label: '成功记录', value: 'success' },
  { label: '错误排查', value: 'errors' },
]
const { timeRange, timeRangeParams, refreshTimeRangeEnd }
  = useUsageTimeRange()

const {
  currentPage,
  providerQuery,
  usagePagination,
  loading,
  analyticsLoading,
  records,
  summary,
  insights,
  refreshingList,
  diagnosticDimension,
  loadUsageRecords,
  refreshUsageRecords,
  handlePageChange,
  handlePageSizeChange,
} = useUsageRecordsTable({
  timeRangeParams,
  refreshTimeRangeEnd,
  recordFilters: recordFilterParams,
  personal: props.personal,
})

const { showDetailModal, selectedUsageRecord, handleViewDetail } = useUsageRecordDetail(Boolean(props.personal))

watch(timeRange, () => {
  refreshTimeRangeEnd()
  currentPage.value = 1
  void loadUsageRecords()
})
</script>

<template>
  <div class="w-full">
    <BasePageHeader :title="personal ? '使用记录' : '使用统计'" description="查看请求用量、性能趋势与调用错误记录">
      <template #actions>
        <BaseSelect v-model="timeRange" :options="usageTimeRangeOptions" class="w-34" />
        <ProviderFilterSegmented
          v-model="providerQuery"
          :disabled="refreshingList"
          class="w-31 shrink-0"
        />
      </template>
    </BasePageHeader>

    <UsageSummaryCards :summary="summary" />
    <UsageInsightsGrid
      v-model:diagnostic-dimension="diagnosticDimension"
      :overview="insights.overview"
      :diagnostics="insights.diagnostics"
      :loading="analyticsLoading"
    >
      <template v-if="!personal" #ranking>
        <UserRankingPanel :range="timeRangeParams" :provider="providerQuery" :filters="recordFilterParams" />
      </template>
    </UsageInsightsGrid>

    <BaseCard
      class="mt-5 flex flex-col"
    >
      <template #header>
        <div class="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h2 class="m-0 text-xl leading-[1.15] font-heavy text-cp-text">
              请求明细
            </h2>
            <p
              class="mt-1.75 mb-0 text-cp leading-[1.15] font-emphasis text-cp-text-secondary"
            >
              成功请求与失败请求明细
            </p>
          </div>
          <BaseSegmented v-model="recordView" label="请求明细类型" :options="recordViewOptions" class="w-52" />
        </div>
      </template>

      <template #body>
        <UsageRecordFiltersBar v-model="recordFilters" class="mb-4" />
        <div
          v-show="recordView === 'success'"
          class="grid min-h-130 min-w-0 flex-1 grid-rows-[auto_minmax(0,1fr)] gap-3"
        >
          <UsageFilters

            :loading="loading"
            :refreshing="refreshingList"
            @refresh="refreshUsageRecords"
          />

          <div class="flex min-h-0 min-w-0 flex-col">
            <UsageRecordsTable
              class="min-h-0 flex-1"
              :columns="visibleColumns"
              :rows="records"
              :loading="loading"
              empty-text="暂无使用记录"
            >
              <template #actions="{ row }">
                <div class="flex items-center justify-start">
                  <BaseIconButton
                    variant="ghost"
                    size="sm"
                    label="查看使用记录详情"
                    @click="handleViewDetail(row)"
                  >
                    <Eye class="size-3.5" />
                  </BaseIconButton>
                </div>
              </template>
            </UsageRecordsTable>
            <BaseTablePagination
              :pagination="usagePagination"
              :loading="loading"
              @page-change="handlePageChange"
              @page-size-change="handlePageSizeChange"
            />
          </div>
        </div>

        <div v-show="recordView === 'errors'" class="min-h-130 min-w-0 flex-1">
          <OpsErrorPanel :time-range-params="timeRangeParams" :record-filters="recordFilterParams" :provider="providerQuery" />
        </div>
      </template>
    </BaseCard>

    <UsageRecordDetailModal v-model="showDetailModal" :record="selectedUsageRecord" />
  </div>
</template>
