<script setup lang="ts">
import type { UsageErrorOrigin, UsageRecordFilters } from '@/api'
import { Eye } from '@lucide/vue'
import { computed, defineAsyncComponent, nextTick, provide, ref, shallowRef, useTemplateRef, watch } from 'vue'

import BaseCard from '@/components/base/BaseCard.vue'
import BaseIconButton from '@/components/base/BaseIconButton.vue'
import BasePageHeader from '@/components/base/BasePageHeader.vue'
import BaseSegmented from '@/components/base/BaseSegmented.vue'
import BaseSelect from '@/components/base/BaseSelect.vue'
import BaseTablePagination from '@/components/base/BaseTable/BaseTablePagination.vue'
import ProviderFilterSegmented from '@/components/ProviderFilterSegmented.vue'
import UsageDateRangePicker from './components/UsageDateRangePicker.vue'
import UsageFilters from './components/UsageFilters.vue'
import UsageInsightsGrid from './components/UsageInsightsGrid.vue'
import UsageRecordFiltersBar from './components/UsageRecordFiltersBar.vue'
import UsageRecordsTable from './components/UsageRecordsTable.vue'
import UsageSummaryCards from './components/UsageSummaryCards.vue'
import UserRankingPanel from './components/UserRankingPanel.vue'
import { useUsageRecordDetail } from './composables/useUsageRecordDetail'
import { useUsageRecordsTable } from './composables/useUsageRecordsTable'
import { useUsageTimeRange } from './composables/useUsageTimeRange'
import { usageRecordColumns } from './constants'
import { errorOriginText } from './utils/opsErrorPresentation'

const props = defineProps<{ personal?: boolean }>()
const OpsErrorPanel = defineAsyncComponent(() => import('./components/OpsErrorPanel.vue'))
const UsageRecordDetailModal = defineAsyncComponent(() => import('./components/UsageRecordDetailModal.vue'))
provide('personalUsage', computed(() => Boolean(props.personal)))
const recordView = shallowRef('success')
const tableModeControl = useTemplateRef<InstanceType<typeof BaseSegmented>>('tableModeControl')
const recordToolbar = useTemplateRef<HTMLDivElement>('recordToolbar')
const recordScrollMaxHeight = 'calc(clamp(56rem, 82dvh, 64rem) - 3.5rem)'

async function setRecordView(view: string) {
  const group = tableModeControl.value?.$el
  const shouldRestoreFocus = group?.contains(document.activeElement)
  recordView.value = view
  if (shouldRestoreFocus) {
    await nextTick()
    tableModeControl.value?.$el.querySelector('[aria-checked="true"]')?.focus()
  }
}
const visibleColumns = computed(() => {
  const columns = props.personal
    ? usageRecordColumns.filter(column => !['userEmail', 'accountEmail', 'upstreamTransport', 'turnState'].includes(column.key))
    : usageRecordColumns
  return recordView.value === 'all'
    ? [{ key: 'logicalOutcome', label: '结果', kind: 'status' as const, size: 'sm' as const }, ...columns]
    : columns
})

const recordFilters = ref<UsageRecordFilters>({ clientApiKeyId: '', groupId: '', accountId: '', userId: '', model: '', clientTransport: '' })
const recordFilterParams = computed(() => Object.fromEntries(Object.entries(recordFilters.value).map(([key, value]) => [key, value.trim() || undefined])))
const recordViewOptions = [
  { label: '成功', value: 'success' },
  { label: '错误', value: 'errors' },
  { label: '全部', value: 'all' },
]
const errorOrigin = shallowRef<UsageErrorOrigin | ''>('')
const errorOrigins: UsageErrorOrigin[] = ['upstream', 'proxy', 'connection', 'unknown']
const errorOriginOptions = [
  { label: '全部来源', value: '' },
  ...errorOrigins.map(value => ({ label: errorOriginText(value), value })),
]
const { timeRange, timeRangeParams, refreshTimeRangeEnd, customDates, setCustomRange }
  = useUsageTimeRange()

const {
  currentPage,
  providerQuery,
  usagePagination,
  loading,
  analyticsLoading,
  diagnosticsLoading,
  records,
  summary,
  insights,
  refreshingList,
  diagnosticDimension,
  performanceMetric,
  healthLoading,
  costLoading,
  performanceLoading,
  loadUsageRecords,
  refreshUsageRecords,
  handlePageChange,
  handlePageSizeChange,
} = useUsageRecordsTable({
  timeRangeParams,
  refreshTimeRangeEnd,
  recordFilters: recordFilterParams,
  recordView,
  personal: props.personal,
})

const { showDetailModal, selectedUsageRecord, handleViewDetail } = useUsageRecordDetail(Boolean(props.personal))
const detailModalLoaded = shallowRef(false)
watch(showDetailModal, (open) => {
  if (open)
    detailModalLoaded.value = true
})

watch([timeRange, customDates], () => {
  refreshTimeRangeEnd()
  currentPage.value = 1
  void loadUsageRecords()
})
</script>

<template>
  <div class="w-full">
    <BasePageHeader :title="personal ? '使用记录' : '使用统计'" description="查看请求用量、性能趋势与调用错误记录">
      <template #actions>
        <UsageDateRangePicker v-model="timeRange" @apply="setCustomRange" />
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
      v-model:performance-metric="performanceMetric"
      :health-loading="healthLoading"
      :cost-loading="costLoading"
      :performance-loading="performanceLoading"
      :overview="insights.overview"
      :diagnostics="insights.diagnostics"
      :loading="analyticsLoading"
      :diagnostics-loading="diagnosticsLoading"
    >
      <template v-if="!personal" #ranking>
        <UserRankingPanel :range="timeRangeParams" :provider="providerQuery" :filters="recordFilterParams" />
      </template>
    </UsageInsightsGrid>

    <BaseCard
      class="mt-5 min-w-0"
    >
      <template #header>
        <h2 class="m-0 text-xl leading-[1.15] font-heavy text-cp-text">
          请求明细
        </h2>
      </template>

      <template #body>
        <UsageRecordFiltersBar v-model="recordFilters" class="mb-3">
          <div ref="recordToolbar" class="ml-auto min-w-0 max-w-full" />
        </UsageRecordFiltersBar>
        <div
          v-if="recordView !== 'errors'"
          class="usage-record-list flex min-w-0 flex-col"
        >
          <div class="flex min-h-0 min-w-0 flex-1 flex-col">
            <UsageRecordsTable
              class="min-h-0 flex-1"
              :columns="visibleColumns"
              :rows="records"
              :all="recordView === 'all'"
              :loading="loading"
              :toolbar-target="recordToolbar"
              :scroll-max-height="recordScrollMaxHeight"
              empty-text="没有匹配的请求记录"
            >
              <template #toolbar-actions>
                <BaseSegmented ref="tableModeControl" :model-value="recordView" label="请求明细类型" :options="recordViewOptions" class="w-34 shrink-0 [&>button]:px-1.5" @update:model-value="setRecordView" />
                <UsageFilters
                  :loading="loading"
                  :refreshing="refreshingList"
                  @refresh="refreshUsageRecords"
                />
              </template>
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

        <div v-else class="usage-record-list min-w-0">
          <OpsErrorPanel
            :time-range-params="timeRangeParams"
            :record-filters="recordFilterParams"
            :provider="providerQuery"
            :error-origin="personal ? undefined : errorOrigin || undefined"
            :toolbar-target="recordToolbar"
            :scroll-max-height="recordScrollMaxHeight"
          >
            <template #toolbar-actions="{ loading: errorsLoading, refreshing, refresh }">
              <BaseSelect v-if="!personal" v-model="errorOrigin" :options="errorOriginOptions" aria-label="错误来源" class="w-30" />
              <BaseSegmented ref="tableModeControl" :model-value="recordView" label="请求明细类型" :options="recordViewOptions" class="w-34 shrink-0 [&>button]:px-1.5" @update:model-value="setRecordView" />
              <UsageFilters :loading="errorsLoading" :refreshing="refreshing" label="刷新错误明细" @refresh="refresh" />
            </template>
          </OpsErrorPanel>
        </div>
      </template>
    </BaseCard>

    <UsageRecordDetailModal v-if="detailModalLoaded" v-model="showDetailModal" :record="selectedUsageRecord" />
  </div>
</template>
