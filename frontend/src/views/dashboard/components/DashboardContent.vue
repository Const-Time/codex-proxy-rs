<script setup lang="ts">
import type { dashboardSnapshotView, dashboardTrendView } from '../composables/useDashboard'
import type { DashboardTrendKind } from '@/api/modules/dashboard'
import { RefreshCw } from '@lucide/vue'

import BaseButton from '@/components/base/BaseButton.vue'
import BaseIconButton from '@/components/base/BaseIconButton.vue'
import BasePageHeader from '@/components/base/BasePageHeader.vue'

import AccountOverviewCard from './AccountOverviewCard.vue'
import DashboardHeartbeat from './DashboardHeartbeat.vue'
import MetricCard from './MetricCard.vue'
import RequestHealthTimelineCard from './RequestHealthTimelineCard.vue'
import RequestTrendCard from './RequestTrendCard.vue'
import UsageRecordCard from './UsageRecordCard.vue'
import WireProfileCard from './WireProfileCard.vue'

type DashboardSnapshotView = ReturnType<typeof dashboardSnapshotView>
type DashboardTrendView = ReturnType<typeof dashboardTrendView>

withDefaults(
  defineProps<{
    loading?: boolean
    refreshing?: boolean
    lastRefreshedAt?: string
    metrics: DashboardSnapshotView['metrics']
    trendPoints: DashboardTrendView['points']
    trendSummary: DashboardTrendView['summary']
    trendLoading?: boolean
    trendError?: string
    summaryError?: string
    healthTimeline: DashboardSnapshotView['healthTimeline']
    accountUsage: DashboardSnapshotView['accountUsage']
    wireProfiles: DashboardSnapshotView['wireProfiles']
    usageRecords: DashboardSnapshotView['usageRecords']
    poolSummary: DashboardSnapshotView['poolSummary']
    capacityInfo: DashboardSnapshotView['capacityInfo']
    rotationStrategy: DashboardSnapshotView['rotationStrategy']
  }>(),
  {
    loading: false,
    refreshing: false,
    lastRefreshedAt: '',
    trendLoading: false,
    trendError: '',
    summaryError: '',
  },
)

const emit = defineEmits<{
  refresh: []
  trendChange: [kind: DashboardTrendKind]
}>()

const trendKind = defineModel<DashboardTrendKind>('trendKind', { required: true })
</script>

<template>
  <div class="w-full">
    <BasePageHeader title="系统概览">
      <template #description>
        <span>当日统计</span>
        <DashboardHeartbeat :updated-at="lastRefreshedAt" />
      </template>
      <template #actions>
        <BaseIconButton
          class="text-cp-primary-text"
          size="md"
          label="刷新概览"
          :loading="loading || refreshing"
          :disabled="loading || refreshing"
          @click="emit('refresh')"
        >
          <template #loading>
            <RefreshCw class="animate-spin motion-reduce:animate-none" :size="19" />
          </template>
          <RefreshCw :size="19" />
        </BaseIconButton>
      </template>
    </BasePageHeader>

    <div v-if="summaryError" class="mt-3 flex min-w-0 flex-wrap items-center justify-between gap-3 rounded-cp-lg bg-cp-warning-container px-4 py-3 text-cp-sm text-cp-warning-on-container" role="alert">
      <span>{{ lastRefreshedAt ? '刷新失败，显示上次更新的数据。' : '概览加载失败。' }} {{ summaryError }}</span>
      <BaseButton variant="secondary" size="sm" :disabled="loading || refreshing" @click="emit('refresh')">
        重试
      </BaseButton>
    </div>

    <div v-if="lastRefreshedAt || !summaryError" :aria-busy="loading" class="min-w-0">
      <section
        class="cp-mobile-metrics mt-6 grid grid-cols-1 gap-4 md:grid-cols-2 2xl:grid-cols-4 2xl:gap-6"
        aria-label="核心指标"
      >
        <MetricCard v-for="metric in metrics" :key="metric.title" :metric="metric" />
      </section>

      <section
        class="mt-6 grid grid-cols-1 gap-6 2xl:grid-cols-[minmax(0,948fr)_minmax(0,608fr)] 2xl:gap-7"
      >
        <RequestTrendCard
          v-model:kind="trendKind"
          :points="trendPoints"
          :summary="trendSummary"
          :loading="trendLoading"
          :error="trendError"
          @trend-change="emit('trendChange', $event)"
        />
        <WireProfileCard :profiles="wireProfiles" />
      </section>

      <AccountOverviewCard
        :accounts="accountUsage"
        :pool="poolSummary"
        :capacity="capacityInfo"
        :rotation-strategy="rotationStrategy"
        class="mt-6"
      />

      <RequestHealthTimelineCard :timeline="healthTimeline" class="mt-6" />

      <UsageRecordCard :rows="usageRecords" class="mt-6" />
    </div>
  </div>
</template>
