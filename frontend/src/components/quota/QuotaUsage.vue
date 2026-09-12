<script setup lang="ts">
import type { QuotaUsageBudget } from './usage'
import { Clock3 } from '@lucide/vue'
import { computed } from 'vue'
import { useUiClock } from '@/composables/useUiClock'
import { formatDateTime } from '@/utils/date'
import { quotaResetLabel, quotaUsagePeriods } from './usage'

const props = defineProps<{ budget: QuotaUsageBudget, label: string }>()
const now = useUiClock()
const periods = computed(() => quotaUsagePeriods(props.budget))
const money = (value: string) => `$${Number(value).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 4 })}`
</script>

<template>
  <div class="@container/quota min-w-0">
    <div class="grid gap-4">
      <div
        v-for="period in periods" :key="period.key"
        class="grid grid-cols-[2rem_minmax(0,1fr)] items-center gap-x-3 gap-y-1.5 @min-[18rem]/quota:grid-cols-[2rem_minmax(0,1fr)_auto]"
      >
        <span class="col-start-1 row-start-1 text-cp-xs font-emphasis text-cp-text-secondary">{{ period.label }}</span>
        <span class="col-start-2 row-start-1 text-right font-mono text-cp-sm whitespace-nowrap tabular-nums @min-[18rem]/quota:col-start-3">
          <span class="text-cp-text-secondary">{{ money(period.used) }}</span>
          <span class="mx-1 text-cp-text-tertiary">/</span>
          <strong class="font-bold text-cp-text">{{ period.percent === null ? '不限' : money(period.limit) }}</strong>
        </span>
        <div
          v-if="period.percent !== null"
          class="col-start-2 row-start-2 h-2 overflow-hidden rounded-full bg-cp-fill-tertiary @min-[18rem]/quota:row-start-1"
          role="meter" :aria-label="`${label}${period.label}额度`"
          :aria-valuenow="period.percent" :aria-valuemin="0" :aria-valuemax="100"
          :aria-valuetext="`已用 ${money(period.used)}，限额 ${money(period.limit)}`"
        >
          <div
            class="h-full rounded-full transition-[width] duration-300 motion-reduce:transition-none"
            :class="period.tone === 'danger' ? 'bg-cp-error' : period.tone === 'warning' ? 'bg-cp-warning' : 'bg-cp-success'"
            :style="{ width: `${period.percent}%` }"
          />
        </div>
        <span
          v-if="period.percent !== null"
          class="col-start-2 row-start-3 inline-flex min-w-0 items-center gap-1 text-cp-xs text-cp-primary @min-[18rem]/quota:col-span-2 @min-[18rem]/quota:row-start-2"
          :title="period.resetsAt ? `重置时间：${formatDateTime(period.resetsAt)}` : undefined"
        >
          <Clock3 class="size-3 shrink-0" aria-hidden="true" />
          {{ quotaResetLabel(period.resetsAt, now.getTime()) }}
        </span>
      </div>
    </div>
  </div>
</template>
