<script setup lang="ts">
import type { AccountQuotaWindow } from '../../constants'

import { computed } from 'vue'
import { useUiClock } from '@/composables/useUiClock'
import AccountRequestTimeline from '../AccountUsageWindow/AccountRequestTimeline.vue'
import {
  quotaWindowLocalUsageDisplay,
  quotaWindowPresentation,
  resolveAccountUsageWindowPresentation,
} from '../AccountUsageWindow/presenter'
import { quotaEstimatePresentation } from './estimate'

const props = defineProps<{
  label: string | null
  windows: AccountQuotaWindow[]
}>()

const now = useUiClock()
const grouped = computed(() => props.windows.length > 1 && Boolean(props.label))
const items = computed(() => props.windows.map((window) => {
  const presentation = quotaWindowPresentation(window, '4px')
  const estimate = quotaEstimatePresentation(window.estimatedQuota, window.estimateHint)
  const view = resolveAccountUsageWindowPresentation({
    window,
    variant: 'detail',
    showLocalValue: true,
    now: now.value.getTime(),
  })

  return {
    key: window.key,
    local: view.mode === 'local',
    localLabel: view.local.label,
    requestDisplay: view.local.requestDisplay,
    requestBars: view.local.requestBars,
    timelineTitle: view.local.timelineTitle,
    durationDisplay: view.local.durationDisplay,
    label: grouped.value
      ? window.windowLabelDisplay
      : window.labelDisplay,
    ariaLabel: window.labelDisplay,
    usedPercent: window.usedPercent,
    usedPercentDisplay: window.usedPercentDisplay,
    localUsageDisplay: quotaWindowLocalUsageDisplay(window),
    resetAtDisplay: window.resetAtDisplay,
    showEstimate: Boolean(window.localUsage),
    estimateDisplay: window.estimatedQuota?.totalUsd != null
      ? Number(window.estimatedQuota.totalUsd).toLocaleString('en-US', { style: 'currency', currency: 'USD', minimumFractionDigits: 2, maximumFractionDigits: 2 })
      : '待估算',
    estimateBilledDisplay: window.estimatedQuota?.billedTotalUsd != null
      ? Number(window.estimatedQuota.billedTotalUsd).toLocaleString('en-US', { style: 'currency', currency: 'USD', minimumFractionDigits: 2, maximumFractionDigits: 2 })
      : '待估算',
    estimateTokens: window.estimatedQuota?.estimatedTokens != null ? new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 2 }).format(window.estimatedQuota.estimatedTokens) : '待估算',
    remainingTokens: window.estimatedQuota?.remainingTokens != null ? new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 2 }).format(window.estimatedQuota.remainingTokens) : '—',
    estimateTitle: estimate.title,
    estimateBasis: estimate.basis,
    estimateLabel: estimate.label,
    estimateWarning: estimate.warning,
    percentTextClass: presentation.percentTextClass,
    barClass: presentation.barClass,
    barStyle: presentation.barStyle,
  }
}))
</script>

<template>
  <section class="grid min-w-0 gap-2">
    <h4
      v-if="grouped && label"
      class="m-0 truncate text-cp-xs leading-4 font-heavy text-cp-text-secondary"
      :title="label"
    >
      {{ label }}
    </h4>

    <div
      class="grid min-w-0 gap-3"
      :class="grouped ? 'grid-cols-2' : 'grid-cols-1'"
    >
      <div v-for="item in items" :key="item.key" class="grid min-w-0 gap-1.5">
        <template v-if="item.local">
          <div class="flex min-w-0 items-baseline justify-between gap-2 text-cp-xs leading-3.5">
            <span class="min-w-0 truncate font-bold text-cp-text-quaternary">
              {{ item.localLabel }}
            </span>
            <strong
              class="shrink-0 font-mono font-heavy tabular-nums text-cp-text"
              :title="`窗口请求：${item.requestDisplay} 次`"
            >
              {{ item.requestDisplay }} 次
            </strong>
          </div>

          <AccountRequestTimeline
            class="h-1.5 w-full"
            :bars="item.requestBars"
            :label="item.timelineTitle"
          />

          <p class="m-0 flex min-w-0 items-center justify-between gap-2 text-[10px] leading-3.5 text-cp-text-tertiary">
            <span class="shrink-0 font-emphasis">统计范围</span>
            <span
              class="min-w-0 truncate font-mono font-emphasis tabular-nums"
              :title="`滚动 ${item.durationDisplay}`"
            >
              滚动 {{ item.durationDisplay }}
            </span>
          </p>
        </template>
        <template v-else>
          <div class="flex min-w-0 items-baseline justify-between gap-2 text-cp-xs leading-3.5">
            <span class="min-w-0 truncate font-bold text-cp-text-quaternary">
              {{ item.label }}
            </span>
            <span class="flex shrink-0 items-baseline gap-1.5 font-mono font-heavy tabular-nums">
              <strong
                v-if="item.localUsageDisplay"
                class="text-cp-text-quaternary"
                :title="`窗口消耗：${item.localUsageDisplay}`"
              >
                {{ item.localUsageDisplay }}
              </strong>
              <strong
                :class="item.percentTextClass"
                :title="`额度已使用：${item.usedPercentDisplay}`"
              >
                {{ item.usedPercentDisplay }}
              </strong>
            </span>
          </div>

          <div
            class="h-1.5 w-full overflow-hidden rounded-full bg-cp-border-secondary"
            role="progressbar"
            :aria-label="item.ariaLabel"
            aria-valuemin="0"
            aria-valuemax="100"
            :aria-valuenow="item.usedPercent ?? undefined"
            :aria-valuetext="item.usedPercentDisplay"
          >
            <div
              class="h-full rounded-full transition-[width,background-color] duration-200 motion-reduce:transition-none"
              :class="item.barClass"
              :style="item.barStyle"
            />
          </div>

          <p class="m-0 flex min-w-0 items-center justify-between gap-2 text-[10px] leading-3.5 text-cp-text-tertiary">
            <span class="shrink-0 font-emphasis">重置</span>
            <span class="min-w-0 truncate font-mono font-emphasis tabular-nums" :title="item.resetAtDisplay">
              {{ item.resetAtDisplay }}
            </span>
          </p>
          <div v-if="item.showEstimate" class="grid gap-1 rounded bg-cp-fill-quaternary px-2 py-1.5 text-cp-xs" :title="item.estimateTitle">
            <div class="flex flex-wrap items-baseline justify-between gap-x-2 gap-y-1">
              <span class="text-cp-text-secondary">预计周期总额 <span class="text-[10px] text-cp-text-quaternary">· {{ item.estimateLabel }}</span></span>
            </div>
            <div class="flex flex-wrap items-baseline justify-between gap-x-2 gap-y-1">
              <span class="text-cp-text-tertiary">原始费用</span>
              <strong class="font-mono tabular-nums text-cp-text">{{ item.estimateDisplay }}</strong>
            </div>
            <div class="flex flex-wrap items-baseline justify-between gap-x-2 gap-y-1">
              <span class="text-cp-text-tertiary">倍率后计费</span>
              <strong class="font-mono tabular-nums text-cp-green-text">{{ item.estimateBilledDisplay }}</strong>
            </div>
            <div class="flex flex-wrap items-baseline justify-between gap-x-2 gap-y-1">
              <span class="text-cp-text-tertiary">预计 Token / 剩余</span>
              <strong class="font-mono tabular-nums text-cp-text">{{ item.estimateTokens }} / {{ item.remainingTokens }}</strong>
            </div>
            <span class="text-[10px] text-cp-text-tertiary">{{ item.estimateBasis }}</span>
            <span v-if="item.estimateWarning" class="text-[10px] text-cp-text-tertiary">{{ item.estimateWarning }}</span>
          </div>
        </template>
      </div>
    </div>
  </section>
</template>
