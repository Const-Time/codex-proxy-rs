<script setup lang="ts">
import type { AccountRow } from '../../constants'
import { ArrowRight, ChartNoAxesCombined, Info, RefreshCw } from '@lucide/vue'
import { computed, ref, watch } from 'vue'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseEmpty from '@/components/base/BaseEmpty.vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import AccountPlanBadge from '../AccountPlanBadge.vue'
import { quotaEstimatePresentation } from '../AccountQuotaPanel/estimate'
import { forecastCapacityRows } from './presentation'

const props = defineProps<{ account: AccountRow, refreshing: boolean }>()
const emit = defineEmits<{ refreshQuota: [accountId: string] }>()
const open = defineModel<boolean>({ required: true })
const selectedKey = ref('')
const windows = computed(() => props.account.quota.windows.filter(window => window.windowSeconds != null || window.estimatedQuota != null))
const selected = computed(() => windows.value.find(window => window.key === selectedKey.value))
const capacity = computed(() => selected.value ? forecastCapacityRows(selected.value) : [])
const explanation = computed(() => quotaEstimatePresentation(selected.value?.estimatedQuota, selected.value?.estimateHint))
const identity = computed(() => props.account.email || props.account.name || props.account.id)
const progress = computed(() => Math.max(0, Math.min(100, selected.value?.usedPercent ?? 0)))

watch([open, () => props.account.id, windows], () => {
  if (!open.value)
    return
  if (!windows.value.some(window => window.key === selectedKey.value)) {
    selectedKey.value = windows.value.find(window => !window.limitId && window.role === 'secondary')?.key
      ?? windows.value.find(window => window.estimatedQuota)?.key
      ?? windows.value[0]?.key
      ?? ''
  }
}, { immediate: true })
</script>

<template>
  <BaseModal v-model="open" title="额度预测" description="按已记录用量与额度观测，估算完整周期的容量" size="md">
    <div class="grid min-w-0 gap-4">
      <div class="flex min-w-0 items-center gap-2">
        <span class="flex size-9 shrink-0 items-center justify-center rounded-lg bg-cp-primary-container text-cp-primary-on-container">
          <ChartNoAxesCombined class="size-4" />
        </span>
        <strong class="min-w-0 break-all text-cp-sm text-cp-text">{{ identity }}</strong>
        <AccountPlanBadge :plan-type="account.planType" :plan-type-display="account.planTypeDisplay" size="sm" />
      </div>

      <div v-if="windows.length > 1" class="flex flex-wrap gap-1 rounded-lg bg-cp-fill-quaternary p-1" role="group" aria-label="预测额度窗口">
        <button
          v-for="window in windows"
          :key="window.key"
          type="button"
          class="min-h-9 rounded-md px-3 py-2 text-cp-xs font-bold outline-none focus-visible:ring-2 focus-visible:ring-cp-control-outline"
          :class="selectedKey === window.key ? 'bg-cp-bg-container text-cp-text shadow-cp-tertiary' : 'text-cp-text-secondary hover:bg-cp-fill-tertiary'"
          :aria-pressed="selectedKey === window.key"
          @click="selectedKey = window.key"
        >
          {{ window.labelDisplay }}
        </button>
      </div>

      <section v-if="selected" class="grid min-w-0 gap-4 rounded-xl bg-cp-fill-quaternary p-4">
        <div class="flex flex-wrap items-center justify-between gap-2 text-cp-xs text-cp-text-secondary">
          <span>{{ selected.labelDisplay }}已用</span>
          <span class="rounded bg-cp-primary-container px-2 py-1 text-cp-primary-on-container">估算值 · 仅供参考</span>
        </div>
        <div>
          <strong class="font-mono text-3xl font-heavy tabular-nums text-cp-text">{{ selected.usedPercentDisplay }}</strong>
          <div
            class="mt-3 h-2.5 overflow-hidden rounded-full bg-cp-border-secondary"
            role="progressbar"
            :aria-label="`${selected.labelDisplay}已用`"
            :aria-valuenow="selected.usedPercent ?? undefined"
            :aria-valuetext="selected.usedPercentDisplay"
            :aria-valuemin="0"
            :aria-valuemax="100"
          >
            <div class="h-full rounded-full bg-cp-primary" :style="{ width: `${progress}%` }" />
          </div>
          <div class="mt-1.5 flex justify-between font-mono text-[10px] text-cp-text-tertiary">
            <span v-for="tick in [0, 25, 50, 75, 100]" :key="tick">{{ tick }}%</span>
          </div>
        </div>

        <div class="grid gap-2">
          <div v-for="row in capacity" :key="row.label" class="grid min-w-0 gap-2 rounded-lg bg-cp-bg-container p-3">
            <span class="text-cp-xs font-bold text-cp-text-secondary">{{ row.label }}</span>
            <div class="grid grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)] items-center gap-2">
              <div class="min-w-0">
                <span class="text-[10px] text-cp-text-tertiary">{{ row.basis }}</span>
                <strong class="mt-1 block break-all font-mono text-cp-sm tabular-nums text-cp-text">{{ row.used }}</strong>
              </div>
              <ArrowRight class="size-4 text-cp-text-tertiary" aria-hidden="true" />
              <div class="min-w-0 text-right">
                <span class="text-[10px] text-cp-text-tertiary">完整周期预测</span>
                <strong class="mt-1 block break-all font-mono text-cp-lg tabular-nums text-cp-green-text">{{ row.total === '待估算' ? row.total : `≈ ${row.total}` }}</strong>
              </div>
            </div>
            <div class="flex flex-wrap items-center justify-between gap-2 border-t border-cp-border-secondary pt-2 text-cp-xs">
              <span class="text-cp-text-tertiary">更新时的剩余估算</span>
              <span class="font-mono tabular-nums text-cp-text-secondary">{{ row.remaining }}</span>
            </div>
          </div>
        </div>

        <div class="grid gap-1 text-cp-xs leading-relaxed text-cp-text-secondary">
          <span>{{ explanation.label }}</span>
          <span>{{ explanation.basis }}</span>
          <span v-if="explanation.warning" class="text-cp-warning-text">{{ explanation.warning }}</span>
          <span>额度重置：{{ selected.resetAtDisplay }}</span>
        </div>
      </section>
      <BaseEmpty v-else title="暂无可预测的额度窗口" description="可刷新额度获取观测；仍需有效的额度比例和本地用量记录。" surface="none" />

      <details class="text-cp-xs leading-relaxed text-cp-text-secondary">
        <summary class="cursor-pointer">
          <Info class="mr-1 inline size-3.5 align-text-bottom" />预测说明
        </summary>
        <p class="mt-2">
          {{ explanation.title }}
        </p>
        <p>已记录 Token 为所选额度周期的本地用量；费用列使用预测采样区间的费用，两者范围可能不同。倍率后计费采用每笔请求保存的历史倍率。</p>
        <p>模型组合、缓存比例、站外使用及观测延迟均会影响结果，预测不是上游承诺额度。</p>
      </details>
    </div>
    <template #footer>
      <span class="mr-auto self-center text-cp-xs text-cp-text-tertiary">额度刷新：{{ account.quota.refreshedAtDisplay }}</span>
      <BaseButton @click="open = false">
        关闭
      </BaseButton>
      <BaseButton variant="primary" :loading="refreshing" @click="emit('refreshQuota', account.id)">
        <template #icon>
          <RefreshCw class="size-4" />
        </template>
        刷新额度
      </BaseButton>
    </template>
  </BaseModal>
</template>
