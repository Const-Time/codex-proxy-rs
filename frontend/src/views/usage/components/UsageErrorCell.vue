<script setup lang="ts">
import type { OpsError, UsageErrorOrigin } from '@/api'
import { CheckCheck, CircleHelp, Network, Server, Unplug } from '@lucide/vue'
import { computed, useId } from 'vue'
import BasePopover from '@/components/base/BasePopover.vue'
import BaseScrollbar from '@/components/base/BaseScrollbar.vue'
import { entryRejected, errorOriginText, failureClassText } from '../utils/opsErrorPresentation'

const props = defineProps<{ record: OpsError, personal: boolean }>()
const detailId = useId()
const origins: Record<UsageErrorOrigin, { icon: typeof Server, tone: string }> = {
  upstream: { icon: Server, tone: 'text-cp-error-text' },
  proxy: { icon: Network, tone: 'text-cp-primary-text' },
  connection: { icon: Unplug, tone: 'text-cp-warning-text' },
  unknown: { icon: CircleHelp, tone: 'text-cp-text-tertiary' },
}
const origin = computed(() => origins[props.record.errorOrigin] ?? origins.unknown)
const outcome = computed(() => entryRejected(props.record) ? '入口拒绝' : '请求失败')
const code = computed(() => props.record.providerErrorCode || props.record.failureClass)
const summary = computed(() => {
  const label = failureClassText(props.record.failureClass)
  return label !== props.record.failureClass
    ? label
    : props.record.message?.replace(/^OpenAI upstream failure: /, '') || code.value || '暂无错误详情'
})
const showClientStatus = computed(() => props.record.clientStatusCode != null
  && props.record.clientStatusCode !== props.record.upstreamStatusCode)
</script>

<template>
  <div class="min-w-0 py-1">
    <div class="flex min-w-0 flex-wrap items-center gap-x-1.5 gap-y-0.5 text-cp-xs text-cp-text-secondary">
      <component :is="origin.icon" v-if="!personal" class="size-3.5 shrink-0" :class="origin.tone" role="img" :aria-label="errorOriginText(record.errorOrigin)">
        <title>{{ errorOriginText(record.errorOrigin) }}</title>
      </component>
      <span class="font-bold">{{ outcome }}</span>
      <span v-if="record.upstreamStatusCode != null && !personal" class="font-mono">上游 {{ record.upstreamStatusCode }}</span>
      <span v-if="showClientStatus || (personal && record.clientStatusCode != null)" class="font-mono">客户端 {{ record.clientStatusCode }}</span>
      <CheckCheck v-if="record.metadata.recoveredAt" class="size-3.5 shrink-0 text-cp-success-text" role="img" aria-label="已自动恢复">
        <title>已自动恢复</title>
      </CheckCheck>
    </div>
    <BasePopover class="mt-1 max-w-full" placement="top-start" trigger="hover-click">
      <template #trigger="{ open }">
        <button
          type="button"
          class="block min-w-0 cursor-pointer truncate rounded-cp-sm border-0 bg-transparent p-0 text-left text-cp-sm leading-normal font-bold text-cp-text focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-cp-control-outline"
          :aria-expanded="open"
          :aria-controls="open ? detailId : undefined"
        >
          {{ summary }}
        </button>
      </template>
      <div :id="detailId" class="w-96 max-w-[calc(100vw-2rem)] p-3" role="region" aria-label="错误摘要">
        <p v-if="code && !personal && code !== summary" class="m-0 mb-2 wrap-anywhere font-mono text-cp-xs font-bold text-cp-text-secondary">
          {{ code }}
        </p>
        <BaseScrollbar max-height="min(14rem, 40dvh)">
          <p class="m-0 pr-2 wrap-anywhere whitespace-pre-wrap text-cp-sm">
            {{ record.message || summary }}
          </p>
        </BaseScrollbar>
      </div>
    </BasePopover>
  </div>
</template>
