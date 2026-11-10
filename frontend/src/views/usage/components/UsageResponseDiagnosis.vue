<script setup lang="ts">
import type { UsageErrorOrigin } from '@/api'
import { computed } from 'vue'
import { errorOriginText, responseStatusText } from '../utils/opsErrorPresentation'
import UsageDetailFieldGrid from './UsageDetailFieldGrid.vue'

const props = defineProps<{
  errorOrigin: UsageErrorOrigin
  clientStatus: number | null
  upstreamStatus: number | null
  clientTransport: string | null
  upstreamTransport: string | null
  failed?: boolean
}>()

const fields = computed(() => [
  ...(props.failed ? [{ label: '错误来源', value: errorOriginText(props.errorOrigin) }] : []),
  { label: '客户端响应', value: responseStatusText(props.clientStatus, props.clientTransport), mono: true },
  { label: '上游响应', value: responseStatusText(props.upstreamStatus, props.upstreamTransport), mono: true },
])
</script>

<template>
  <section class="min-w-0 rounded-cp-card bg-cp-fill-quaternary px-4 py-3.5">
    <h3 class="m-0 text-cp-sm leading-none font-heavy text-cp-text-secondary">
      响应诊断
    </h3>
    <UsageDetailFieldGrid :items="fields" />
    <p class="mt-3 mb-0 text-cp-xs leading-relaxed text-cp-text-secondary">
      HTTP 200 只表示响应已开始，流式请求仍可能中途失败；WebSocket 握手状态也不代表生成结果。
    </p>
  </section>
</template>
