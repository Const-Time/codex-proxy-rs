<script setup lang="ts">
import type { UsageDisplayRecord } from '../utils/records'
import { computed } from 'vue'
import { usageLatencyDetails } from '../utils/records'

const props = defineProps<{ record: UsageDisplayRecord }>()
const details = computed(() => usageLatencyDetails(props.record))
</script>

<template>
  <div
    class="grid justify-items-end gap-1.5 whitespace-nowrap font-mono text-cp-sm leading-none font-heavy tabular-nums"
    title="生成速率 = 输出 Token ÷ 首字到完成的耗时；TTFT 为首字等待时间。缺少有效采样时不估算。"
  >
    <span class="text-cp-text">{{ details.generationRateCompactDisplay }}</span>
    <span class="text-cp-xs text-cp-text-tertiary">TTFT {{ details.firstTokenDisplay }}</span>
  </div>
</template>
