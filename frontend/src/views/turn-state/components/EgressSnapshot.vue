<script setup lang="ts">
import type { TurnStateEgress } from '@/api/modules/turn-state'
import { egressLabel, egressRegion } from '../records'

defineProps<{ value?: TurnStateEgress | null }>()
</script>

<template>
  <div class="grid gap-1 text-cp-xs">
    <span class="break-all font-mono text-cp-sm">{{ value?.ip ?? '未确认' }}</span>
    <span class="text-cp-text-secondary">{{ egressRegion(value) }}</span>
    <span
      class="text-cp-text-tertiary"
      title="独立检测连接的出口，不是上游请求实际 IP。轮换/API 采集每次重新检测；固定采集及业务出口快照最多缓存 60 秒。"
    >
      {{ egressLabel(value) }}<template v-if="value"> · {{ new Date(value.detectedAt * 1000).toLocaleString() }}</template>
    </span>
  </div>
</template>
