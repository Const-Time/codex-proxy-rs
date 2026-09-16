<script setup lang="ts">
import type { RequestTraceEvent } from '@/api'
import { computed } from 'vue'
import { requestProfiles } from '../utils/requestProfile'
import UsageDetailFieldGrid from './UsageDetailFieldGrid.vue'

const props = defineProps<{ events: RequestTraceEvent[], requestCreatedAt?: string }>()
const profiles = computed(() => requestProfiles(props.events, props.requestCreatedAt))
</script>

<template>
  <section class="mb-3 min-w-0 rounded-cp bg-cp-bg-container p-3" aria-label="上游请求参数">
    <h4 class="m-0 text-cp-sm font-bold text-cp-text">
      上游请求参数
    </h4>
    <p class="mt-2 text-cp-xs leading-relaxed text-cp-text-secondary">
      按当次尝试保存，配置变更不会改写历史。出口 IP 为上次代理检测结果；参数已准备不代表握手成功、上游已接受或地区识别一致。复用 WebSocket 时，准备的握手参数不代表重新发送过握手。
    </p>
    <p v-if="!profiles.length" class="text-cp-xs text-cp-text-tertiary">
      本次未保存上游参数快照，可能是旧记录、尚未到达发送边界、当前平台未采集或事件已被淘汰。
    </p>
    <details v-for="profile in profiles" :key="profile.sequence" :open="profiles.length === 1" class="mt-2 min-w-0 rounded-lg bg-cp-fill-quaternary p-3">
      <summary class="cursor-pointer text-cp-xs font-bold text-cp-text">
        尝试 {{ profile.attemptIndex }} · +{{ profile.elapsedMs }} ms · 参数快照 #{{ profile.sequence }}
      </summary>
      <UsageDetailFieldGrid :items="profile.items.map(item => ({ ...item, wrap: true }))" />
    </details>
    <p v-if="profiles.length" class="mt-2 mb-0 text-cp-xs text-cp-text-tertiary">
      地区改写为 0 可能表示字段原本一致、未携带匹配字段或采用透传。位置摘要每类最多两处，字段最多 48 字符，请求头最多 128 字符；诊断包使用同一份摘要。此处不采集实际 TLS ClientHello/JA3，也不表示风控判定结果。
    </p>
  </section>
</template>
