<script setup lang="ts">
import UsageResponseDiagnosis from '../../src/views/usage/components/UsageResponseDiagnosis.vue'
</script>

<template>
  <main class="mx-auto grid max-w-3xl gap-4 p-4">
    <h1>响应诊断预览</h1>
    <UsageResponseDiagnosis error-origin="upstream" :client-status="200" :upstream-status="429" client-transport="http_sse" upstream-transport="http_sse" failed />
    <UsageResponseDiagnosis error-origin="connection" :client-status="101" :upstream-status="101" client-transport="websocket" upstream-transport="websocket" failed />
    <UsageResponseDiagnosis error-origin="proxy" :client-status="503" :upstream-status="null" client-transport="http" :upstream-transport="null" failed />
    <UsageResponseDiagnosis error-origin="unknown" :client-status="null" :upstream-status="null" client-transport="websocket" upstream-transport="websocket" failed />
  </main>
</template>
