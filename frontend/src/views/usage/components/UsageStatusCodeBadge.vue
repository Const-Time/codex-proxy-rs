<script setup lang="ts">
defineProps<{
  statusCode?: number | null
}>()

function statusClass(statusCode?: number | null) {
  if (statusCode === undefined || statusCode === null) {
    return 'bg-cp-fill-quaternary text-cp-text-secondary'
  }

  if (statusCode >= 200 && statusCode < 300) {
    return 'bg-cp-success-container text-cp-success-on-container'
  }

  if (statusCode >= 300 && statusCode < 400) {
    return 'bg-cp-warning-container text-cp-warning-on-container'
  }

  return 'bg-cp-error-container text-cp-error-on-container'
}
</script>

<template>
  <span
    class="inline-flex h-6 min-w-12 items-center justify-center rounded-full px-2 font-mono text-cp-sm leading-none font-bold"
    :class="statusClass(statusCode)"
  >
    {{ statusCode ?? '—' }}
  </span>
</template>
