<script setup lang="ts">
withDefaults(defineProps<{
  tone?: 'neutral' | 'info' | 'success' | 'danger'
}>(), {
  tone: 'neutral',
})

const toneClasses = {
  neutral: 'bg-cp-fill-tertiary text-cp-text-secondary',
  info: 'bg-cp-primary-container text-cp-primary-on-container',
  success: 'bg-cp-success-container text-cp-success-on-container',
  danger: 'bg-cp-error-container text-cp-error-on-container',
}
</script>

<template>
  <span class="inline-flex w-fit shrink-0 items-center whitespace-nowrap rounded-full px-2 py-1 text-cp-xs leading-none font-bold" :class="toneClasses[tone]">
    <slot />
  </span>
</template>
