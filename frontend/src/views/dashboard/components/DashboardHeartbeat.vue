<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, useTemplateRef, watch } from 'vue'
import { useSharedReducedMotion } from '@/composables/useSharedMedia'

const props = withDefaults(
  defineProps<{
    updatedAt?: string
  }>(),
  {
    updatedAt: '',
  },
)

const preferredMotion = useSharedReducedMotion()
const heartbeatDot = useTemplateRef<HTMLElement>('heartbeatDot')
const heartbeatRing = useTemplateRef<HTMLElement>('heartbeatRing')
const statusLabel = computed(() =>
  props.updatedAt ? `自动刷新 30s，最近刷新 ${props.updatedAt}` : '自动刷新 30s，等待首次刷新',
)

let activeAnimations: Animation[] = []

function cancelPulse() {
  for (const animation of activeAnimations)
    animation.cancel()
  activeAnimations = []
}

function pulse() {
  const dot = heartbeatDot.value
  const ring = heartbeatRing.value
  if (!dot || !ring || preferredMotion.value === 'reduce')
    return

  cancelPulse()
  activeAnimations = [
    dot.animate(
      [
        { transform: 'scale(0.72)', opacity: 0.55 },
        { transform: 'scale(1)', opacity: 1 },
      ],
      { duration: 420, easing: 'cubic-bezier(0.34, 1.56, 0.64, 1)' },
    ),
    ring.animate(
      [
        { transform: 'scale(0.55)', opacity: 0.34 },
        { transform: 'scale(2.65)', opacity: 0 },
      ],
      { duration: 650, easing: 'cubic-bezier(0.22, 1, 0.36, 1)' },
    ),
  ]
}

watch(
  () => props.updatedAt,
  async (updatedAt, previousUpdatedAt) => {
    if (!updatedAt || updatedAt === previousUpdatedAt)
      return
    await nextTick()
    pulse()
  },
)

onBeforeUnmount(cancelPulse)
</script>

<template>
  <span class="inline-flex items-center gap-1.75" :aria-label="statusLabel" :title="statusLabel">
    <span class="relative inline-flex size-2 shrink-0 items-center justify-center">
      <span ref="heartbeatRing" class="absolute size-2 rounded-full bg-cp-success opacity-0" />
      <span ref="heartbeatDot" class="relative size-1.5 rounded-full bg-cp-success" />
    </span>
    <span>自动刷新 30s</span>
  </span>
</template>
