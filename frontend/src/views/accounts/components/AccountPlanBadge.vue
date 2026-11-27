<script setup lang="ts">
import { computed } from 'vue'

import { resolveAccountPlanDisplay } from '../utils/quotaPresentation'
import { stableVisualIndex } from '../utils/visualTone'

const props = withDefaults(
  defineProps<{
    planType?: string | null
    authenticationKind?: string
    planTypeDisplay: string
    size?: 'xs' | 'sm' | 'md'
  }>(),
  {
    planType: null,
    size: 'md',
  },
)

const planPalettes: Record<string, string> = {
  api: 'bg-cp-cyan-container-strong text-cp-cyan-on-container',
  free: 'bg-cp-cyan-container text-cp-cyan-on-container',
  plus: 'bg-cp-blue-container-strong text-cp-blue-on-container',
  team: 'bg-cp-green-container-strong text-cp-green-on-container',
  business: 'bg-cp-green-container-strong text-cp-green-on-container',
  pro: 'bg-cp-purple-container-strong text-cp-purple-on-container',
  prolite: 'bg-cp-purple-container text-cp-purple-on-container',
  ultra: 'bg-cp-orange-container-strong text-cp-orange-on-container',
}

const fallbackPalettes = [
  'bg-cp-blue-container text-cp-blue-on-container',
  'bg-cp-green-container text-cp-green-on-container',
  'bg-cp-cyan-container text-cp-cyan-on-container',
  'bg-cp-orange-container text-cp-orange-on-container',
] as const

const plan = computed(() => resolveAccountPlanDisplay({
  authenticationKind: props.authenticationKind,
  planType: props.planType,
  planTypeDisplay: props.planTypeDisplay,
}))
const rawPlanType = computed(() => plan.value.rawPlanType)
const displayText = computed(() => plan.value.displayText)

const sizeClass = computed(() => {
  if (props.size === 'xs')
    return 'min-h-4.5 rounded-full px-1.5 text-[10px] font-bold'
  return props.size === 'sm'
    ? 'min-h-5 rounded-full px-1.75 text-cp-xs font-bold'
    : 'min-h-5.5 rounded-full px-2 text-cp-xs font-heavy'
})

const paletteClass = computed(() => {
  const key = plan.value.paletteKey
  const rawKey = rawPlanType.value.toLowerCase()
  const planPalette = planPalettes[key] ?? planPalettes[rawKey]
  if (planPalette)
    return planPalette

  return fallbackPalettes[stableVisualIndex(rawKey || key, fallbackPalettes.length)]
})
</script>

<template>
  <span
    class="inline-flex max-w-full items-center justify-center py-0.5 text-center leading-tight shadow-cp-tertiary"
    :class="[sizeClass, paletteClass]"
    :title="rawPlanType || undefined"
  >
    <span class="min-w-0 wrap-anywhere whitespace-normal">{{ displayText }}</span>
  </span>
</template>
