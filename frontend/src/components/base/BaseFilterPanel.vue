<script setup lang="ts">
import { ChevronDown, SlidersHorizontal } from '@lucide/vue'
import { useMediaQuery } from '@vueuse/core'
import { ref, useId } from 'vue'
import BaseButton from './BaseButton.vue'

withDefaults(defineProps<{ label?: string, activeCount?: number }>(), { label: '筛选条件', activeCount: 0 })
const mobile = useMediaQuery('(max-width: 639px)')
const expanded = ref(false)
const panelId = useId()
</script>

<template>
  <div class="min-w-0">
    <BaseButton
      v-if="mobile"
      class="mb-3 w-full justify-between"
      :aria-expanded="expanded"
      :aria-controls="panelId"
      @click="expanded = !expanded"
    >
      <SlidersHorizontal class="size-4" />
      {{ label }}{{ activeCount ? `（${activeCount}）` : '' }}
      <ChevronDown class="ml-auto size-4" :class="expanded ? 'rotate-180' : ''" />
    </BaseButton>
    <div v-show="!mobile || expanded" :id="panelId">
      <slot />
    </div>
  </div>
</template>
