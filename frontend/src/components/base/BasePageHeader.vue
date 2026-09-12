<script setup lang="ts">
import { Menu } from '@lucide/vue'
import { inject } from 'vue'

defineProps<{
  title: string
  description?: string
}>()
const openMobileSidebar = inject<() => void>('openMobileSidebar')
</script>

<template>
  <header
    class="cp-page-header flex min-h-17 shrink-0 flex-wrap items-start justify-between gap-x-4 gap-y-3"
  >
    <div class="min-w-0 flex-1 basis-64">
      <div class="flex min-w-0 items-center gap-3">
        <button
          v-if="openMobileSidebar"
          type="button"
          class="inline-flex size-11 shrink-0 items-center justify-center rounded-cp border-0 bg-cp-fill-secondary text-cp-text min-[961px]:hidden"
          aria-label="打开侧边栏"
          @click="openMobileSidebar"
        >
          <Menu class="size-5" />
        </button>
        <h1 class="m-0 text-[34px] leading-[1.15] font-extrabold text-cp-text">
          {{ title }}
        </h1>
      </div>
      <p
        v-if="description || $slots.description"
        class="mt-2.5 mb-0 flex items-center gap-2 text-cp-xl leading-[1.15] font-semibold text-cp-text-secondary"
      >
        <slot name="description">
          {{ description }}
        </slot>
      </p>
    </div>
    <div
      v-if="$slots.actions"
      class="cp-page-actions mt-0.5 flex shrink-0 items-center justify-end gap-2"
    >
      <slot name="actions" />
    </div>
  </header>
</template>
