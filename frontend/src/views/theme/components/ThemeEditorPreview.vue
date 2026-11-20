<script setup lang="ts">
import type { CSSProperties } from 'vue'
import type { ThemeEditorPreview } from '../composables/useThemeEditor'
import type { ThemeName } from '@/theme'

import { defineAsyncComponent, h, onBeforeUnmount, onMounted, shallowRef } from 'vue'
import BaseSegmented from '@/components/base/BaseSegmented.vue'

import ThemePreviewCanvas from './ThemePreviewCanvas.vue'

defineProps<{
  theme: ThemeName
  style: CSSProperties
}>()

const previewReady = shallowRef(false)
const stylesRevision = shallowRef(0)
let frame = 0
const previewError = () => h('p', { role: 'alert', class: 'p-4 text-cp-error' }, '预览加载失败，请刷新页面后重试。')
const ThemeDashboardPreview = defineAsyncComponent({
  async loader() {
    const component = await import('./ThemeDashboardPreview.vue')
    stylesRevision.value += 1
    return component
  },
  errorComponent: previewError,
})
const ThemeComponentPreview = defineAsyncComponent({
  async loader() {
    const component = await import('./ThemeComponentPreview.vue')
    stylesRevision.value += 1
    return component
  },
  errorComponent: previewError,
})

onMounted(() => {
  frame = requestAnimationFrame(() => {
    frame = requestAnimationFrame(() => {
      previewReady.value = true
    })
  })
})
onBeforeUnmount(() => cancelAnimationFrame(frame))

const preview = defineModel<ThemeEditorPreview>({ required: true })

const previewOptions = [
  { label: '首页画板', value: 'page' },
  { label: '组件概览', value: 'components' },
]
</script>

<template>
  <section class="grid h-full min-h-0 grid-rows-[auto_minmax(0,1fr)] gap-3 overflow-hidden" aria-label="主题实时预览">
    <header class="shrink-0 flex flex-wrap items-center justify-between gap-3 rounded-cp-lg bg-cp-bg-container px-3 py-2.5 shadow-cp-tertiary">
      <div>
        <strong class="block text-cp-sm font-heavy text-cp-text">实时预览</strong>
        <span class="mt-0.5 block text-[9px] font-emphasis text-cp-text-quaternary">拖动画板或缩放查看细节</span>
      </div>
      <BaseSegmented v-model="preview" label="预览类型" size="sm" :options="previewOptions" />
    </header>

    <ThemePreviewCanvas
      class="h-full min-h-0"
      :theme="theme"
      :style="style"
      :styles-revision="stylesRevision"
    >
      <template v-if="previewReady">
        <ThemeDashboardPreview v-if="preview === 'page'" />
        <ThemeComponentPreview v-else />
      </template>
    </ThemePreviewCanvas>
  </section>
</template>
