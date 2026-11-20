<script setup lang="ts">
import { useResizeObserver } from '@vueuse/core'
import { computed, shallowRef, useId, useTemplateRef } from 'vue'
import BaseCheckbox from '@/components/base/BaseCheckbox.vue'
import BasePopover from '@/components/base/BasePopover.vue'

const props = defineProps<{ id: string, unavailable: boolean, disabled: boolean }>()
const selected = defineModel<boolean>({ required: true })
const descriptionId = useId()
const label = useTemplateRef<HTMLElement>('label')
const truncated = shallowRef(false)
const accessibleLabel = computed(() => props.unavailable ? `${props.id}（当前目录未返回）` : props.id)

useResizeObserver(label, () => {
  truncated.value = Boolean(label.value && label.value.scrollWidth > label.value.clientWidth)
})
</script>

<template>
  <BasePopover class="min-w-0" placement="top-start" trigger="hover" :disabled="!truncated">
    <template #trigger="{ open }">
      <BaseCheckbox
        v-model="selected"
        class="min-h-11 w-full min-w-0 rounded-cp px-3 py-2.5 transition-colors duration-150 motion-reduce:transition-none"
        :class="selected ? 'bg-cp-primary-container text-cp-primary-on-container' : 'bg-cp-fill-quaternary text-cp-text-secondary hover:bg-cp-fill-tertiary hover:text-cp-text'"
        :label="accessibleLabel"
        :aria-describedby="open ? descriptionId : undefined"
        show-label
        :disabled="disabled"
      >
        <template #label>
          <span ref="label" class="block truncate">{{ id }}</span>
        </template>
      </BaseCheckbox>
    </template>
    <div :id="descriptionId" role="tooltip" class="max-w-[min(26rem,calc(100vw-2rem))] px-3 py-2 wrap-anywhere font-mono text-cp-sm">
      {{ accessibleLabel }}
    </div>
  </BasePopover>
</template>
