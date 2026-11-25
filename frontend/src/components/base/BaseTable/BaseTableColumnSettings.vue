<script setup lang="ts">
import type { TableColumnOption } from './useTableColumns'
import { Columns3 } from '@lucide/vue'
import { computed, defineAsyncComponent, shallowRef, useId, useTemplateRef } from 'vue'
import BaseIconButton from '../BaseIconButton.vue'
import BasePopover from '../BasePopover.vue'

defineProps<{
  options: TableColumnOption[]
}>()

const emit = defineEmits<{
  change: [key: string, visible: boolean]
  reorder: [keys: string[]]
  reset: []
}>()

const BaseTableColumnSettingsPanel = defineAsyncComponent(() => import('./BaseTableColumnSettingsPanel.vue'))

const open = shallowRef(false)
const panelId = useId()
const triggerRef = useTemplateRef<InstanceType<typeof BaseIconButton>>('trigger')
const triggerElement = computed<HTMLElement | null>(() => {
  const el = triggerRef.value?.$el
  return el instanceof HTMLElement ? el : null
})

function closeAndFocus() {
  open.value = false
  triggerElement.value?.focus()
}
</script>

<template>
  <BasePopover
    v-model="open"
    placement="bottom-end"
    :arrow-surface-class="{ top: 'bg-(--cp-popover-header-bg)' }"
  >
    <template #trigger>
      <BaseIconButton
        ref="trigger"
        label="显示列"
        variant="ghost"
        :pressed="open"
        aria-haspopup="dialog"
        :aria-expanded="open"
        :aria-controls="open ? panelId : undefined"
      >
        <Columns3 class="size-4.5" aria-hidden="true" />
      </BaseIconButton>
    </template>

    <BaseTableColumnSettingsPanel
      v-if="open"
      :panel-id="panelId"
      :options="options"
      :trigger-el="triggerElement"
      @change="(key, visible) => emit('change', key, visible)"
      @reorder="keys => emit('reorder', keys)"
      @reset="emit('reset')"
      @close="open = false"
      @close-and-focus="closeAndFocus"
    />
  </BasePopover>
</template>
