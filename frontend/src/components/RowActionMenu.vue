<script setup lang="ts">
import { Ellipsis } from '@lucide/vue'
import { nextTick, shallowRef, useId, useTemplateRef, watch } from 'vue'
import BaseIconButton from '@/components/base/BaseIconButton.vue'
import BasePopover from '@/components/base/BasePopover.vue'

defineProps<{ label: string, loading?: boolean }>()
const open = shallowRef(false)
const menuId = useId()
const trigger = useTemplateRef<InstanceType<typeof BaseIconButton>>('trigger')
const menu = useTemplateRef<HTMLElement>('menu')

function items() {
  return [...(menu.value?.querySelectorAll<HTMLButtonElement>('[role="menuitem"]:enabled') ?? [])]
}

function close() {
  open.value = false
  trigger.value?.$el.focus()
}

function select(action: () => void) {
  close()
  action()
}

function onMenuKeydown(event: KeyboardEvent) {
  if (event.key === 'Escape') {
    event.preventDefault()
    event.stopPropagation()
    close()
    return
  }
  if (event.key === 'Tab') {
    close()
    return
  }
  if (!['ArrowDown', 'ArrowUp', 'Home', 'End'].includes(event.key))
    return
  event.preventDefault()
  const buttons = items()
  const index = buttons.indexOf(document.activeElement as HTMLButtonElement)
  const next = event.key === 'Home'
    ? 0
    : event.key === 'End'
      ? buttons.length - 1
      : event.key === 'ArrowDown'
        ? (index + 1) % buttons.length
        : (index + buttons.length - 1) % buttons.length
  buttons[next]?.focus()
}

watch(open, async (value) => {
  if (value) {
    await nextTick()
    items()[0]?.focus()
  }
})
</script>

<template>
  <BasePopover v-model="open" placement="bottom-end">
    <template #trigger>
      <BaseIconButton ref="trigger" size="sm" :label="label" :pressed="open" :loading="loading" :aria-expanded="open" :aria-controls="open ? menuId : undefined" aria-haspopup="menu" @keydown.down.prevent="open = true">
        <Ellipsis class="size-4" />
      </BaseIconButton>
    </template>
    <div :id="menuId" ref="menu" class="w-42 p-1.5" role="menu" tabindex="-1" :aria-label="label" @keydown="onMenuKeydown">
      <slot :select="select" />
    </div>
  </BasePopover>
</template>
