<script setup lang="ts">
import type { AccountGroup } from '@/api'
import { Pencil, Power, Trash2 } from '@lucide/vue'

import BaseIconButton from '@/components/base/BaseIconButton.vue'
import BaseMenuItem from '@/components/base/BaseMenuItem.vue'
import RowActionMenu from '@/components/RowActionMenu.vue'

defineProps<{
  group: AccountGroup
  updatingStatus: boolean
  deleting: boolean
}>()
const emit = defineEmits<{
  edit: [group: AccountGroup]
  toggle: [group: AccountGroup]
  delete: [group: AccountGroup]
}>()
</script>

<template>
  <div class="flex items-center gap-1">
    <BaseIconButton
      variant="ghost"
      size="sm"
      label="编辑分组"
      @click.stop="emit('edit', group)"
    >
      <Pencil class="size-3.5 text-cp-link" />
    </BaseIconButton>
    <RowActionMenu v-slot="{ select }" :label="`${group.name}的更多操作`" :loading="updatingStatus || deleting">
      <BaseMenuItem role="menuitem" tabindex="-1" @click="select(() => emit('toggle', group))">
        <template #icon>
          <Power class="size-3.5" />
        </template>
        {{ group.enabled ? '禁用分组' : '启用分组' }}
      </BaseMenuItem>
      <BaseMenuItem role="menuitem" tabindex="-1" tone="destructive" @click="select(() => emit('delete', group))">
        <template #icon>
          <Trash2 class="size-3.5" />
        </template>
        删除分组
      </BaseMenuItem>
    </RowActionMenu>
  </div>
</template>
