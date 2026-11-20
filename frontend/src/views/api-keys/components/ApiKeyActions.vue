<script setup lang="ts">
import type { getApiKeys } from '@/api'
import { Pencil, Power, Terminal, Trash2, Upload } from '@lucide/vue'

import BaseIconButton from '@/components/base/BaseIconButton.vue'
import BaseMenuItem from '@/components/base/BaseMenuItem.vue'
import RowActionMenu from '@/components/RowActionMenu.vue'

type ApiKeyRow = Awaited<ReturnType<typeof getApiKeys>>['items'][number]

defineProps<{
  apiKey: ApiKeyRow
  deleting: boolean
  updatingStatus: boolean
  revealing: boolean
}>()

const emit = defineEmits<{
  use: [apiKey: ApiKeyRow]
  importCcs: [apiKey: ApiKeyRow]
  toggle: [apiKey: ApiKeyRow]
  delete: [apiKey: ApiKeyRow]
  edit: [apiKey: ApiKeyRow]
}>()
</script>

<template>
  <div class="flex items-center justify-start gap-0.5">
    <BaseIconButton
      variant="ghost"
      size="sm"
      label="编辑密钥"
      @click.stop="emit('edit', apiKey)"
    >
      <Pencil class="size-3.5 text-cp-link" />
    </BaseIconButton>
    <RowActionMenu v-slot="{ select }" label="更多密钥操作">
      <BaseMenuItem role="menuitem" tabindex="-1" :loading="revealing" @click="select(() => emit('use', apiKey))">
        <template #icon>
          <Terminal class="size-3.5 text-cp-primary-text" />
        </template>
        使用密钥
      </BaseMenuItem>
      <BaseMenuItem role="menuitem" tabindex="-1" :disabled="revealing" @click="select(() => emit('importCcs', apiKey))">
        <template #icon>
          <Upload class="size-3.5 text-cp-link" />
        </template>
        导入 CCSwitch
      </BaseMenuItem>
      <BaseMenuItem role="menuitem" tabindex="-1" :loading="updatingStatus" @click="select(() => emit('toggle', apiKey))">
        <template #icon>
          <Power class="size-3.5" :class="apiKey.enabled ? 'text-cp-warning' : 'text-cp-success'" />
        </template>
        {{ apiKey.enabled ? '禁用密钥' : '启用密钥' }}
      </BaseMenuItem>
    </RowActionMenu>
    <BaseIconButton
      variant="ghost"
      size="sm"
      label="删除密钥"
      :disabled="deleting"
      @click.stop="emit('delete', apiKey)"
    >
      <Trash2 class="size-3.5 text-cp-error" />
    </BaseIconButton>
  </div>
</template>
