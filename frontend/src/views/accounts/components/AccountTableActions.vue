<script setup lang="ts">
import type { AccountRow } from '../constants'
import { KeyRound, Pencil, RefreshCw, RotateCcw, Trash2, Wifi } from '@lucide/vue'

import BaseIconButton from '@/components/base/BaseIconButton.vue'
import BaseMenuItem from '@/components/base/BaseMenuItem.vue'
import RowActionMenu from '@/components/RowActionMenu.vue'

defineProps<{
  account: AccountRow
  deleting: boolean
  recovering: boolean
  refreshing: boolean
  testing: boolean
}>()

const emit = defineEmits<{
  edit: [account: AccountRow]
  delete: [account: AccountRow]
  recover: [accountId: string]
  test: [account: AccountRow]
  refresh: [accountId: string]
  reauthorize: [account: AccountRow]
}>()
</script>

<template>
  <div class="relative flex items-center justify-start gap-1">
    <BaseIconButton
      variant="ghost"
      size="sm"
      label="编辑账号"
      @click.stop="emit('edit', account)"
    >
      <Pencil class="size-3.5 text-cp-link" />
    </BaseIconButton>

    <BaseIconButton
      variant="ghost"
      size="sm"
      label="删除账号"
      :disabled="deleting"
      @click.stop="emit('delete', account)"
    >
      <Trash2 class="size-3.5 text-cp-error" />
    </BaseIconButton>

    <RowActionMenu v-slot="{ select }" label="更多账号操作">
      <BaseMenuItem
        role="menuitem" tabindex="-1"
        :loading="testing"
        :disabled="testing"
        @click.stop="select(() => emit('test', account))"
      >
        <template #icon>
          <Wifi class="size-3.5 text-cp-text-quaternary" />
        </template>
        测试连接
      </BaseMenuItem>
      <BaseMenuItem
        v-if="account.authenticationKind === 'oauth'" role="menuitem"
        tabindex="-1"
        :loading="refreshing"
        :disabled="refreshing"
        @click.stop="select(() => emit('refresh', account.id))"
      >
        <template #loading>
          <RefreshCw class="size-3.5 animate-spin text-cp-text-quaternary motion-reduce:animate-none" />
        </template>
        <template #icon>
          <RefreshCw class="size-3.5 text-cp-text-quaternary" />
        </template>
        刷新令牌
      </BaseMenuItem>
      <BaseMenuItem v-if="account.authenticationKind === 'oauth'" role="menuitem" tabindex="-1" @click.stop="select(() => emit('reauthorize', account))">
        <template #icon>
          <KeyRound class="size-3.5 text-cp-text-quaternary" />
        </template>
        重新授权
      </BaseMenuItem>
      <BaseMenuItem
        role="menuitem" tabindex="-1"
        :loading="recovering"
        :disabled="recovering"
        @click.stop="select(() => emit('recover', account.id))"
      >
        <template #icon>
          <RotateCcw class="size-3.5 text-cp-text-quaternary" />
        </template>
        恢复状态
      </BaseMenuItem>
    </RowActionMenu>
  </div>
</template>
