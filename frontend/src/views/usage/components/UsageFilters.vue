<script setup lang="ts">
import { RefreshCw } from '@lucide/vue'

import BaseIconButton from '@/components/base/BaseIconButton.vue'

defineProps<{
  refreshing: boolean
  loading: boolean
}>()

const emit = defineEmits<{
  refresh: []
}>()
</script>

<template>
  <div class="flex w-full items-center gap-3" role="group" aria-label="使用记录筛选与操作">
    <div class="ml-auto flex shrink-0 items-center justify-end">
      <BaseIconButton
        variant="ghost"
        size="md"
        label="刷新使用记录"
        :loading="refreshing"
        :disabled="loading || refreshing"
        @click="emit('refresh')"
      >
        <template #loading>
          <RefreshCw class="size-4.5 animate-spin motion-reduce:animate-none" />
        </template>
        <RefreshCw class="size-4.5" />
      </BaseIconButton>
    </div>
  </div>
</template>
