<script setup lang="ts">
import { Plus, Trash2 } from '@lucide/vue'

import BaseButton from '@/components/base/BaseButton.vue'

defineProps<{
  batchDeleting: boolean
  selectedCount: number
}>()

const emit = defineEmits<{
  create: []
  deleteSelected: []
}>()
</script>

<template>
  <div
    class="flex w-full flex-wrap items-center gap-3"
    role="group"
    aria-label="API Key 筛选与操作"
  >
    <div class="flex shrink-0 items-center justify-end gap-2 md:ml-auto">
      <BaseButton
        v-if="selectedCount > 0"
        variant="destructive"
        :disabled="batchDeleting"
        @click="emit('deleteSelected')"
      >
        <template #icon>
          <Trash2 class="size-4" />
        </template>
        删除选中 ({{ selectedCount }})
      </BaseButton>
      <BaseButton variant="primary" @click="emit('create')">
        <template #icon>
          <Plus class="size-4" />
        </template>
        创建 API Key
      </BaseButton>
    </div>
  </div>
</template>
