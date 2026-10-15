<script setup lang="ts">
import { Download, Pencil, Trash2, Upload } from '@lucide/vue'
import BaseButton from '@/components/base/BaseButton.vue'

defineProps<{ selectedCount: number, batchDeleting: boolean, exportingAccounts: boolean }>()
const emit = defineEmits<{ deleteSelected: [], exportSelected: [], create: [], editSelected: [] }>()
</script>

<template>
  <div
    class="flex min-w-0 flex-wrap items-center justify-end gap-2"
  >
    <BaseButton
      v-if="selectedCount > 0"
      variant="secondary"
      class="whitespace-nowrap"
      @click="emit('editSelected')"
    >
      <Pencil class="size-4 text-cp-link" />
      批量编辑账号
    </BaseButton>
    <BaseButton
      v-if="selectedCount > 0"
      variant="destructive"
      class="whitespace-nowrap"
      :disabled="batchDeleting"
      @click="emit('deleteSelected')"
    >
      <Trash2 class="size-4" />
      删除选中 ({{ selectedCount }})
    </BaseButton>
    <BaseButton
      v-if="selectedCount > 0"
      variant="secondary"
      class="whitespace-nowrap"
      :loading="exportingAccounts"
      @click="emit('exportSelected')"
    >
      <template #icon>
        <Download class="size-4" />
      </template>
      导出选中 ({{ selectedCount }})
    </BaseButton>
    <BaseButton
      variant="primary"
      class="whitespace-nowrap"
      @click="emit('create')"
    >
      <Upload class="size-4" />
      导入账号
    </BaseButton>
  </div>
</template>
