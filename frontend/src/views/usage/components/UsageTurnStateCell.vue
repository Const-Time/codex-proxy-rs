<script setup lang="ts">
import { Copy } from '@lucide/vue'
import { computed } from 'vue'
import BaseIconButton from '@/components/base/BaseIconButton.vue'
import { useCopyText } from '@/composables/useCopyText'
import { turnStateDisplay } from '../utils/turnState'

const props = defineProps<{ value?: string | null }>()
const state = computed(() => turnStateDisplay(props.value))
const copyText = useCopyText()
</script>

<template>
  <div v-if="state.token" class="flex min-w-0 max-w-full items-center gap-2">
    <span
      class="inline-flex shrink-0 items-center rounded-md bg-cp-fill-tertiary px-2 py-1 font-mono text-cp-xs font-bold tabular-nums text-cp-text-secondary"
      :title="`${state.length} 个字符（仅表示长度）`"
      :aria-label="`Turn-state 长度：${state.length} 个字符`"
    >
      {{ state.length }}
    </span>
    <code class="min-w-0 flex-1 truncate font-mono text-cp-sm text-cp-text-secondary" :title="state.token">
      {{ state.token }}
    </code>
    <BaseIconButton
      variant="ghost"
      size="sm"
      label="复制完整 turn-state"
      class="shrink-0"
      @click.stop="copyText(state.token, { successText: 'Turn-state 已复制' })"
    >
      <Copy class="size-3.5" />
    </BaseIconButton>
  </div>
  <span v-else class="text-cp-text-quaternary">—</span>
</template>
