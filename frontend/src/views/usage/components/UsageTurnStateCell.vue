<script setup lang="ts">
import { Copy } from '@lucide/vue'
import { computed } from 'vue'
import BaseIconButton from '@/components/base/BaseIconButton.vue'
import { useCopyText } from '@/composables/useCopyText'
import { turnStateDisplay, turnStateSourceDisplay } from '../utils/turnState'

const props = defineProps<{ value?: string | null, source?: string | null }>()
const state = computed(() => turnStateDisplay(props.value))
const sourceDisplay = computed(() => turnStateSourceDisplay(props.source))
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
    <span class="shrink-0 text-cp-xs text-cp-text-tertiary" :title="sourceDisplay.description">
      {{ sourceDisplay.label }}
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
  <span v-else class="text-cp-text-quaternary" title="未记录有效的 turn-state（历史记录不会自动补齐）">—</span>
</template>
