<script setup lang="ts">
import type { AccountCreateProvider } from './model'
import { LayoutGrid } from '@lucide/vue'
import BaseSegmented from '@/components/base/BaseSegmented.vue'
import { AntigravityIcon, OpenaiIcon, XaiIcon } from '@/components/icons/BrandIcons'
import { PROVIDER_DISPLAY_NAMES } from '@/utils/providers'

withDefaults(
  defineProps<{
    disabled?: boolean
    selected?: AccountCreateProvider | ''
  }>(),
  {
    disabled: false,
  },
)

const emit = defineEmits<{
  select: [provider: AccountCreateProvider]
}>()

const providers = [
  {
    value: 'batch' as const,
    label: '批量导入',
    icon: LayoutGrid,
  },
  {
    value: 'openai' as const,
    label: PROVIDER_DISPLAY_NAMES.openai,
    icon: OpenaiIcon,
  },
  {
    value: 'xai' as const,
    label: PROVIDER_DISPLAY_NAMES.xai,
    icon: XaiIcon,
  },
  {
    value: 'antigravity' as const,
    label: PROVIDER_DISPLAY_NAMES.antigravity,
    icon: AntigravityIcon,
  },
]

function selectProvider(value: string) {
  const provider = providers.find(provider => provider.value === value)
  if (provider)
    emit('select', provider.value)
}
</script>

<template>
  <BaseSegmented
    :model-value="selected ?? ''"
    label="选择账号平台"
    :options="providers"
    :disabled="disabled"
    display="icon"
    class="w-41"
    @update:model-value="selectProvider"
  />
</template>
