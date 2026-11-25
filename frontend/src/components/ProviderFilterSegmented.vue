<script setup lang="ts">
import { LayoutGrid } from '@lucide/vue'

import { OpenaiIcon, XaiIcon } from '@/components/icons/BrandIcons'
import { PROVIDER_DISPLAY_NAMES } from '@/utils/providers'
import BaseSegmented from './base/BaseSegmented.vue'

withDefaults(
  defineProps<{
    disabled?: boolean
  }>(),
  {
    disabled: false,
  },
)

const provider = defineModel<string>({ required: true })

const providerOptions = [
  { label: '全部平台', value: '', icon: LayoutGrid },
  { label: PROVIDER_DISPLAY_NAMES.openai, value: 'openai', icon: OpenaiIcon },
  { label: PROVIDER_DISPLAY_NAMES.xai, value: 'xai', icon: XaiIcon },
]
</script>

<template>
  <BaseSegmented
    v-model="provider"
    label="按平台筛选"
    :options="providerOptions"
    :disabled="disabled"
    display="icon"
  />
</template>
