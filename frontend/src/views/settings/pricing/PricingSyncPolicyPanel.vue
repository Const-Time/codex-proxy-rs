<script setup lang="ts">
import type { PricingSyncPolicy } from '@/api'
import { computed, ref, watch } from 'vue'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseSelect from '@/components/base/BaseSelect.vue'

const props = defineProps<{ policy: PricingSyncPolicy, disabled: boolean, saving: boolean }>()
defineEmits<{ save: [policy: PricingSyncPolicy] }>()
const mode = ref('new')
const savedMode = computed(() => !props.policy.enabled ? 'off' : props.policy.updateExisting ? 'all' : 'new')
watch(savedMode, (value) => {
  mode.value = value
}, { immediate: true })
const options = [
  { label: '自动加入新模型', value: 'new' },
  { label: '自动加入并更新已有价格', value: 'all' },
  { label: '仅手动同步', value: 'off' },
]
</script>

<template>
  <div class="mb-4 flex flex-wrap items-center gap-3 rounded-cp bg-cp-fill-quaternary p-3">
    <div class="min-w-0 flex-1 text-cp-xs text-cp-text-secondary">
      <p class="m-0 font-medium text-cp-text">
        Sub2API / LiteLLM 远程价目
      </p>
      <p class="mb-0 mt-1">
        后台每 10 分钟检查；SHA-256 校验失败时保留旧价目。人工单价和倍率不会被覆盖。
      </p>
      <p v-if="mode === 'all'" class="mb-0 mt-1">
        保存后，来源价格变动将自动影响后续新请求；历史账单与进行中的请求不变。
      </p>
    </div>
    <BaseSelect v-model="mode" class="w-56" aria-label="价格自动同步策略" :options="options" :disabled="disabled" />
    <BaseButton :disabled="disabled || mode === savedMode" :loading="saving" @click="$emit('save', { enabled: mode !== 'off', updateExisting: mode === 'all' })">
      保存策略
    </BaseButton>
  </div>
</template>
