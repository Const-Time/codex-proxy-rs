<script setup lang="ts">
import type { CostComponent, MultiplierScope } from '@/api'
import BaseCheckbox from '@/components/base/BaseCheckbox.vue'
import BaseSegmented from '@/components/base/BaseSegmented.vue'
import { costComponents } from '@/utils/multiplierScope'

defineProps<{ disabled?: boolean }>()
const scope = defineModel<MultiplierScope>({ required: true })
function toggle(component: CostComponent, selected: boolean) {
  const items = scope.value === 'all' ? costComponents.map(c => c.value) : scope.value.components
  scope.value = { components: selected ? [...new Set([...items, component])] : items.filter(c => c !== component) }
}
</script>

<template>
  <div class="grid gap-3" role="group" aria-label="倍率作用范围">
    <BaseSegmented :model-value="scope === 'all' ? 'all' : 'components'" label="倍率作用范围" :disabled="disabled" :options="[{ label: '全部费用', value: 'all' }, { label: '指定费用项', value: 'components' }]" @update:model-value="scope = $event === 'all' ? 'all' : { components: costComponents.map(c => c.value) }" />
    <div v-if="scope !== 'all'" class="flex flex-wrap gap-4">
      <BaseCheckbox v-for="item in costComponents" :key="item.value" :label="item.label" show-label :disabled="disabled" :model-value="scope.components.includes(item.value)" @update:model-value="toggle(item.value, $event)" />
    </div>
    <p v-if="scope !== 'all' && !scope.components.length" role="alert" class="m-0 text-cp-xs text-cp-error">
      请至少选择一个费用项。
    </p>
    <p class="m-0 text-cp-xs text-cp-text-secondary">
      {{ scope === 'all' ? '应用于全部费用，包含非 Token 费用。' : '可多选；未选项和非 Token 费用保持 1 倍。无法拆分的费用不应用分项倍率。' }}
    </p>
  </div>
</template>
