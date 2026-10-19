<script setup lang="ts">
import type { UsageTimeRange } from '../composables/useUsageTimeRange'
import { computed, ref } from 'vue'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseSelect from '@/components/base/BaseSelect.vue'
import { usageTimeRangeOptions } from '../constants'
import { customUsageRange, usageDateBounds } from '../utils/dateRange'

const emit = defineEmits<{ apply: [range: [string, string]] }>()
const model = defineModel<UsageTimeRange>({ required: true })
const bounds = ref(usageDateBounds())
const start = ref(bounds.value.max)
const end = ref(bounds.value.max)
const valid = computed(() => customUsageRange(start.value, end.value) !== null)
function apply() {
  bounds.value = usageDateBounds()
  if (customUsageRange(start.value, end.value))
    emit('apply', [start.value, end.value])
}
</script>

<template>
  <div class="flex flex-wrap items-center gap-2" @focusin="bounds = usageDateBounds()">
    <BaseSelect v-model="model" :options="usageTimeRangeOptions" class="w-34" aria-label="统计时间范围" />
    <template v-if="model === 'custom'">
      <BaseInput v-model="start" type="date" :min="bounds.min" :max="end || bounds.max" aria-label="开始日期" class="w-40" />
      <span class="text-cp-text-secondary">至</span>
      <BaseInput v-model="end" type="date" :min="start || bounds.min" :max="bounds.max" aria-label="结束日期" class="w-40" />
      <BaseButton :disabled="!valid" @click="apply">
        查询
      </BaseButton>
      <span class="text-xs text-cp-text-secondary">可查询最近 31 天</span>
    </template>
  </div>
</template>
