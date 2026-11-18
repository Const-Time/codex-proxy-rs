<script setup lang="ts">
import type { AccountDispatchHours } from '@/api'
import { computed } from 'vue'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseFormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseSelect from '@/components/base/BaseSelect.vue'
import BaseSwitch from '@/components/base/BaseSwitch.vue'

defineProps<{ disabled: boolean }>()
const hours = defineModel<AccountDispatchHours>({ required: true })
const zoneOptions = computed(() => [...new Set([hours.value.timeZone, 'Asia/Shanghai', 'UTC', 'Asia/Tokyo', 'Europe/London', 'Europe/Berlin', 'America/New_York', 'America/Los_Angeles'])].map(value => ({ value, label: value })))
function toggle(enabled: boolean) {
  hours.value = { ...hours.value, enabled, windows: enabled && !hours.value.windows.length ? [{ start: '09:00', end: '18:00' }] : hours.value.windows }
}
</script>

<template>
  <section class="grid gap-3" aria-label="允许调度的时段">
    <BaseSwitch :model-value="hours.enabled" label="限制每日调度时段" show-label :disabled="disabled" @update:model-value="toggle" />
    <template v-if="hours.enabled">
      <BaseFormItem label="执行时区">
        <BaseSelect v-model="hours.timeZone" :options="zoneOptions" :disabled="disabled" />
      </BaseFormItem>
      <div v-for="(window, index) in hours.windows" :key="index" class="grid grid-cols-[1fr_1fr_auto] items-end gap-2">
        <BaseFormItem label="开始">
          <BaseInput v-model="window.start" type="time" :disabled="disabled" />
        </BaseFormItem>
        <BaseFormItem label="结束">
          <BaseInput v-model="window.end" type="time" :disabled="disabled" />
        </BaseFormItem>
        <BaseButton size="sm" variant="secondary" :disabled="disabled" @click="hours.windows.splice(index, 1)">
          移除
        </BaseButton>
      </div>
      <div>
        <BaseButton size="sm" variant="secondary" :disabled="disabled || hours.windows.length >= 16" @click="hours.windows.push({ start: '09:00', end: '18:00' })">
          添加时段
        </BaseButton>
      </div>
      <p class="m-0 text-cp-xs text-cp-text-secondary">
        每天重复。结束时间早于开始时间表示跨午夜。到结束时间后停止分配新请求，已开始的请求正常完成。
      </p>
    </template>
  </section>
</template>
