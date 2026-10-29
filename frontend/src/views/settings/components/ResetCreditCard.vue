<script setup lang="ts">
import BaseCard from '@/components/base/BaseCard.vue'
import BaseFormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseForm from '@/components/base/BaseForm/index.vue'
import BaseInput from '@/components/base/BaseInput.vue'

defineProps<{ disabled?: boolean }>()
const leadMinutes = defineModel<string>({ required: true })
const maxWeeklyRemainingPercent = defineModel<string>('maxWeeklyRemainingPercent', { required: true })
</script>

<template>
  <BaseCard id="reset-credits" title="主动重置卡" description="在账号的「额度重置」中开启自动使用">
    <BaseForm class="max-w-6xl sm:grid-cols-2">
      <BaseFormItem label="提前使用时间（分钟）" description="全局设置，对已开启自动使用的账号生效">
        <BaseInput v-model="leadMinutes" :disabled="disabled" type="number" min="1" max="1440" step="1" aria-label="重置卡提前使用时间" />
      </BaseFormItem>
      <BaseFormItem label="剩余周额度上限（%）" description="高于此值时暂不自动使用，留空表示不限制">
        <BaseInput v-model="maxWeeklyRemainingPercent" :disabled="disabled" type="number" min="0" max="100" step="1" placeholder="不限制" aria-label="重置卡剩余周额度上限" />
      </BaseFormItem>
    </BaseForm>
  </BaseCard>
</template>
