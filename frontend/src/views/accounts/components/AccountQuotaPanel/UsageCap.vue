<script setup lang="ts">
import type { Account } from '@/api'
import { computed, shallowRef, watch } from 'vue'
import { batchUpdateAccounts, getAccountDetail } from '@/api'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseFormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import { toast } from '@/components/base/BaseToast'
import { useAsyncAction } from '@/composables/useAsyncAction'
import { parseQuotaUsageLimits } from '../../utils/quotaUsageLimits'

const props = defineProps<{ account: Account }>()
const emit = defineEmits<{ accountUpdated: [account: Account] }>()
const shortTerm = shallowRef('')
const weekly = shallowRef('')
const action = useAsyncAction()
watch(() => [props.account.id, props.account.quotaUsageLimits] as const, () => {
  shortTerm.value = String(props.account.quotaUsageLimits.shortTermPercent)
  weekly.value = String(props.account.quotaUsageLimits.weeklyPercent)
}, { immediate: true })
const limits = computed(() => parseQuotaUsageLimits(shortTerm.value, weekly.value))
const changed = computed(() => limits.value?.shortTermPercent !== props.account.quotaUsageLimits.shortTermPercent
  || limits.value?.weeklyPercent !== props.account.quotaUsageLimits.weeklyPercent)

async function save() {
  if (!limits.value || action.loading.value)
    return
  const accountId = props.account.id
  const quotaUsageLimits = limits.value
  await action.run(async () => {
    try {
      await batchUpdateAccounts({ accountIds: [accountId], quotaUsageLimits })
      toast.success('额度调用上限已保存')
    }
    finally {
      const { account } = await getAccountDetail({ accountId })
      if (props.account.id === accountId)
        emit('accountUpdated', account)
    }
  }, { errorText: '额度调用上限保存失败' })
}
</script>

<template>
  <section class="mt-3 grid gap-3 border-t border-cp-border-secondary pt-3" aria-label="额度调用上限">
    <h4 class="m-0 text-cp-sm font-heavy text-cp-text">
      额度调用上限
    </h4>
    <div class="grid gap-3 sm:grid-cols-2">
      <BaseFormItem label="短周期已用上限（%）">
        <BaseInput v-model="shortTerm" type="number" min="1" max="100" step="1" aria-label="短周期额度已用上限" placeholder="100" :disabled="action.loading.value" />
      </BaseFormItem>
      <BaseFormItem label="周额度已用上限（%）">
        <BaseInput v-model="weekly" type="number" min="1" max="100" step="1" aria-label="周额度已用上限" placeholder="100" :disabled="action.loading.value" />
      </BaseFormItem>
    </div>
    <p class="m-0 text-cp-xs text-cp-text-secondary">
      达到任一上限后停止分配新请求，窗口重置后自动恢复。100 或留空表示不额外限制；以最近观测额度为准，进行中的请求不会中断。
    </p>
    <p v-if="!limits" role="alert" class="m-0 text-cp-xs text-cp-error">
      请输入 1–100 的整数。
    </p>
    <div class="flex justify-end">
      <BaseButton size="sm" :disabled="!limits || !changed" :loading="action.loading.value" @click="save">
        保存上限
      </BaseButton>
    </div>
  </section>
</template>
