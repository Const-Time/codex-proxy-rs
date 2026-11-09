<script setup lang="ts">
import type { Account } from '@/api'
import { computed } from 'vue'
import { batchUpdateAccounts, getAccountDetail } from '@/api'
import BaseSwitch from '@/components/base/BaseSwitch.vue'
import { useAsyncAction } from '@/composables/useAsyncAction'

const props = defineProps<{
  account: Account
}>()

const emit = defineEmits<{ accountUpdated: [account: Account] }>()
const saveAction = useAsyncAction()
async function toggle(enabled: boolean) {
  const accountId = props.account.id
  await saveAction.run(async () => {
    try {
      await batchUpdateAccounts({ accountIds: [accountId], autoUseQuotaCredits: enabled })
    }
    finally {
      const { account } = await getAccountDetail({ accountId })
      if (props.account.id === accountId)
        emit('accountUpdated', account)
    }
  })
}

const integerFormatter = new Intl.NumberFormat('zh-CN')
const balanceDisplay = computed(() => {
  const balance = props.account.quota.credits?.balance?.trim()
  const decimal = balance?.match(/^(-?)(\d+)(?:\.(\d+))?$/)
  if (!decimal)
    return null
  const [, sign, integer, fraction] = decimal
  // 点数可能包含高精度小数，整数分组与小数文本分别处理，避免转成浮点数。
  const grouped = `${sign}${integerFormatter.format(BigInt(integer!))}`
  return fraction ? `${grouped}.${fraction}` : grouped
})
const value = computed(() => {
  if (props.account.quota.credits?.unlimited)
    return '无限'
  if (balanceDisplay.value !== null)
    return balanceDisplay.value
  if (props.account.quota.credits && !props.account.quota.credits.hasCredits)
    return '暂无可用点数'
  return '未提供余额'
})
const hasBalance = computed(() => !props.account.quota.credits?.unlimited && balanceDisplay.value !== null)
</script>

<template>
  <div class="flex min-w-0 flex-wrap items-baseline justify-between gap-x-4 gap-y-1 rounded-cp bg-cp-fill-quaternary px-4 py-3.5" aria-label="额度点数">
    <span class="text-cp-sm font-heavy text-cp-text">额度点数</span>
    <span class="flex min-w-0 items-baseline gap-1.5">
      <strong
        class="min-w-0 text-right text-cp-sm font-heavy wrap-anywhere"
        :class="hasBalance ? 'font-mono tabular-nums text-cp-text' : 'text-cp-text-secondary'"
      >
        {{ value }}
      </strong>
      <span v-if="hasBalance" class="shrink-0 text-cp-xs text-cp-text-tertiary">点</span>
    </span>
    <div class="flex w-full items-center justify-between gap-4 border-t border-cp-border-secondary pt-3 mt-2">
      <div>
        <div class="text-cp-sm text-cp-text">
          自动使用额度点数
        </div>
        <p class="m-0 mt-1 text-cp-xs text-cp-text-secondary">
          套餐额度耗尽后允许使用点数，默认关闭。
        </p>
      </div>
      <BaseSwitch :model-value="account.autoUseQuotaCredits" :disabled="saveAction.loading.value" label="自动使用额度点数" :show-label="false" @update:model-value="toggle" />
    </div>
  </div>
</template>
