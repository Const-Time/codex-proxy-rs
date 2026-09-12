<script setup lang="ts">
import { Gauge, Timer, Zap } from '@lucide/vue'

import BaseCard from '@/components/base/BaseCard.vue'
import BaseFormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseForm from '@/components/base/BaseForm/index.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseSwitch from '@/components/base/BaseSwitch.vue'

defineProps<{ disabled?: boolean }>()
const subscriptionAutoResetEnabled = defineModel<boolean>('subscriptionAutoResetEnabled', { required: true })

const maxConcurrentPerAccount = defineModel<string>('maxConcurrentPerAccount', { required: true })
const refreshMarginSeconds = defineModel<string>('refreshMarginSeconds', { required: true })
const refreshConcurrency = defineModel<string>('refreshConcurrency', { required: true })
const requestIntervalMs = defineModel<string>('requestIntervalMs', { required: true })
</script>

<template>
  <BaseCard
    title="运行参数"
    description="请求节奏、账号并发和 Token 刷新"
  >
    <BaseForm class="max-w-6xl sm:grid-cols-2">
      <BaseFormItem
        label="单账号默认最大并发"
        description="账号未单独设置时使用的并发上限"
      >
        <BaseInput
          v-model="maxConcurrentPerAccount"
          aria-label="单账号默认最大并发"
          type="number"
        >
          <template #prefix>
            <Gauge class="size-4" />
          </template>
        </BaseInput>
      </BaseFormItem>

      <BaseFormItem
        label="提前刷新秒数"
        description="Token 过期前多少秒触发刷新"
      >
        <BaseInput
          v-model="refreshMarginSeconds"
          aria-label="提前刷新秒数"
          type="number"
        >
          <template #prefix>
            <Timer class="size-4" />
          </template>
        </BaseInput>
      </BaseFormItem>

      <BaseFormItem
        label="刷新并发数"
        description="同时刷新 Token 的最大请求数，减小可避免限流"
      >
        <BaseInput
          v-model="refreshConcurrency"
          aria-label="刷新并发数"
          type="number"
        >
          <template #prefix>
            <Zap class="size-4" />
          </template>
        </BaseInput>
      </BaseFormItem>

      <BaseFormItem
        label="请求间隔 ms"
        description="控制同一账号两次调度之间的最小等待时间"
      >
        <BaseInput
          v-model="requestIntervalMs"
          aria-label="请求间隔 ms"
          type="number"
        >
          <template #prefix>
            <Timer class="size-4" />
          </template>
        </BaseInput>
      </BaseFormItem>
    </BaseForm>
    <div class="mt-5 flex items-center justify-between gap-4 border-t border-cp-border pt-5">
      <div>
        <p class="font-medium text-cp-text">
          自动重置订阅分组配额
        </p>
        <p class="mt-1 text-cp-sm text-cp-text-secondary">
          账号主动重置或上游周窗口变化时，自动重置关联分组下用户的日、周配额。关闭后仍可手动重置，订阅自身到期刷新不受影响；重新开启不补做关闭期间的重置。保存后生效。
        </p>
      </div>
      <BaseSwitch v-model="subscriptionAutoResetEnabled" label="自动重置订阅分组配额" :disabled="disabled" />
    </div>
  </BaseCard>
</template>
