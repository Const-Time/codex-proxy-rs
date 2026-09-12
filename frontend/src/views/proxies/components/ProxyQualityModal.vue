<script setup lang="ts">
import type { OutboundProxyRecord, ProxyQualityCheck, ProxyQualityReport } from '@/api'
import { Activity, LoaderCircle, RefreshCw } from '@lucide/vue'
import { computed, onBeforeUnmount, shallowRef, watch } from 'vue'
import { testProxyQuality } from '@/api'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import { errorMessage } from '@/utils/async'
import { formatDateTime } from '@/utils/date'

const props = defineProps<{ proxy: OutboundProxyRecord | null }>()
const emit = defineEmits<{ tested: [] }>()
const open = defineModel<boolean>({ required: true })
const report = shallowRef<ProxyQualityReport | null>(null)
const loading = shallowRef(false)
const error = shallowRef('')
let generation = 0
const labels: Record<ProxyQualityCheck['status'], string> = {
  passed: '通过',
  warning: '告警',
  failed: '失败',
  challenge: '挑战',
}
const tones: Record<ProxyQualityCheck['status'], string> = {
  passed: 'bg-cp-success-container text-cp-success',
  warning: 'bg-cp-warning-container text-cp-warning',
  failed: 'bg-cp-error-container text-cp-error',
  challenge: 'bg-cp-warning-container text-cp-warning',
}
const scoreTone = computed(() => !report.value || report.value.score >= 90
  ? 'text-cp-success'
  : report.value.score >= 60 ? 'text-cp-warning' : 'text-cp-error')

async function run() {
  const proxy = props.proxy
  if (!proxy || loading.value)
    return
  const current = ++generation
  report.value = null
  error.value = ''
  loading.value = true
  try {
    const result = await testProxyQuality({ id: proxy.id, revision: proxy.revision })
    emit('tested')
    if (current === generation)
      report.value = result
  }
  catch (cause) {
    if (current === generation)
      error.value = errorMessage(cause, '质量检测失败，请稍后重试')
  }
  finally {
    if (current === generation)
      loading.value = false
  }
}

watch([open, () => props.proxy?.id, () => props.proxy?.revision], () => {
  generation += 1
  loading.value = false
  report.value = null
  error.value = ''
  if (open.value)
    void run()
})
onBeforeUnmount(() => {
  generation += 1
})
</script>

<template>
  <BaseModal v-model="open" title="代理质量检测报告" :description="proxy?.name" size="lg">
    <div class="space-y-5" aria-live="polite" :aria-busy="loading">
      <p class="m-0 break-all font-mono text-cp-xs text-cp-text-secondary">
        {{ proxy?.endpoint }}
      </p>
      <div v-if="loading" class="flex min-h-64 flex-col items-center justify-center gap-4 text-center">
        <LoaderCircle class="size-8 animate-spin text-cp-primary-text" aria-hidden="true" />
        <div>
          <p class="m-0 font-emphasis text-cp-text">
            正在检测代理质量
          </p>
          <p class="mt-2 text-cp-sm text-cp-text-secondary">
            检测出口、平台 HTTPS 和 Codex WebSocket，预计最多 30 秒。
          </p>
        </div>
      </div>
      <div v-else-if="error" role="alert" class="rounded-xl border border-cp-border p-5 text-cp-sm text-cp-error">
        {{ error }}
      </div>
      <template v-else-if="report">
        <div class="rounded-xl border border-cp-border bg-cp-bg-layout p-4 sm:p-5">
          <div class="flex items-start justify-between gap-4">
            <div class="min-w-0">
              <div class="flex items-center gap-2 font-emphasis text-cp-text">
                <Activity class="size-4 text-cp-primary-text" aria-hidden="true" />
                检测完成
              </div>
              <p class="mt-2 flex flex-wrap gap-x-3 gap-y-1 text-cp-sm text-cp-text-secondary">
                <span>通过 {{ report.passed }} 项</span><span>告警 {{ report.warnings }} 项</span>
                <span>失败 {{ report.failed }} 项</span><span>挑战 {{ report.challenges }} 项</span>
              </p>
            </div>
            <div class="shrink-0 text-right" :class="scoreTone">
              <div class="text-3xl font-bold tabular-nums">
                {{ report.score }}
              </div>
              <div class="mt-1 text-cp-xs">
                可达性评分 · {{ report.grade }}
              </div>
            </div>
          </div>
          <dl class="mb-0 mt-4 grid grid-cols-1 gap-3 text-cp-xs sm:grid-cols-2">
            <div>
              <dt class="inline text-cp-text-secondary">
                出口 IP：
              </dt><dd class="ml-0 inline break-all font-mono">
                {{ report.exitIp ?? '未获取' }}
              </dd>
            </div>
            <div>
              <dt class="inline text-cp-text-secondary">
                基础耗时：
              </dt><dd class="ml-0 inline tabular-nums">
                {{ report.basicLatencyMs }} ms
              </dd>
            </div>
            <div>
              <dt class="inline text-cp-text-secondary">
                检测时间：
              </dt><dd class="ml-0 inline">
                {{ formatDateTime(report.testedAt) }}
              </dd>
            </div>
            <div>
              <dt class="inline text-cp-text-secondary">
                总耗时：
              </dt><dd class="ml-0 inline tabular-nums">
                {{ report.durationMs }} ms
              </dd>
            </div>
          </dl>
        </div>
        <div class="overflow-x-auto rounded-xl border border-cp-border">
          <table class="cp-mobile-record-table w-full min-w-130 border-collapse text-left text-cp-sm">
            <caption class="sr-only">
              各目标的连通状态、HTTP 状态码和响应耗时
            </caption>
            <thead class="bg-cp-bg-layout text-cp-xs text-cp-text-secondary">
              <tr>
                <th scope="col" class="px-3 py-3">
                  检测项
                </th><th scope="col" class="px-3 py-3">
                  状态
                </th><th scope="col" class="px-3 py-3">
                  HTTP
                </th><th scope="col" class="px-3 py-3">
                  耗时
                </th><th scope="col" class="px-3 py-3">
                  说明
                </th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="check in report.checks" :key="check.name" class="border-t border-cp-border">
                <th scope="row" class="whitespace-nowrap px-3 py-3 font-emphasis">
                  {{ check.name }}
                </th>
                <td data-label="状态" class="px-3 py-3">
                  <span class="whitespace-nowrap rounded-full px-2 py-1 text-cp-xs" :class="tones[check.status]">{{ labels[check.status] }}</span>
                </td>
                <td data-label="HTTP" class="px-3 py-3 font-mono text-cp-xs">
                  {{ check.httpStatus ?? '—' }}
                </td>
                <td data-label="耗时" class="whitespace-nowrap px-3 py-3 tabular-nums">
                  {{ check.latencyMs }} ms
                </td>
                <td data-label="说明" class="min-w-44 px-3 py-3 text-cp-xs leading-relaxed text-cp-text-secondary">
                  {{ check.message }}
                </td>
              </tr>
            </tbody>
          </table>
        </div>
        <p class="m-0 text-cp-xs leading-relaxed text-cp-text-secondary">
          评分按每项通过 100、告警 50、失败或挑战 0 分取平均，仅反映本次可达性。401 / 405 表示目标可达；WebSocket 只有 101 才表示握手成功。实际模型调用请结合账号连接测试确认。
        </p>
      </template>
    </div>
    <template #footer>
      <div class="flex items-center justify-end gap-2">
        <BaseButton @click="open = false">
          关闭
        </BaseButton>
        <BaseButton variant="primary" :loading="loading" :disabled="loading" @click="run">
          <template #icon>
            <RefreshCw class="size-4" />
          </template>
          重新检测
        </BaseButton>
      </div>
    </template>
  </BaseModal>
</template>
