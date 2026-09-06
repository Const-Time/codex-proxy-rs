<script setup lang="ts">
import { computed } from 'vue'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseScrollbar from '@/components/base/BaseScrollbar.vue'
import { useAsyncAction } from '@/composables/useAsyncAction'
import { useDownload } from '@/composables/useDownload'
import { useRequestDiagnostics } from '../composables/useRequestDiagnostics'
import UsageDetailCodePanel from './UsageDetailCodePanel.vue'

const props = defineProps<{ requestId: string }>()
const { downloadJson } = useDownload()
const { loading: exporting, run: runExport } = useAsyncAction()
const { selectedId, detail, loading, error, refresh } = useRequestDiagnostics(() => props.requestId)
const trace = computed(() => detail.value?.trace)
const events = computed(() => (trace.value?.events ?? []).map(event => ({
  ...event,
  content: JSON.stringify(event.data, null, 2),
})))

function download() {
  if (!detail.value || exporting.value)
    return
  const record = detail.value
  const bundle = {
    schemaVersion: 1,
    exportedAt: new Date().toISOString(),
    request: {
      requestId: record.requestId,
      route: record.route,
      model: record.model,
      provider: record.provider,
      accountId: record.accountId,
      clientTransport: record.clientTransport,
      upstreamTransport: record.upstreamTransport,
      upstreamRequestId: record.upstreamRequestId,
      responseId: record.responseId,
      statusCode: record.statusCode,
      attemptCount: record.attemptCount,
      createdAt: record.createdAt,
    },
    trace: record.trace,
    attempts: record.attempts,
    attemptsComplete: record.attemptsComplete,
    relatedRequests: record.relatedRequests,
  }
  return runExport(
    () => downloadJson(bundle, `diagnostics-${record.requestId.replace(/[^\w-]/g, '_')}.json`),
    { errorText: '导出诊断包失败', minimumMs: 400 },
  )
}
</script>

<template>
  <section class="mt-3 min-w-0 rounded-cp-card bg-cp-fill-quaternary px-4 py-3.5" aria-label="请求诊断">
    <div class="flex flex-wrap items-center justify-between gap-3">
      <h3 class="m-0 text-cp-sm font-heavy text-cp-text-secondary">
        请求诊断
      </h3>
      <div class="flex flex-wrap gap-2">
        <BaseButton v-if="selectedId !== requestId" size="sm" @click="selectedId = requestId">
          返回本次请求
        </BaseButton>
        <BaseButton size="sm" :disabled="loading" @click="refresh">
          刷新
        </BaseButton>
        <BaseButton size="sm" :loading="exporting" :disabled="!detail" @click="download">
          导出诊断包
        </BaseButton>
      </div>
    </div>
    <p class="mt-1 mb-3 break-all font-mono text-cp-xs leading-relaxed text-cp-text-secondary">
      {{ selectedId }}
    </p>
    <p v-if="loading" role="status" class="text-cp-sm text-cp-text-secondary">
      正在加载诊断记录…
    </p>
    <p v-else-if="error" role="alert" class="text-cp-sm text-cp-error-text">
      {{ error }}
    </p>
    <template v-else-if="detail">
      <div v-if="detail.relatedRequests?.length" class="mb-3 flex flex-wrap gap-2">
        <BaseButton v-for="related in detail.relatedRequests" :key="related.requestId" size="sm" @click="selectedId = related.requestId">
          {{ related.relation === 'recovered_by' ? '查看恢复请求' : '查看先前失败' }} · {{ related.requestId }}
        </BaseButton>
      </div>
      <p v-if="!trace" class="text-cp-sm text-cp-text-secondary">
        这条记录没有保存诊断时间线。旧记录无法补回当时未采集的事件。
      </p>
      <template v-else>
        <div class="mb-3 grid gap-1 text-cp-xs leading-relaxed">
          <p class="m-0 text-cp-text-secondary">
            已观测 {{ trace.totalEvents }} 个阶段或事件，展示 {{ events.length }} 条摘要。连续增量合并计数；正文和敏感字段仅保留摘要。
          </p>
          <p v-if="trace.droppedEvents" role="status" class="m-0 text-cp-warning-text">
            达到保存上限，{{ trace.droppedEvents }} 个事件已被淘汰；保留请求开头与最近事件。
          </p>
        </div>
        <BaseScrollbar max-height="32rem">
          <ol class="m-0 grid list-none gap-2 p-0 pr-3">
            <li v-for="event in events" :key="event.sequence" class="min-w-0 rounded-cp bg-cp-bg-container">
              <details>
                <summary class="cursor-pointer rounded-cp px-3 py-2.5 break-all text-cp-xs leading-relaxed hover:bg-cp-fill-quaternary focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-cp-primary">
                  <span class="font-mono text-cp-text-secondary">+{{ event.elapsedMs }} ms · #{{ event.sequence }}</span>
                  <span class="mx-2 font-mono text-cp-text">{{ event.stage }}</span>
                  <span v-if="event.attemptIndex" class="text-cp-text-secondary">尝试 {{ event.attemptIndex }}</span>
                  <span v-if="event.exchangeId" class="text-cp-text-secondary"> · 交换 {{ event.exchangeId }}</span>
                  <span v-if="event.count > 1" class="text-cp-text-secondary"> · {{ event.count }} 次（至 +{{ event.lastElapsedMs }} ms）</span>
                </summary>
                <div class="mx-3 border-t border-cp-border-secondary py-3">
                  <UsageDetailCodePanel title="诊断事实" :content="event.content" max-height="280px" />
                </div>
              </details>
            </li>
          </ol>
        </BaseScrollbar>
        <p class="mt-3 mb-0 text-cp-xs leading-relaxed text-cp-text-secondary">
          时间线保存至执行结束。客户端最终写入、进程退出等后续事实，可用此请求 ID 检索服务器日志。
        </p>
      </template>
    </template>
  </section>
</template>
