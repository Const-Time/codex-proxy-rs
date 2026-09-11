<script setup lang="ts">
import type { UsageListRecord, UsageRecordDetail, UsageSummaryResponse } from '@/api/modules/usage'
import { onMounted, ref } from 'vue'
import { getUsageRecordDetail, getUsageRecords, getUsageRecordSummary } from '@/api/modules/usage'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseCard from '@/components/base/BaseCard.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import BasePageHeader from '@/components/base/BasePageHeader.vue'
import { errorMessage } from '@/utils/async'

const records = ref<UsageListRecord[]>([])
const summary = ref<UsageSummaryResponse | null>(null)
const detail = ref<UsageRecordDetail | null>(null)
const detailOpen = ref(false)
const detailLoading = ref(false)
const detailError = ref('')
const loading = ref(false)
const error = ref('')
const days = ref('7')
const search = ref('')
const page = ref(1)
const total = ref(0)
let generation = 0
let detailGeneration = 0
async function load(reset = false) {
  if (reset)
    page.value = 1
  const current = ++generation
  loading.value = true
  error.value = ''
  const end = new Date()
  const start = new Date(end.getTime() - Number(days.value) * 86400000)
  const query = { startTime: start.toISOString(), endTime: end.toISOString(), search: search.value.trim() || undefined }
  try {
    const [result, aggregate] = await Promise.all([getUsageRecords({ ...query, currentPage: page.value, pageSize: 25 }), getUsageRecordSummary(query)])
    if (current !== generation)
      return
    records.value = result.items
    total.value = result.total
    summary.value = aggregate
  }
  catch (cause) {
    if (current === generation) {
      records.value = []
      summary.value = null
      error.value = errorMessage(cause, '使用记录加载失败')
    }
  }
  finally {
    if (current === generation)
      loading.value = false
  }
}
async function showDetail(id: string) {
  const current = ++detailGeneration
  detailOpen.value = true
  detailLoading.value = true
  detail.value = null
  detailError.value = ''
  try {
    const result = await getUsageRecordDetail({ id })
    if (current === detailGeneration)
      detail.value = result
  }
  catch (cause) {
    if (current === detailGeneration)
      detailError.value = errorMessage(cause, '详情加载失败')
  }
  finally {
    if (current === detailGeneration)
      detailLoading.value = false
  }
}
function move(delta: number) {
  page.value += delta
  void load()
}
onMounted(() => load())
</script>

<template>
  <div class="flex flex-col gap-5">
    <BasePageHeader title="我的使用记录" description="查看自己的请求用量与费用。删除密钥后，历史记录仍会保留。" />
    <div class="flex flex-wrap gap-3">
      <select v-model="days" aria-label="时间范围" class="rounded-cp border border-cp-border bg-cp-bg px-3 py-2 text-cp-text" @change="load(true)">
        <option value="1">
          最近 24 小时
        </option><option value="7">
          最近 7 天
        </option><option value="30">
          最近 30 天
        </option>
      </select>
      <BaseInput v-model="search" aria-label="搜索使用记录" placeholder="请求 ID 或模型" class="max-w-sm" @keyup.enter="load(true)" />
      <BaseButton variant="primary" :loading="loading" @click="load(true)">
        查询
      </BaseButton>
    </div>
    <p v-if="error" role="alert" class="text-cp-error">
      {{ error }}
    </p>
    <div v-if="summary" class="grid gap-4 sm:grid-cols-3">
      <BaseCard>
        <p class="text-cp-text-secondary">
          请求数
        </p><p class="text-2xl font-semibold">
          {{ summary.totalRequests }}
        </p>
      </BaseCard>
      <BaseCard>
        <p class="text-cp-text-secondary">
          Token 总量
        </p><p class="text-2xl font-semibold">
          {{ summary.totalTokens }}
        </p>
      </BaseCard>
      <BaseCard>
        <p class="text-cp-text-secondary">
          平均延迟
        </p><p class="text-2xl font-semibold">
          {{ summary.averageLatencyMs }}
        </p>
      </BaseCard>
    </div>
    <BaseCard>
      <div class="overflow-x-auto">
        <table class="w-full text-left text-cp-sm">
          <thead class="text-cp-text-secondary">
            <tr>
              <th class="p-3">
                时间
              </th><th class="p-3">
                模型
              </th><th class="p-3">
                输入 / 输出 Token
              </th><th class="p-3">
                费用
              </th><th class="p-3">
                延迟
              </th><th class="p-3">
                操作
              </th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="record in records" :key="record.id" class="border-t border-cp-border">
              <td class="whitespace-nowrap p-3">
                {{ record.createdAtDisplay }}
              </td><td class="p-3">
                {{ record.requestedModel ?? record.model ?? '—' }}
              </td>
              <td class="p-3">
                {{ record.tokenDetails.inputTokensDisplay }} / {{ record.tokenDetails.outputTokensDisplay }}
              </td>
              <td class="p-3">
                {{ record.billing?.totalAmountDisplay ?? '—' }}
              </td><td class="p-3">
                {{ record.latencyMs == null ? '—' : `${record.latencyMs} ms` }}
              </td>
              <td class="p-3">
                <BaseButton variant="secondary" @click="showDetail(record.id)">
                  详情
                </BaseButton>
              </td>
            </tr>
            <tr v-if="!loading && !records.length">
              <td colspan="6" class="p-8 text-center text-cp-text-secondary">
                当前时间范围内没有使用记录
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <div class="mt-4 flex items-center justify-end gap-3">
        <span class="text-cp-sm text-cp-text-secondary">共 {{ total }} 条 · 第 {{ page }} 页</span><BaseButton variant="secondary" :disabled="loading || page <= 1" @click="move(-1)">
          上一页
        </BaseButton><BaseButton variant="secondary" :disabled="loading || page * 25 >= total" @click="move(1)">
          下一页
        </BaseButton>
      </div>
    </BaseCard>
    <BaseModal v-model="detailOpen" title="使用记录详情" size="md">
      <p v-if="detailLoading">
        正在加载…
      </p><p v-if="detailError" role="alert">
        {{ detailError }}
      </p>
      <dl v-if="detail" class="grid grid-cols-[auto_1fr] gap-x-5 gap-y-3 text-cp-sm">
        <dt>请求 ID</dt><dd class="break-all font-mono">
          {{ detail.id }}
        </dd><dt>模型</dt><dd>{{ detail.requestedModel ?? detail.model }}</dd>
        <dt>时间</dt><dd>{{ detail.createdAtDisplay }}</dd><dt>分组</dt><dd>{{ detail.routingGroupNamesSnapshot?.join('、') ?? '—' }}</dd>
        <dt>输入 Token</dt><dd>{{ detail.tokenDetails.inputTokensDisplay }}</dd><dt>输出 Token</dt><dd>{{ detail.tokenDetails.outputTokensDisplay }}</dd>
        <dt>缓存 Token</dt><dd>{{ detail.tokenDetails.cachedTokensDisplay }}</dd><dt>费用</dt><dd>{{ detail.billing?.totalAmountDisplay ?? '—' }}</dd><dt>延迟</dt><dd>{{ detail.latencyMsDisplay }}</dd>
      </dl>
    </BaseModal>
  </div>
</template>
