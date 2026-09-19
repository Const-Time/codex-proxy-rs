<script setup lang="ts">
import type { TurnStateProbeRecord, TurnStateRecordPage } from '@/api/modules/turn-state'
import type { BaseTableColumn } from '@/components/base/BaseTable/columns'
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { getTurnStateRecords } from '@/api/modules/turn-state'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseCard from '@/components/base/BaseCard.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import BaseSelect from '@/components/base/BaseSelect.vue'
import BaseTable from '@/components/base/BaseTable/index.vue'
import { useRequestState } from '@/composables/useRequestState'
import { decisionLabel, recordRange } from '../records'
import EgressSnapshot from './EgressSnapshot.vue'

const props = defineProps<{ accounts: Array<{ value: string, label: string }> }>()
const data = ref<TurnStateRecordPage | null>(null)
const account = ref('')
const phase = ref('')
const decision = ref('')
const model = ref('')
const hours = ref('24')
const page = ref(1)
const pageSize = 20
const selected = ref<TurnStateProbeRecord | null>(null)
const detailOpen = ref(false)
const cycle = ref<{ id: string, startedAt: string } | null>(null)
const request = useRequestState()
const { loading, error } = request
const accountOptions = computed(() => [{ value: '', label: '全部账号' }, ...props.accounts])
const phaseOptions = [{ value: '', label: '全部阶段' }, { value: 'collect', label: '候选采集' }, { value: 'verify', label: '业务出口验证' }]
const decisions = ['', 'accepted', 'verified', 'rejected', 'failed', 'running', 'interrupted', 'legacy'].map(value => ({ value, label: value ? decisionLabel(value) : '全部结果' }))
const columns: BaseTableColumn<TurnStateProbeRecord>[] = [
  { key: 'time', label: '时间 / 账号', kind: 'custom', size: 'xl' },
  { key: 'model', label: '模型 / 阶段', kind: 'custom', size: 'xl' },
  { key: 'egress', label: '出口 IP / 归属地区', kind: 'custom', size: '2xl' },
  { key: 'http', label: 'HTTP / 完成情况', kind: 'custom', size: 'lg' },
  { key: 'shape', label: 'State 结构', kind: 'custom', size: 'lg' },
  { key: 'decision', label: '处理结果', kind: 'custom', size: 'lg' },
  { key: 'latency', label: '耗时', kind: 'numeric', size: 'sm' },
  { key: 'actions', label: '操作', kind: 'actions', size: 'sm' },
]
const metrics = computed(() => {
  const s = data.value?.stats
  return [
    { label: '匹配探测请求', value: s?.total ?? '—', note: `${s?.completed ?? 0} 次流正常完成` },
    { label: '未入选 / 失败', value: s ? s.rejected + s.failed : '—', note: `${s?.rejected ?? 0} 次未入选 · ${s?.failed ?? 0} 次失败或中断` },
    { label: '已知 Token', value: s?.knownTokens.toLocaleString() ?? '—', note: `输入 + 输出 · ${s?.unknownTokens ?? 0} 条未知，不按零计` },
    { label: '平均请求耗时', value: s?.averageLatencyMs == null ? '—' : `${(s.averageLatencyMs / 1000).toFixed(2)} s`, note: '包含超时；不含出口检测耗时' },
  ]
})
let timer: ReturnType<typeof setInterval> | undefined

async function load() {
  const id = request.start()
  try {
    const result = await getTurnStateRecords({
      ...(cycle.value
        ? {
            from: new Date(Date.parse(cycle.value.startedAt) - 3600000).toISOString(),
            to: new Date(Date.parse(cycle.value.startedAt) + 3600000).toISOString(),
          }
        : recordRange(Number(hours.value))),
      cycleId: cycle.value?.id,
      page: page.value,
      pageSize,
      accountId: account.value || undefined,
      phase: phase.value || undefined,
      decision: decision.value || undefined,
      model: model.value.trim() || undefined,
    })
    if (request.isCurrent(id)) {
      data.value = result
      const lastPage = Math.max(1, Math.ceil(result.stats.total / pageSize))
      if (page.value > lastPage)
        page.value = lastPage
    }
  }
  catch (e) {
    request.fail(id, e)
  }
  finally {
    request.finish(id)
  }
}
function search() {
  if (page.value === 1)
    void load()
  else
    page.value = 1
}
function related() {
  if (!selected.value)
    return
  cycle.value = { id: selected.value.cycleId, startedAt: selected.value.startedAt }
  account.value = ''
  phase.value = ''
  decision.value = ''
  model.value = ''
  detailOpen.value = false
  search()
}
watch([account, phase, decision, hours], search)
watch(page, () => void load())
onMounted(() => {
  void load()
  timer = setInterval(() => {
    if (!loading.value)
      void load()
  }, 10000)
})
onBeforeUnmount(() => {
  clearInterval(timer)
  request.invalidate()
})
</script>

<template>
  <div class="grid gap-5">
    <p class="text-cp-sm text-cp-text-secondary">
      采集与验证分别记录，不进入业务请求明细、不扣用户余额。验证通过不等于候选已发布；配置变化时可能撤销发布。
    </p>
    <div class="grid grid-cols-2 gap-4 xl:grid-cols-4">
      <BaseCard v-for="metric in metrics" :key="metric.label">
        <p class="text-cp-sm text-cp-text-secondary">
          {{ metric.label }}
        </p>
        <p class="my-3 text-2xl font-semibold tabular-nums">
          {{ metric.value }}
        </p>
        <p class="text-cp-xs text-cp-text-tertiary">
          {{ metric.note }}
        </p>
      </BaseCard>
    </div>
    <BaseCard title="独立探测记录">
      <div v-if="cycle" class="mb-4 flex flex-wrap items-center gap-3 text-cp-sm">
        <span class="break-all">关联周期：{{ cycle.id }}（该记录前后 1 小时）</span><BaseButton size="sm" @click="cycle = null; search()">
          返回全部记录
        </BaseButton>
      </div>
      <div class="mb-4 flex flex-wrap gap-3">
        <BaseSelect v-model="account" :options="accountOptions" aria-label="筛选账号" class="w-48" />
        <BaseSelect v-model="phase" :options="phaseOptions" aria-label="筛选阶段" class="w-40" />
        <BaseSelect v-model="decision" :options="decisions" aria-label="筛选处理结果" class="w-40" />
        <BaseSelect v-model="hours" :disabled="!!cycle" :options="[{ value: '24', label: '近 24 小时' }, { value: '168', label: '近 7 天' }, { value: '720', label: '近 30 天' }]" aria-label="时间范围" class="w-36" />
        <BaseInput v-model="model" placeholder="精确模型名" aria-label="筛选精确模型" class="w-48" @keydown.enter="search" />
        <BaseButton :loading="loading" @click="search">
          查询
        </BaseButton>
      </div>
      <p v-if="error" role="alert" class="mb-3 text-cp-error">
        {{ error }}（下方保留上次成功加载的数据）
      </p>
      <BaseTable :columns="columns" :rows="data?.items ?? []" :loading="loading" empty-text="当前筛选没有探测记录">
        <template #time="{ row }">
          <div>
            {{ new Date(row.startedAt).toLocaleString() }}<p class="mt-1 text-cp-xs text-cp-text-secondary">
              {{ row.accountName }}
            </p>
          </div>
        </template>
        <template #model="{ row }">
          <div>
            {{ row.model }}<p class="mt-1 text-cp-xs text-cp-text-secondary">
              {{ row.phase === 'collect' ? '候选采集' : '业务出口验证' }} · {{ row.routeName }}
            </p>
          </div>
        </template>
        <template #egress="{ row }">
          <EgressSnapshot :value="row.facts.egress" />
        </template>
        <template #http="{ row }">
          <div>
            {{ row.facts.status ?? '—' }}<p class="mt-1 text-cp-xs text-cp-text-secondary">
              {{ row.facts.completed ? '流正常完成' : row.facts.decision === 'running' ? '进行中' : '未确认正常完成' }}
            </p>
          </div>
        </template>
        <template #shape="{ row }">
          <div>
            <span class="rounded-cp bg-cp-fill-tertiary px-2 py-1 font-mono">{{ row.facts.stateLength ?? '—' }}</span><p class="mt-2 text-cp-xs text-cp-text-secondary">
              {{ row.facts.shape ? `${row.facts.shape.blocks} 块` : '结构未知' }} · {{ row.phase === 'collect' ? '上游返回' : '实际发送' }}
            </p>
          </div>
        </template>
        <template #decision="{ row }">
          <span :class="['accepted', 'verified'].includes(row.facts.decision) ? 'text-cp-success' : 'text-cp-text-secondary'">{{ decisionLabel(row.facts.decision) }}</span>
        </template>
        <template #latency="{ row }">
          {{ row.facts.latencyMs == null ? '—' : `${(row.facts.latencyMs / 1000).toFixed(2)} s` }}
        </template>
        <template #actions="{ row }">
          <BaseButton size="sm" variant="ghost" @click="selected = row; detailOpen = true">
            详情
          </BaseButton>
        </template>
      </BaseTable>
      <div class="mt-4 flex flex-wrap items-center justify-between gap-3 text-cp-sm">
        <span>共 {{ data?.stats.total ?? 0 }} 条 · 第 {{ page }} / {{ Math.max(1, Math.ceil((data?.stats.total ?? 0) / pageSize)) }} 页</span>
        <div class="flex gap-2">
          <BaseButton :disabled="loading || page <= 1" @click="page--">
            上一页
          </BaseButton><BaseButton :disabled="loading || page * pageSize >= (data?.stats.total ?? 0)" @click="page++">
            下一页
          </BaseButton>
        </div>
      </div>
    </BaseCard>
    <BaseModal v-model="detailOpen" title="探测详情" :description="selected?.id" size="lg">
      <div v-if="selected" class="grid gap-5">
        <p :class="['accepted', 'verified'].includes(selected.facts.decision) ? 'text-cp-success' : 'text-cp-warning'">
          {{ decisionLabel(selected.facts.decision) }}
        </p>
        <p class="rounded-cp bg-cp-fill-quaternary p-3">
          {{ selected.facts.message ?? (selected.phase === 'collect' ? '候选规则检查通过；后续需要业务出口验证。' : '业务出口验证完成；是否发布以运行概览为准。') }}
        </p>
        <dl class="grid grid-cols-2 gap-4 text-cp-sm">
          <div>
            <dt class="text-cp-text-tertiary">
              账号 / 精确模型
            </dt><dd class="break-all">
              {{ selected.accountName }} / {{ selected.model }}
            </dd>
          </div>
          <div>
            <dt class="text-cp-text-tertiary">
              出口 / 阶段
            </dt><dd>{{ selected.routeName }} / {{ selected.phase === 'collect' ? '采集' : '验证' }}</dd>
          </div>
          <div class="col-span-2">
            <dt class="mb-2 text-cp-text-tertiary">
              出口 IP / 归属地区
            </dt><dd><EgressSnapshot :value="selected.facts.egress" /></dd>
          </div>
          <div>
            <dt class="text-cp-text-tertiary">
              HTTP / 流完成
            </dt><dd>{{ selected.facts.status ?? '—' }} / {{ selected.facts.completed ? '已完成' : '未确认正常完成' }}</dd>
          </div>
          <div>
            <dt class="text-cp-text-tertiary">
              输入 / 输出 Token
            </dt><dd>{{ selected.facts.inputTokens ?? '未知' }} / {{ selected.facts.outputTokens ?? '未知' }}</dd>
          </div>
          <div>
            <dt class="text-cp-text-tertiary">
              字符数 / 密文块
            </dt><dd>{{ selected.facts.stateLength ?? '—' }} / {{ selected.facts.shape?.blocks ?? '—' }}</dd>
          </div>
          <div>
            <dt class="text-cp-text-tertiary">
              State 指纹
            </dt><dd class="break-all font-mono">
              {{ selected.facts.fingerprint ?? '—' }}
            </dd>
          </div>
          <div>
            <dt class="text-cp-text-tertiary">
              估算到期时间
            </dt><dd>{{ selected.facts.expiresAt ? new Date(selected.facts.expiresAt * 1000).toLocaleString() : '—' }}</dd>
          </div>
          <div>
            <dt class="text-cp-text-tertiary">
              原因代码
            </dt><dd>{{ selected.facts.reason ?? '—' }}</dd>
          </div>
          <div class="col-span-2">
            <dt class="text-cp-text-tertiary">
              关联采集 / 验证周期
            </dt><dd class="break-all font-mono">
              {{ selected.cycleId }}
            </dd>
          </div>
        </dl>
        <BaseButton v-if="selected.facts.decision !== 'legacy'" @click="related">
          查看同周期采集 / 验证记录
        </BaseButton>
        <p class="text-cp-xs text-cp-text-tertiary">
          独立出口检测最多缓存 60 秒，不证明本次上游请求使用了相同 IP；轮换出口仅供参考。地区识别失败保留未知。日志不保存 State 原文或代理认证。
        </p>
      </div>
    </BaseModal>
  </div>
</template>
