<script setup lang="ts">
import type { Account } from '@/api/modules/accounts'
import type { TurnStateEgress, TurnStateRecordPage, TurnStateSettings, TurnStateTarget, TurnStateView } from '@/api/modules/turn-state'
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { onBeforeRouteLeave, useRoute } from 'vue-router'
import { getAccountModels, getAccounts, refreshAccountModels } from '@/api/modules/accounts'
import { getTurnState, getTurnStateRecords, saveTurnState, testTurnStatePool, turnStateAction } from '@/api/modules/turn-state'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseCard from '@/components/base/BaseCard.vue'
import BaseCheckbox from '@/components/base/BaseCheckbox.vue'
import BaseConfirmModal from '@/components/base/BaseConfirmModal.vue'
import FormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseNumberInput from '@/components/base/BaseNumberInput.vue'
import BasePageHeader from '@/components/base/BasePageHeader.vue'
import BaseSegmented from '@/components/base/BaseSegmented.vue'
import BaseSelect from '@/components/base/BaseSelect.vue'
import BaseSwitch from '@/components/base/BaseSwitch.vue'
import { toast } from '@/components/base/BaseToast'
import { useRequestState } from '@/composables/useRequestState'
import { errorMessage } from '@/utils/async'
import AccountMaintenance from './components/AccountMaintenance.vue'
import EgressSnapshot from './components/EgressSnapshot.vue'
import ProbeRecords from './components/ProbeRecords.vue'
import { maintenanceModels, remainingLabel, shapeLabel, takeoverPolicyLabel, waitReasonLabel } from './presenter'
import { ratio, recordRange } from './records'

const route = useRoute()
const activeTab = ref(route.query.account ? 'settings' : 'overview')
const settingsTab = ref('accounts')
const accountDirty = ref(false)
const accountBusy = ref(false)
const overview = ref<TurnStateRecordPage | null>(null)
const overviewRequest = useRequestState()
const { error: overviewError } = overviewRequest
const poolEgress = ref<Record<string, TurnStateEgress | null>>({})
const tabs = [{ value: 'overview', label: '运行概览' }, { value: 'records', label: '探测记录' }, { value: 'settings', label: '维护配置' }]
const settingsTabs = [{ value: 'accounts', label: '账号与模型' }, { value: 'proxies', label: '采集代理' }, { value: 'global', label: '全局策略' }]
const view = ref<TurnStateView | null>(null)
const draft = ref<TurnStateSettings | null>(null)
const savedDraft = ref('')
const busy = ref(false)
const loading = ref(false)
const loadError = ref('')
const accounts = ref<Account[]>([])
const accountSearch = ref('')
const selectedAccount = ref(typeof route.query.account === 'string' ? route.query.account : '')
const selectedModels = ref<string[]>([])
const modelCatalog = ref<Array<{ id: string, label: string }>>([])
const modelSearch = ref('')
const modelRequest = useRequestState()
const { loading: loadingModels, error: modelsError } = modelRequest
const modelOptions = computed(() => {
  const account = accounts.value.find(a => a.id === selectedAccount.value)
  return account ? maintenanceModels(modelCatalog.value, account.modelAccess) : []
})
const filteredModels = computed(() => modelOptions.value.filter(option =>
  option.label.toLowerCase().includes(modelSearch.value.trim().toLowerCase()),
))
const selectedPool = ref('')
const expanded = ref('')
const confirmDiscard = ref(false)
const now = ref(Date.now() / 1000)
const poolResults = ref<Record<string, string>>({})
const dirty = computed(() => !!draft.value && JSON.stringify(draft.value) !== savedDraft.value)
const accountOptions = computed(() => accounts.value.map(a => ({ value: a.id, label: `${a.name} · ${a.email ?? a.id}` })))
const poolOptions = computed(() => draft.value?.pools.map(p => ({ value: p.id, label: p.name || '未命名代理池' })) ?? [])
const modeOptions = [{ value: 'fixed', label: '固定出口（单次采集）' }, { value: 'rotating', label: '每连接轮换入口' }, { value: 'gateway', label: '轮换入口（旧配置）' }, { value: 'api', label: 'API 提取式' }]
const numericFields = [
  { key: 'headerLength', label: 'Header 字符数（0 = 基线集合）', min: 0, max: 4096 },
  { key: 'cipherBlocks', label: '密文块数（与字符数同时为 0）', min: 0, max: 250 },
  { key: 'ttlSeconds', label: '估算 TTL（秒）', min: 60, max: 86400 },
  { key: 'refreshBeforeSeconds', label: '提前刷新（秒）', min: 10, max: 86399 },
  { key: 'minimumRemainingSeconds', label: '候选最小剩余（秒）', min: 11, max: 86399 },
  { key: 'concurrency', label: '探测并发', min: 1, max: 4 },
  { key: 'maxAttempts', label: '每周期最多采集次数', min: 1, max: 10 },
  { key: 'timeoutSeconds', label: '单请求超时（秒）', min: 5, max: 60 },
  { key: 'retrySeconds', label: '失败冷却（秒）', min: 30, max: 3600 },
] as const
const directions: Record<string, string> = { rejected: '候选未入选', candidate: '探测候选', injected: '接管注入', request: '请求携带', returned: '上游返回', session: '会话状态' }
const statuses: Record<string, string> = { paused: '已暂停', probing: '探测中', ready: '候选可用', expired: '已过期', empty: '等待获取' }
const groups = computed(() => [...new Set(view.value?.targets.map(t => t.accountId) ?? [])].map(id => ({
  id,
  targets: view.value!.targets.filter(t => t.accountId === id),
})))
const metrics = computed(() => {
  const s = overview.value?.stats
  return [
    { label: '当前有效维护项', value: `${view.value?.targets.filter(t => t.status === 'ready').length ?? 0} / ${view.value?.targets.length ?? 0}`, note: '已暂停的维护项不计入有效项' },
    { label: '采集入选率', value: s ? ratio(s.accepted, s.collectionResults) : '—', note: `${s?.accepted ?? 0} 次入选 / ${s?.collectionResults ?? 0} 次已知结果` },
    { label: '验证通过率', value: s ? ratio(s.verified, s.verificationResults) : '—', note: `${s?.verified ?? 0} 次通过 / ${s?.verificationResults ?? 0} 次已知结果` },
    { label: '实际探测请求', value: s?.total ?? '—', note: '近 24 小时 · 采集与验证分别计数' },
  ]
})
function recentEgress(accountId: string, phase: string) {
  return overview.value?.items.find(r => r.accountId === accountId && r.phase === phase)?.facts.egress
}
function recentPoolEgress(poolId: string) {
  return poolEgress.value[poolId] ?? overview.value?.items.find(r => r.poolId === poolId)?.facts.egress
}
function configureAccount(accountId: string) {
  if (accountDirty.value) {
    toast.warning('请先保存或重新读取当前账号策略')
    return
  }
  selectedAccount.value = accountId
  activeTab.value = 'settings'
  settingsTab.value = 'accounts'
}
async function reloadOverview() {
  if (overviewRequest.loading.value)
    return
  const id = overviewRequest.start()
  try {
    const result = await getTurnStateRecords({ ...recordRange(), page: 1, pageSize: 100 })
    if (overviewRequest.isCurrent(id))
      overview.value = result
  }
  catch (e) {
    overviewRequest.fail(id, e)
  }
  finally {
    overviewRequest.finish(id)
  }
}
onBeforeRouteLeave(() => {
  if (!dirty.value && !accountDirty.value && !accountBusy.value)
    return true
  toast.warning('请先保存或放弃未保存的维护设置')
  return false
})
let poll: ReturnType<typeof setInterval> | undefined
let clock: ReturnType<typeof setInterval> | undefined
let disposed = false

function apply(result: TurnStateView) {
  view.value = result
  draft.value = {
    revision: result.revision,
    policy: { ...result.policy },
    pools: result.pools.map(({ endpoint: _endpoint, hasSecret: _secret, ...pool }) => ({ ...pool })),
    targets: result.targets.map(({ accountId, model, poolId, enabled }) => ({ accountId, model, poolId, enabled })),
  }
  savedDraft.value = JSON.stringify(draft.value)
}

async function reload(reset = false) {
  if (loading.value || busy.value || accountBusy.value)
    return
  loading.value = true
  try {
    const result = await getTurnState()
    if (disposed)
      return
    loadError.value = ''
    if (!draft.value || reset || !dirty.value)
      apply(result)
    else
      view.value = result // 轮询不覆盖未保存的输入或秘密。
  }
  catch (error) {
    if (!disposed)
      loadError.value = errorMessage(error, '状态维护加载失败')
  }
  finally {
    loading.value = false
  }
}

async function searchAccounts() {
  try {
    const result = await getAccounts({ page: 1, pageSize: 100, provider: 'openai', search: accountSearch.value })
    if (!disposed)
      accounts.value = result.items
  }
  catch (error) {
    toast.error(errorMessage(error, '账号加载失败'))
  }
}

async function loadModels(refresh = false) {
  const accountId = selectedAccount.value
  if (!accountId)
    return
  const requestId = modelRequest.start()
  try {
    const result = await (refresh ? refreshAccountModels : getAccountModels)({ accountId })
    if (modelRequest.isCurrent(requestId) && selectedAccount.value === accountId) {
      modelCatalog.value = result.models
      selectedModels.value = selectedModels.value.filter(id => modelOptions.value.some(option => option.value === id))
    }
  }
  catch (error) {
    modelRequest.fail(requestId, error)
  }
  finally {
    modelRequest.finish(requestId)
  }
}

watch(selectedAccount, () => {
  modelRequest.invalidate()
  modelCatalog.value = []
  selectedModels.value = []
  modelSearch.value = ''
  modelsError.value = ''
  void loadModels()
})

function selectModel(model: string, selected: boolean) {
  selectedModels.value = selected
    ? [...new Set([...selectedModels.value, model])]
    : selectedModels.value.filter(id => id !== model)
}

async function save() {
  if (!draft.value || busy.value || accountDirty.value || accountBusy.value)
    return
  busy.value = true
  try {
    apply(await saveTurnState(draft.value))
    toast.success('已保存并热生效；仅改变候选规则或出口时撤下旧值')
  }
  catch (error) {
    toast.error(errorMessage(error, '保存失败'))
  }
  finally {
    busy.value = false
  }
}

function addPool() {
  draft.value?.pools.push({ id: crypto.randomUUID(), name: '', enabled: true, mode: 'rotating', endpoint: '', bearer: '', jsonPointer: '' })
}

function removePool(id: string) {
  if (!draft.value)
    return
  if (draft.value.targets.some(t => t.poolId === id)) {
    toast.warning('请先移除或更换引用此池的维护项')
    return
  }
  draft.value.pools = draft.value.pools.filter(p => p.id !== id)
}

function addTargets() {
  if (!draft.value || !selectedAccount.value || !selectedPool.value) {
    toast.warning('请选择账号和代理池')
    return
  }
  if (busy.value || loadingModels.value || modelsError.value || !selectedModels.value.length) {
    toast.warning('请先加载并选择该账号的可用模型')
    return
  }
  const models = selectedModels.value.filter(model =>
    modelOptions.value.some(option => option.value === model)
    && !draft.value!.targets.some(t => t.accountId === selectedAccount.value && t.model === model),
  )
  if (draft.value.targets.length + models.length > 128) {
    toast.warning('维护项最多 128 条，请减少本次选择')
    return
  }
  for (const model of models)
    draft.value.targets.push({ accountId: selectedAccount.value, model, poolId: selectedPool.value, enabled: true })
  selectedModels.value = []
}

async function run(target: TurnStateTarget, action: 'refresh' | 'pause' | 'resume' | 'revoke') {
  if (dirty.value || busy.value || accountDirty.value || accountBusy.value) {
    toast.warning('请先保存或重新加载未保存的设置')
    return
  }
  busy.value = true
  try {
    apply(await turnStateAction({ accountId: target.accountId, model: target.model, action }))
    toast.success(action === 'refresh' ? '已登记刷新，执行前仍检查预算、账号并发与冷却；请查看调度状态' : '操作已生效')
  }
  catch (error) {
    toast.error(errorMessage(error, '操作失败'))
  }
  finally {
    busy.value = false
  }
}

async function testPool(id: string) {
  if (dirty.value || busy.value) {
    toast.warning('请先保存代理池设置')
    return
  }
  busy.value = true
  try {
    const result = await testTurnStatePool(id)
    poolEgress.value[id] = result.egress
    poolResults.value[id] = `IPv4: ${result.ipv4Address ?? '未确认'} · IPv6: ${result.ipv6Address ?? '未确认'} · ${result.message}`
  }
  catch (error) {
    poolResults.value[id] = errorMessage(error, '测试失败')
  }
  finally {
    busy.value = false
  }
}

function accountName(id: string) {
  return accounts.value.find(a => a.id === id)?.name ?? id
}
function date(seconds: number | null) {
  return seconds === null ? '—' : new Date(seconds * 1000).toLocaleString()
}
function key(target: { accountId: string, model: string }) {
  return `${target.accountId}:${target.model}`
}

onMounted(() => {
  void reload()
  void reloadOverview()
  void searchAccounts()
  if (selectedAccount.value)
    void loadModels()
  poll = setInterval(() => {
    void reload()
    if (activeTab.value !== 'records')
      void reloadOverview()
  }, 5000)
  clock = setInterval(() => {
    now.value = Date.now() / 1000
  }, 1000)
})
onBeforeUnmount(() => {
  disposed = true
  clearInterval(poll)
  clearInterval(clock)
})
</script>

<template>
  <div class="flex flex-col gap-6">
    <BasePageHeader title="状态维护" description="Turn-state 候选采集、验证与自动刷新">
      <template #actions>
        <span v-if="view" class="self-center text-cp-sm" :class="view.policy.enabled ? 'text-cp-success' : 'text-cp-warning'">{{ view.policy.enabled ? '自动维护已开启' : '全局维护已关闭' }}</span>
        <BaseButton :disabled="busy || accountBusy" :loading="loading" @click="dirty ? confirmDiscard = true : reload(true)">
          {{ dirty ? '放弃修改' : '刷新状态' }}
        </BaseButton>
        <BaseButton variant="primary" :disabled="!draft || !dirty || accountDirty || accountBusy" :loading="busy" @click="save">
          保存设置
        </BaseButton>
      </template>
    </BasePageHeader>
    <BaseSegmented v-model="activeTab" label="状态维护分区" :options="tabs" class="self-start" />
    <p v-if="loadError" role="alert" class="text-cp-error">
      {{ loadError }}
    </p>
    <p v-if="!draft && loading" role="status" class="text-cp-text-secondary">
      正在加载配置…
    </p>
    <template v-if="draft">
      <template v-if="activeTab === 'overview'">
        <p v-if="overviewError" role="alert" class="text-cp-error">
          探测指标更新失败：{{ overviewError }}，下方保留上次数据。
        </p>
        <div class="grid grid-cols-2 gap-4 xl:grid-cols-4">
          <BaseCard v-for="metric in metrics" :key="metric.label">
            <p class="text-cp-sm text-cp-text-secondary">
              {{ metric.label }}
            </p><p class="my-3 text-2xl font-semibold tabular-nums">
              {{ metric.value }}
            </p><p class="text-cp-xs text-cp-text-tertiary">
              {{ metric.note }}
            </p>
          </BaseCard>
        </div>
        <BaseCard title="候选产生过程" description="HTTP 成功不代表候选入选；验证通过不代表配置变更后仍会发布。比率仅统计已知结果，历史未知与进行中不按失败计。">
          <div class="grid gap-4 sm:grid-cols-3">
            <div>独立代理采集 <strong>{{ overview?.stats.collections ?? '—' }} 次</strong></div><div>规则检查入选 <strong>{{ overview?.stats.accepted ?? '—' }} 次</strong></div><div>业务出口验证通过 <strong>{{ overview?.stats.verified ?? '—' }} 次</strong></div>
          </div>
        </BaseCard>
      </template>
      <ProbeRecords v-if="activeTab === 'records'" :accounts="accountOptions" />
      <BaseSegmented v-if="activeTab === 'settings'" v-model="settingsTab" label="维护配置分类" :options="settingsTabs" class="self-start" :disabled="accountBusy" />
      <BaseCard v-show="activeTab === 'settings' && settingsTab === 'global'" title="全局策略" description="签发时间来自 token；TTL 是经验配置，不是上游保证。长度匹配不等于能力提升证明。">
        <BaseSwitch v-model="draft.policy.enabled" label="启用自动维护" :show-label="true" :disabled="busy || accountDirty || accountBusy" />
        <div class="mt-5 grid grid-cols-2 gap-4 lg:grid-cols-3">
          <div v-for="field in numericFields.filter(f => !['headerLength', 'cipherBlocks'].includes(f.key))" :key="field.key" class="flex flex-col gap-2 text-cp-sm text-cp-text-secondary">
            {{ field.label }}
            <BaseNumberInput v-model="draft.policy[field.key]" :label="field.label" :min="field.min" :max="field.max" :disabled="busy || accountDirty || accountBusy" />
          </div>
        </div>
        <details class="mt-5 rounded-cp border border-cp-border p-4">
          <summary class="text-cp-sm">
            高级候选规则
          </summary>
          <div class="mt-4 grid gap-4 sm:grid-cols-2">
            <div v-for="field in numericFields.filter(f => ['headerLength', 'cipherBlocks'].includes(f.key))" :key="field.key" class="grid gap-2 text-cp-sm">
              {{ field.label }}<BaseNumberInput v-model="draft.policy[field.key]" :label="field.label" :min="field.min" :max="field.max" :disabled="busy || accountDirty || accountBusy" />
            </div>
          </div>
        </details>
        <p class="mt-4 text-cp-sm text-cp-text-tertiary">
          候选最小剩余时间必须大于提前刷新时间。每次合格采集最多追加一次业务出口验证，会消耗上游额度；
          到期无新值时回到原有请求逻辑；认证失败暂停维护项，限流进入账号级冷却。
          字符数和块数同时为 0 时接受 292 / 10 块与 332 / 12 块两种候选形态，并非能力判定。
          账号开关、小时预算与自动发现统一在本页「账号与模型」中设置；指纹收敛仍在账号管理中设置。
        </p>
      </BaseCard>
      <BaseCard v-show="activeTab === 'settings' && settingsTab === 'proxies'" title="候选采集代理池" description="只用于采集，不修改账号业务出口。固定出口单次采集；轮换入口新连接不保证新 IP。">
        <template #actions>
          <BaseButton :disabled="busy || accountDirty || accountBusy || draft.pools.length >= 16" @click="addPool">
            添加代理池
          </BaseButton>
        </template>
        <p v-if="!draft.pools.length" class="text-cp-text-tertiary">
          尚未配置代理池。先添加、保存，再测试出口。
        </p>
        <div v-for="pool in draft.pools" :key="pool.id" class="mb-4 rounded-cp border border-cp-border p-4">
          <div class="mb-4">
            <p class="mb-2 text-cp-xs text-cp-text-tertiary">
              最近检测出口（独立检测，不代表本次请求已确认出口）
            </p><EgressSnapshot :value="recentPoolEgress(pool.id)" />
          </div>
          <div class="grid gap-4 md:grid-cols-2">
            <FormItem label="名称">
              <BaseInput v-model="pool.name" :disabled="busy || accountDirty || accountBusy" placeholder="例如：IPv6 探测池" />
            </FormItem>
            <FormItem label="接入方式">
              <BaseSelect v-model="pool.mode" :options="modeOptions" :disabled="busy || accountDirty || accountBusy" />
            </FormItem>
            <FormItem :label="pool.mode !== 'api' ? '代理 URL（可包含用户名和密码）' : 'HTTPS 提取接口 URL'" class="md:col-span-2">
              <BaseInput
                :model-value="pool.endpoint ?? ''" type="password" autocomplete="new-password" :disabled="busy || accountDirty || accountBusy"
                :placeholder="pool.mode !== 'api' ? 'socks5h://用户名:密码@入口:端口；编辑时留空保留' : 'https://供应商/提取接口；编辑时留空保留'"
                @update:model-value="pool.endpoint = $event || undefined"
              />
              <span class="text-cp-xs text-cp-text-tertiary">已保存：{{ view?.pools.find(p => p.id === pool.id)?.endpoint ?? '未保存' }}。原连接信息不回显，修改时填写完整 URL。</span>
            </FormItem>
            <template v-if="pool.mode === 'api'">
              <FormItem label="Bearer 密钥（可选，留空保留）">
                <BaseInput :model-value="pool.bearer ?? ''" type="password" autocomplete="new-password" :disabled="busy || accountDirty || accountBusy" @update:model-value="pool.bearer = $event || undefined" />
              </FormItem>
              <FormItem label="JSON Pointer">
                <BaseInput v-model="pool.jsonPointer" :disabled="busy || accountDirty || accountBusy" placeholder="例如 /data/0/url；纯文本 URL 则留空" />
              </FormItem>
              <BaseButton size="sm" :disabled="busy || accountDirty || accountBusy" @click="pool.bearer = ''">
                清除已保存的 Bearer 密钥
              </BaseButton>
              <p class="text-cp-xs text-cp-text-tertiary">
                GET 接口；返回完整代理 URL 或由 Pointer 指向一个 URL 字符串。每次采集重新提取，不缓存未知有效期的地址。
              </p>
            </template>
          </div>
          <div class="mt-4 flex flex-wrap items-center gap-3">
            <BaseSwitch v-model="pool.enabled" label="启用代理池" :show-label="true" :disabled="busy || accountDirty || accountBusy" />
            <BaseButton size="sm" :disabled="busy || dirty || accountDirty || accountBusy" @click="testPool(pool.id)">
              测试 IPv4 / IPv6 出口
            </BaseButton>
            <BaseButton size="sm" variant="ghost" :disabled="busy || accountDirty || accountBusy" @click="removePool(pool.id)">
              移除
            </BaseButton>
          </div>
          <p v-if="poolResults[pool.id]" role="status" class="mt-3 text-cp-sm text-cp-text-secondary">
            {{ poolResults[pool.id] }}
          </p>
        </div>
      </BaseCard>
      <BaseCard v-show="activeTab === 'settings' && settingsTab === 'accounts'" title="账号与模型" description="账号接管、小时预算、空闲策略与自动发现集中设置。修改候选规则或出口才会撤下旧值，不中断已发出的请求。">
        <div class="grid gap-3 md:grid-cols-2">
          <div class="flex gap-2">
            <BaseInput v-model="accountSearch" aria-label="搜索 OpenAI 账号" placeholder="搜索 OpenAI 账号（最多显示 100 条）" class="flex-1" />
            <BaseButton :disabled="busy || accountDirty || accountBusy" @click="searchAccounts">
              搜索
            </BaseButton>
          </div>
          <BaseSelect v-model="selectedAccount" :options="accountOptions" :disabled="busy || accountDirty || accountBusy" aria-label="选择账号" placeholder="选择账号" />
          <BaseSelect v-model="selectedPool" :options="poolOptions" aria-label="选择探测代理池" placeholder="选择探测代理池" />
        </div>
        <AccountMaintenance v-if="selectedAccount && view" :account-id="selectedAccount" :view="view" :blocked="busy || dirty" @saved="apply" @dirty="accountDirty = $event" @busy="accountBusy = $event" />
        <div v-if="selectedAccount" class="mt-3 grid gap-3 rounded-cp border border-cp-border p-3">
          <div class="flex gap-2">
            <BaseInput v-model="modelSearch" aria-label="搜索账号可用模型" placeholder="搜索可用模型（可多选）" class="flex-1" />
            <BaseButton :disabled="busy || accountDirty || accountBusy || loadingModels" :loading="loadingModels" @click="loadModels(true)">
              从上游刷新模型
            </BaseButton>
          </div>
          <p v-if="modelsError" role="alert" class="text-cp-sm text-cp-error">
            模型加载失败：{{ modelsError }}。请重试，不会使用其他账号的模型。
          </p>
          <p v-else-if="loadingModels" role="status" class="text-cp-sm text-cp-text-secondary">
            正在加载所选账号的模型…
          </p>
          <p v-else-if="!modelOptions.length" role="status" class="text-cp-sm text-cp-text-secondary">
            暂无可用于维护的模型。可尝试从上游刷新，或检查账号的模型白名单 / 黑名单。
          </p>
          <p v-else-if="!filteredModels.length" class="text-cp-sm text-cp-text-secondary">
            没有匹配搜索词的模型。
          </p>
          <div v-else class="grid max-h-64 gap-3 overflow-y-auto md:grid-cols-2">
            <BaseCheckbox
              v-for="option in filteredModels" :key="option.value"
              :model-value="selectedModels.includes(option.value)" :label="option.label" :show-label="true"
              :disabled="busy || accountDirty || accountBusy || loadingModels || !!modelsError || draft.targets.some(t => t.accountId === selectedAccount && t.model === option.value)"
              @update:model-value="selectModel(option.value, $event)"
            />
          </div>
          <p class="text-cp-xs text-cp-text-tertiary">
            已选 {{ selectedModels.length }} 个。列表按账号模型限制过滤，已添加项不可重复选择；图片模型不参与 State 探测。模型目录不保证即时额度或上游可用性。
          </p>
        </div>
        <BaseButton class="mt-3 self-start" :disabled="busy || accountDirty || accountBusy || loadingModels || !!modelsError || !selectedModels.length || draft.targets.length >= 128" @click="addTargets">
          添加维护项
        </BaseButton>
        <div v-for="(target, index) in draft.targets" :key="key(target)" class="mt-4 flex flex-wrap items-center gap-3 rounded-cp bg-cp-fill-quaternary p-3">
          <span class="min-w-0 flex-1 break-all text-cp-sm">{{ accountName(target.accountId) }} / {{ target.model }}</span>
          <BaseSelect v-model="target.poolId" :options="poolOptions" :disabled="busy || accountDirty || accountBusy" aria-label="维护项代理池" class="w-48" />
          <BaseSwitch v-model="target.enabled" :label="`启用 ${target.model}`" :disabled="busy || accountDirty || accountBusy" />
          <BaseButton size="sm" variant="ghost" :disabled="busy || accountDirty || accountBusy" @click="draft.targets.splice(index, 1)">
            移除
          </BaseButton>
        </div>
      </BaseCard>
      <BaseCard v-if="activeTab === 'overview'" title="账号与维护项" description="每 5 秒刷新。预算按账号共享；后台任务不会随页面关闭而终止。">
        <p class="mb-3 text-cp-sm text-cp-text-secondary">
          候选可用、开关开启不代表每条请求都已接管：HTTP 与 WS 的新轮次非续接请求可注入候选，
          WS 通过请求帧传递，不降级为 HTTP；同轮及原生续接不重新选取候选。
          请在请求详情中查看实际发送的 turn-state 及其来源。
        </p>
        <p v-if="!view?.targets.length" class="text-cp-text-tertiary">
          保存维护项后将在这里展示状态。
        </p>
        <section v-for="group in groups" :key="group.id" class="mb-5 rounded-cp border border-cp-border p-4">
          <div class="mb-4 flex flex-wrap items-center justify-between gap-3">
            <div>
              <h3 class="font-semibold">
                {{ accountName(group.id) }}
              </h3><p class="mt-1 text-cp-sm" :class="group.targets[0]!.hourlyUsed >= group.targets[0]!.hourlyLimit ? 'text-cp-warning' : 'text-cp-text-secondary'">
                小时预算 {{ group.targets[0]!.hourlyUsed }} / {{ group.targets[0]!.hourlyLimit }} · {{ date(group.targets[0]!.budgetResetsAt) }} 重置 · 同账号模型共享
              </p>
            </div>
            <BaseButton size="sm" :disabled="accountBusy" @click="configureAccount(group.id)">
              账号设置
            </BaseButton>
          </div>
          <div class="mb-4 grid gap-4 border-b border-cp-border pb-4 sm:grid-cols-2">
            <div>
              <p class="mb-2 text-cp-xs text-cp-text-tertiary">
                采集出口 · 最近探测记录快照
              </p><EgressSnapshot :value="recentEgress(group.id, 'collect')" />
            </div>
            <div>
              <p class="mb-2 text-cp-xs text-cp-text-tertiary">
                业务出口 · 最近探测记录快照
              </p><EgressSnapshot :value="recentEgress(group.id, 'verify')" />
            </div>
          </div>
          <div v-for="target in group.targets" :key="key(target)" class="mb-4 rounded-cp bg-cp-fill-quaternary p-4">
            <div class="flex flex-wrap items-start justify-between gap-3">
              <div>
                <h3 class="m-0 break-all text-cp font-semibold">
                  {{ target.model }}
                </h3>
                <p class="mt-2 text-cp-sm" :class="target.status === 'ready' ? 'text-cp-success' : 'text-cp-text-secondary'">
                  {{ statuses[target.status] }} · {{ remainingLabel(target.expiresAt, now) }} · 候选 {{ target.candidateCount }}/3 · {{ takeoverPolicyLabel(target.takeover, view!.policy.enabled) }}
                </p>
              </div>
              <div class="flex flex-wrap gap-2">
                <BaseButton size="sm" :disabled="busy || dirty || accountDirty || accountBusy || target.status === 'probing' || !target.enabled || !view?.policy.enabled || target.hourlyUsed >= target.hourlyLimit" @click="run(target, 'refresh')">
                  {{ target.hourlyUsed >= target.hourlyLimit ? '预算已耗尽' : '立即刷新' }}
                </BaseButton>
                <BaseButton size="sm" :disabled="busy || dirty || accountDirty || accountBusy" @click="run(target, target.enabled ? 'pause' : 'resume')">
                  {{ target.enabled ? '暂停' : '恢复' }}
                </BaseButton>
                <BaseButton size="sm" variant="ghost" :disabled="busy || dirty || accountDirty || accountBusy" @click="run(target, 'revoke')">
                  撤销并暂停
                </BaseButton>
                <BaseButton size="sm" variant="ghost" @click="expanded = expanded === key(target) ? '' : key(target)">
                  诊断
                </BaseButton>
              </div>
            </div>
            <p class="text-cp-sm text-cp-text-tertiary">
              指纹 {{ target.fingerprint ?? '—' }} · 签发 {{ date(target.issuedAt) }} · 下次探测 {{ date(target.nextProbeAt) }}
            </p>
            <p class="text-cp-sm text-cp-text-secondary">
              {{ waitReasonLabel(target.waitReason) }} · {{ target.automatic ? '自动发现' : '手动配置' }}
            </p>
            <p class="text-cp-xs text-cp-text-tertiary">
              最近业务 {{ date(target.lastTrafficAt) }}
            </p>
            <p class="text-cp-sm text-cp-text-secondary">
              {{ target.lastMessage }}
            </p>
            <div v-if="expanded === key(target)" class="mt-4 grid gap-2">
              <p v-if="!target.history.length" class="text-cp-sm text-cp-text-tertiary">
                暂无诊断记录。
              </p>
              <div v-for="(entry, i) in [...target.history].reverse()" :key="i" class="rounded-cp bg-cp-fill-quaternary p-3 text-cp-sm">
                <p class="font-semibold">
                  {{ directions[entry.direction] ?? entry.direction }} · {{ shapeLabel(entry.shape, view!.policy) }} · {{ date(entry.at) }}
                </p>
                <p class="font-mono text-cp-xs">
                  {{ entry.fingerprint || '无 token' }}
                </p>
                <p v-if="entry.shape">
                  {{ entry.shape.headerLength }} 字符 / {{ entry.shape.totalBytes }} 字节 / 密文 {{ entry.shape.blocks }} 块 · 签发 {{ date(entry.shape.issuedAt) }}
                </p>
                <p class="text-cp-text-secondary">
                  {{ entry.message }}
                </p>
              </div>
            </div>
          </div>
        </section>
        <p class="text-cp-xs text-cp-text-tertiary">
          出口展示来自近 24 小时最近 100 条探测记录；没有记录时显示未知。独立检测不是请求实际出口证明，轮换出口仅供参考。
        </p>
      </BaseCard>
      <p v-if="dirty" role="status" class="text-cp-warning">
        有未保存设置。点击顶部“保存设置”后热生效；运行状态轮询不会覆盖编辑内容。
      </p>
    </template>
    <BaseConfirmModal v-model="confirmDiscard" title="放弃未保存设置？" description="未保存的代理认证和维护设置将被清除，运行中的配置不受影响。" confirm-text="放弃并重新加载" @confirm="confirmDiscard = false; reload(true)" />
  </div>
</template>
