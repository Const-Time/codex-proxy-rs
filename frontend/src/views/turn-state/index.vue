<script setup lang="ts">
import type { Account } from '@/api/modules/accounts'
import type { TurnStateSettings, TurnStateTarget, TurnStateView } from '@/api/modules/turn-state'
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { getAccounts } from '@/api/modules/accounts'
import { getTurnState, saveTurnState, testTurnStatePool, turnStateAction } from '@/api/modules/turn-state'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseCard from '@/components/base/BaseCard.vue'
import BaseConfirmModal from '@/components/base/BaseConfirmModal.vue'
import FormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BasePageHeader from '@/components/base/BasePageHeader.vue'
import BaseSelect from '@/components/base/BaseSelect.vue'
import BaseSwitch from '@/components/base/BaseSwitch.vue'
import { toast } from '@/components/base/BaseToast'
import { errorMessage } from '@/utils/async'
import { exactModels, remainingLabel, shapeLabel, waitReasonLabel } from './presenter'

const view = ref<TurnStateView | null>(null)
const draft = ref<TurnStateSettings | null>(null)
const savedDraft = ref('')
const busy = ref(false)
const loading = ref(false)
const loadError = ref('')
const accounts = ref<Account[]>([])
const accountSearch = ref('')
const selectedAccount = ref('')
const models = ref('')
const selectedPool = ref('')
const expanded = ref('')
const confirmDiscard = ref(false)
const now = ref(Date.now() / 1000)
const poolResults = ref<Record<string, string>>({})
const dirty = computed(() => JSON.stringify(draft.value) !== savedDraft.value)
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
const directions: Record<string, string> = { candidate: '探测候选', injected: '接管注入', request: '请求携带', returned: '上游返回', session: '会话状态' }
const statuses: Record<string, string> = { paused: '已暂停', probing: '探测中', ready: '可用', expired: '已过期', empty: '等待获取' }
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
  if (loading.value || busy.value)
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
      loadError.value = errorMessage(error, 'State 管理加载失败')
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

async function save() {
  if (!draft.value || busy.value)
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
  try {
    for (const model of exactModels(models.value)) {
      if (!draft.value.targets.some(t => t.accountId === selectedAccount.value && t.model === model)) {
        draft.value.targets.push({ accountId: selectedAccount.value, model, poolId: selectedPool.value, enabled: true })
      }
    }
    models.value = ''
  }
  catch (error) {
    toast.error(errorMessage(error, '模型名不合法'))
  }
}

async function run(target: TurnStateTarget, action: 'refresh' | 'pause' | 'resume' | 'revoke') {
  if (dirty.value || busy.value) {
    toast.warning('请先保存或重新加载未保存的设置')
    return
  }
  busy.value = true
  try {
    apply(await turnStateAction({ accountId: target.accountId, model: target.model, action }))
    toast.success(action === 'refresh' ? '已排队，将由后台任务执行' : '操作已生效')
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
  void searchAccounts()
  poll = setInterval(() => void reload(), 5000)
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
    <BasePageHeader title="State 自动维护" description="独立探测出口 · 精确账号与模型 · 到期停止注入">
      <template #actions>
        <BaseButton :disabled="busy" :loading="loading" @click="dirty ? confirmDiscard = true : reload(true)">
          {{ dirty ? '放弃修改' : '刷新状态' }}
        </BaseButton>
        <BaseButton variant="primary" :disabled="!draft || !dirty" :loading="busy" @click="save">
          保存设置
        </BaseButton>
      </template>
    </BasePageHeader>
    <p v-if="loadError" role="alert" class="text-cp-error">
      {{ loadError }}
    </p>
    <p v-if="!draft && loading" role="status" class="text-cp-text-secondary">
      正在加载配置…
    </p>
    <template v-if="draft">
      <BaseCard title="维护策略" description="签发时间来自 token；TTL 是经验配置，不是上游保证。长度匹配不等于能力提升证明。">
        <BaseSwitch v-model="draft.policy.enabled" label="启用自动维护" :show-label="true" :disabled="busy" />
        <div class="mt-5 grid grid-cols-2 gap-4 lg:grid-cols-3">
          <label v-for="field in numericFields" :key="field.key" :for="`state-${field.key}`" class="flex flex-col gap-2 text-cp-sm text-cp-text-secondary">
            {{ field.label }}
            <input :id="`state-${field.key}`" v-model.number="draft.policy[field.key]" type="number" :min="field.min" :max="field.max" :disabled="busy" class="state-number">
          </label>
        </div>
        <p class="mt-4 text-cp-sm text-cp-text-tertiary">
          候选最小剩余时间必须大于提前刷新时间。每次合格采集最多追加一次业务出口验证，会消耗上游额度；
          到期无新值时回到原有请求逻辑；认证失败暂停维护项，限流进入账号级冷却。
          字符数和块数同时为 0 时接受 292 / 10 块与 332 / 12 块两种候选形态，并非能力判定。
          还须在账号管理的「更多操作 → 指纹 / Turn-state」开启账号接管；默认不接管。
        </p>
      </BaseCard>
      <BaseCard title="候选采集代理池" description="只用于采集，不修改账号业务出口。固定出口单次采集；轮换入口新连接不保证新 IP。">
        <template #actions>
          <BaseButton :disabled="busy || draft.pools.length >= 16" @click="addPool">
            添加代理池
          </BaseButton>
        </template>
        <p v-if="!draft.pools.length" class="text-cp-text-tertiary">
          尚未配置代理池。先添加、保存，再测试出口。
        </p>
        <div v-for="pool in draft.pools" :key="pool.id" class="mb-4 rounded-cp border border-cp-border p-4">
          <div class="grid gap-4 md:grid-cols-2">
            <FormItem label="名称">
              <BaseInput v-model="pool.name" :disabled="busy" placeholder="例如：IPv6 探测池" />
            </FormItem>
            <FormItem label="接入方式">
              <BaseSelect v-model="pool.mode" :options="modeOptions" :disabled="busy" />
            </FormItem>
            <FormItem :label="pool.mode !== 'api' ? '代理 URL（可包含用户名和密码）' : 'HTTPS 提取接口 URL'" class="md:col-span-2">
              <BaseInput
                :model-value="pool.endpoint ?? ''" type="password" autocomplete="new-password" :disabled="busy"
                :placeholder="pool.mode !== 'api' ? 'socks5h://用户名:密码@入口:端口；编辑时留空保留' : 'https://供应商/提取接口；编辑时留空保留'"
                @update:model-value="pool.endpoint = $event || undefined"
              />
              <span class="text-cp-xs text-cp-text-tertiary">已保存：{{ view?.pools.find(p => p.id === pool.id)?.endpoint ?? '未保存' }}。原连接信息不回显，修改时填写完整 URL。</span>
            </FormItem>
            <template v-if="pool.mode === 'api'">
              <FormItem label="Bearer 密钥（可选，留空保留）">
                <BaseInput :model-value="pool.bearer ?? ''" type="password" autocomplete="new-password" :disabled="busy" @update:model-value="pool.bearer = $event || undefined" />
              </FormItem>
              <FormItem label="JSON Pointer">
                <BaseInput v-model="pool.jsonPointer" :disabled="busy" placeholder="例如 /data/0/url；纯文本 URL 则留空" />
              </FormItem>
              <BaseButton size="sm" :disabled="busy" @click="pool.bearer = ''">
                清除已保存的 Bearer 密钥
              </BaseButton>
              <p class="text-cp-xs text-cp-text-tertiary">
                GET 接口；返回完整代理 URL 或由 Pointer 指向一个 URL 字符串。每次采集重新提取，不缓存未知有效期的地址。
              </p>
            </template>
          </div>
          <div class="mt-4 flex flex-wrap items-center gap-3">
            <BaseSwitch v-model="pool.enabled" label="启用代理池" :show-label="true" :disabled="busy" />
            <BaseButton size="sm" :disabled="busy || dirty" @click="testPool(pool.id)">
              测试 IPv4 / IPv6 出口
            </BaseButton>
            <BaseButton size="sm" variant="ghost" :disabled="busy" @click="removePool(pool.id)">
              移除
            </BaseButton>
          </div>
          <p v-if="poolResults[pool.id]" role="status" class="mt-3 text-cp-sm text-cp-text-secondary">
            {{ poolResults[pool.id] }}
          </p>
        </div>
      </BaseCard>
      <BaseCard title="账号与模型" description="每个账号 × 精确上游模型独立维护。修改候选规则或出口才会撤下旧值；不会中断已发出的请求。">
        <div class="grid gap-3 md:grid-cols-2">
          <div class="flex gap-2">
            <BaseInput v-model="accountSearch" aria-label="搜索 OpenAI 账号" placeholder="搜索 OpenAI 账号（最多显示 100 条）" class="flex-1" />
            <BaseButton :disabled="busy" @click="searchAccounts">
              搜索
            </BaseButton>
          </div>
          <BaseSelect v-model="selectedAccount" :options="accountOptions" aria-label="选择账号" placeholder="选择账号" />
          <BaseInput v-model="models" aria-label="精确上游模型" placeholder="精确上游模型，多模型用逗号分隔" />
          <BaseSelect v-model="selectedPool" :options="poolOptions" aria-label="选择探测代理池" placeholder="选择探测代理池" />
        </div>
        <BaseButton class="mt-3 self-start" :disabled="busy || draft.targets.length >= 128" @click="addTargets">
          添加维护项
        </BaseButton>
        <div v-for="(target, index) in draft.targets" :key="key(target)" class="mt-4 flex flex-wrap items-center gap-3 rounded-cp bg-cp-fill-quaternary p-3">
          <span class="min-w-0 flex-1 break-all text-cp-sm">{{ accountName(target.accountId) }} / {{ target.model }}</span>
          <BaseSelect v-model="target.poolId" :options="poolOptions" :disabled="busy" aria-label="维护项代理池" class="w-48" />
          <BaseSwitch v-model="target.enabled" :label="`启用 ${target.model}`" :disabled="busy" />
          <BaseButton size="sm" variant="ghost" :disabled="busy" @click="draft.targets.splice(index, 1)">
            移除
          </BaseButton>
        </div>
      </BaseCard>
      <BaseCard title="运行状态与诊断" description="每 5 秒刷新。仅显示指纹和结构，不回显 state 原文；后台任务不会随页面关闭而终止。">
        <p v-if="!view?.targets.length" class="text-cp-text-tertiary">
          保存维护项后将在这里展示状态。
        </p>
        <div v-for="target in view?.targets" :key="key(target)" class="mb-4 rounded-cp border border-cp-border p-4">
          <div class="flex flex-wrap items-start justify-between gap-3">
            <div>
              <h3 class="m-0 break-all text-cp font-semibold">
                {{ accountName(target.accountId) }} · {{ target.model }}
              </h3>
              <p class="mt-2 text-cp-sm" :class="target.status === 'ready' ? 'text-cp-success' : 'text-cp-text-secondary'">
                {{ statuses[target.status] }} · {{ remainingLabel(target.expiresAt, now) }} · 候选 {{ target.candidateCount }}/3 · 账号接管{{ target.takeover ? '开启' : '关闭' }}
              </p>
            </div>
            <div class="flex flex-wrap gap-2">
              <BaseButton size="sm" :disabled="busy || dirty || target.status === 'probing' || !target.enabled || !view?.policy.enabled" @click="run(target, 'refresh')">
                立即刷新
              </BaseButton>
              <BaseButton size="sm" :disabled="busy || dirty" @click="run(target, target.enabled ? 'pause' : 'resume')">
                {{ target.enabled ? '暂停' : '恢复' }}
              </BaseButton>
              <BaseButton size="sm" variant="ghost" :disabled="busy || dirty" @click="run(target, 'revoke')">
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
            {{ waitReasonLabel(target.waitReason) }} · 小时预算 {{ target.hourlyUsed }}/{{ target.hourlyLimit }} · {{ target.automatic ? '自动发现' : '手动配置' }}
          </p>
          <p class="text-cp-xs text-cp-text-tertiary">
            预算重置 {{ date(target.budgetResetsAt) }} · 最近业务 {{ date(target.lastTrafficAt) }}
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
      </BaseCard>
      <p v-if="dirty" role="status" class="text-cp-warning">
        有未保存设置。点击顶部“保存设置”后热生效；运行状态轮询不会覆盖编辑内容。
      </p>
    </template>
    <BaseConfirmModal v-model="confirmDiscard" title="放弃未保存设置？" description="未保存的代理认证和维护设置将被清除，运行中的配置不受影响。" confirm-text="放弃并重新加载" @confirm="confirmDiscard = false; reload(true)" />
  </div>
</template>

<style scoped>
.state-number {
  min-width: 0;
  height: 38px;
  padding: 0 12px;
  border: 1px solid var(--cp-border);
  border-radius: 8px;
  background: var(--cp-input-bg, transparent);
  color: var(--cp-text);
}
.state-number:focus-visible {
  outline: 2px solid var(--cp-primary);
  outline-offset: 2px;
}
</style>
