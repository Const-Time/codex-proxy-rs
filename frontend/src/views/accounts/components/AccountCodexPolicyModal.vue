<script setup lang="ts">
import type { AccountRow } from '../constants'
import type { TurnStateMaintenance, TurnStateView } from '@/api/modules/turn-state'
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { getTurnState, saveAccountTurnState } from '@/api/modules/turn-state'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import BaseSelect from '@/components/base/BaseSelect.vue'
import BaseSwitch from '@/components/base/BaseSwitch.vue'
import { toast } from '@/components/base/BaseToast'
import { errorMessage } from '@/utils/async'

const props = defineProps<{ account: AccountRow | null }>()
const open = defineModel<boolean>({ required: true })
const loading = ref(false)
const saving = ref(false)
const loadError = ref('')
const view = ref<TurnStateView | null>(null)
const fingerprintConvergence = ref(false)
const takeover = ref(false)
const maintenance = ref<TurnStateMaintenance>({ maxProbesPerHour: 30, idleSeconds: 3600, autoModels: false, poolId: null })
const poolOptions = computed(() => [
  { value: '', label: '请选择自动发现模型使用的代理池' },
  ...(view.value?.pools.map(p => ({ value: p.id, label: p.name })) ?? []),
])
const targets = computed(() => view.value?.targets.filter(t => t.accountId === props.account?.id) ?? [])
let generation = 0

async function load() {
  const requestGeneration = ++generation
  view.value = null
  loadError.value = ''
  fingerprintConvergence.value = false
  takeover.value = false
  loading.value = false
  if (!open.value || props.account?.provider !== 'openai')
    return
  const accountId = props.account.id
  loading.value = true
  try {
    const result = await getTurnState()
    if (requestGeneration !== generation)
      return
    view.value = result
    const policy = result.accounts.find(p => p.accountId === accountId)
    fingerprintConvergence.value = policy?.fingerprintConvergence ?? false
    takeover.value = policy?.takeover ?? false
    maintenance.value = { maxProbesPerHour: 30, idleSeconds: 3600, autoModels: false, poolId: null, ...policy?.maintenance }
  }
  catch (error) {
    if (requestGeneration === generation)
      loadError.value = errorMessage(error, '账号策略加载失败')
  }
  finally {
    if (requestGeneration === generation)
      loading.value = false
  }
}

async function save() {
  if (saving.value || !view.value || !props.account)
    return
  saving.value = true
  const requestGeneration = generation
  try {
    await saveAccountTurnState(props.account.id, {
      revision: view.value.revision,
      fingerprintConvergence: fingerprintConvergence.value,
      takeover: takeover.value,
      maintenance: maintenance.value,
    })
    if (requestGeneration !== generation)
      return
    toast.success('账号策略已保存，后续请求生效')
    open.value = false
  }
  catch (error) {
    if (requestGeneration === generation)
      loadError.value = errorMessage(error, '保存失败，请重新加载策略后重试')
  }
  finally {
    saving.value = false
  }
}

watch([open, () => props.account?.id], () => void load())
onBeforeUnmount(() => {
  generation++
})
</script>

<template>
  <BaseModal v-model="open" title="Codex 账号策略" :description="account?.name" size="lg" :dismissible="!saving">
    <div class="grid gap-5">
      <p v-if="loading" role="status" class="text-cp-text-secondary">
        正在读取账号策略…
      </p>
      <div v-if="loadError" role="alert" class="rounded-cp bg-cp-error-container p-3 text-cp-error">
        {{ loadError }}
        <BaseButton size="sm" :disabled="saving" @click="load">
          重新加载
        </BaseButton>
      </div>
      <section class="rounded-cp border border-cp-border p-4">
        <BaseSwitch v-model="fingerprintConvergence" label="启用指纹收敛" :show-label="true" :disabled="loading || saving || !view" />
        <p class="mt-3 text-cp-sm text-cp-text-secondary">
          按账号与客户端隔离出站身份，统一请求头与 metadata，保留 UUIDv7 时间戳和复合窗口形态。
          WebSocket 握手不携带 turn-state，状态仍通过每一帧传递。
        </p>
        <p class="mt-2 text-cp-xs text-cp-text-tertiary">
          切换会改变后续请求的出站身份并撤下本账号候选；建议在没有进行中会话时操作。
        </p>
      </section>
      <section class="rounded-cp border border-cp-border p-4">
        <BaseSwitch v-model="takeover" label="HTTP / WS turn-state 接管" :show-label="true" :disabled="loading || saving || !view" />
        <p class="mt-3 text-cp-sm text-cp-text-secondary">
          HTTP 与 WS 的新轮次非续接请求使用本账号、精确模型下的有效候选替换状态。
          WS 逐帧注入，不强制改用 HTTP；同轮或原生续接不重新选取候选。
          关闭或无有效候选时保留原有安全透传逻辑。
        </p>
        <p class="mt-2 text-cp-xs text-cp-text-tertiary">
          候选最多 3 个 / 模型；过期停止使用，不因返回 312 或候选耗尽禁用账号。
        </p>
      </section>
      <section class="grid gap-4 rounded-cp border border-cp-border p-4">
        <h3 class="font-semibold">
          账号维护预算与自动发现
        </h3>
        <label for="state-hourly-budget" class="grid gap-2 text-cp-sm">
          每小时最多探测请求（采集与验证分别计数）
          <input id="state-hourly-budget" v-model.number="maintenance.maxProbesPerHour" type="number" min="1" max="600" class="rounded-cp border border-cp-border bg-transparent p-2" :disabled="loading || saving || !view">
        </label>
        <label for="state-idle-seconds" class="grid gap-2 text-cp-sm">
          空闲后停止探测（秒；0 表示手动维护项不限制空闲）
          <input id="state-idle-seconds" v-model.number="maintenance.idleSeconds" type="number" min="0" max="86400" class="rounded-cp border border-cp-border bg-transparent p-2" :disabled="loading || saving || !view">
        </label>
        <BaseSwitch v-model="maintenance.autoModels" label="从真实业务自动发现精确模型" :show-label="true" :disabled="loading || saving || !view" />
        <BaseSelect :model-value="maintenance.poolId ?? ''" :options="poolOptions" aria-label="自动发现模型的代理池" :disabled="loading || saving || !view" @update:model-value="maintenance.poolId = String($event || '') || null" />
        <p class="text-cp-xs text-cp-text-tertiary">
          自动发现最多保留 8 个模型，不由维护探测递归触发；空闲窗口为 0 时，自动模型仍在空闲 1 小时后停止探测。
          手动刷新、修改设置和重启不会重置预算或绕过上游冷却。
          维护请求单独标记为系统流量，不扣用户余额。
        </p>
      </section>
      <div v-if="view" class="rounded-cp bg-cp-fill-quaternary p-4 text-cp-sm">
        <p>全局维护：{{ view.policy.enabled ? '已开启' : '已关闭，接管暂不生效' }} · 本账号维护项：{{ targets.length }}</p>
        <p v-if="!targets.length" class="mt-2 text-cp-text-secondary">
          还需在「State 自动维护」中配置精确模型和候选来源；只开账号开关不会启动探测。
        </p>
        <p v-for="target in targets" :key="target.model" class="mt-2 break-all text-cp-text-secondary">
          {{ target.model }} · {{ target.enabled ? '维护开启' : '维护暂停' }} · {{ target.candidateCount }} 个未过期候选
        </p>
        <p class="mt-2 text-cp-text-tertiary">
          两个账号开关默认关闭、互相独立。长度仅作观测，不保证模型能力；主动探测会消耗额度。
        </p>
      </div>
    </div>
    <template #footer>
      <BaseButton :disabled="saving" @click="open = false">
        取消
      </BaseButton>
      <BaseButton variant="primary" :loading="saving" :disabled="loading || !view" @click="save">
        保存账号策略
      </BaseButton>
    </template>
  </BaseModal>
</template>
