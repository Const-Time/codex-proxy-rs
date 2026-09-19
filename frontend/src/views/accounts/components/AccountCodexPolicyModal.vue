<script setup lang="ts">
import type { AccountRow } from '../constants'
import type { TurnStateView } from '@/api/modules/turn-state'
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { getTurnState, saveAccountTurnState } from '@/api/modules/turn-state'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import BaseSwitch from '@/components/base/BaseSwitch.vue'
import { toast } from '@/components/base/BaseToast'
import { errorMessage } from '@/utils/async'

const props = defineProps<{ account: AccountRow | null }>()
const router = useRouter()
const open = defineModel<boolean>({ required: true })
const loading = ref(false)
const saving = ref(false)
const loadError = ref('')
const view = ref<TurnStateView | null>(null)
const fingerprintConvergence = ref(false)
const takeover = ref(false)
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
      <section class="grid gap-3 rounded-cp border border-cp-border p-4">
        <p class="text-cp-sm text-cp-text-secondary">
          小时预算、空闲策略、自动发现模型与采集代理已集中到「状态维护 → 维护配置 → 账号与模型」。本窗口只保存指纹与接管开关，不覆盖维护预算。
        </p>
        <BaseButton :disabled="loading || saving || !props.account || fingerprintConvergence !== (view?.accounts.find(p => p.accountId === props.account?.id)?.fingerprintConvergence ?? false) || takeover !== (view?.accounts.find(p => p.accountId === props.account?.id)?.takeover ?? false)" @click="open = false; router.push({ path: '/turn-state', query: { account: props.account!.id } })">
          前往状态维护
        </BaseButton>
        <p class="text-cp-xs text-cp-text-tertiary">
          修改本窗口开关后，请先保存再前往。
        </p>
      </section>
      <div v-if="view" class="rounded-cp bg-cp-fill-quaternary p-4 text-cp-sm">
        <p>全局维护：{{ view.policy.enabled ? '已开启' : '已关闭，接管暂不生效' }} · 本账号维护项：{{ targets.length }}</p>
        <p v-if="!targets.length" class="mt-2 text-cp-text-secondary">
          还需在「状态维护」中配置精确模型和候选来源；只开账号开关不会启动探测。
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
