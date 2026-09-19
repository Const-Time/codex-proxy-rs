<script setup lang="ts">
import type { TurnStateMaintenance, TurnStateView } from '@/api/modules/turn-state'
import { computed, ref, watch } from 'vue'
import { saveAccountTurnState } from '@/api/modules/turn-state'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseNumberInput from '@/components/base/BaseNumberInput.vue'
import BaseSelect from '@/components/base/BaseSelect.vue'
import BaseSwitch from '@/components/base/BaseSwitch.vue'
import { toast } from '@/components/base/BaseToast'
import { errorMessage } from '@/utils/async'

const props = defineProps<{ accountId: string, view: TurnStateView, blocked: boolean }>()
const emit = defineEmits<{ saved: [view: TurnStateView], dirty: [value: boolean], busy: [value: boolean] }>()
const maintenance = ref<TurnStateMaintenance>({ maxProbesPerHour: 30, idleSeconds: 3600, autoModels: false, poolId: null })
const takeover = ref(false)
const saving = ref(false)
const revision = ref(0)
const fingerprint = ref(false)
const baseline = ref('')
const snapshot = computed(() => JSON.stringify({ takeover: takeover.value, maintenance: maintenance.value }))
const dirty = computed(() => baseline.value !== snapshot.value)
const poolOptions = computed(() => [{ value: '', label: '选择自动发现模型的采集代理' }, ...props.view.pools.map(p => ({ value: p.id, label: p.name }))])
const target = computed(() => props.view.targets.find(t => t.accountId === props.accountId))

function reset() {
  const p = props.view.accounts.find(a => a.accountId === props.accountId)
  takeover.value = p?.takeover ?? false
  fingerprint.value = p?.fingerprintConvergence ?? false
  revision.value = props.view.revision
  maintenance.value = { maxProbesPerHour: 30, idleSeconds: 3600, autoModels: false, poolId: null, ...p?.maintenance }
  baseline.value = snapshot.value
}
watch(() => props.accountId, reset, { immediate: true })
watch(() => props.view, () => {
  if (!dirty.value && !saving.value)
    reset()
})
watch(dirty, value => emit('dirty', value), { immediate: true })
async function save() {
  if (props.blocked || saving.value)
    return
  saving.value = true
  emit('busy', true)
  const accountId = props.accountId
  try {
    const result = await saveAccountTurnState(accountId, { revision: revision.value, fingerprintConvergence: fingerprint.value, takeover: takeover.value, maintenance: { ...maintenance.value } })
    emit('saved', result)
    baseline.value = snapshot.value
    revision.value = result.revision
    toast.success('账号维护策略已保存，已用预算不会清零')
  }
  catch (e) {
    toast.error(errorMessage(e, '保存失败，请重新读取最新账号策略'))
  }
  finally {
    saving.value = false
    emit('busy', false)
  }
}
</script>

<template>
  <section class="my-4 grid gap-4 rounded-cp border border-cp-border p-4" aria-label="账号维护策略">
    <div class="flex flex-wrap items-center justify-between gap-3">
      <h3 class="font-semibold">
        账号维护策略
      </h3>
      <span class="text-cp-sm text-cp-text-secondary">小时预算 {{ target?.hourlyUsed ?? 0 }} / {{ maintenance.maxProbesPerHour }} · 同账号模型共享</span>
    </div>
    <BaseSwitch v-model="takeover" label="HTTP / WS Turn-state 接管" :show-label="true" :disabled="saving || blocked" />
    <div class="grid gap-4 sm:grid-cols-2">
      <div class="grid gap-2 text-cp-sm">
        每小时最多探测请求<BaseNumberInput v-model="maintenance.maxProbesPerHour" label="每小时最多探测请求" :min="1" :max="600" :disabled="saving || blocked" />
      </div>
      <div class="grid gap-2 text-cp-sm">
        空闲后停止探测（秒）<BaseNumberInput v-model="maintenance.idleSeconds" label="空闲后停止探测秒数" :min="0" :max="86400" :disabled="saving || blocked" />
      </div>
    </div>
    <BaseSwitch v-model="maintenance.autoModels" label="从真实业务自动发现模型" :show-label="true" :disabled="saving || blocked" />
    <BaseSelect :model-value="maintenance.poolId ?? ''" :options="poolOptions" aria-label="自动发现采集代理" :disabled="saving || blocked" @update:model-value="maintenance.poolId = String($event || '') || null" />
    <p class="text-cp-xs text-cp-text-tertiary">
      采集与验证分别计数；手动刷新不绕过预算和冷却。空闲秒数为 0 时手动项不限空闲，自动发现项仍在空闲 1 小时后停探；自动发现最多 8 个模型。指纹收敛仍在账号管理中设置。
    </p>
    <div class="flex flex-wrap gap-3">
      <BaseButton variant="primary" :loading="saving" :disabled="blocked || !dirty" @click="save">
        保存账号策略
      </BaseButton>
      <BaseButton :disabled="saving || blocked" @click="reset">
        重新读取账号策略
      </BaseButton>
      <span v-if="dirty" role="status" class="self-center text-cp-xs text-cp-warning">账号策略未保存</span>
      <span v-if="blocked" class="self-center text-cp-xs text-cp-warning">请先保存或放弃页面其它设置</span>
    </div>
  </section>
</template>
