<script setup lang="ts">
import type { OutboundProxyTest } from '@/api'
import { Wifi } from '@lucide/vue'
import { computed, shallowRef, watch } from 'vue'
import { testProxyEgress } from '@/api'
import BaseFormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseIconButton from '@/components/base/BaseIconButton.vue'
import BasePopover from '@/components/base/BasePopover.vue'
import BaseSelect from '@/components/base/BaseSelect.vue'
import { toast } from '@/components/base/BaseToast'
import { useProxyCatalog } from '@/composables/useProxyCatalog'
import { useRequestState } from '@/composables/useRequestState'
import { errorMessage } from '@/utils/async'

const props = withDefaults(defineProps<{ accountId?: string, endpoint?: string | null, disabled?: boolean, preserve?: boolean, error?: string }>(), { preserve: true })
const mode = defineModel<string>('mode', { required: true })
const proxyId = defineModel<string>('proxyId', { required: true })
const { proxies, loading } = useProxyCatalog()
// 目录不再携带账号明细；脱敏地址无法证明绑定关系，保持项只展示账号已有地址。
const options = computed(() => [
  ...(props.preserve
    ? [{
        label: '保持当前',
        value: 'preserve',
        description: props.endpoint || (props.accountId ? '直连' : '保留各账号的连接设置'),
      }]
    : []),
  { label: '直连', value: 'direct', description: '不使用代理' },
  ...proxies.value.map(proxy => ({
    label: `${proxy.name}${proxy.lastTest?.success ? '' : proxy.lastTest ? '（测试失败）' : '（未测试）'}`,
    value: proxy.id,
    description: proxy.endpoint,
    disabled: proxy.lastTest?.success !== true,
  })),
])
const selection = computed({
  get: () => mode.value === 'proxy' ? proxyId.value : mode.value,
  set: (value: string) => {
    const nextMode = value === 'preserve' || value === 'direct' ? value : 'proxy'
    mode.value = nextMode
    proxyId.value = nextMode === 'proxy' ? value : ''
  },
})
const request = useRequestState()
const testing = request.loading
const detailsOpen = shallowRef(false)
const selectedProxy = computed(() => mode.value === 'proxy' ? proxies.value.find(proxy => proxy.id === proxyId.value) : undefined)
const probeKey = computed(() => {
  if (mode.value === 'direct')
    return 'direct'
  const proxy = selectedProxy.value
  return proxy ? `${proxy.id}:${proxy.revision}` : undefined
})
const results = shallowRef<Record<string, OutboundProxyTest>>({})
const result = computed(() => probeKey.value ? results.value[probeKey.value] : undefined)

watch([probeKey, () => props.accountId], () => {
  request.invalidate()
  detailsOpen.value = false
}, { flush: 'sync' })

async function testSelection() {
  const key = probeKey.value
  if (!key || testing.value || props.disabled)
    return
  const requestId = request.start()
  try {
    const tested = await testProxyEgress({ proxyId: selectedProxy.value?.id })
    if (!request.isCurrent(requestId))
      return
    results.value = { ...results.value, [key]: tested }
    if (tested.success)
      detailsOpen.value = true
    else
      toast.error(tested.message || '出口测试失败')
  }
  catch (error: unknown) {
    if (request.isCurrent(requestId))
      toast.error(errorMessage(error, '出口测试失败'))
  }
  finally {
    request.finish(requestId)
  }
}
</script>

<template>
  <BaseFormItem label="出站隧道" :error="error">
    <div class="flex min-w-0 items-center gap-2">
      <BaseSelect v-model="selection" class="min-w-0 flex-1" :options="options" :disabled="disabled || loading" aria-label="出站隧道" />
      <BaseIconButton v-if="probeKey" label="测试当前出口" :disabled="disabled || loading" :loading="testing" @click="testSelection">
        <Wifi class="size-4" :class="result?.success ? 'text-cp-success-text' : undefined" />
      </BaseIconButton>
      <BasePopover v-if="result?.success" v-model="detailsOpen" placement="bottom-end" trigger="hover-click">
        <template #trigger="{ open }">
          <button type="button" class="shrink-0 rounded-cp border-0 bg-cp-success-container px-2 py-1 font-mono text-cp-xs text-cp-success-on-container" :aria-expanded="open" aria-label="查看出口测试结果">
            {{ result.latencyMs }} ms
          </button>
        </template>
        <div class="grid gap-1 px-3 py-2 text-cp-sm">
          <span>出口 IP <code>{{ result.exitIp || '—' }}</code></span>
          <span v-if="result.location">{{ [result.location.country, result.location.region, result.location.city].filter(Boolean).join(' · ') }}</span>
        </div>
      </BasePopover>
    </div>
  </BaseFormItem>
</template>
