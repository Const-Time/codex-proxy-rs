<script setup lang="ts">
import type { PluginInstance, PluginManagementPage } from '@/api/modules/plugins'
import { onBeforeUnmount, ref, watch } from 'vue'
import { callPluginRoute } from '@/api/modules/plugins'
import { PLUGIN_MANAGEMENT_BRIDGE, PLUGIN_MANAGEMENT_BRIDGE_VERSION } from './management/bridge-contract'
import { assemblePluginManagementPage, readPluginFrameTheme } from './management/frame'

const props = defineProps<{
  plugin: PluginInstance
  page: PluginManagementPage
}>()
const frame = ref<HTMLIFrameElement>()
const source = ref('')
const failure = ref('')
const height = ref(600)
let controller = new AbortController()
let channel = ''
let session = ''
let inFlight = 0
let lastId = 0
function dispose() {
  controller.abort()
  source.value = ''
}
async function load() {
  dispose()
  controller = new AbortController()
  const current = controller
  channel = crypto.randomUUID()
  session = crypto.randomUUID()
  lastId = 0
  inFlight = 0
  failure.value = ''
  try {
    const result = await assemblePluginManagementPage({ ...props.plugin, target: { instanceId: props.plugin.id, revision: props.plugin.revision } }, props.page, channel, session, location.origin, readPluginFrameTheme(), 600, current.signal)
    if (!current.signal.aborted)
      source.value = result.srcdoc
  }
  catch (error) {
    if (!current.signal.aborted)
      failure.value = error instanceof Error ? error.message : '插件页面加载失败'
  }
}
async function receive(event: MessageEvent) {
  const message = event.data
  if (event.source !== frame.value?.contentWindow || event.origin !== 'null' || !message || typeof message !== 'object'
    || message.target !== PLUGIN_MANAGEMENT_BRIDGE || message.version !== PLUGIN_MANAGEMENT_BRIDGE_VERSION
    || message.channel !== channel || message.session !== session || !Number.isSafeInteger(message.id) || message.id <= lastId) {
    return
  }
  lastId = message.id
  const current = controller
  const currentChannel = channel
  const currentSession = session
  const reply = (ok: boolean, payload?: unknown, error?: string) => {
    if (!current.signal.aborted)
      frame.value?.contentWindow?.postMessage({ target: PLUGIN_MANAGEMENT_BRIDGE, version: 2, channel: currentChannel, session: currentSession, id: message.id, type: message.type === 'models-request' ? 'models-error' : 'result', ok, payload, error }, '*')
  }
  if (message.type === 'resize') {
    const value = message.payload?.height
    if (Number.isSafeInteger(value) && value > 0)
      height.value = Math.min(16000, Math.max(300, value))
    return
  }
  if (message.type !== 'request') {
    reply(false, undefined, '此宿主尚未开放该页面桥能力')
    return
  }
  const p = message.payload
  if (!p || typeof p.method !== 'string' || typeof p.path !== 'string' || p.path.length > 512
    || (p.query != null && (typeof p.query !== 'string' || new TextEncoder().encode(p.query).length > 8192))
    || (p.contentType != null && typeof p.contentType !== 'string')
    || (p.body != null && (!(p.body instanceof ArrayBuffer) || p.body.byteLength > 1024 * 1024)) || inFlight >= 8) {
    reply(false, undefined, '插件请求无效或并发已满')
    return
  }
  inFlight++
  try {
    reply(true, await callPluginRoute({ instanceId: props.plugin.id, revision: props.plugin.revision }, p, current.signal))
  }
  catch (error) {
    reply(false, undefined, error instanceof Error ? error.message : '插件调用失败')
  }
  finally {
    if (current === controller)
      inFlight--
  }
}
watch(() => [props.plugin.id, props.plugin.revision, props.page.id], load, { immediate: true })
window.addEventListener('message', receive)
onBeforeUnmount(() => {
  dispose()
  window.removeEventListener('message', receive)
})
</script>

<template>
  <p v-if="failure" role="alert" class="text-red-600">
    {{ failure }}
  </p>
  <iframe v-else-if="source" ref="frame" :srcdoc="source" sandbox="allow-scripts" :title="page.title" :style="{ height: `${height}px` }" class="w-full border-0" />
  <p v-else class="text-cp-text-secondary">
    正在加载插件页面…
  </p>
</template>
