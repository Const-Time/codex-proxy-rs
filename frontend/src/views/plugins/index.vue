<script setup lang="ts">
import type { PluginAccountSchedulerBinding, PluginArtifact, PluginInstance, PluginManagementPage, PluginMiddlewareBinding, PluginModelRouterBinding, PluginObserverBinding, PluginRetryPolicyBinding, PluginUpstreamAdapterBinding } from '@/api/modules/plugins'
import { computed, onMounted, ref, watch } from 'vue'
import { getApiKeys } from '@/api/modules/api-keys'
import { getPluginArtifacts, getPlugins, removePluginArtifact, savePlugin, uninstallPlugin, uploadPlugin } from '@/api/modules/plugins'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseCard from '@/components/base/BaseCard.vue'
import BaseCheckbox from '@/components/base/BaseCheckbox.vue'
import BaseConfirmModal from '@/components/base/BaseConfirmModal.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BasePageHeader from '@/components/base/BasePageHeader.vue'
import BaseSelect from '@/components/base/BaseSelect.vue'
import BaseTextarea from '@/components/base/BaseTextarea.vue'
import PluginFrame from './PluginFrame.vue'

const instances = ref<PluginInstance[]>([])
const artifacts = ref<PluginArtifact[]>([])
const busy = ref(false)
const failure = ref('')
const editing = ref<PluginInstance>()
const editOpen = ref(false)
const name = ref('')
const inferenceKey = ref('')
const keySearch = ref('')
const keyOptions = ref<{ label: string, value: string }[]>([])
async function searchKeys() {
  await perform(async () => {
    const result = await getApiKeys({ limit: 100, search: keySearch.value })
    keyOptions.value = result.items.filter(key => key.enabled).map(key => ({ label: `${key.name} · ${key.prefix}`, value: key.id }))
  })
}
const digest = ref('')
const configuration = ref('{}')
const routerEnabled = ref(false)
const routerOrder = ref('0')
const routerScope = ref<'all' | 'keys' | 'groups'>('all')
const routerIds = ref('')
const routerFailure = ref<'reject' | 'delegate'>('reject')
function routerBinding(): PluginModelRouterBinding | null {
  if (!routerEnabled.value)
    return null
  const order = Number(routerOrder.value)
  if (!Number.isInteger(order) || order < -10_000 || order > 10_000)
    throw new Error('模型路由顺序必须是 -10000 到 10000 的整数')
  const ids = routerIds.value.split(/\s+/u).filter(Boolean)
  if (routerScope.value !== 'all' && (!ids.length || ids.length > 256 || new Set(ids).size !== ids.length))
    throw new Error('请填写不重复的路由作用范围 ID，最多 256 个')
  return { order, failurePolicy: routerFailure.value, scope: routerScope.value === 'all' ? { kind: 'all' } : { kind: routerScope.value, ids } }
}
const upstreamEnabled = ref(false)
const upstreamScope = ref<'all' | 'keys' | 'groups'>('all')
const upstreamIds = ref('')
function upstreamBinding(): PluginUpstreamAdapterBinding | null {
  if (!upstreamEnabled.value)
    return null
  const ids = upstreamIds.value.split(/\s+/u).filter(Boolean)
  if (upstreamScope.value !== 'all' && (!ids.length || ids.length > 256 || new Set(ids).size !== ids.length))
    throw new Error('请填写不重复的适配器作用范围 ID，最多 256 个')
  return { scope: upstreamScope.value === 'all' ? { kind: 'all' } : { kind: upstreamScope.value, ids } }
}
const observerEnabled = ref(false)
const observerOrder = ref('0')
const observerCompleted = ref(true)
const observerWebSocket = ref(false)
const observerCompletedScope = ref<'all' | 'keys' | 'groups'>('all')
const observerWebSocketScope = ref<'all' | 'keys' | 'groups'>('all')
const observerCompletedIds = ref('')
const observerWebSocketIds = ref('')
function observationScope(kind: 'all' | 'keys' | 'groups', input: string): PluginMiddlewareBinding['scope'] {
  const ids = input.split(/\s+/u).filter(Boolean)
  if (kind !== 'all' && (!ids.length || ids.length > 256 || new Set(ids).size !== ids.length))
    throw new Error('请填写不重复的观察作用范围 ID，最多 256 个')
  return kind === 'all' ? { kind: 'all' } : { kind, ids }
}
function observerBinding(): PluginObserverBinding | null {
  if (!observerEnabled.value)
    return null
  const order = Number(observerOrder.value)
  if (!Number.isInteger(order) || order < -10_000 || order > 10_000)
    throw new Error('观察顺序必须是 -10000 到 10000 的整数')
  if (!observerCompleted.value && !observerWebSocket.value)
    throw new Error('请至少选择一种观察事件')
  return {
    order,
    requestCompleted: observerCompleted.value ? observationScope(observerCompletedScope.value, observerCompletedIds.value) : null,
    websocketResponse: observerWebSocket.value ? observationScope(observerWebSocketScope.value, observerWebSocketIds.value) : null,
  }
}
const retryEnabled = ref(false)
const retryOrder = ref('0')
const retryScope = ref<'all' | 'keys' | 'groups'>('all')
const retryIds = ref('')
function retryBinding(): PluginRetryPolicyBinding | null {
  if (!retryEnabled.value)
    return null
  const order = Number(retryOrder.value)
  if (!Number.isInteger(order) || order < -10_000 || order > 10_000)
    throw new Error('重试策略顺序必须是 -10000 到 10000 的整数')
  const ids = retryIds.value.split(/\s+/u).filter(Boolean)
  if (retryScope.value !== 'all' && (!ids.length || ids.length > 256 || new Set(ids).size !== ids.length))
    throw new Error('请填写不重复的重试作用范围 ID，最多 256 个')
  return { order, scope: retryScope.value === 'all' ? { kind: 'all' } : { kind: retryScope.value, ids } }
}
const schedulerEnabled = ref(false)
const schedulerOrder = ref('0')
const schedulerScope = ref<'all' | 'keys' | 'groups'>('all')
const schedulerIds = ref('')
const schedulerFailure = ref<'reject' | 'delegate'>('reject')
function schedulerBinding(): PluginAccountSchedulerBinding | null {
  if (!schedulerEnabled.value)
    return null
  const order = Number(schedulerOrder.value)
  if (!Number.isInteger(order) || order < -10_000 || order > 10_000)
    throw new Error('账号调度顺序必须是 -10000 到 10000 的整数')
  const ids = schedulerIds.value.split(/\s+/u).filter(Boolean)
  if (schedulerScope.value !== 'all' && (!ids.length || ids.length > 256 || new Set(ids).size !== ids.length))
    throw new Error('请填写不重复的调度作用范围 ID，最多 256 个')
  return { order, failurePolicy: schedulerFailure.value, scope: schedulerScope.value === 'all' ? { kind: 'all' } : { kind: schedulerScope.value, ids } }
}
const middlewareEnabled = ref(false)
const middlewareOrder = ref('0')
const middlewareHttp = ref(false)
const middlewareWebSocket = ref(false)
const middlewareService = ref(false)
const middlewareRequest = ref(true)
const middlewareAttempt = ref(false)
const middlewareScope = ref<'all' | 'keys' | 'groups'>('all')
const middlewareIds = ref('')
function middlewareBinding(): PluginMiddlewareBinding | null {
  if (!middlewareEnabled.value)
    return null
  const order = Number(middlewareOrder.value)
  if (!Number.isInteger(order) || order < -10_000 || order > 10_000)
    throw new Error('中间件顺序必须是 -10000 到 10000 的整数')
  const mounts: PluginMiddlewareBinding['mounts'] = []
  if (middlewareHttp.value)
    mounts.push('http')
  if (middlewareWebSocket.value)
    mounts.push('websocket')
  if (middlewareService.value)
    mounts.push('service')
  if (middlewareRequest.value)
    mounts.push('request')
  if (middlewareAttempt.value)
    mounts.push('attempt')
  if (!mounts.length)
    throw new Error('请选择中间件挂载位置')
  if ((middlewareHttp.value || middlewareWebSocket.value || middlewareService.value) && middlewareScope.value !== 'all')
    throw new Error('HTTP、WebSocket 会话与宿主服务中间件需选择全部请求')
  const ids = middlewareIds.value.split(/\s+/u).filter(Boolean)
  if (middlewareScope.value !== 'all' && (!ids.length || ids.length > 256 || new Set(ids).size !== ids.length))
    throw new Error('作用范围需填写 1～256 个不重复的 ID')
  return { order, mounts, scope: middlewareScope.value === 'all' ? { kind: 'all' } : { kind: middlewareScope.value, ids } }
}
const authenticationEnabled = ref(false)
const authenticationPolicy = ref<'reject' | 'delegate'>('reject')
const authenticationIdentities = ref('{}')
const secrets = ref<Record<string, string>>({})
const removeSecrets = ref<Record<string, boolean>>({})
const trust = ref(false)
const selected = ref<{
  plugin: PluginInstance
  page: PluginManagementPage
}>()
watch(() => [selected.value?.plugin.id, selected.value?.plugin.revision], () => {
  inferenceKey.value = ''
}, { flush: 'sync' })
const removing = ref<PluginInstance>()
const confirmDelete = ref(false)
const removingArtifact = ref('')
const confirmArtifactDelete = ref(false)
const artifact = computed(() => artifacts.value.find(a => a.sha256 === digest.value))
const secretFields = computed(() => [...new Set([...(artifact.value?.manifest.secretFields ?? []), ...(editing.value?.configuredSecrets ?? [])])])
watch(digest, () => {
  trust.value = false
})
const options = computed(() => artifacts.value.filter(a => !editing.value || a.pluginId === editing.value.artifact.pluginId).map(a => ({ label: `${a.displayName} · ${a.version} · ${a.sha256.slice(0, 8)}`, value: a.sha256 })))
async function refresh() {
  const [items, packages] = await Promise.all([getPlugins(), getPluginArtifacts()])
  instances.value = items
  artifacts.value = packages
  if (selected.value) {
    const item = items.find(p => p.id === selected.value?.plugin.id && p.revision === selected.value?.plugin.revision && p.status === 'ready')
    if (!item)
      selected.value = undefined
  }
}
async function perform(work: () => Promise<void>) {
  if (busy.value)
    return
  busy.value = true
  failure.value = ''
  try {
    await work()
    await refresh()
  }
  catch (error) {
    failure.value = error instanceof Error ? error.message : '插件操作失败，请刷新核实状态'
    selected.value = undefined
  }
  finally {
    busy.value = false
  }
}
function edit(item?: PluginInstance) {
  selected.value = undefined
  editing.value = item
  editOpen.value = true
  name.value = item?.name ?? ''
  digest.value = item?.artifact.sha256 ?? artifacts.value[0]?.sha256 ?? ''
  configuration.value = JSON.stringify(item?.configuration ?? {}, null, 2)
  routerEnabled.value = !!item?.modelRouter
  routerOrder.value = String(item?.modelRouter?.order ?? 0)
  routerScope.value = item?.modelRouter?.scope.kind ?? 'all'
  routerIds.value = item?.modelRouter && item.modelRouter.scope.kind !== 'all' ? item.modelRouter.scope.ids.join('\n') : ''
  routerFailure.value = item?.modelRouter?.failurePolicy ?? 'reject'
  upstreamEnabled.value = !!item?.upstreamAdapter
  upstreamScope.value = item?.upstreamAdapter?.scope.kind ?? 'all'
  upstreamIds.value = item?.upstreamAdapter && item.upstreamAdapter.scope.kind !== 'all' ? item.upstreamAdapter.scope.ids.join('\n') : ''
  observerEnabled.value = !!item?.observer
  observerOrder.value = String(item?.observer?.order ?? 0)
  observerCompleted.value = item?.observer ? !!item.observer.requestCompleted : true
  observerWebSocket.value = !!item?.observer?.websocketResponse
  observerCompletedScope.value = item?.observer?.requestCompleted?.kind ?? 'all'
  observerWebSocketScope.value = item?.observer?.websocketResponse?.kind ?? 'all'
  observerCompletedIds.value = item?.observer?.requestCompleted && item.observer.requestCompleted.kind !== 'all' ? item.observer.requestCompleted.ids.join('\n') : ''
  observerWebSocketIds.value = item?.observer?.websocketResponse && item.observer.websocketResponse.kind !== 'all' ? item.observer.websocketResponse.ids.join('\n') : ''
  retryEnabled.value = !!item?.retryPolicy
  retryOrder.value = String(item?.retryPolicy?.order ?? 0)
  retryScope.value = item?.retryPolicy?.scope.kind ?? 'all'
  retryIds.value = item?.retryPolicy && item.retryPolicy.scope.kind !== 'all' ? item.retryPolicy.scope.ids.join('\n') : ''
  schedulerEnabled.value = !!item?.accountScheduler
  schedulerOrder.value = String(item?.accountScheduler?.order ?? 0)
  schedulerScope.value = item?.accountScheduler?.scope.kind ?? 'all'
  schedulerIds.value = item?.accountScheduler && item.accountScheduler.scope.kind !== 'all' ? item.accountScheduler.scope.ids.join('\n') : ''
  schedulerFailure.value = item?.accountScheduler?.failurePolicy ?? 'reject'
  middlewareEnabled.value = !!item?.middleware
  middlewareOrder.value = String(item?.middleware?.order ?? 0)
  middlewareHttp.value = item?.middleware?.mounts.includes('http') ?? false
  middlewareWebSocket.value = item?.middleware?.mounts.includes('websocket') ?? false
  middlewareService.value = item?.middleware?.mounts.includes('service') ?? false
  middlewareRequest.value = item?.middleware?.mounts.includes('request') ?? true
  middlewareAttempt.value = item?.middleware?.mounts.includes('attempt') ?? false
  middlewareScope.value = item?.middleware?.scope.kind ?? 'all'
  middlewareIds.value = item?.middleware && item.middleware.scope.kind !== 'all' ? item.middleware.scope.ids.join('\n') : ''
  authenticationEnabled.value = !!item?.frontendAuthentication
  authenticationPolicy.value = item?.frontendAuthentication?.failurePolicy ?? 'reject'
  authenticationIdentities.value = JSON.stringify(item?.frontendAuthentication?.identities ?? {}, null, 2)
  secrets.value = {}
  removeSecrets.value = {}
  trust.value = false
}
async function upload(event: Event) {
  const input = event.target as HTMLInputElement
  const file = input.files?.[0]
  if (!file)
    return
  await perform(async () => {
    if (file.size > 32 * 1024 * 1024)
      throw new Error('插件包不能超过 32 MiB')
    const result = await uploadPlugin(file)
    await refresh()
    edit()
    digest.value = result.sha256
    name.value = result.displayName
  })
  input.value = ''
}
async function save(enabled: boolean) {
  await perform(async () => {
    const values: Record<string, string | null> = {}
    for (const key of secretFields.value) {
      if (removeSecrets.value[key])
        values[key] = null
      else if (secrets.value[key])
        values[key] = secrets.value[key]!
    }
    await savePlugin(editing.value?.id, { observer: observerBinding(), upstreamAdapter: upstreamBinding(), retryPolicy: retryBinding(), accountScheduler: schedulerBinding(), modelRouter: routerBinding(), middleware: middlewareBinding(), frontendAuthentication: authenticationEnabled.value ? { failurePolicy: authenticationPolicy.value, identities: JSON.parse(authenticationIdentities.value) } : null, revision: editing.value?.revision ?? 0, name: name.value, artifactSha256: digest.value, configuration: JSON.parse(configuration.value), secrets: values, enabled, trustedProcess: trust.value })
    editOpen.value = false
    secrets.value = {}
  })
}
async function disable(item: PluginInstance) {
  selected.value = undefined
  await perform(async () => {
    await savePlugin(item.id, { observer: item.observer ?? null, upstreamAdapter: item.upstreamAdapter ?? null, retryPolicy: item.retryPolicy ?? null, accountScheduler: item.accountScheduler ?? null, modelRouter: item.modelRouter ?? null, middleware: item.middleware ?? null, frontendAuthentication: item.frontendAuthentication ?? null, revision: item.revision, name: item.name, artifactSha256: item.artifact.sha256, configuration: item.configuration, secrets: {}, enabled: false, trustedProcess: false })
  })
}
onMounted(() => perform(async () => { }))
</script>

<template>
  <div class="space-y-5">
    <BasePageHeader title="插件" description="安装可信插件，管理配置、运行状态和版本。" />
    <div class="flex flex-wrap items-center gap-3">
      <label for="plugin-upload" class="cursor-pointer rounded-lg border border-cp-border p-3">
        上传插件包（.tar.gz）
        <input id="plugin-upload" type="file" accept=".gz,.tgz" :disabled="busy" class="sr-only" @change="upload">
      </label>
      <BaseButton :disabled="busy || !artifacts.length" @click="edit()">
        创建实例
      </BaseButton>
      <BaseButton variant="secondary" :disabled="busy" @click="perform(async () => {})">
        刷新
      </BaseButton>
    </div>
    <p v-if="failure" role="alert" class="text-red-600">
      {{ failure }}
    </p>
    <BaseCard v-if="editOpen" class="space-y-4 p-5">
      <h2 class="text-lg font-semibold">
        {{ editing ? '配置与版本切换' : '安装插件' }}
      </h2>
      <!-- eslint-disable-next-line vue-a11y/label-has-for -->
      <label for="plugin-name" class="block">实例名称<BaseInput id="plugin-name" v-model="name" placeholder="实例名称" :disabled="busy" /></label>
      <BaseSelect v-model="digest" aria-label="插件制品版本" :options="options" :disabled="busy" placeholder="选择已上传制品" />
      <p v-if="artifact?.versionWarning" class="text-amber-600">
        {{ artifact.versionWarning }}
      </p>
      <p v-if="artifact?.unsupportedCapabilities.length" class="text-amber-600">
        尚未支持：{{ artifact.unsupportedCapabilities.join('、') }}。可以保存配置，暂不能启用。
      </p>
      <!-- The shared field component forwards id to its native control. -->
      <!-- eslint-disable-next-line vue-a11y/label-has-for -->
      <label for="plugin-configuration" class="block">配置（JSON）<BaseTextarea id="plugin-configuration" v-model="configuration" :rows="8" :disabled="busy" /></label>
      <details><summary>查看配置要求</summary><pre class="overflow-auto text-sm">{{ JSON.stringify(artifact?.manifest.configurationSchema, null, 2) }}</pre></details>
      <div v-for="field in secretFields" :key="field" class="space-y-2">
        <!-- eslint-disable-next-line vue-a11y/label-has-for -->
        <label :for="`plugin-secret-${field}`" class="block">{{ field }}<BaseInput :id="`plugin-secret-${field}`" v-model="secrets[field]" type="password" autocomplete="new-password" :placeholder="editing?.configuredSecrets.includes(field) ? '已配置，留空保留' : '填写敏感配置'" :disabled="busy || !artifact?.manifest.secretFields.includes(field)" /></label>
        <BaseCheckbox v-if="editing?.configuredSecrets.includes(field)" v-model="removeSecrets[field]" show-label :label="`清除 ${field}`" :disabled="busy" />
      </div>
      <details class="space-y-3">
        <summary>模型路由</summary>
        <BaseCheckbox v-model="routerEnabled" show-label label="使用此插件选择模型和 Provider" :disabled="busy" />
        <template v-if="routerEnabled">
          <!-- eslint-disable-next-line vue-a11y/label-has-for -->
          <label for="plugin-router-order" class="block">执行顺序（较小的先执行）<BaseInput id="plugin-router-order" v-model="routerOrder" type="number" :disabled="busy" /></label>
          <BaseSelect v-model="routerScope" aria-label="模型路由作用范围" :options="[{ label: '全部请求', value: 'all' }, { label: '指定 Key', value: 'keys' }, { label: '指定分组', value: 'groups' }]" :disabled="busy" />
          <!-- eslint-disable-next-line vue-a11y/label-has-for -->
          <label v-if="routerScope !== 'all'" for="plugin-router-ids" class="block">{{ routerScope === 'keys' ? 'Key ID' : '分组 ID' }}（每行一个）<BaseTextarea id="plugin-router-ids" v-model="routerIds" :rows="3" :disabled="busy" /></label>
          <BaseSelect v-model="routerFailure" aria-label="路由插件故障行为" :options="[{ label: '拒绝请求', value: 'reject' }, { label: '委托后续路由策略', value: 'delegate' }]" :disabled="busy" />
          <p class="text-sm text-cp-text-secondary">
            路由选择仍受 Key 权限、模型能力和续接约束。首个明确决定生效；明确拒绝不会继续委托。插件变更或停用会撤销旧请求。
          </p>
        </template>
      </details>
      <details class="space-y-3">
        <summary>账号调度</summary>
        <BaseCheckbox v-model="schedulerEnabled" show-label label="使用此插件选择上游账号" :disabled="busy" />
        <template v-if="schedulerEnabled">
          <!-- eslint-disable-next-line vue-a11y/label-has-for -->
          <label for="plugin-scheduler-order" class="block">执行顺序（较小的先执行）<BaseInput id="plugin-scheduler-order" v-model="schedulerOrder" type="number" :disabled="busy" /></label>
          <BaseSelect v-model="schedulerScope" aria-label="账号调度作用范围" :options="[{ label: '全部请求', value: 'all' }, { label: '指定 Key', value: 'keys' }, { label: '指定分组', value: 'groups' }]" :disabled="busy" />
          <!-- eslint-disable-next-line vue-a11y/label-has-for -->
          <label v-if="schedulerScope !== 'all'" for="plugin-scheduler-ids" class="block">{{ schedulerScope === 'keys' ? 'Key ID' : '分组 ID' }}（每行一个）<BaseTextarea id="plugin-scheduler-ids" v-model="schedulerIds" :rows="3" :disabled="busy" /></label>
          <BaseSelect v-model="schedulerFailure" aria-label="调度插件故障行为" :options="[{ label: '拒绝请求', value: 'reject' }, { label: '使用宿主账号调度', value: 'delegate' }]" :disabled="busy" />
          <p class="text-sm text-cp-text-secondary">
            按顺序只调用首个匹配的调度插件，委托时使用宿主选号。账号选择仍受 Key 权限、续接绑定和可用容量约束。明确拒绝会终止请求；插件变更或停用会撤销旧请求。
          </p>
        </template>
      </details>
      <details class="space-y-3">
        <summary>上游适配器</summary>
        <BaseCheckbox v-model="upstreamEnabled" show-label label="使用此插件适配上游请求" :disabled="busy" />
        <template v-if="upstreamEnabled">
          <BaseSelect v-model="upstreamScope" aria-label="上游适配器作用范围" :options="[{ label: '全部请求', value: 'all' }, { label: '指定 Key', value: 'keys' }, { label: '指定分组', value: 'groups' }]" :disabled="busy" />
          <!-- eslint-disable-next-line vue-a11y/label-has-for -->
          <label v-if="upstreamScope !== 'all'" for="plugin-upstream-ids" class="block">{{ upstreamScope === 'keys' ? 'Key ID' : '分组 ID' }}（每行一个）<BaseTextarea id="plugin-upstream-ids" v-model="upstreamIds" :rows="3" :disabled="busy" /></label>
          <p class="text-sm text-cp-text-secondary">
            使用插件声明的 Provider 和公开模型范围，重叠绑定会被拒绝。账号权限、选择和计费仍由网关管理；匹配的适配器不可用时请求会失败。当前可保存绑定，启用支持以制品提示为准。
          </p>
        </template>
      </details>
      <details class="space-y-3">
        <summary>重试策略</summary>
        <BaseCheckbox v-model="retryEnabled" show-label label="由此插件决定是否继续恢复请求" :disabled="busy" />
        <template v-if="retryEnabled">
          <!-- eslint-disable-next-line vue-a11y/label-has-for -->
          <label for="plugin-retry-order" class="block">执行顺序（较小的先执行）<BaseInput id="plugin-retry-order" v-model="retryOrder" type="number" :disabled="busy" /></label>
          <BaseSelect v-model="retryScope" aria-label="重试策略作用范围" :options="[{ label: '全部请求', value: 'all' }, { label: '指定 Key', value: 'keys' }, { label: '指定分组', value: 'groups' }]" :disabled="busy" />
          <!-- eslint-disable-next-line vue-a11y/label-has-for -->
          <label v-if="retryScope !== 'all'" for="plugin-retry-ids" class="block">{{ retryScope === 'keys' ? 'Key ID' : '分组 ID' }}（每行一个）<BaseTextarea id="plugin-retry-ids" v-model="retryIds" :rows="3" :disabled="busy" /></label>
          <p class="text-sm text-cp-text-secondary">
            依次咨询匹配的策略，插件委托或故障时交给后续策略处理。明确停止会保留原始失败。整条链最多等待两秒；插件不能增加重试次数、缩短退避或绕过安全恢复条件。
          </p>
        </template>
      </details>
      <details class="space-y-3 rounded-lg border border-cp-border p-3">
        <summary>观察订阅</summary>
        <p class="text-sm text-cp-text-secondary">
          观察插件可读取请求终态、用量和费用，以及实际上游 WebSocket 的原始响应内容。请选择需要共享的事件和作用范围。
        </p>
        <BaseCheckbox v-model="observerEnabled" show-label label="向此插件投递观察事件" :disabled="busy" />
        <template v-if="observerEnabled">
          <!-- eslint-disable-next-line vue-a11y/label-has-for -->
          <label for="plugin-observer-order" class="block">执行顺序（较小的先执行）<BaseInput id="plugin-observer-order" v-model="observerOrder" type="number" :disabled="busy" /></label>
          <BaseCheckbox v-model="observerCompleted" show-label label="请求完成事件" :disabled="busy" />
          <template v-if="observerCompleted">
            <BaseSelect v-model="observerCompletedScope" aria-label="请求完成观察范围" :options="[{ label: '全部请求', value: 'all' }, { label: '指定 Key', value: 'keys' }, { label: '指定分组', value: 'groups' }]" :disabled="busy" />
            <!-- eslint-disable-next-line vue-a11y/label-has-for -->
            <label v-if="observerCompletedScope !== 'all'" for="plugin-observer-completed-ids" class="block">{{ observerCompletedScope === 'keys' ? 'Key ID' : '分组 ID' }}（每行一个）<BaseTextarea id="plugin-observer-completed-ids" v-model="observerCompletedIds" :rows="3" :disabled="busy" /></label>
          </template>
          <BaseCheckbox v-model="observerWebSocket" show-label label="实际上游 WebSocket 响应帧" :disabled="busy" />
          <template v-if="observerWebSocket">
            <BaseSelect v-model="observerWebSocketScope" aria-label="WebSocket 观察范围" :options="[{ label: '全部请求', value: 'all' }, { label: '指定 Key', value: 'keys' }, { label: '指定分组', value: 'groups' }]" :disabled="busy" />
            <!-- eslint-disable-next-line vue-a11y/label-has-for -->
            <label v-if="observerWebSocketScope !== 'all'" for="plugin-observer-websocket-ids" class="block">{{ observerWebSocketScope === 'keys' ? 'Key ID' : '分组 ID' }}（每行一个）<BaseTextarea id="plugin-observer-websocket-ids" v-model="observerWebSocketIds" :rows="3" :disabled="busy" /></label>
          </template>
          <p class="text-sm text-cp-text-secondary">
            观察失败或停用不会改变请求结果。投递有容量和时间限制，繁忙时可能跳过事件；插件可用事件 ID 自行去重。
          </p>
        </template>
      </details>
      <details class="space-y-3">
        <summary>中间件绑定</summary>
        <p class="text-sm text-cp-text-secondary">
          仅用于声明中间件能力的插件。当前可保存绑定，待请求入口接入后才能启用；是否支持以制品上方的提示为准。
        </p>
        <BaseCheckbox v-model="middlewareEnabled" show-label label="配置中间件的顺序与作用范围" :disabled="busy" />
        <template v-if="middlewareEnabled">
          <!-- eslint-disable-next-line vue-a11y/label-has-for -->
          <label for="plugin-middleware-order" class="block">执行顺序（较小的先执行）<BaseInput id="plugin-middleware-order" v-model="middlewareOrder" type="number" :disabled="busy" /></label>
          <div class="flex flex-wrap gap-4">
            <BaseCheckbox v-model="middlewareHttp" show-label label="HTTP 入口" :disabled="busy" />
            <BaseCheckbox v-model="middlewareWebSocket" show-label label="WebSocket 会话" :disabled="busy" />
            <BaseCheckbox v-model="middlewareService" show-label label="宿主服务调用" :disabled="busy" />
            <BaseCheckbox v-model="middlewareRequest" show-label label="每个逻辑请求" :disabled="busy" />
            <BaseCheckbox v-model="middlewareAttempt" show-label label="每次上游尝试" :disabled="busy" />
          </div>
          <BaseSelect v-model="middlewareScope" aria-label="中间件作用范围" :options="[{ label: '全部请求', value: 'all' }, { label: '指定 Key', value: 'keys' }, { label: '指定分组', value: 'groups' }]" :disabled="busy" />
          <p v-if="middlewareHttp || middlewareWebSocket || middlewareService" class="text-sm text-cp-text-secondary">
            HTTP、WebSocket 会话与宿主服务调用需选择全部请求。指定 Key 或分组仅适用于逻辑请求与上游尝试。
          </p>
          <!-- eslint-disable-next-line vue-a11y/label-has-for -->
          <label v-if="middlewareScope !== 'all'" for="plugin-middleware-ids" class="block">{{ middlewareScope === 'keys' ? 'Key ID' : '分组 ID' }}（每行一个）<BaseTextarea id="plugin-middleware-ids" v-model="middlewareIds" :rows="3" :disabled="busy" /></label>
          <p class="text-sm text-cp-text-secondary">
            范围只决定哪些请求调用插件，不增加 Key 权限。同序号按实例 ID 排序；修改或停用会撤销旧调用。升级准备失败仍使用旧版本。
          </p>
        </template>
      </details>
      <details class="space-y-3">
        <summary>外部认证</summary>
        <p class="text-sm text-cp-text-secondary">
          仅用于声明入口认证能力的插件。同一时间只能启用一个认证器；此配置不会发送给插件。变更或停用会撤销旧授权。
        </p>
        <BaseCheckbox v-model="authenticationEnabled" show-label label="使用此插件认证客户端请求" :disabled="busy" />
        <template v-if="authenticationEnabled">
          <BaseSelect v-model="authenticationPolicy" aria-label="不匹配时的认证行为" :options="[{ label: '拒绝所有未匹配请求', value: 'reject' }, { label: '仅对明确未匹配的请求尝试普通 Key', value: 'delegate' }]" :disabled="busy" />
          <!-- eslint-disable-next-line vue-a11y/label-has-for -->
          <label for="plugin-auth-identities" class="block">外部身份到本地 Key 的映射（JSON）<BaseTextarea id="plugin-auth-identities" v-model="authenticationIdentities" :rows="5" :disabled="busy" placeholder="{&quot;external-user&quot;: &quot;key_id&quot;}" /></label>
          <p class="text-sm text-cp-text-secondary">
            填写 Key ID，不要填写密钥。映射身份将使用该 Key 所属用户的分组权限、倍率与共享额度。认证器超时、拒绝或返回未知身份时不会回退。
          </p>
        </template>
      </details>
      <p class="text-sm text-cp-text-secondary">
        插件进程以网关的系统身份运行，可访问主机文件、凭据和网络。仅安装可信来源的插件。
      </p>
      <BaseCheckbox v-model="trust" show-label label="我信任此插件及所选版本，允许它在网关上运行" :disabled="busy" />
      <div class="flex gap-3">
        <BaseButton :disabled="busy || !trust || !artifact || !!artifact.unsupportedCapabilities.length" @click="save(true)">
          保存并启用
        </BaseButton>
        <BaseButton variant="secondary" :disabled="busy || !artifact" @click="save(false)">
          保存并停用
        </BaseButton>
        <BaseButton variant="secondary" :disabled="busy" @click="editOpen = false; secrets = {}">
          取消
        </BaseButton>
      </div>
    </BaseCard>
    <p v-if="!instances.length && !busy" class="text-cp-text-secondary">
      还没有安装插件。
    </p>
    <BaseCard v-for="item in instances" :key="item.id" class="space-y-3 p-5">
      <div class="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 class="font-semibold">
            {{ item.name }}
          </h2><p class="text-sm text-cp-text-secondary">
            {{ item.artifact.pluginId }} · {{ item.artifact.version }} · {{ { ready: '运行中', disabled: '已停用', failed: '启动失败或进程已退出' }[item.status] ?? item.status }}
          </p>
        </div>
        <div class="flex gap-2">
          <BaseButton variant="secondary" :disabled="busy" @click="edit(item)">
            配置 / 切换版本
          </BaseButton>
          <BaseButton v-if="item.enabled" variant="secondary" :disabled="busy" @click="disable(item)">
            停用
          </BaseButton>
          <BaseButton v-else variant="secondary" :disabled="busy" @click="removing = item; confirmDelete = true">
            卸载
          </BaseButton>
        </div>
      </div>
      <div v-if="item.status === 'ready'" class="flex gap-2">
        <BaseButton v-for="page in item.pages" :key="page.id" variant="secondary" @click="selected = { plugin: item, page }; editOpen = false">
          {{ page.title }}
        </BaseButton>
      </div>
    </BaseCard>
    <BaseCard v-if="selected" class="space-y-3 p-4">
      <p class="text-sm text-cp-text-secondary">
        推理使用本人 Key 的权限与额度，按现有倍率计费。更换 Key 或关闭页面会取消进行中的调用。
      </p>
      <div class="flex flex-wrap gap-3">
        <BaseInput v-model="keySearch" aria-label="搜索本人推理 Key" placeholder="搜索本人 Key" class="max-w-xs" />
        <BaseButton variant="secondary" :disabled="busy" @click="searchKeys">
          查询 Key
        </BaseButton>
        <BaseSelect v-model="inferenceKey" aria-label="选择推理 Key" :options="keyOptions" placeholder="选择本人 Key 后允许推理" class="max-w-sm" />
        <BaseButton variant="secondary" @click="selected = undefined; inferenceKey = ''">
          关闭页面
        </BaseButton>
      </div>
      <PluginFrame :key="`${selected.plugin.id}/${selected.plugin.revision}/${selected.page.id}`" :plugin="selected.plugin" :page="selected.page" :client-key-id="inferenceKey" />
    </BaseCard>
    <details v-if="artifacts.length" class="space-y-3">
      <summary>已上传制品（{{ artifacts.length }}）</summary>
      <div v-for="pkg in artifacts" :key="pkg.sha256" class="flex flex-wrap items-center justify-between gap-3 p-3">
        <span>{{ pkg.displayName }} · {{ pkg.version }} · {{ pkg.sha256.slice(0, 8) }}</span>
        <BaseButton variant="secondary" :disabled="busy || instances.some(item => item.artifact.sha256 === pkg.sha256)" @click="removingArtifact = pkg.sha256; confirmArtifactDelete = true">
          删除制品
        </BaseButton>
      </div>
    </details>
    <BaseConfirmModal v-model="confirmArtifactDelete" title="删除制品" description="仅能删除没有实例使用的制品。删除后需要重新上传才能安装该版本。" destructive :loading="busy" @confirm="perform(async () => { await removePluginArtifact(removingArtifact); confirmArtifactDelete = false })" />
    <BaseConfirmModal v-model="confirmDelete" title="卸载插件" description="将删除该实例的配置和私有状态。已上传制品保留，重新安装不会恢复旧状态。" destructive :loading="busy" @confirm="perform(async () => { if (removing) await uninstallPlugin(removing); confirmDelete = false })" />
  </div>
</template>
