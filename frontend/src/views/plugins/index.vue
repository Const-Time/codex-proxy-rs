<script setup lang="ts">
import type { PluginFormState } from './bindings'
import type { PluginArtifact, PluginInstance, PluginManagementPage, PluginOnlineRelease } from '@/api/modules/plugins'
import { PackageOpen, Plus, RefreshCw, Upload } from '@lucide/vue'
import { computed, defineAsyncComponent, onMounted, ref, shallowRef, watch } from 'vue'
import { getApiKeys } from '@/api/modules/api-keys'
import {
  checkPluginUpdate,
  getPluginArtifacts,
  getPlugins,
  installPluginUpdate,
  removePluginArtifact,
  savePlugin,
  uninstallPlugin,
  uploadPlugin,
} from '@/api/modules/plugins'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseCard from '@/components/base/BaseCard.vue'
import BaseConfirmModal from '@/components/base/BaseConfirmModal.vue'
import BaseEmpty from '@/components/base/BaseEmpty.vue'
import BaseIconButton from '@/components/base/BaseIconButton.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseMenuItem from '@/components/base/BaseMenuItem.vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import BasePageHeader from '@/components/base/BasePageHeader.vue'
import BaseSegmented from '@/components/base/BaseSegmented.vue'
import BaseSelect from '@/components/base/BaseSelect.vue'
import RowActionMenu from '@/components/RowActionMenu.vue'
import { useIdSet } from '@/composables/useIdSet'
import { errorMessage } from '@/utils/async'
import { createPluginFormState, parsePluginPayload } from './bindings'

const PluginFrame = defineAsyncComponent(() => import('./PluginFrame.vue'))
const PluginEditorModal = defineAsyncComponent(() => import('./PluginEditorModal.vue'))

const ONLINE_UPDATABLE_PLUGINS = new Set(['local.model-health', 'cpr.shared-budget-manager'])

const instances = ref<PluginInstance[]>([])
const artifacts = ref<PluginArtifact[]>([])
const refreshing = shallowRef(false)
const uploading = shallowRef(false)
const saving = shallowRef(false)
const mutating = shallowRef(false)
const keySearching = shallowRef(false)
const checkingPlugins = useIdSet<string>()
const failure = shallowRef('')
const editorError = shallowRef('')
const keyError = shallowRef('')
const view = shallowRef('installed')
const search = shallowRef('')
const fileInput = ref<HTMLInputElement>()
const notices = ref<Record<string, string>>({})
const onlineUpdates = ref<Record<string, { revision: number, release: PluginOnlineRelease }>>({})

const tabs = computed(() => [
  { label: `已安装 (${instances.value.length})`, value: 'installed' },
  { label: `已上传制品 (${artifacts.value.length})`, value: 'artifacts' },
])
const query = computed(() => search.value.trim().toLowerCase())
const filteredInstances = computed(() => instances.value.filter(item =>
  [item.name, item.artifact.pluginId, item.artifact.version].some(value => value.toLowerCase().includes(query.value)),
))
const filteredArtifacts = computed(() => artifacts.value.filter(item =>
  [item.displayName, item.pluginId, item.version, item.sha256].some(value => value.toLowerCase().includes(query.value)),
))
const statusLabels: Record<string, string> = {
  ready: '运行中',
  disabled: '已停用',
  failed: '启动失败',
}

const confirmUpdate = shallowRef(false)
const updating = shallowRef<PluginInstance>()
const editing = shallowRef<PluginInstance>()
const editOpen = shallowRef(false)
const editorLoaded = shallowRef(false)
const form = ref<PluginFormState>(createPluginFormState())

const inferenceKey = shallowRef('')
const keySearch = shallowRef('')
const keyOptions = ref<{ label: string, value: string }[]>([])
const selected = ref<{ plugin: PluginInstance, page: PluginManagementPage }>()
const managementOpen = computed({
  get: () => Boolean(selected.value),
  set(open) {
    if (!open)
      selected.value = undefined
  },
})
const removing = shallowRef<PluginInstance>()
const confirmDelete = shallowRef(false)
const removingArtifact = shallowRef('')
const confirmArtifactDelete = shallowRef(false)

const busy = computed(() => refreshing.value || uploading.value || saving.value || mutating.value)
const updateDescription = computed(() => {
  const item = updating.value
  const release = item && onlineUpdates.value[item.id]?.release
  return item && release
    ? `从已配置的发布源下载并更新至 ${release.version}，保留当前实例的配置、密钥、定时计划和检测历史。更新会中断正在运行的插件调用，请先暂停检测。升级成功后会清理未被其他实例使用的旧制品。`
    : ''
})

watch(() => [selected.value?.plugin.id, selected.value?.plugin.revision], () => {
  inferenceKey.value = ''
  keyError.value = ''
}, { flush: 'sync' })

async function refresh() {
  refreshing.value = true
  try {
    const [items, packages] = await Promise.all([getPlugins(), getPluginArtifacts()])
    instances.value = items
    artifacts.value = packages
    if (selected.value) {
      const item = items.find(
        p => p.id === selected.value?.plugin.id && p.revision === selected.value?.plugin.revision && p.status === 'ready',
      )
      if (!item)
        selected.value = undefined
    }
  }
  catch (error) {
    failure.value = errorMessage(error, '插件列表加载失败，请刷新重试')
  }
  finally {
    refreshing.value = false
  }
}

async function searchKeys() {
  if (keySearching.value)
    return
  keySearching.value = true
  keyError.value = ''
  try {
    const result = await getApiKeys({ limit: 100, search: keySearch.value.trim() || undefined })
    keyOptions.value = result.items
      .filter(key => key.enabled)
      .map(key => ({ label: `${key.name} · ${key.prefix}`, value: key.id }))
  }
  catch (error) {
    keyError.value = errorMessage(error, '查询推理 Key 失败')
  }
  finally {
    keySearching.value = false
  }
}

async function checkOnline(item: PluginInstance) {
  await checkingPlugins.run(item.id, async () => {
    failure.value = ''
    try {
      delete onlineUpdates.value[item.id]
      const release = await checkPluginUpdate(item)
      notices.value[item.id] = release ? `发现新版本 ${release.version}` : '当前平台没有可用的新版本'
      if (release)
        onlineUpdates.value[item.id] = { revision: item.revision, release }
    }
    catch (error) {
      failure.value = errorMessage(error, '检查插件更新失败')
    }
  })
}

async function applyOnline() {
  if (mutating.value)
    return
  mutating.value = true
  failure.value = ''
  try {
    const item = updating.value
    const pending = item && onlineUpdates.value[item.id]
    if (!item || !pending || pending.revision !== item.revision)
      throw new Error('请重新检查更新')
    await installPluginUpdate(item, pending.release)
    delete onlineUpdates.value[item.id]
    notices.value[item.id] = '更新成功，已保留原实例配置和历史'
    confirmUpdate.value = false
    await refresh()
  }
  catch (error) {
    failure.value = errorMessage(error, '在线更新插件失败')
  }
  finally {
    mutating.value = false
  }
}

function openEditor(item?: PluginInstance, pkg?: PluginArtifact) {
  editorLoaded.value = true
  editorError.value = ''
  failure.value = ''
  selected.value = undefined
  editing.value = item
  form.value = createPluginFormState(item, artifacts.value, pkg)
  editOpen.value = true
}

async function upload(event: Event) {
  const input = event.target as HTMLInputElement
  const file = input.files?.[0]
  if (!file || uploading.value)
    return
  uploading.value = true
  failure.value = ''
  try {
    if (file.size > 32 * 1024 * 1024)
      throw new Error('插件包不能超过 32 MiB')
    const result = await uploadPlugin(file)
    await refresh()
    const matches = instances.value.filter(item => item.artifact.pluginId === result.pluginId)
    if (matches.length === 1) {
      openEditor(matches[0])
      form.value.digest = result.sha256
    }
    else if (matches.length > 1) {
      editOpen.value = false
      failure.value = '新版已上传。存在多个同名插件实例，请在要升级的实例中点击“配置”，配置和历史会保留。'
    }
    else {
      openEditor(undefined, result)
    }
  }
  catch (error) {
    failure.value = errorMessage(error, '上传插件包失败')
  }
  finally {
    uploading.value = false
    input.value = ''
  }
}

async function save(enabled: boolean) {
  if (saving.value)
    return
  editorError.value = ''
  failure.value = ''
  const selectedArtifact = artifacts.value.find(item => item.sha256 === form.value.digest)
  const secretFields = [
    ...new Set([
      ...(selectedArtifact?.manifest.secretFields ?? []),
      ...(editing.value?.configuredSecrets ?? []),
    ]),
  ]
  let payload: ReturnType<typeof parsePluginPayload>
  try {
    payload = parsePluginPayload(form.value, secretFields, editing.value?.revision ?? 0, enabled)
  }
  catch (error) {
    editorError.value = errorMessage(error, '插件配置校验失败')
    return
  }

  saving.value = true
  try {
    await savePlugin(editing.value?.id, payload)
    editOpen.value = false
    form.value.secrets = {}
    await refresh()
  }
  catch (error) {
    editorError.value = errorMessage(error, '保存插件配置失败')
  }
  finally {
    saving.value = false
  }
}

async function disable(item: PluginInstance) {
  if (mutating.value)
    return
  mutating.value = true
  failure.value = ''
  try {
    await savePlugin(item.id, {
      observer: item.observer ?? null,
      upstreamAdapter: item.upstreamAdapter ?? null,
      retryPolicy: item.retryPolicy ?? null,
      accountScheduler: item.accountScheduler ?? null,
      modelRouter: item.modelRouter ?? null,
      middleware: item.middleware ?? null,
      frontendAuthentication: item.frontendAuthentication ?? null,
      revision: item.revision,
      name: item.name,
      artifactSha256: item.artifact.sha256,
      configuration: item.configuration,
      secrets: {},
      enabled: false,
      trustedProcess: false,
    })
    if (selected.value?.plugin.id === item.id)
      selected.value = undefined
    await refresh()
  }
  catch (error) {
    failure.value = errorMessage(error, '停用插件失败')
  }
  finally {
    mutating.value = false
  }
}

async function confirmRemoveArtifact() {
  if (mutating.value || !removingArtifact.value)
    return
  mutating.value = true
  failure.value = ''
  try {
    await removePluginArtifact(removingArtifact.value)
    confirmArtifactDelete.value = false
    await refresh()
  }
  catch (error) {
    failure.value = errorMessage(error, '删除制品失败')
  }
  finally {
    mutating.value = false
  }
}

async function confirmUninstall() {
  if (mutating.value || !removing.value)
    return
  mutating.value = true
  failure.value = ''
  try {
    const target = removing.value
    await uninstallPlugin(target)
    if (selected.value?.plugin.id === target.id)
      selected.value = undefined
    confirmDelete.value = false
    await refresh()
  }
  catch (error) {
    failure.value = errorMessage(error, '卸载插件失败')
  }
  finally {
    mutating.value = false
  }
}

onMounted(() => void refresh())
</script>

<template>
  <div class="space-y-5">
    <BasePageHeader title="插件" description="安装和管理网关插件。">
      <template #actions>
        <input ref="fileInput" type="file" aria-label="上传插件包" accept=".gz,.tgz" hidden :disabled="busy" @change="upload">
        <BaseButton variant="secondary" :loading="uploading" :disabled="busy" @click="fileInput?.click()">
          <template #icon>
            <Upload class="size-4" />
          </template>
          上传插件包
        </BaseButton>
        <BaseButton :disabled="busy || !artifacts.length" @click="openEditor()">
          <template #icon>
            <Plus class="size-4" />
          </template>
          创建实例
        </BaseButton>
      </template>
    </BasePageHeader>

    <div class="flex flex-wrap items-center justify-between gap-3">
      <BaseSegmented v-model="view" :options="tabs" label="插件列表" />
      <div class="flex max-w-full items-center gap-2">
        <BaseInput v-model="search" clearable aria-label="搜索插件" placeholder="搜索名称、标识或版本" />
        <BaseIconButton label="刷新插件列表" :loading="refreshing" :disabled="busy" @click="refresh">
          <RefreshCw class="size-4" />
        </BaseIconButton>
      </div>
    </div>

    <p v-if="failure && !editOpen && !managementOpen" role="alert" class="m-0 rounded-cp bg-cp-error-container px-4 py-3 text-cp-sm text-cp-error-on-container">
      {{ failure }}
    </p>

    <div v-if="view === 'installed'" class="grid gap-3">
      <BaseEmpty v-if="!filteredInstances.length && !refreshing" :icon="PackageOpen" :title="query ? '没有匹配的插件' : '还没有安装插件'" />
      <BaseCard v-for="item in filteredInstances" :key="item.id" class="space-y-3 p-4">
        <div class="flex flex-wrap items-center justify-between gap-3">
          <div class="min-w-0">
            <div class="flex flex-wrap items-center gap-2">
              <h2 class="m-0 font-semibold break-all text-cp-text">
                {{ item.name }}
              </h2>
              <span
                class="rounded-full bg-cp-fill-alter px-2 py-0.5 text-cp-xs font-medium"
                :class="item.status === 'ready' ? 'text-cp-success' : item.status === 'failed' ? 'text-cp-error' : 'text-cp-text-secondary'"
              >
                {{ statusLabels[item.status] ?? item.status }}
              </span>
            </div>
            <p class="mt-1 mb-0 text-cp-xs break-all text-cp-text-secondary">
              {{ item.artifact.pluginId }} · {{ item.artifact.version }}
            </p>
          </div>
          <div class="flex flex-wrap items-center gap-2">
            <BaseButton
              v-for="page in item.status === 'ready' ? item.pages : []"
              :key="page.id"
              size="sm"
              variant="secondary"
              @click="failure = ''; selected = { plugin: item, page }"
            >
              {{ page.title }}
            </BaseButton>
            <BaseButton size="sm" variant="secondary" :disabled="busy" @click="openEditor(item)">
              配置
            </BaseButton>
            <RowActionMenu v-slot="{ select }" :label="`${item.name}的更多操作`" :loading="busy || checkingPlugins.ids.value.has(item.id)">
              <BaseMenuItem v-if="ONLINE_UPDATABLE_PLUGINS.has(item.artifact.pluginId)" role="menuitem" tabindex="-1" @click="select(() => checkOnline(item))">
                检查更新
              </BaseMenuItem>
              <BaseMenuItem v-if="onlineUpdates[item.id]?.revision === item.revision" role="menuitem" tabindex="-1" @click="select(() => { updating = item; confirmUpdate = true })">
                在线更新
              </BaseMenuItem>
              <BaseMenuItem v-if="item.enabled" role="menuitem" tabindex="-1" @click="select(() => disable(item))">
                停用
              </BaseMenuItem>
              <BaseMenuItem role="menuitem" tabindex="-1" tone="destructive" @click="select(() => { removing = item; confirmDelete = true })">
                卸载
              </BaseMenuItem>
            </RowActionMenu>
          </div>
        </div>
        <p v-if="notices[item.id]" class="m-0 text-sm text-cp-text-secondary">
          {{ notices[item.id] }}
          <a v-if="onlineUpdates[item.id]" :href="onlineUpdates[item.id]!.release.releaseUrl" target="_blank" rel="noopener noreferrer" class="text-cp-link underline hover:text-cp-link-hover">查看发布说明</a>
        </p>
        <p v-if="item.runtimeLoad" class="m-0 text-xs text-cp-text-secondary" title="实时采样，各项可能重叠；刷新列表更新">
          在途 {{ item.runtimeLoad.activeCalls }} · 执行 {{ item.runtimeLoad.executingCalls }} · 排队 {{ item.runtimeLoad.queuedCalls }} · 等待恢复 {{ item.runtimeLoad.resumingCalls }} · 回调 {{ item.runtimeLoad.pendingCallbacks }} · 流 {{ item.runtimeLoad.activeStreams }}
        </p>
      </BaseCard>
    </div>

    <div v-else class="grid gap-3">
      <BaseEmpty v-if="!filteredArtifacts.length && !refreshing" :icon="PackageOpen" :title="query ? '没有匹配的制品' : '还没有上传制品'" />
      <BaseCard v-for="pkg in filteredArtifacts" :key="pkg.sha256" class="flex flex-wrap items-center justify-between gap-3 p-4">
        <div class="min-w-0">
          <h2 class="m-0 font-semibold break-all text-cp-text">
            {{ pkg.displayName }}
          </h2>
          <p class="mt-1 mb-0 text-cp-xs break-all text-cp-text-secondary">
            {{ pkg.pluginId }} · {{ pkg.version }} · {{ pkg.sha256.slice(0, 8) }}
          </p>
        </div>
        <div class="flex flex-wrap gap-2">
          <BaseButton variant="secondary" size="sm" :disabled="busy" @click="openEditor(undefined, pkg)">
            创建实例
          </BaseButton>
          <BaseButton variant="secondary" size="sm" :disabled="busy || instances.some(item => item.artifact.sha256 === pkg.sha256)" @click="removingArtifact = pkg.sha256; confirmArtifactDelete = true">
            删除制品
          </BaseButton>
        </div>
      </BaseCard>
    </div>

    <BaseModal v-model="managementOpen" :title="selected?.page.title ?? '插件管理'" size="xl">
      <div v-if="selected" class="space-y-3">
        <p class="m-0 text-sm text-cp-text-secondary">
          推理使用本人 Key 的权限与额度，按现有倍率计费。更换 Key 或关闭页面会取消页面直接发起的推理；已启动的后台检测不受影响，请在插件内暂停或结束。
        </p>
        <div class="flex flex-wrap items-center gap-2">
          <BaseInput v-model="keySearch" aria-label="搜索本人推理 Key" placeholder="搜索本人 Key" class="max-w-xs" @keydown.enter="searchKeys" />
          <BaseButton variant="secondary" :loading="keySearching" :disabled="keySearching" @click="searchKeys">
            查询 Key
          </BaseButton>
          <BaseSelect
            v-model="inferenceKey"
            searchable
            aria-label="选择推理 Key"
            :options="keyOptions"
            placeholder="选择本人 Key 后允许推理"
            class="min-w-56 max-w-sm"
          />
        </div>
        <p v-if="keyError" role="alert" class="m-0 text-cp-xs text-cp-error">
          {{ keyError }}
        </p>
        <PluginFrame :key="[selected.plugin.id, selected.plugin.revision, selected.page.id].join('/')" :plugin="selected.plugin" :page="selected.page" :client-key-id="inferenceKey" />
      </div>
    </BaseModal>

    <PluginEditorModal
      v-if="editorLoaded"
      v-model="editOpen"
      v-model:form="form"
      :editing="editing"
      :artifacts="artifacts"
      :saving="saving"
      :error="editorError"
      @save="save"
    />

    <BaseConfirmModal v-model="confirmUpdate" title="在线更新插件" :description="updateDescription" :loading="mutating" @confirm="applyOnline" />
    <BaseConfirmModal v-model="confirmArtifactDelete" title="删除制品" description="仅能删除没有实例使用的制品。删除后需要重新上传才能安装该版本。" destructive :loading="mutating" @confirm="confirmRemoveArtifact" />
    <BaseConfirmModal v-model="confirmDelete" title="卸载插件" description="将停止该插件并删除实例配置和私有状态，进行中的调用会被取消。已上传制品保留，重新安装不会恢复旧状态。" destructive :loading="mutating" @confirm="confirmUninstall" />
  </div>
</template>
