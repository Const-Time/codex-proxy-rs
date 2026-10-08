<script setup lang="ts">
import type { PluginArtifact, PluginInstance, PluginManagementPage } from '@/api/modules/plugins'
import { computed, onMounted, ref, watch } from 'vue'
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
const digest = ref('')
const configuration = ref('{}')
const secrets = ref<Record<string, string>>({})
const removeSecrets = ref<Record<string, boolean>>({})
const trust = ref(false)
const selected = ref<{
  plugin: PluginInstance
  page: PluginManagementPage
}>()
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
    await savePlugin(editing.value?.id, { revision: editing.value?.revision ?? 0, name: name.value, artifactSha256: digest.value, configuration: JSON.parse(configuration.value), secrets: values, enabled, trustedProcess: trust.value })
    editOpen.value = false
    secrets.value = {}
  })
}
async function disable(item: PluginInstance) {
  selected.value = undefined
  await perform(async () => {
    await savePlugin(item.id, { revision: item.revision, name: item.name, artifactSha256: item.artifact.sha256, configuration: item.configuration, secrets: {}, enabled: false, trustedProcess: false })
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
    <BaseCard v-if="selected" class="p-4">
      <PluginFrame :key="`${selected.plugin.id}/${selected.plugin.revision}/${selected.page.id}`" :plugin="selected.plugin" :page="selected.page" />
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
