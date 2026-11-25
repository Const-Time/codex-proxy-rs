<script setup lang="ts">
import type { PluginFormState } from './bindings'
import type { PluginArtifact, PluginInstance } from '@/api/modules/plugins'
import { computed, ref, watch } from 'vue'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseCheckbox from '@/components/base/BaseCheckbox.vue'
import BaseEmpty from '@/components/base/BaseEmpty.vue'
import BaseFormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import BaseSegmented from '@/components/base/BaseSegmented.vue'
import BaseSelect from '@/components/base/BaseSelect.vue'
import BaseTextarea from '@/components/base/BaseTextarea.vue'
import { SCOPE_OPTIONS } from './bindings'

const props = defineProps<{
  editing?: PluginInstance
  artifacts: PluginArtifact[]
  saving: boolean
  error: string
}>()

const emit = defineEmits<{
  save: [enabled: boolean]
}>()

const open = defineModel<boolean>({ required: true })
const form = defineModel<PluginFormState>('form', { required: true })

const editSection = ref('configuration')
const editSections = [
  { label: '基本配置', value: 'configuration' },
  { label: '密钥', value: 'secrets' },
  { label: '能力绑定', value: 'bindings' },
]

const artifact = computed(() => props.artifacts.find(item => item.sha256 === form.value.digest))
const secretFields = computed(() => [
  ...new Set([
    ...(artifact.value?.manifest.secretFields ?? []),
    ...(props.editing?.configuredSecrets ?? []),
  ]),
])
const artifactOptions = computed(() =>
  props.artifacts
    .filter(item => !props.editing || item.pluginId === props.editing.artifact.pluginId)
    .map(item => ({
      label: `${item.displayName} · ${item.version} · ${item.sha256.slice(0, 8)}`,
      value: item.sha256,
    })),
)

watch(() => form.value.digest, () => {
  form.value.trust = false
})

watch(open, (isOpen) => {
  if (isOpen) {
    editSection.value = 'configuration'
    return
  }
  form.value.secrets = {}
  form.value.removeSecrets = {}
  form.value.trust = false
})

function closeModal() {
  open.value = false
}
</script>

<template>
  <BaseModal
    v-model="open"
    :title="editing ? '配置插件' : '安装插件'"
    size="lg"
    :dismissible="!saving"
  >
    <div class="space-y-4">
      <BaseSegmented v-model="editSection" :options="editSections" label="插件配置分区" />

      <p v-if="error" role="alert" class="m-0 rounded-cp bg-cp-error-container px-3 py-2 text-cp-sm text-cp-error-on-container">
        {{ error }}
      </p>

      <div v-show="editSection === 'configuration'" class="space-y-4">
        <BaseFormItem label="实例名称">
          <BaseInput v-model="form.name" placeholder="实例名称" :disabled="saving" />
        </BaseFormItem>

        <BaseFormItem label="插件制品版本">
          <BaseSelect
            v-model="form.digest"
            aria-label="插件制品版本"
            :options="artifactOptions"
            :disabled="saving"
            placeholder="选择已上传制品"
          />
        </BaseFormItem>

        <p v-if="editing && form.digest !== editing.artifact.sha256" class="m-0 text-cp-sm text-cp-text-secondary">
          升级成功后会清理未被其他实例使用的旧制品。
        </p>

        <p
          v-if="artifact?.versionWarning"
          class="m-0 rounded-cp bg-cp-warning-container px-3 py-2 text-cp-sm text-cp-warning-on-container"
        >
          {{ artifact.versionWarning }}
        </p>
        <p
          v-if="artifact?.unsupportedCapabilities.length"
          class="m-0 rounded-cp bg-cp-warning-container px-3 py-2 text-cp-sm text-cp-warning-on-container"
        >
          尚未支持：{{ artifact.unsupportedCapabilities.join('、') }}。可以保存配置，暂不能启用。
        </p>

        <BaseFormItem label="配置（JSON）">
          <BaseTextarea v-model="form.configuration" :rows="8" :disabled="saving" />
        </BaseFormItem>

        <details class="rounded-cp border border-cp-border-secondary bg-cp-fill-quaternary/50 px-3 py-2">
          <summary class="cursor-pointer text-cp-sm font-semibold text-cp-text-secondary">
            查看配置要求
          </summary>
          <pre class="mt-2 max-h-60 overflow-auto font-mono text-cp-xs text-cp-text">{{ JSON.stringify(artifact?.manifest.configurationSchema, null, 2) }}</pre>
        </details>
      </div>

      <div v-show="editSection === 'secrets'" class="space-y-4">
        <BaseEmpty v-if="!secretFields.length" title="这个版本没有密钥配置" size="sm" />
        <div v-for="field in secretFields" :key="field" class="space-y-2">
          <BaseFormItem :label="field">
            <div class="flex items-center gap-2">
              <BaseInput
                v-model="form.secrets[field]"
                type="password"
                autocomplete="new-password"
                :placeholder="form.removeSecrets[field] ? '保存时清除' : editing?.configuredSecrets.includes(field) ? '已配置，留空保留' : '填写密钥'"
                :disabled="saving || form.removeSecrets[field] || !artifact?.manifest.secretFields.includes(field)"
              />
              <BaseButton
                v-if="editing?.configuredSecrets.includes(field)"
                size="sm"
                variant="secondary"
                :disabled="saving"
                @click="form.removeSecrets[field] = !form.removeSecrets[field]"
              >
                {{ form.removeSecrets[field] ? '撤销清除' : '清除' }}
              </BaseButton>
            </div>
          </BaseFormItem>
        </div>
      </div>

      <div v-show="editSection === 'bindings'" class="space-y-3">
        <details class="space-y-3 rounded-cp border border-cp-border-secondary p-3">
          <summary class="cursor-pointer text-cp-sm font-semibold text-cp-text">
            模型路由
          </summary>
          <BaseCheckbox v-model="form.router.enabled" show-label label="使用此插件选择模型和 Provider" :disabled="saving" />
          <template v-if="form.router.enabled">
            <BaseFormItem label="执行顺序（较小的先执行）">
              <BaseInput v-model="form.router.order" type="number" :disabled="saving" />
            </BaseFormItem>
            <BaseFormItem label="作用范围">
              <BaseSelect v-model="form.router.scope" aria-label="模型路由作用范围" :options="SCOPE_OPTIONS" :disabled="saving" />
            </BaseFormItem>
            <BaseFormItem v-if="form.router.scope !== 'all'" :label="`${form.router.scope === 'keys' ? 'Key ID' : '分组 ID'}（每行一个）`">
              <BaseTextarea v-model="form.router.ids" :rows="3" :disabled="saving" />
            </BaseFormItem>
            <BaseFormItem label="故障行为">
              <BaseSelect
                v-model="form.router.failurePolicy"
                aria-label="路由插件故障行为"
                :options="[{ label: '拒绝请求', value: 'reject' }, { label: '委托后续路由策略', value: 'delegate' }]"
                :disabled="saving"
              />
            </BaseFormItem>
            <p class="m-0 text-cp-xs text-cp-text-secondary">
              路由选择仍受 Key 权限、模型能力和续接约束。首个明确决定生效；明确拒绝不会继续委托。插件变更或停用会撤销旧请求。
            </p>
          </template>
        </details>

        <details class="space-y-3 rounded-cp border border-cp-border-secondary p-3">
          <summary class="cursor-pointer text-cp-sm font-semibold text-cp-text">
            账号调度
          </summary>
          <BaseCheckbox v-model="form.scheduler.enabled" show-label label="使用此插件选择上游账号" :disabled="saving" />
          <template v-if="form.scheduler.enabled">
            <BaseFormItem label="执行顺序（较小的先执行）">
              <BaseInput v-model="form.scheduler.order" type="number" :disabled="saving" />
            </BaseFormItem>
            <BaseFormItem label="作用范围">
              <BaseSelect v-model="form.scheduler.scope" aria-label="账号调度作用范围" :options="SCOPE_OPTIONS" :disabled="saving" />
            </BaseFormItem>
            <BaseFormItem v-if="form.scheduler.scope !== 'all'" :label="`${form.scheduler.scope === 'keys' ? 'Key ID' : '分组 ID'}（每行一个）`">
              <BaseTextarea v-model="form.scheduler.ids" :rows="3" :disabled="saving" />
            </BaseFormItem>
            <BaseFormItem label="故障行为">
              <BaseSelect
                v-model="form.scheduler.failurePolicy"
                aria-label="调度插件故障行为"
                :options="[{ label: '拒绝请求', value: 'reject' }, { label: '使用宿主账号调度', value: 'delegate' }]"
                :disabled="saving"
              />
            </BaseFormItem>
            <p class="m-0 text-cp-xs text-cp-text-secondary">
              按顺序只调用首个匹配的调度插件，委托时使用宿主选号。账号选择仍受 Key 权限、续接绑定和可用容量约束。明确拒绝会终止请求；插件变更或停用会撤销旧请求。
            </p>
          </template>
        </details>

        <details class="space-y-3 rounded-cp border border-cp-border-secondary p-3">
          <summary class="cursor-pointer text-cp-sm font-semibold text-cp-text">
            上游适配器
          </summary>
          <BaseCheckbox v-model="form.upstream.enabled" show-label label="使用此插件适配上游请求" :disabled="saving" />
          <template v-if="form.upstream.enabled">
            <BaseFormItem label="作用范围">
              <BaseSelect v-model="form.upstream.scope" aria-label="上游适配器作用范围" :options="SCOPE_OPTIONS" :disabled="saving" />
            </BaseFormItem>
            <BaseFormItem v-if="form.upstream.scope !== 'all'" :label="`${form.upstream.scope === 'keys' ? 'Key ID' : '分组 ID'}（每行一个）`">
              <BaseTextarea v-model="form.upstream.ids" :rows="3" :disabled="saving" />
            </BaseFormItem>
            <p class="m-0 text-cp-xs text-cp-text-secondary">
              使用插件声明的 Provider 和公开模型范围，重叠绑定会被拒绝。账号权限、选择和计费仍由网关管理；匹配的适配器不可用时请求会失败。当前可保存绑定，启用支持以制品提示为准。
            </p>
          </template>
        </details>

        <details class="space-y-3 rounded-cp border border-cp-border-secondary p-3">
          <summary class="cursor-pointer text-cp-sm font-semibold text-cp-text">
            重试策略
          </summary>
          <BaseCheckbox v-model="form.retry.enabled" show-label label="由此插件决定是否继续恢复请求" :disabled="saving" />
          <template v-if="form.retry.enabled">
            <BaseFormItem label="执行顺序（较小的先执行）">
              <BaseInput v-model="form.retry.order" type="number" :disabled="saving" />
            </BaseFormItem>
            <BaseFormItem label="作用范围">
              <BaseSelect v-model="form.retry.scope" aria-label="重试策略作用范围" :options="SCOPE_OPTIONS" :disabled="saving" />
            </BaseFormItem>
            <BaseFormItem v-if="form.retry.scope !== 'all'" :label="`${form.retry.scope === 'keys' ? 'Key ID' : '分组 ID'}（每行一个）`">
              <BaseTextarea v-model="form.retry.ids" :rows="3" :disabled="saving" />
            </BaseFormItem>
            <p class="m-0 text-cp-xs text-cp-text-secondary">
              依次咨询匹配的策略，插件委托或故障时交给后续策略处理。明确停止会保留原始失败。整条链最多等待两秒；插件不能增加重试次数、缩短退避或绕过安全恢复条件。
            </p>
          </template>
        </details>

        <details class="space-y-3 rounded-cp border border-cp-border-secondary p-3">
          <summary class="cursor-pointer text-cp-sm font-semibold text-cp-text">
            观察订阅
          </summary>
          <p class="m-0 text-cp-xs text-cp-text-secondary">
            观察插件可读取请求终态、用量和费用，以及实际上游 WebSocket 的原始响应内容。请选择需要共享的事件和作用范围。
          </p>
          <BaseCheckbox v-model="form.observer.enabled" show-label label="向此插件投递观察事件" :disabled="saving" />
          <template v-if="form.observer.enabled">
            <BaseFormItem label="执行顺序（较小的先执行）">
              <BaseInput v-model="form.observer.order" type="number" :disabled="saving" />
            </BaseFormItem>
            <BaseCheckbox v-model="form.observer.completed" show-label label="请求完成事件" :disabled="saving" />
            <template v-if="form.observer.completed">
              <BaseFormItem label="请求完成观察范围">
                <BaseSelect v-model="form.observer.completedScope" aria-label="请求完成观察范围" :options="SCOPE_OPTIONS" :disabled="saving" />
              </BaseFormItem>
              <BaseFormItem v-if="form.observer.completedScope !== 'all'" :label="`${form.observer.completedScope === 'keys' ? 'Key ID' : '分组 ID'}（每行一个）`">
                <BaseTextarea v-model="form.observer.completedIds" :rows="3" :disabled="saving" />
              </BaseFormItem>
            </template>
            <BaseCheckbox v-model="form.observer.webSocket" show-label label="实际上游 WebSocket 响应帧" :disabled="saving" />
            <template v-if="form.observer.webSocket">
              <BaseFormItem label="WebSocket 观察范围">
                <BaseSelect v-model="form.observer.webSocketScope" aria-label="WebSocket 观察范围" :options="SCOPE_OPTIONS" :disabled="saving" />
              </BaseFormItem>
              <BaseFormItem v-if="form.observer.webSocketScope !== 'all'" :label="`${form.observer.webSocketScope === 'keys' ? 'Key ID' : '分组 ID'}（每行一个）`">
                <BaseTextarea v-model="form.observer.webSocketIds" :rows="3" :disabled="saving" />
              </BaseFormItem>
            </template>
            <p class="m-0 text-cp-xs text-cp-text-secondary">
              观察失败或停用不会改变请求结果。投递有容量和时间限制，繁忙时可能跳过事件；插件可用事件 ID 自行去重。
            </p>
          </template>
        </details>

        <details class="space-y-3 rounded-cp border border-cp-border-secondary p-3">
          <summary class="cursor-pointer text-cp-sm font-semibold text-cp-text">
            中间件绑定
          </summary>
          <p class="m-0 text-cp-xs text-cp-text-secondary">
            仅用于声明中间件能力的插件。当前可保存绑定，待请求入口接入后才能启用；是否支持以制品上方的提示为准。
          </p>
          <BaseCheckbox v-model="form.middleware.enabled" show-label label="配置中间件的顺序与作用范围" :disabled="saving" />
          <template v-if="form.middleware.enabled">
            <BaseFormItem label="执行顺序（较小的先执行）">
              <BaseInput v-model="form.middleware.order" type="number" :disabled="saving" />
            </BaseFormItem>
            <div class="flex flex-wrap gap-4">
              <BaseCheckbox v-model="form.middleware.http" show-label label="HTTP 入口" :disabled="saving" />
              <BaseCheckbox v-model="form.middleware.webSocket" show-label label="WebSocket 会话" :disabled="saving" />
              <BaseCheckbox v-model="form.middleware.service" show-label label="宿主服务调用" :disabled="saving" />
              <BaseCheckbox v-model="form.middleware.request" show-label label="每个逻辑请求" :disabled="saving" />
              <BaseCheckbox v-model="form.middleware.attempt" show-label label="每次上游尝试" :disabled="saving" />
            </div>
            <BaseFormItem label="作用范围">
              <BaseSelect v-model="form.middleware.scope" aria-label="中间件作用范围" :options="SCOPE_OPTIONS" :disabled="saving" />
            </BaseFormItem>
            <p v-if="form.middleware.http || form.middleware.webSocket || form.middleware.service" class="m-0 text-cp-xs text-cp-text-secondary">
              HTTP、WebSocket 会话与宿主服务调用需选择全部请求。指定 Key 或分组仅适用于逻辑请求与上游尝试。
            </p>
            <BaseFormItem v-if="form.middleware.scope !== 'all'" :label="`${form.middleware.scope === 'keys' ? 'Key ID' : '分组 ID'}（每行一个）`">
              <BaseTextarea v-model="form.middleware.ids" :rows="3" :disabled="saving" />
            </BaseFormItem>
            <p class="m-0 text-cp-xs text-cp-text-secondary">
              范围只决定哪些请求调用插件，不增加 Key 权限。同序号按实例 ID 排序；修改或停用会撤销旧调用。升级准备失败仍使用旧版本。
            </p>
          </template>
        </details>

        <details class="space-y-3 rounded-cp border border-cp-border-secondary p-3">
          <summary class="cursor-pointer text-cp-sm font-semibold text-cp-text">
            外部认证
          </summary>
          <p class="m-0 text-cp-xs text-cp-text-secondary">
            仅用于声明入口认证能力的插件。同一时间只能启用一个认证器；此配置不会发送给插件。变更或停用会撤销旧授权。
          </p>
          <BaseCheckbox v-model="form.authentication.enabled" show-label label="使用此插件认证客户端请求" :disabled="saving" />
          <template v-if="form.authentication.enabled">
            <BaseFormItem label="不匹配时的认证行为">
              <BaseSelect
                v-model="form.authentication.failurePolicy"
                aria-label="不匹配时的认证行为"
                :options="[{ label: '拒绝所有未匹配请求', value: 'reject' }, { label: '仅对明确未匹配的请求尝试普通 Key', value: 'delegate' }]"
                :disabled="saving"
              />
            </BaseFormItem>
            <BaseFormItem label="外部身份到本地 Key 的映射（JSON）">
              <BaseTextarea
                v-model="form.authentication.identities"
                :rows="5"
                :disabled="saving"
                placeholder="{&quot;external-user&quot;: &quot;key_id&quot;}"
              />
            </BaseFormItem>
            <p class="m-0 text-cp-xs text-cp-text-secondary">
              填写 Key ID，不要填写密钥。映射身份将使用该 Key 所属用户的分组权限、倍率与共享额度。认证器超时、拒绝或返回未知身份时不会回退。
            </p>
          </template>
        </details>
      </div>
    </div>

    <template #footer>
      <div class="w-full space-y-3">
        <p class="m-0 text-sm text-cp-text-secondary">
          插件进程以网关的系统身份运行，可访问主机文件、凭据和网络。仅安装可信来源的插件。
        </p>
        <BaseCheckbox v-model="form.trust" show-label label="我信任此插件及所选版本，允许它在网关上运行" :disabled="saving" />
        <div class="flex flex-wrap justify-end gap-2">
          <BaseButton variant="secondary" :disabled="saving" @click="closeModal">
            取消
          </BaseButton>
          <BaseButton variant="secondary" :loading="saving" :disabled="saving || !artifact" @click="emit('save', false)">
            保存并停用
          </BaseButton>
          <BaseButton
            variant="primary"
            :loading="saving"
            :disabled="saving || !form.trust || !artifact || !!artifact.unsupportedCapabilities.length"
            @click="emit('save', true)"
          >
            保存并启用
          </BaseButton>
        </div>
      </div>
    </template>
  </BaseModal>
</template>
