<script setup lang="ts">
import type { OutboundProxyRecord, ProxyLocationPolicy } from '@/api'
import { Eye, EyeOff, Save, Wifi } from '@lucide/vue'
import { computed, reactive, shallowRef, watch } from 'vue'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseFormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseForm from '@/components/base/BaseForm/index.vue'
import BaseIconButton from '@/components/base/BaseIconButton.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import BaseSelect from '@/components/base/BaseSelect.vue'
import { proxyConnectionFields, proxyConnectionUpdate, proxyProtocols } from './connection'

const props = defineProps<{
  proxy: OutboundProxyRecord | null
  saving: boolean
}>()
const emit = defineEmits<{
  save: [testAfter: boolean]
}>()
const open = defineModel<boolean>({ required: true })
const name = defineModel<string>('name', { required: true })
const proxyUrl = defineModel<string>('proxyUrl', { required: true })
const locationPolicy = defineModel<ProxyLocationPolicy>('locationPolicy', { required: true })
const locationModes = [
  { label: '自动识别出口地区', value: 'auto' },
  { label: '手动设置', value: 'manual' },
  { label: '保留客户端地区', value: 'passthrough' },
]
const locationMode = computed({
  get: () => locationPolicy.value.mode,
  set: (mode: ProxyLocationPolicy['mode']) => {
    locationPolicy.value = mode === 'manual'
      ? { mode, location: { ...(props.proxy?.lastTest?.location ?? { country: '', region: '', city: '', timezone: '' }) } }
      : { mode }
  },
})
const showSecret = shallowRef(false)
const changeConnection = shallowRef(false)
const connection = reactive(proxyConnectionFields())
const connectionError = shallowRef('')
const connectionDisabled = computed(() => props.saving || (!!props.proxy && !changeConnection.value))
const authenticationOptions = [
  { label: '无需认证', value: 'none' },
  { label: '账号认证（密码可选）', value: 'password' },
]
const title = computed(() => props.proxy ? '编辑代理' : '新增代理')

watch(open, (visible) => {
  showSecret.value = false
  connectionError.value = ''
  changeConnection.value = false
  proxyUrl.value = ''
  Object.assign(connection, visible ? proxyConnectionFields(props.proxy?.endpoint, props.proxy?.hasAuthentication) : proxyConnectionFields())
}, { immediate: true })

watch(connection, () => {
  connectionError.value = ''
  proxyUrl.value = ''
})

function save(testAfter: boolean) {
  try {
    proxyUrl.value = proxyConnectionUpdate(connection, !!props.proxy, changeConnection.value)
    connectionError.value = ''
    emit('save', testAfter)
  }
  catch (error) {
    connectionError.value = error instanceof Error ? error.message : '请检查代理连接信息'
  }
}

function toggleConnection() {
  changeConnection.value = !changeConnection.value
  Object.assign(connection, proxyConnectionFields(props.proxy?.endpoint, props.proxy?.hasAuthentication))
  showSecret.value = false
  connectionError.value = ''
  proxyUrl.value = ''
}
</script>

<template>
  <BaseModal v-model="open" :title="title" size="md" :dismissible="!saving">
    <BaseForm class="grid gap-5">
      <BaseFormItem label="代理名称" required>
        <BaseInput v-model="name" maxlength="100" :disabled="saving" aria-label="代理名称" placeholder="例如：本机代理" />
      </BaseFormItem>
      <div v-if="proxy" class="flex flex-wrap items-center justify-between gap-2">
        <span class="text-cp-xs text-cp-text-tertiary">{{ changeConnection ? '更换连接时请重新填写认证信息，原密码不回显。' : '已保存的连接与认证信息将原样保留。' }}</span>
        <BaseButton variant="secondary" :disabled="saving" @click="toggleConnection">
          {{ changeConnection ? '保留原连接' : '修改连接信息' }}
        </BaseButton>
      </div>
      <div class="grid grid-cols-1 gap-4 sm:grid-cols-2">
        <BaseFormItem label="代理类型" required>
          <BaseSelect v-model="connection.protocol" :options="proxyProtocols" :disabled="connectionDisabled" aria-label="代理类型" />
        </BaseFormItem>
        <BaseFormItem label="端口" required>
          <BaseInput v-model="connection.port" inputmode="numeric" maxlength="5" placeholder="例如：1080" :disabled="connectionDisabled" aria-label="代理端口" />
        </BaseFormItem>
      </div>
      <BaseFormItem label="IP / 主机名" required description="支持 IPv4、IPv6 和域名，无需填写协议前缀。">
        <BaseInput v-model="connection.host" autocomplete="off" spellcheck="false" placeholder="例如：192.168.1.10 或 proxy.example.com" :disabled="connectionDisabled" aria-label="代理 IP 或主机名" />
      </BaseFormItem>
      <BaseFormItem label="认证方式">
        <BaseSelect v-model="connection.authentication" :options="authenticationOptions" :disabled="connectionDisabled" aria-label="代理认证方式" />
      </BaseFormItem>
      <div v-if="connection.authentication === 'password'" class="grid grid-cols-1 gap-4 sm:grid-cols-2">
        <BaseFormItem label="账号" :required="!connectionDisabled">
          <BaseInput v-model="connection.username" autocomplete="off" :placeholder="connectionDisabled ? '已保存，不回显' : '代理账号'" :disabled="connectionDisabled" aria-label="代理账号" />
        </BaseFormItem>
        <BaseFormItem label="密码（可选）">
          <BaseInput v-model="connection.password" :type="showSecret ? 'text' : 'password'" autocomplete="new-password" :placeholder="connectionDisabled ? '已保存，不回显' : '无密码可留空'" :disabled="connectionDisabled" aria-label="代理密码">
            <template #suffix>
              <BaseIconButton :label="showSecret ? '隐藏代理密码' : '显示代理密码'" :disabled="connectionDisabled" @click="showSecret = !showSecret">
                <EyeOff v-if="showSecret" class="size-4" />
                <Eye v-else class="size-4" />
              </BaseIconButton>
            </template>
          </BaseInput>
        </BaseFormItem>
      </div>
      <p v-if="connectionError" role="alert" class="m-0 text-cp-sm text-cp-error">
        {{ connectionError }}
      </p>
      <p v-if="proxy?.accountCount && changeConnection" class="m-0 text-cp-sm text-cp-warning-text">
        将更新 {{ proxy.accountCount }} 个关联账号的出口。
      </p>
      <BaseFormItem label="请求地区" description="绑定此代理的账号共用地区设置。自动模式在测试代理时查询出口 IP 的位置并缓存；识别失败时保留客户端信息。">
        <BaseSelect v-model="locationMode" :options="locationModes" :disabled="saving" aria-label="请求地区模式" />
      </BaseFormItem>
      <div v-if="locationPolicy.mode === 'manual'" class="grid grid-cols-1 gap-3 sm:grid-cols-2">
        <BaseFormItem label="国家代码" required>
          <BaseInput v-model="locationPolicy.location.country" maxlength="2" placeholder="US" :disabled="saving" aria-label="国家代码" />
        </BaseFormItem>
        <BaseFormItem label="地区 / 州" required>
          <BaseInput v-model="locationPolicy.location.region" maxlength="128" placeholder="California" :disabled="saving" aria-label="地区或州" />
        </BaseFormItem>
        <BaseFormItem label="城市" required>
          <BaseInput v-model="locationPolicy.location.city" maxlength="128" placeholder="Los Angeles" :disabled="saving" aria-label="城市" />
        </BaseFormItem>
        <BaseFormItem label="时区" required>
          <BaseInput v-model="locationPolicy.location.timezone" maxlength="128" placeholder="America/Los_Angeles" :disabled="saving" aria-label="时区" />
        </BaseFormItem>
      </div>
      <p v-if="proxy?.lastTest?.location" class="m-0 text-cp-xs text-cp-text-tertiary">
        最近识别：{{ proxy.lastTest.location.country }} / {{ proxy.lastTest.location.region }} / {{ proxy.lastTest.location.city }} · {{ proxy.lastTest.location.timezone }}
      </p>
    </BaseForm>
    <template #footer>
      <BaseButton variant="secondary" :disabled="saving" @click="open = false">
        取消
      </BaseButton>
      <BaseButton variant="secondary" :loading="saving" @click="save(false)">
        <template #icon>
          <Save class="size-4" />
        </template>
        保存代理
      </BaseButton>
      <BaseButton variant="primary" :loading="saving" @click="save(true)">
        <template #icon>
          <Wifi class="size-4" />
        </template>
        保存并测试
      </BaseButton>
    </template>
  </BaseModal>
</template>
