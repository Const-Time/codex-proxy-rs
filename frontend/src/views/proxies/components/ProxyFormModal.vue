<script setup lang="ts">
import type { OutboundProxyRecord, ProxyLocationPolicy } from '@/api'
import { Eye, EyeOff, Save, Wifi } from '@lucide/vue'
import { computed, shallowRef, watch } from 'vue'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseFormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseForm from '@/components/base/BaseForm/index.vue'
import BaseIconButton from '@/components/base/BaseIconButton.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import BaseSelect from '@/components/base/BaseSelect.vue'

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
const title = computed(() => props.proxy ? '编辑代理' : '新增代理')
const connectionDescription = computed(() => props.proxy
  ? '留空保留当前连接和认证信息；填写新地址时，请包含所需的用户名和密码。'
  : '支持 HTTP、HTTPS、SOCKS5 和 SOCKS5H，可在地址中包含用户名和密码。')

watch(open, () => {
  showSecret.value = false
})
</script>

<template>
  <BaseModal v-model="open" :title="title" size="md" :dismissible="!saving">
    <BaseForm class="grid gap-5">
      <BaseFormItem label="代理名称" required>
        <BaseInput v-model="name" maxlength="100" :disabled="saving" aria-label="代理名称" placeholder="例如：本机代理" />
      </BaseFormItem>
      <BaseFormItem label="代理 URL" :required="!proxy" :description="connectionDescription">
        <BaseInput
          v-model="proxyUrl"
          :type="showSecret ? 'text' : 'password'"
          autocomplete="new-password"
          :disabled="saving"
          aria-label="代理 URL"
          :placeholder="proxy?.endpoint ?? 'socks5h://user:password@host:1080'"
        >
          <template #suffix>
            <BaseIconButton :label="showSecret ? '隐藏代理地址' : '显示代理地址'" :disabled="saving" @click="showSecret = !showSecret">
              <EyeOff v-if="showSecret" class="size-4" />
              <Eye v-else class="size-4" />
            </BaseIconButton>
          </template>
        </BaseInput>
      </BaseFormItem>
      <p v-if="proxy?.accountCount && proxyUrl.trim()" class="m-0 text-cp-sm text-cp-warning-text">
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
      <BaseButton variant="secondary" :loading="saving" @click="emit('save', false)">
        <template #icon>
          <Save class="size-4" />
        </template>
        保存代理
      </BaseButton>
      <BaseButton variant="primary" :loading="saving" @click="emit('save', true)">
        <template #icon>
          <Wifi class="size-4" />
        </template>
        保存并测试
      </BaseButton>
    </template>
  </BaseModal>
</template>
