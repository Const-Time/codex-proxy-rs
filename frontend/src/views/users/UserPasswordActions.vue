<script setup lang="ts">
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { setUserPassword } from '@/api/modules/users'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseConfirmModal from '@/components/base/BaseConfirmModal.vue'
import BaseFormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import { toast } from '@/components/base/BaseToast'
import { useAuthStore } from '@/stores/modules/auth'
import { errorMessage } from '@/utils/async'

const props = defineProps<{ userId: string, disabled: boolean }>()
const emit = defineEmits<{ busy: [value: boolean], selfReset: [] }>()
const auth = useAuthStore()
const router = useRouter()
const newPassword = ref('')
const generatedPassword = ref('')
const busy = ref(false)
const confirmReset = ref(false)
const changedSelf = ref(false)

async function change(reset: boolean) {
  if (busy.value || props.disabled)
    return
  if (!reset && newPassword.value.length < 12) {
    toast.warning('请输入至少 12 位新密码')
    return
  }
  busy.value = true
  emit('busy', true)
  const self = props.userId === auth.user?.id
  try {
    const result = await setUserPassword(reset ? { id: props.userId, reset: true } : { id: props.userId, newPassword: newPassword.value })
    generatedPassword.value = result.generatedPassword ?? ''
    newPassword.value = ''
    confirmReset.value = false
    changedSelf.value = self
    if (self && reset)
      emit('selfReset')
    toast.success(reset ? '密码已重置，请复制保存新密码' : '密码已修改，原有登录会话已失效')
    if (self && !reset)
      await signInAgain()
  }
  catch (cause) {
    toast.error(errorMessage(cause, '密码修改失败'))
  }
  finally {
    busy.value = false
    emit('busy', false)
  }
}
async function copyPassword() {
  try {
    await navigator.clipboard.writeText(generatedPassword.value)
    toast.success('新密码已复制')
  }
  catch { toast.warning('复制失败，请手动选中并复制新密码') }
}
async function signInAgain() {
  generatedPassword.value = ''
  auth.invalidateSession()
  await router.replace('/login')
}
</script>

<template>
  <div class="grid gap-3 border-t border-cp-border pt-4">
    <BaseFormItem label="修改密码" description="独立生效，无需点击保存。修改后该用户所有登录会话失效，密钥及授权分组保留。">
      <BaseInput v-model="newPassword" type="password" autocomplete="new-password" aria-label="新密码" placeholder="至少 12 位，不修改请留空" :disabled="disabled || busy || changedSelf" />
    </BaseFormItem>
    <div class="flex flex-wrap gap-2">
      <BaseButton variant="secondary" :disabled="disabled || busy || changedSelf || !newPassword" @click="change(false)">
        修改密码
      </BaseButton>
      <BaseButton variant="secondary" :disabled="disabled || busy || changedSelf" @click="confirmReset = true">
        随机重置密码
      </BaseButton>
    </div>
    <div v-if="generatedPassword" role="status" class="grid gap-3 rounded-cp-lg bg-cp-bg-layout p-3">
      <p class="text-cp-sm text-cp-text-secondary">
        新密码仅在本次显示，请复制保存{{ changedSelf ? '，然后重新登录' : '' }}。
      </p>
      <BaseInput :model-value="generatedPassword" readonly aria-label="生成的新密码" autocomplete="off" class="font-mono" />
      <div class="flex gap-2">
        <BaseButton variant="primary" @click="copyPassword">
          复制新密码
        </BaseButton>
        <BaseButton v-if="changedSelf" variant="secondary" @click="signInAgain">
          已保存，重新登录
        </BaseButton>
      </div>
    </div>
    <BaseConfirmModal v-model="confirmReset" title="随机重置密码" description="将生成 32 位随机密码并立即替换旧密码，该用户需要重新登录。" confirm-text="重置密码" :loading="busy" @confirm="change(true)" />
  </div>
</template>
