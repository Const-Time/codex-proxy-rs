<script setup lang="ts">
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { changePassword } from '@/api/modules/users'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseCard from '@/components/base/BaseCard.vue'
import BaseFormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BasePageHeader from '@/components/base/BasePageHeader.vue'
import {
  toast,
} from '@/components/base/BaseToast'
import QuotaUsage from '@/components/quota/QuotaUsage.vue'
import { useUserGroupCatalog } from '@/composables/useUserGroupCatalog'
import { useAuthStore } from '@/stores/modules/auth'
import { errorMessage } from '@/utils/async'

const auth = useAuthStore()
const router = useRouter()
const {
  groups,
  loading,
  loadGroups,
} = useUserGroupCatalog()
const currentPassword = ref('')
const newPassword = ref('')
const confirmation = ref('')
const saving = ref(false)
async function save() {
  if (saving.value)
    return
  if (newPassword.value.length < 12 || newPassword.value !== confirmation.value) {
    toast.warning('新密码至少 12 位，且两次输入须一致')
    return
  }
  saving.value = true
  try {
    await changePassword({ currentPassword: currentPassword.value, newPassword: newPassword.value })
    currentPassword.value = ''
    newPassword.value = ''
    confirmation.value = ''
    await auth.logout()
    toast.success('密码已修改，请重新登录')
    await router.push('/login')
  }
  catch (cause) {
    toast.error(errorMessage(cause, '密码修改失败'))
  }
  finally {
    saving.value = false
  }
}
</script>

<template>
  <div class="flex flex-col gap-5">
    <BasePageHeader title="个人资料" :description="`${auth.user?.username || auth.user?.email || ''} · ${auth.isAdmin ? '管理员' : '普通用户'}`" />
    <BaseCard>
      <div class="flex items-center justify-between gap-3">
        <h2 class="text-lg font-semibold">
          我的分组额度
        </h2><BaseButton variant="secondary" :loading="loading" @click="loadGroups">
          刷新
        </BaseButton>
      </div>
      <p class="text-cp-sm text-cp-text-secondary">
        每个分组内，你的所有密钥共享日限额和周限额。删除或新建密钥不会重置已用金额。
      </p>
      <div class="grid gap-4 md:grid-cols-2">
        <div v-for="group in groups" :key="group.id" class="rounded-cp border border-cp-border p-4">
          <h3 class="mb-4 font-semibold">
            {{ group.name }}{{ group.enabled ? '' : '（已禁用）' }}
          </h3>
          <QuotaUsage :budget="group" :label="group.name" />
        </div>
      </div>
      <p v-if="!loading && !groups.length" class="text-cp-text-secondary">
        暂未分配分组，请联系管理员。
      </p>
    </BaseCard>
    <BaseCard>
      <h2 class="text-lg font-semibold">
        修改密码
      </h2>
      <form class="grid max-w-lg gap-4" @submit.prevent="save">
        <BaseFormItem label="当前密码" required>
          <BaseInput v-model="currentPassword" type="password" autocomplete="current-password" aria-label="当前密码" :disabled="saving" />
        </BaseFormItem>
        <BaseFormItem label="新密码" required>
          <BaseInput v-model="newPassword" type="password" autocomplete="new-password" aria-label="新密码" placeholder="至少 12 位" :disabled="saving" />
        </BaseFormItem>
        <BaseFormItem label="确认新密码" required>
          <BaseInput v-model="confirmation" type="password" autocomplete="new-password" aria-label="确认新密码" :disabled="saving" />
        </BaseFormItem>
        <BaseButton variant="primary" :loading="saving" @click="save">
          修改密码并重新登录
        </BaseButton>
      </form>
    </BaseCard>
  </div>
</template>
