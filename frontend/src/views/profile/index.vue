<script setup lang="ts">
import { KeyRound, RefreshCw, UserRound, Wallet } from '@lucide/vue'
import { computed, ref } from 'vue'
import { useRouter } from 'vue-router'
import { changePassword } from '@/api/modules/users'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseCard from '@/components/base/BaseCard.vue'
import BaseEmpty from '@/components/base/BaseEmpty.vue'
import BaseFormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseIconButton from '@/components/base/BaseIconButton.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BasePageHeader from '@/components/base/BasePageHeader.vue'
import BaseSkeleton from '@/components/base/BaseSkeleton.vue'
import BaseStatusBadge from '@/components/base/BaseStatusBadge.vue'
import { toast } from '@/components/base/BaseToast'
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
const displayName = computed(() => auth.user?.username || auth.user?.email)
const canSave = computed(() => !!currentPassword.value && !!newPassword.value && !!confirmation.value)
async function save() {
  if (!canSave.value || saving.value)
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
  <div class="flex min-w-0 flex-col gap-5">
    <BasePageHeader title="个人资料" description="查看账号身份和分组额度，管理登录密码" />

    <div class="grid items-start gap-5 xl:grid-cols-[minmax(0,1fr)_minmax(300px,0.8fr)]">
      <div class="grid min-w-0 gap-5">
        <BaseCard title="账号信息">
          <div class="flex min-w-0 items-start gap-3 rounded-cp bg-cp-fill-quaternary p-4">
            <span class="grid size-10 shrink-0 place-items-center rounded-cp bg-cp-primary-container text-cp-primary-on-container">
              <UserRound class="size-5" aria-hidden="true" />
            </span>
            <div class="grid min-w-0 gap-1">
              <div class="flex flex-wrap items-center gap-2">
                <strong class="min-w-0 break-all">{{ displayName }}</strong>
                <BaseStatusBadge :tone="auth.isAdmin ? 'info' : 'neutral'">
                  {{ auth.isAdmin ? '管理员' : '普通用户' }}
                </BaseStatusBadge>
              </div>
              <span v-if="auth.user?.email && auth.user.email !== displayName" class="text-cp-sm break-all text-cp-text-secondary">{{ auth.user.email }}</span>
            </div>
          </div>
        </BaseCard>

        <BaseCard title="我的分组额度" description="同一分组内的密钥共享日额度与周额度。删除或新建密钥不会重置已用金额。">
          <template #actions>
            <BaseIconButton label="刷新分组额度" :loading="loading" @click="loadGroups">
              <RefreshCw class="size-4" />
            </BaseIconButton>
          </template>
          <div v-if="groups.length" class="grid gap-3 2xl:grid-cols-2" :aria-busy="loading">
            <section v-for="group in groups" :key="group.id" class="min-w-0 rounded-cp-lg bg-cp-fill-quaternary p-4">
              <div class="mb-4 flex flex-wrap items-center gap-2">
                <span class="size-2.5 shrink-0 rounded-xs" :style="{ backgroundColor: group.color }" aria-hidden="true" />
                <h3 class="m-0 min-w-0 break-words font-bold text-cp-text">
                  {{ group.name }}
                </h3>
                <BaseStatusBadge v-if="!group.enabled">
                  已禁用
                </BaseStatusBadge>
              </div>
              <QuotaUsage :budget="group" :label="group.name" />
            </section>
          </div>
          <div v-else-if="loading" class="grid gap-3" role="status" aria-label="正在加载分组额度">
            <BaseSkeleton class="h-24" />
          </div>
          <BaseEmpty v-else title="暂未分配分组" description="请联系管理员开通分组权限。" :icon="Wallet" size="sm" />
        </BaseCard>
      </div>

      <BaseCard title="登录安全" description="修改密码后需要重新登录。">
        <form class="grid gap-4" @submit.prevent="save">
          <BaseFormItem label="当前密码" required>
            <BaseInput v-model="currentPassword" type="password" autocomplete="current-password" aria-label="当前密码" :disabled="saving" />
          </BaseFormItem>
          <BaseFormItem label="新密码" required>
            <BaseInput v-model="newPassword" type="password" autocomplete="new-password" aria-label="新密码" placeholder="至少 12 位" :disabled="saving" />
          </BaseFormItem>
          <BaseFormItem label="确认新密码" required>
            <BaseInput v-model="confirmation" type="password" autocomplete="new-password" aria-label="确认新密码" :disabled="saving" />
          </BaseFormItem>
          <div class="flex justify-end border-t border-cp-border-secondary pt-4">
            <BaseButton variant="primary" type="submit" :disabled="!canSave" :loading="saving">
              <template #icon>
                <KeyRound class="size-4" />
              </template>
              修改密码
            </BaseButton>
          </div>
        </form>
      </BaseCard>
    </div>
  </div>
</template>
