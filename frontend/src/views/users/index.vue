<script setup lang="ts">
import type { User } from '@/api/modules/users'
import { computed, onMounted, ref } from 'vue'
import { createUser, getUsers, updateUser } from '@/api/modules/users'
import AccountGroupCheckboxGrid from '@/components/AccountGroupCheckboxGrid.vue'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseCard from '@/components/base/BaseCard.vue'
import BaseFormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import BasePageHeader from '@/components/base/BasePageHeader.vue'
import {
  toast,
} from '@/components/base/BaseToast'
import { useAccountGroupCatalog } from '@/composables/useAccountGroupCatalog'
import { useAuthStore } from '@/stores/modules/auth'
import { errorMessage } from '@/utils/async'
import { isLoginEmail } from '@/utils/email'

const {
  groups,
  loading: groupsLoading,
  loadGroups,
} = useAccountGroupCatalog()
const users = ref<User[]>([])
const auth = useAuthStore()
const loading = ref(false)
const saving = ref(false)
const error = ref('')
const open = ref(false)
const editing = ref<User | null>(null)
const username = ref('')
const password = ref('')
const groupIds = ref<string[]>([])
const quotaMultipliers = ref<Record<string, string>>({})
const enabled = ref(true)
const maxConcurrency = ref('')
const requestsPerMinute = ref('')
const search = ref('')
const visibleUsers = computed(() => users.value.filter(user => user.username.toLowerCase().includes(search.value.toLowerCase())))
const groupNames = (user: User) => user.groupIds.map(id => groups.value.find(group => group.id === id)?.name ?? id).join('、') || '未分配'

async function load() {
  loading.value = true
  error.value = ''
  try {
    users.value = await getUsers()
  }
  catch (cause) { error.value = errorMessage(cause, '用户加载失败') }
  finally {
    loading.value = false
  }
}
function edit(user: User | null) {
  editing.value = user
  username.value = user?.username ?? ''
  password.value = ''
  groupIds.value = [...(user?.groupIds ?? [])]
  quotaMultipliers.value = { ...(user?.quotaMultipliers ?? {}) }
  enabled.value = user?.enabled ?? true
  maxConcurrency.value = String(user?.maxConcurrency || '')
  requestsPerMinute.value = String(user?.requestsPerMinute || '')
  open.value = true
  void loadGroups()
}
async function save() {
  if (saving.value)
    return
  if (!isLoginEmail(username.value.trim())) {
    toast.warning('请输入有效的邮箱地址作为登录账号')
    return
  }
  if (!editing.value && password.value.length < 12) {
    toast.warning('请输入至少 12 位初始密码')
    return
  }
  saving.value = true
  try {
    const limits = { maxConcurrency: Number(maxConcurrency.value || 0), requestsPerMinute: Number(requestsPerMinute.value || 0) }
    if (Object.values(limits).some(value => !Number.isSafeInteger(value) || value < 0)) {
      toast.warning('并发和 RPM 必须为非负整数，0 表示不限制')
      return
    }
    if (editing.value)
      await updateUser({ username: username.value.trim(), quotaMultipliers: Object.fromEntries(groupIds.value.map(id => [id, quotaMultipliers.value[id] || '1'])), id: editing.value.id, enabled: enabled.value, groupIds: groupIds.value, ...limits })
    else await createUser({ username: username.value.trim(), password: password.value, groupIds: groupIds.value, ...limits })
    password.value = ''
    open.value = false
    toast.success(editing.value ? '用户已更新' : '普通用户已创建')
    await load()
    if (editing.value?.id === auth.user?.id)
      await auth.checkAuth()
  }
  catch (cause) {
    toast.error(errorMessage(cause, '保存失败'))
  }
  finally {
    saving.value = false
  }
}
onMounted(load)
</script>

<template>
  <div class="flex flex-col gap-5">
    <BasePageHeader title="用户管理" description="创建普通用户并分配可用分组。用户自行创建和管理密钥。" />
    <div class="flex flex-wrap items-center gap-3">
      <BaseInput v-model="search" aria-label="搜索用户邮箱" placeholder="搜索邮箱" class="max-w-sm" />
      <BaseButton variant="primary" @click="edit(null)">
        新建用户
      </BaseButton>
      <BaseButton variant="secondary" :loading="loading" @click="load">
        刷新
      </BaseButton>
    </div>
    <p v-if="error" role="alert" class="text-cp-error">
      {{ error }}
    </p>
    <BaseCard>
      <div class="overflow-x-auto">
        <table class="w-full text-left text-cp-sm">
          <thead class="text-cp-text-secondary">
            <tr>
              <th class="p-3">
                登录邮箱
              </th><th class="p-3">
                角色
              </th><th class="p-3">
                状态
              </th><th class="p-3">
                授权分组
              </th><th class="p-3">
                并发 / RPM
              </th><th class="p-3">
                操作
              </th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="user in visibleUsers" :key="user.id" class="border-t border-cp-border">
              <td class="p-3 font-medium">
                {{ user.username }}
              </td>
              <td class="p-3">
                {{ user.role === 'admin' ? '管理员' : '普通用户' }}
              </td>
              <td class="p-3">
                {{ user.enabled ? '已启用' : '已禁用' }}
              </td>
              <td class="max-w-md p-3">
                {{ groupNames(user) }}
              </td>
              <td class="p-3 font-mono">
                {{ user.maxConcurrency || '不限' }} / {{ user.requestsPerMinute || '不限' }}
              </td>
              <td class="p-3">
                <BaseButton v-if="user.role !== 'admin' || user.id === auth.user?.id" variant="secondary" @click="edit(user)">
                  编辑
                </BaseButton>
              </td>
            </tr>
            <tr v-if="!loading && !visibleUsers.length">
              <td colspan="6" class="p-6 text-center text-cp-text-secondary">
                没有匹配的用户
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </BaseCard>
    <BaseModal v-model="open" :title="editing ? '编辑用户' : '新建普通用户'" :dismissible="!saving" size="md">
      <div class="grid gap-5">
        <BaseFormItem label="登录邮箱（用户名）" required description="仅支持邮箱登录。修改后请使用新邮箱登录，密码、密钥和历史记录保留。">
          <BaseInput v-model="username" aria-label="登录邮箱" type="email" :disabled="saving" maxlength="128" autocomplete="off" placeholder="name@example.com" />
        </BaseFormItem>
        <BaseFormItem v-if="!editing" label="初始密码" required>
          <BaseInput v-model="password" aria-label="初始密码" type="password" autocomplete="new-password" :disabled="saving" placeholder="至少 12 位" />
        </BaseFormItem>
        <label v-if="editing && editing.role !== 'admin'" for="user-enabled" class="flex items-center gap-2"><input id="user-enabled" v-model="enabled" type="checkbox" :disabled="saving">启用用户</label>
        <p v-if="editing && editing.role !== 'admin'" class="text-cp-sm text-cp-text-secondary">
          禁用后无法登录，已有密钥不能发起新请求。
        </p>
        <div v-if="editing?.role !== 'admin'" class="grid gap-4 sm:grid-cols-2">
          <BaseFormItem label="最大并发">
            <BaseInput v-model="maxConcurrency" type="number" min="0" step="1" aria-label="最大并发" placeholder="不限制" :disabled="saving" />
          </BaseFormItem>
          <BaseFormItem label="每分钟请求数（RPM）">
            <BaseInput v-model="requestsPerMinute" type="number" min="0" step="1" aria-label="每分钟请求数（RPM）" placeholder="不限制" :disabled="saving" />
          </BaseFormItem>
        </div>
        <p v-if="editing?.role !== 'admin'" class="text-cp-sm text-cp-text-secondary">
          0 表示不限制。同一用户的所有密钥共用并发和 RPM 限制。
        </p>
        <BaseFormItem label="授权分组">
          <AccountGroupCheckboxGrid v-model="groupIds" :groups="groups" :loading="groupsLoading" :disabled="saving" />
        </BaseFormItem>
        <p v-if="editing?.role === 'admin'" class="text-cp-sm text-cp-text-secondary">
          授权分组用于您自己的密钥和额度，不影响系统管理权限。取消授权后，该分组的已有密钥将无法发起新请求。
        </p>
        <div v-if="editing && groupIds.length" class="grid gap-3">
          <p class="text-cp-sm text-cp-text-secondary">
            额度倍率同时调整该用户的日限和周限，不改变消费计价。默认 1 倍，分组不限额时仍不限额。
          </p>
          <BaseFormItem v-for="id in groupIds" :key="id" :label="`${groups.find(group => group.id === id)?.name ?? id} · 额度倍率`">
            <BaseInput v-model="quotaMultipliers[id]" type="number" min="0.01" max="1000" step="0.01" placeholder="1" :aria-label="`${groups.find(group => group.id === id)?.name ?? id}额度倍率`" :disabled="saving" />
          </BaseFormItem>
        </div>
      </div>
      <template #footer>
        <BaseButton variant="secondary" :disabled="saving" @click="open = false; password = ''">
          取消
        </BaseButton><BaseButton variant="primary" :loading="saving" @click="save">
          保存
        </BaseButton>
      </template>
    </BaseModal>
  </div>
</template>
