<script setup lang="ts">
import type { User } from '@/api/modules/users'
import { Search } from '@lucide/vue'
import { computed, onMounted, ref } from 'vue'
import { createUser, deleteUser, getUsers, setUserEnabled, updateUser } from '@/api/modules/users'
import AccountGroupCheckboxGrid from '@/components/AccountGroupCheckboxGrid.vue'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseCard from '@/components/base/BaseCard.vue'
import BaseConfirmModal from '@/components/base/BaseConfirmModal.vue'
import BaseFormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import BasePageHeader from '@/components/base/BasePageHeader.vue'
import BaseSwitch from '@/components/base/BaseSwitch.vue'
import {
  toast,
} from '@/components/base/BaseToast'
import { useAccountGroupCatalog } from '@/composables/useAccountGroupCatalog'
import { useAuthStore } from '@/stores/modules/auth'
import { errorMessage } from '@/utils/async'
import { isLoginEmail } from '@/utils/email'
import UserPasswordActions from './UserPasswordActions.vue'

const {
  groups,
  loading: groupsLoading,
  loadGroups,
} = useAccountGroupCatalog()
const users = ref<User[]>([])
const auth = useAuthStore()
const loading = ref(false)
const saving = ref(false)
const passwordBusy = ref(false)
const selfReset = ref(false)
const actionBusy = ref(false)
const actionTarget = ref<User | null>(null)
const actionType = ref<'delete' | 'status'>('status')
const confirmAction = ref(false)
const busy = computed(() => saving.value || passwordBusy.value || actionBusy.value)
const error = ref('')
const open = ref(false)
const editing = ref<User | null>(null)
const email = ref('')
const username = ref('')
const password = ref('')
const groupIds = ref<string[]>([])
const quotaMultipliers = ref<Record<string, string>>({})
const enabled = ref(true)
const maxConcurrency = ref('')
const requestsPerMinute = ref('')
const search = ref('')
const visibleUsers = computed(() => users.value.filter(user => `${user.email} ${user.username}`.toLowerCase().includes(search.value.toLowerCase())))
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
  selfReset.value = false
  editing.value = user
  email.value = user?.email ?? ''
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
  if (busy.value)
    return
  if (!isLoginEmail(email.value.trim())) {
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
      await updateUser({ email: email.value.trim(), username: username.value.trim(), quotaMultipliers: Object.fromEntries(groupIds.value.map(id => [id, quotaMultipliers.value[id] || '1'])), id: editing.value.id, enabled: enabled.value, groupIds: groupIds.value, ...limits })
    else await createUser({ email: email.value.trim(), username: username.value.trim(), password: password.value, groupIds: groupIds.value, ...limits })
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
function requestAction(user: User, action: 'delete' | 'status') {
  actionTarget.value = user
  actionType.value = action
  confirmAction.value = true
}
async function runAction() {
  const user = actionTarget.value
  if (!user || busy.value)
    return
  actionBusy.value = true
  try {
    if (actionType.value === 'delete')
      await deleteUser(user.id)
    else await setUserEnabled(user.id, !user.enabled)
    confirmAction.value = false
    toast.success(actionType.value === 'delete' ? '用户已删除，密钥已撤销' : user.enabled ? '用户已禁用' : '用户已启用')
    await load()
  }
  catch (cause) { toast.error(errorMessage(cause, '操作失败')) }
  finally { actionBusy.value = false }
}
onMounted(load)
</script>

<template>
  <div class="flex flex-col gap-5">
    <BasePageHeader title="用户管理" description="创建普通用户并分配可用分组。用户自行创建和管理密钥。" />
    <div class="flex flex-wrap items-center gap-3">
      <BaseInput v-model="search" aria-label="搜索邮箱或用户名" placeholder="搜索邮箱或用户名" class="w-full sm:w-80">
        <template #prefix>
          <Search class="size-4" />
        </template>
      </BaseInput>
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
    <BaseCard class="cp-mobile-table-host">
      <div class="overflow-x-auto">
        <table class="cp-mobile-record-table w-full text-left text-cp-sm">
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
              <td data-label="用户 / 邮箱" class="p-3 font-medium">
                <div>
                  {{ user.email }}
                  <div v-if="user.username" class="text-cp-xs text-cp-text-secondary">
                    {{ user.username }}
                  </div>
                </div>
              </td>
              <td data-label="角色" class="p-3">
                {{ user.role === 'admin' ? '管理员' : '普通用户' }}
              </td>
              <td data-label="状态" class="p-3">
                {{ user.enabled ? '已启用' : '已禁用' }}
              </td>
              <td data-label="授权分组" class="max-w-md p-3">
                {{ groupNames(user) }}
              </td>
              <td data-label="并发 / RPM" class="p-3 font-mono">
                {{ user.maxConcurrency || '不限' }} / {{ user.requestsPerMinute || '不限' }}
              </td>
              <td class="cp-mobile-actions p-3">
                <div class="flex items-center gap-2 whitespace-nowrap">
                  <BaseButton v-if="user.role !== 'admin' || user.id === auth.user?.id" variant="secondary" :disabled="busy" @click="edit(user)">
                    编辑
                  </BaseButton>
                  <BaseButton v-if="user.role !== 'admin'" variant="secondary" :disabled="busy" @click="requestAction(user, 'status')">
                    {{ user.enabled ? '禁用' : '启用' }}
                  </BaseButton>
                  <BaseButton v-if="user.role !== 'admin'" variant="destructive" :disabled="busy" @click="requestAction(user, 'delete')">
                    删除
                  </BaseButton>
                </div>
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
    <BaseModal v-model="open" :title="editing ? '编辑用户' : '新建普通用户'" :dismissible="!busy && !selfReset" size="md">
      <div class="grid gap-5">
        <BaseFormItem label="登录邮箱" required description="仅支持邮箱登录。修改后请使用新邮箱登录，密码、密钥和历史记录保留。">
          <BaseInput v-model="email" aria-label="登录邮箱" type="email" :disabled="saving" maxlength="128" autocomplete="off" placeholder="name@example.com" />
        </BaseFormItem>
        <BaseFormItem label="用户名（选填）" description="用于显示，与登录邮箱分开；不填写时显示邮箱。">
          <BaseInput v-model="username" aria-label="用户名" maxlength="128" :disabled="saving" placeholder="自定义用户名" />
        </BaseFormItem>
        <BaseFormItem v-if="!editing" label="初始密码" required>
          <BaseInput v-model="password" aria-label="初始密码" type="password" autocomplete="new-password" :disabled="saving" placeholder="至少 12 位" />
        </BaseFormItem>
        <UserPasswordActions v-if="open && editing" :key="editing.id" :user-id="editing.id" :disabled="saving" @busy="passwordBusy = $event" @self-reset="selfReset = true" />
        <div v-if="editing && editing.role !== 'admin'" class="flex items-center gap-2">
          <BaseSwitch v-model="enabled" label="启用用户" :disabled="saving" /><span>启用用户</span>
        </div>
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
        <BaseButton variant="secondary" :disabled="busy || selfReset" @click="open = false; password = ''">
          取消
        </BaseButton><BaseButton variant="primary" :loading="saving" :disabled="passwordBusy || selfReset" @click="save">
          保存
        </BaseButton>
      </template>
    </BaseModal>
    <BaseConfirmModal
      v-model="confirmAction"
      :title="actionType === 'delete' ? '删除用户' : actionTarget?.enabled ? '禁用用户' : '启用用户'"
      :description="actionType === 'delete' ? `确定删除 ${actionTarget?.email}？该用户所有密钥将被删除，历史用量与操作日志保留。此操作无法撤销。` : `确定${actionTarget?.enabled ? '禁用' : '启用'} ${actionTarget?.email}？${actionTarget?.enabled ? '该用户将无法登录，已有密钥不能发起新请求。' : '该用户可重新登录并使用已有密钥。'}`"
      :destructive="actionType === 'delete'"
      :loading="actionBusy"
      @confirm="runAction"
    />
  </div>
</template>
