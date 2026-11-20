<script setup lang="ts">
import type { User } from '@/api/modules/users'
import { Pencil, Plus, Power, RefreshCw, Search, Trash2 } from '@lucide/vue'
import { computed, onMounted, ref, watch } from 'vue'
import { createUser, deleteUser, getUsers, setUserEnabled, updateUser } from '@/api/modules/users'
import AccountGroupCheckboxGrid from '@/components/AccountGroupCheckboxGrid.vue'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseCard from '@/components/base/BaseCard.vue'
import BaseConfirmModal from '@/components/base/BaseConfirmModal.vue'
import BaseFormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseIconButton from '@/components/base/BaseIconButton.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseMenuItem from '@/components/base/BaseMenuItem.vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import BasePageHeader from '@/components/base/BasePageHeader.vue'
import BaseSegmented from '@/components/base/BaseSegmented.vue'
import BaseStatusBadge from '@/components/base/BaseStatusBadge.vue'
import BaseSwitch from '@/components/base/BaseSwitch.vue'
import { defineTableColumns } from '@/components/base/BaseTable/columns'
import BaseTable from '@/components/base/BaseTable/index.vue'
import {
  toast,
} from '@/components/base/BaseToast'
import RowActionMenu from '@/components/RowActionMenu.vue'
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
const columns = defineTableColumns<User>([
  { key: 'identity', label: '用户 / 邮箱', kind: 'identity' },
  { key: 'role', label: '角色', kind: 'meta', size: 'md' },
  { key: 'status', label: '状态', kind: 'status' },
  { key: 'groups', label: '授权分组', kind: 'custom', size: '2xl' },
  { key: 'limits', label: '并发 / RPM', kind: 'numeric', size: 'lg' },
  { key: 'actions', label: '操作', kind: 'actions', size: 'sm' },
])
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
const section = ref('identity')
const formSections = computed(() => [
  { label: '基本资料', value: 'identity', disabled: selfReset.value || passwordBusy.value },
  { label: '权限与额度', value: 'access', disabled: selfReset.value || passwordBusy.value },
  ...(editing.value ? [{ label: '登录安全', value: 'security', disabled: saving.value }] : []),
])
const email = ref('')
const username = ref('')
const password = ref('')
const groupIds = ref<string[]>([])
const quotaMultipliers = ref<Record<string, string>>({})
const enabled = ref(true)
const maxConcurrency = ref('')
const requestsPerMinute = ref('')
const search = ref('')
const searchQuery = computed(() => search.value.trim().toLowerCase())
const visibleUsers = computed(() => users.value.filter(user =>
  [user.email, user.username].some(value => value?.toLowerCase().includes(searchQuery.value)),
))
const groupCatalog = computed(() => new Map(groups.value.map(group => [group.id, group])))

watch(open, (value) => {
  if (!value)
    password.value = ''
})

async function load() {
  if (loading.value)
    return
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
  section.value = 'identity'
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
  if (!groups.value.length && !groupsLoading.value)
    void loadGroups()
}
async function save() {
  if (busy.value)
    return
  if (!isLoginEmail(email.value.trim())) {
    section.value = 'identity'
    toast.warning('请输入有效的邮箱地址作为登录账号')
    return
  }
  if (!editing.value && password.value.length < 12) {
    section.value = 'identity'
    toast.warning('请输入至少 12 位初始密码')
    return
  }
  saving.value = true
  try {
    const limits = { maxConcurrency: Number(maxConcurrency.value || 0), requestsPerMinute: Number(requestsPerMinute.value || 0) }
    if (Object.values(limits).some(value => !Number.isSafeInteger(value) || value < 0)) {
      section.value = 'access'
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
  <div class="cp-mobile-auto-height flex h-full min-h-0 w-full flex-col gap-5">
    <BasePageHeader title="用户管理" description="管理登录账号、授权分组和请求限制" />
    <BaseCard class="cp-mobile-auto-height cp-mobile-table-host flex h-[calc(100dvh-136px)] min-h-125 min-w-0 flex-col">
      <p v-if="error" role="alert" class="mb-3 rounded-cp bg-cp-error-container px-3 py-2 text-cp-sm text-cp-error-on-container">
        {{ error }}
      </p>
      <BaseTable table-id="users" class="min-h-48 flex-1" density="compact" :columns="columns" :loading="loading" :rows="visibleUsers" :empty-text="searchQuery ? '没有匹配的用户' : '暂无用户'">
        <template #toolbar>
          <BaseInput v-model="search" aria-label="搜索邮箱或用户名" placeholder="搜索邮箱或用户名" class="w-full sm:max-w-75" clearable>
            <template #prefix>
              <Search class="size-4" />
            </template>
          </BaseInput>
        </template>
        <template #toolbar-actions>
          <BaseButton variant="primary" :disabled="loading || busy" @click="edit(null)">
            <template #icon>
              <Plus class="size-4" />
            </template>
            新建用户
          </BaseButton>
          <BaseIconButton label="刷新用户" :loading="loading" :disabled="busy" @click="load">
            <RefreshCw class="size-4" />
          </BaseIconButton>
        </template>
        <template #identity="{ row: user }">
          <div class="grid min-w-0 gap-1 py-1">
            <strong class="truncate text-cp-text" :title="user.username || user.email">
              {{ user.username || user.email }}
            </strong>
            <span v-if="user.username" class="truncate text-cp-xs text-cp-text-secondary" :title="user.email">
              {{ user.email }}
            </span>
          </div>
        </template>
        <template #role="{ row: user }">
          <BaseStatusBadge :tone="user.role === 'admin' ? 'info' : 'neutral'">
            {{ user.role === 'admin' ? '管理员' : '普通用户' }}
          </BaseStatusBadge>
        </template>
        <template #status="{ row: user }">
          <BaseStatusBadge :tone="user.enabled ? 'success' : 'neutral'">
            {{ user.enabled ? '已启用' : '已禁用' }}
          </BaseStatusBadge>
        </template>
        <template #groups="{ row: user }">
          <div v-if="user.groupIds.length" class="flex min-w-0 flex-wrap gap-1.5 py-1 max-sm:justify-end">
            <span
              v-for="id in user.groupIds"
              :key="id"
              class="inline-flex min-w-0 max-w-full items-center gap-1.5 rounded-cp-sm bg-cp-fill-quaternary px-2 py-1 text-cp-xs text-cp-text-secondary"
              :title="groupCatalog.get(id)?.name ?? id"
            >
              <span class="size-2 shrink-0 rounded-xs bg-cp-fill-tertiary" :style="{ backgroundColor: groupCatalog.get(id)?.color }" aria-hidden="true" />
              <span class="truncate">{{ groupCatalog.get(id)?.name ?? id }}</span>
            </span>
          </div>
          <span v-else class="text-cp-text-tertiary">未分配</span>
        </template>
        <template #limits="{ row: user }">
          <span class="whitespace-nowrap">{{ user.maxConcurrency || '不限' }} / {{ user.requestsPerMinute || '不限' }}</span>
        </template>
        <template #actions="{ row: user }">
          <div class="min-w-0">
            <div class="flex items-center gap-1 max-sm:justify-end">
              <BaseIconButton v-if="user.role !== 'admin' || user.id === auth.user?.id" size="sm" label="编辑用户" :disabled="busy" @click="edit(user)">
                <Pencil class="size-3.5 text-cp-link" />
              </BaseIconButton>
              <RowActionMenu v-if="user.role !== 'admin'" v-slot="{ select }" :label="`${user.email}的更多操作`" :loading="busy">
                <BaseMenuItem role="menuitem" tabindex="-1" @click="select(() => requestAction(user, 'status'))">
                  <template #icon>
                    <Power class="size-3.5" />
                  </template>
                  {{ user.enabled ? '禁用用户' : '启用用户' }}
                </BaseMenuItem>
                <BaseMenuItem role="menuitem" tabindex="-1" tone="destructive" @click="select(() => requestAction(user, 'delete'))">
                  <template #icon>
                    <Trash2 class="size-3.5" />
                  </template>
                  删除用户
                </BaseMenuItem>
              </RowActionMenu>
            </div>
          </div>
        </template>
      </BaseTable>
      <div class="mt-3 shrink-0 text-cp-xs text-cp-text-tertiary">
        显示 {{ visibleUsers.length }} / {{ users.length }} 位用户
      </div>
    </BaseCard>
    <BaseModal v-model="open" :title="editing ? '编辑用户' : '新建普通用户'" :dismissible="!busy && !selfReset" size="lg">
      <div class="grid gap-5">
        <BaseSegmented v-model="section" label="用户设置分区" :options="formSections" class="w-full sm:w-fit" :disabled="saving" />

        <section v-show="section === 'identity'" class="grid gap-4" aria-label="基本资料">
          <div class="grid gap-4 sm:grid-cols-2">
            <BaseFormItem label="登录邮箱" required description="修改后请使用新邮箱登录。">
              <BaseInput v-model="email" aria-label="登录邮箱" type="email" :disabled="busy || selfReset" maxlength="128" autocomplete="off" placeholder="name@example.com" />
            </BaseFormItem>
            <BaseFormItem label="用户名（选填）" description="留空时显示邮箱。">
              <BaseInput v-model="username" aria-label="用户名" maxlength="128" :disabled="busy || selfReset" placeholder="自定义用户名" />
            </BaseFormItem>
          </div>
          <BaseFormItem v-if="!editing" label="初始密码" required>
            <BaseInput v-model="password" aria-label="初始密码" type="password" autocomplete="new-password" :disabled="busy" placeholder="至少 12 位" />
          </BaseFormItem>
          <div v-if="editing && editing.role !== 'admin'" class="rounded-cp bg-cp-fill-quaternary p-3">
            <BaseSwitch v-model="enabled" label="启用用户" show-label :disabled="busy || selfReset" />
            <p class="mt-2 text-cp-sm text-cp-text-secondary">
              禁用后无法登录，已有密钥不能发起新请求。
            </p>
          </div>
        </section>

        <section v-show="section === 'access'" class="grid gap-5" aria-label="权限与额度">
          <BaseFormItem label="授权分组" :description="editing?.role === 'admin' ? '分组授权控制个人密钥和额度。取消授权后，该分组的已有密钥将无法发起新请求。' : undefined">
            <AccountGroupCheckboxGrid v-model="groupIds" :groups="groups" :loading="groupsLoading" :disabled="busy || selfReset" />
          </BaseFormItem>
          <div v-if="editing?.role !== 'admin'" class="grid gap-3 rounded-cp bg-cp-fill-quaternary p-3">
            <div class="grid gap-4 sm:grid-cols-2">
              <BaseFormItem label="最大并发">
                <BaseInput v-model="maxConcurrency" type="number" min="0" step="1" aria-label="最大并发" placeholder="不限制" :disabled="busy || selfReset" />
              </BaseFormItem>
              <BaseFormItem label="每分钟请求数（RPM）">
                <BaseInput v-model="requestsPerMinute" type="number" min="0" step="1" aria-label="每分钟请求数（RPM）" placeholder="不限制" :disabled="busy || selfReset" />
              </BaseFormItem>
            </div>
            <p class="text-cp-sm text-cp-text-secondary">
              同一用户的所有密钥共用并发和 RPM 限制。0 表示不限制。
            </p>
          </div>
          <div v-if="editing && groupIds.length" class="grid gap-3">
            <p class="text-cp-sm text-cp-text-secondary">
              倍率同时调整日额度和周额度，默认 1 倍。
            </p>
            <div class="grid gap-3 sm:grid-cols-2">
              <BaseFormItem v-for="id in groupIds" :key="id" :label="`${groupCatalog.get(id)?.name ?? id} · 额度倍率`">
                <BaseInput v-model="quotaMultipliers[id]" type="number" min="0.01" max="1000" step="0.01" placeholder="1" :aria-label="`${groupCatalog.get(id)?.name ?? id}额度倍率`" :disabled="busy || selfReset" />
              </BaseFormItem>
            </div>
          </div>
        </section>

        <section v-if="open && editing" v-show="section === 'security'" aria-label="登录安全">
          <UserPasswordActions :key="editing.id" :user-id="editing.id" :disabled="saving" @busy="passwordBusy = $event" @self-reset="selfReset = true" />
        </section>
      </div>
      <template #footer>
        <BaseButton variant="secondary" :disabled="busy || selfReset" @click="open = false">
          {{ section === 'security' ? '关闭' : '取消' }}
        </BaseButton>
        <BaseButton v-if="section !== 'security'" variant="primary" :loading="saving" :disabled="passwordBusy || selfReset" @click="save">
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
