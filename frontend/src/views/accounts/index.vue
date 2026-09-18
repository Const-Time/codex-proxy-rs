<script setup lang="ts">
import type { AccountRow } from './constants'
import { ChevronDown, Grid2X2 } from '@lucide/vue'
import { ref } from 'vue'

import AccountGroupMarks from '@/components/AccountGroupMarks.vue'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseCard from '@/components/base/BaseCard.vue'
import BaseCheckbox from '@/components/base/BaseCheckbox.vue'
import BaseConfirmModal from '@/components/base/BaseConfirmModal.vue'
import BasePageHeader from '@/components/base/BasePageHeader.vue'
import BaseTablePagination from '@/components/base/BaseTable/BaseTablePagination.vue'
import BaseTable from '@/components/base/BaseTable/index.vue'
import LastUsedAtCell from '@/components/LastUsedAtCell.vue'
import ProviderIconGroup from '@/components/ProviderIconGroup.vue'
import { useAccountGroupCatalog } from '@/composables/useAccountGroupCatalog'
import AccountBatchEditModal from './components/AccountBatchEditModal.vue'
import AccountCodexPolicyModal from './components/AccountCodexPolicyModal.vue'
import AccountConnectionTestModal from './components/AccountConnectionTestModal.vue'
import AccountCreateModal from './components/AccountCreateModal/index.vue'
import AccountEditModal from './components/AccountEditModal.vue'
import AccountFilters from './components/AccountFilters.vue'
import AccountIdentityCell from './components/AccountIdentityCell.vue'
import AccountOverviewCards from './components/AccountOverviewCards.vue'
import AccountPlanBadge from './components/AccountPlanBadge.vue'
import AccountQuotaPanel from './components/AccountQuotaPanel/index.vue'
import AccountQuotaSummaryCell from './components/AccountQuotaSummaryCell/index.vue'
import AccountStatusBadge from './components/AccountStatusBadge/index.vue'
import AccountTableActions from './components/AccountTableActions.vue'
import AccountUsagePanel from './components/AccountUsagePanel.vue'
import { useAccountBatchEditor } from './composables/useAccountBatchEditor'
import { useAccountConnectionTest } from './composables/useAccountConnectionTest'
import { useAccountEditor } from './composables/useAccountEditor'
import { useAccountMutations } from './composables/useAccountMutations'
import { useAccountsQuery } from './composables/useAccountsQuery'
import { useAccountsTable } from './composables/useAccountsTable'
import { accountColumns, derivedAccountStatus } from './constants'

const selectedIds = ref<Set<string>>(new Set())
const codexPolicyAccount = ref<AccountRow | null>(null)
const showCodexPolicy = ref(false)
const {
  loading,
  hasLoaded,
  loadError,
  accounts,
  loadAccounts,
  refreshAccountsSilently,
  searchQuery,
  providerQuery,
  statusQuery,
  groupQuery,
  sort,
  accountSummary,
  accountPagination,
  replaceAccount,
  handlePageChange,
  handlePageSizeChange,
  handleSortChange,
} = useAccountsQuery()

const {
  groups,
  loading: groupsLoading,
  loadGroups,
} = useAccountGroupCatalog()

const {
  showCreateModal,
  showDeleteModal,
  showSingleDeleteModal,
  pendingDeleteAccount,
  recoveringAccountIds,
  refreshingAccountIds,
  refreshingQuotaAccountIds,
  deletingAccount,
  creatingAccount,
  authorizingOAuth,
  batchDeleting,
  exportingAccounts,
  reauthorizingAccount,
  createForm,
  handleCreate,
  handleAuthorizeOAuth,
  openCreateAccount,
  openReauthorizeAccount,
  requestDeleteAccount,
  handleDelete,
  handleBatchDelete,
  handleExportAccounts,
  handleRecover,
  handleRefresh,
  handleRefreshQuota,
} = useAccountMutations({
  accounts,
  selectedIds,
  reload: () => Promise.all([loadAccounts(), loadGroups()]),
  replaceAccount,
})

const {
  showConnectionTestModal,
  testingAccount,
  connectionTestStatus,
  connectionTestModel,
  connectionTestLogs,
  connectionTestError,
  connectionTestStartedAt,
  connectionTestFinishedAt,
  connectionTestDurationMs,
  testingConnectionIds,
  loadingConnectionTestModels,
  refreshingConnectionTestModels,
  connectionTestSelectedModel,
  connectionTestModelOptions,
  connectionTestStatusView,
  openConnectionTest,
  handleRefreshConnectionTestModels,
  handleTestConnection,
} = useAccountConnectionTest({ reload: refreshAccountsSilently })

const {
  expandedAccountIds,
  allSelected,
  indeterminate,
  selectedRowKeys,
  expandedRowKeys,
  toggleSelection,
  toggleExpanded,
  toggleAll,
} = useAccountsTable(accounts, selectedIds)

const {
  showBatchEditModal,
  schedulingEnabled: batchSchedulingEnabled,
  concurrencyLimit: batchConcurrencyLimit,
  weight: batchWeight,
  modelAccess: batchModelAccess,
  updateScheduling: batchUpdateScheduling,
  catalogAccountId: batchCatalogAccountId,
  proxyMode: batchProxyMode,
  proxyId: batchProxyId,
  selectedGroupIds: batchGroupIds,
  saving: savingBatchEdit,
  open: openBatchEdit,
  save: saveBatchEdit,
} = useAccountBatchEditor({
  accounts,
  selectedIds,
  reloadAccounts: loadAccounts,
  reloadGroups: loadGroups,
})

const {
  showEditModal,
  editingAccount,
  schedulingEnabled,
  concurrencyLimit: editingConcurrencyLimit,
  weight: editingWeight,
  modelAccess: editingModelAccess,
  proxyMode: editingProxyMode,
  proxyId: editingProxyId,
  selectedGroupIds: editingGroupIds,
  saving: savingAccountEdit,
  open: openAccountEdit,
  save: saveAccountEdit,
} = useAccountEditor({
  accounts,
  reloadAccounts: loadAccounts,
  reloadGroups: loadGroups,
})
</script>

<template>
  <div class="flex min-h-0 w-full flex-col xl:h-full xl:overflow-hidden">
    <BasePageHeader
      class="h-17"
      title="账号管理"
      description="维护账号池，查看可用性、配额与使用状态"
    />

    <AccountOverviewCards :summary="accountSummary" :pending="!hasLoaded" />

    <BaseCard
      class="cp-mobile-table-host mt-4 flex flex-col xl:h-[calc(100dvh-250px)] xl:min-h-125"
    >
      <template #header>
        <AccountFilters
          v-model:search="searchQuery"
          v-model:status="statusQuery"
          v-model:provider="providerQuery"
          v-model:group="groupQuery"
          :groups="groups"
          :groups-loading="groupsLoading"
          :selected-count="selectedIds.size"
          :batch-deleting="batchDeleting"
          :exporting-accounts="exportingAccounts"
          @delete-selected="showDeleteModal = true"
          @export-selected="handleExportAccounts"
          @create="openCreateAccount"
          @edit-selected="openBatchEdit"
        />
      </template>

      <template #body>
        <div class="flex min-h-0 flex-col xl:h-full">
          <div v-if="loadError" role="alert" class="mb-3 flex shrink-0 flex-wrap items-center justify-between gap-2 rounded-cp bg-cp-error-container px-4 py-3 text-cp-sm text-cp-error">
            <span>账号加载失败：{{ loadError }}。{{ hasLoaded ? '当前保留上次加载的数据。' : '无法读取账号数据，不代表账号已被删除。' }}</span>
            <BaseButton size="sm" :loading="loading" @click="loadAccounts()">
              重新加载
            </BaseButton>
          </div>
          <BaseTable
            class="flex-none [--cp-table-row-height:72px] sm:h-100! sm:min-h-100 xl:h-auto! xl:min-h-0 xl:flex-1"
            mobile-actions-first
            :columns="accountColumns"
            :rows="accounts"
            :loading="loading"
            :selected-row-keys="selectedRowKeys"
            :expanded-row-keys="expandedRowKeys"
            :sort="sort"
            :empty-text="loadError ? '账号数据读取失败，请重试' : '暂无账号数据'"
            @sort-change="handleSortChange"
          >
            <template #expander="{ row }">
              <button
                type="button"
                class="inline-flex size-6 cursor-pointer items-center justify-center rounded-md border-0 bg-transparent text-cp-text-secondary transition hover:bg-cp-bg-text-hover hover:text-cp-text"
                :title="expandedAccountIds.has(row.id) ? '收起统计' : '展开统计'"
                :aria-expanded="expandedAccountIds.has(row.id)"
                @click.stop="toggleExpanded(row.id)"
              >
                <ChevronDown
                  class="size-3.5 transition-transform"
                  :class="expandedAccountIds.has(row.id) ? '' : '-rotate-90'"
                />
              </button>
            </template>

            <template #header-selection>
              <BaseCheckbox
                :model-value="allSelected"
                :indeterminate="indeterminate"
                label="选择当前页账号"
                @update:model-value="toggleAll"
              />
            </template>

            <template #selection="{ row }">
              <BaseCheckbox
                :model-value="selectedIds.has(row.id)"
                label="选择账号"
                @update:model-value="toggleSelection(row.id)"
              />
            </template>

            <template #identity="{ row }">
              <AccountIdentityCell :account="row" />
            </template>

            <template #provider="{ row }">
              <ProviderIconGroup
                :provider="row.provider"
                :authentication-kind="row.authenticationKind"
              />
            </template>

            <template #status="{ row }">
              <AccountStatusBadge
                :status="derivedAccountStatus(row)"
                :error-reason="row.errorReason"
                :error-message="row.errorMessage"
                :rate-limited-until="row.quota.rateLimitedUntil"
                :next-refresh-at="row.nextRefreshAt"
              />
            </template>

            <template #planType="{ row }">
              <AccountPlanBadge :plan-type="row.planType" :plan-type-display="row.planTypeDisplay" />
            </template>

            <template #usage="{ row }">
              <AccountQuotaSummaryCell :account="row" />
            </template>

            <template #groups="{ row }">
              <div class="flex w-full justify-center">
                <AccountGroupMarks :groups="row.groups" />
              </div>
            </template>

            <template #capacity="{ row }">
              <span
                class="inline-flex items-center gap-1 whitespace-nowrap text-xs leading-none"
                :class="(row.usedSlots ?? 0) > 0 ? 'text-cp-success' : 'text-cp-text'"
                title="当前占用 / 并发总容量"
              >
                <Grid2X2 class="size-3 shrink-0" aria-hidden="true" />
                <span class="font-mono font-semibold tabular-nums">{{ row.usedSlots ?? '未知' }} / {{ row.totalSlots }}</span>
              </span>
            </template>

            <template #lastUsedAt="{ row }">
              <LastUsedAtCell :value="row.usage.lastUsedAt" />
            </template>

            <template #actions="{ row }">
              <AccountTableActions
                :account="row"
                :deleting="deletingAccount"
                :recovering="recoveringAccountIds.has(row.id)"
                :refreshing="refreshingAccountIds.has(row.id)"
                :testing="testingConnectionIds.has(row.id)"
                @edit="openAccountEdit"
                @codex-policy="account => { codexPolicyAccount = account; showCodexPolicy = true }"
                @delete="requestDeleteAccount"
                @recover="handleRecover"
                @refresh="handleRefresh"
                @reauthorize="openReauthorizeAccount"
                @test="openConnectionTest"
              />
            </template>

            <template #expanded="{ row }">
              <div class="grid items-stretch gap-3 p-4 lg:grid-cols-[1.05fr_2.45fr] xl:min-h-[19.25rem]">
                <AccountQuotaPanel
                  :account="row"
                  :refreshing="refreshingQuotaAccountIds.has(row.id)"
                  @account-updated="replaceAccount"
                  @refresh-quota="handleRefreshQuota"
                />
                <AccountUsagePanel
                  :account="row"
                  :refreshing="refreshingQuotaAccountIds.has(row.id)"
                  @refresh-quota="handleRefreshQuota"
                />
              </div>
            </template>
          </BaseTable>
          <BaseTablePagination
            :pagination="accountPagination"
            :loading="loading"
            @page-change="handlePageChange"
            @page-size-change="handlePageSizeChange"
          />
        </div>
      </template>
    </BaseCard>

    <AccountConnectionTestModal
      v-model="showConnectionTestModal"
      v-model:selected-model="connectionTestSelectedModel"
      :account="testingAccount"
      :duration-ms="connectionTestDurationMs"
      :error="connectionTestError"
      :finished-at="connectionTestFinishedAt"
      :loading-models="loadingConnectionTestModels"
      :refreshing-models="refreshingConnectionTestModels"
      :logs="connectionTestLogs"
      :model="connectionTestModel"
      :model-options="connectionTestModelOptions"
      :started-at="connectionTestStartedAt"
      :status="connectionTestStatus"
      :status-view="connectionTestStatusView"
      @refresh-models="handleRefreshConnectionTestModels()"
      @test="handleTestConnection()"
    />

    <AccountCreateModal
      v-model="showCreateModal"
      v-model:form="createForm"
      :account="reauthorizingAccount"
      :groups="groups"
      :groups-loading="groupsLoading"
      :oauth-loading="authorizingOAuth"
      :reauthorizing="Boolean(reauthorizingAccount)"
      :saving="creatingAccount"
      @create="handleCreate"
      @generate-oauth="handleAuthorizeOAuth"
    />

    <AccountCodexPolicyModal v-model="showCodexPolicy" :account="codexPolicyAccount" />
    <AccountEditModal
      v-model="showEditModal"
      v-model:enabled="schedulingEnabled"
      v-model:concurrency-limit="editingConcurrencyLimit"
      v-model:weight="editingWeight"
      v-model:model-access="editingModelAccess"
      v-model:proxy-mode="editingProxyMode"
      v-model:proxy-id="editingProxyId"
      v-model:selected-group-ids="editingGroupIds"
      :account="editingAccount"
      :groups="groups"
      :groups-loading="groupsLoading"
      :saving="savingAccountEdit"
      @save="saveAccountEdit"
    />

    <AccountBatchEditModal
      v-model="showBatchEditModal"
      v-model:enabled="batchSchedulingEnabled"
      v-model:concurrency-limit="batchConcurrencyLimit"
      v-model:weight="batchWeight"
      v-model:model-access="batchModelAccess"
      v-model:update-scheduling="batchUpdateScheduling"
      v-model:proxy-mode="batchProxyMode"
      v-model:proxy-id="batchProxyId"
      v-model:selected-group-ids="batchGroupIds"
      :catalog-account-id="batchCatalogAccountId"
      :selected-count="selectedIds.size"
      :groups="groups"
      :groups-loading="groupsLoading"
      :saving="savingBatchEdit"
      @save="saveBatchEdit"
    />

    <BaseConfirmModal
      v-model="showDeleteModal"
      title="确认删除"
      description="删除后该账号将不再参与调度，此操作不可撤销"
      destructive
      confirm-text="确认删除"
      :loading="batchDeleting"
      @confirm="handleBatchDelete"
    >
      <p class="m-0">
        确定要删除选中的 {{ selectedIds.size }} 个账号吗？此操作不可撤销
      </p>
    </BaseConfirmModal>

    <BaseConfirmModal
      v-model="showSingleDeleteModal"
      title="删除账号"
      description="删除后该账号将不再参与调度，此操作不可撤销"
      destructive
      confirm-text="确认删除"
      :loading="deletingAccount"
      @confirm="handleDelete"
    >
      <p class="m-0">
        确定要删除
        {{
          pendingDeleteAccount?.email
            || pendingDeleteAccount?.accountId
            || pendingDeleteAccount?.id
            || '该账号'
        }}
        吗？
      </p>
    </BaseConfirmModal>
  </div>
</template>
