<script setup lang="ts">
import { ref, watch } from 'vue'

import BaseCard from '@/components/base/BaseCard.vue'
import BaseCheckbox from '@/components/base/BaseCheckbox.vue'
import BaseConfirmModal from '@/components/base/BaseConfirmModal.vue'
import BasePageHeader from '@/components/base/BasePageHeader.vue'
import BaseTablePagination from '@/components/base/BaseTable/BaseTablePagination.vue'
import BaseTable from '@/components/base/BaseTable/index.vue'
import LastUsedAtCell from '@/components/LastUsedAtCell.vue'
import { usePageSelection } from '@/composables/usePageSelection'
import { useUserGroupCatalog } from '@/composables/useUserGroupCatalog'
import ApiKeyActions from './components/ApiKeyActions.vue'
import ApiKeyBudgetCell from './components/ApiKeyBudgetCell.vue'
import ApiKeyCreateModal from './components/ApiKeyCreateModal.vue'
import ApiKeyFilters from './components/ApiKeyFilters.vue'
import ApiKeyIdentityCell from './components/ApiKeyIdentityCell.vue'
import ApiKeyPrefixCell from './components/ApiKeyPrefixCell.vue'
import ApiKeyScopeCell from './components/ApiKeyScopeCell.vue'
import ApiKeyStatusBadge from './components/ApiKeyStatusBadge.vue'
import ApiKeyUseModal from './components/ApiKeyUseModal.vue'
import { useApiKeyMutations } from './composables/useApiKeyMutations'
import { useApiKeysQuery } from './composables/useApiKeysQuery'
import { useApiKeyUse } from './composables/useApiKeyUse'
import { apiKeyColumns } from './constants'

const selectedIds = ref<Set<string>>(new Set())
const {
  loading,
  apiKeys,
  loadApiKeys,
  sort,
  apiKeyPagination,
  handlePageChange,
  handlePageSizeChange,
  handleSortChange,
} = useApiKeysQuery()

const {
  groups,
  loading: loadingGroups,
  loadGroups,
} = useUserGroupCatalog()

const {
  showFormModal,
  showDeleteModal,
  showSingleDeleteModal,
  showKeyModal,
  createdKey,
  createdKeyName,
  editingKey,
  pendingDeleteKey,
  savingKey,
  deletingKey,
  batchDeleting,
  updatingStatusKeyIds,
  revealingKeyIds,
  form,
  openCreate,
  openEdit,
  requestSave,
  requestDeleteKey,
  handleDelete,
  handleBatchDelete,
  handleToggleStatus,
  copyToClipboard,
  revealPlaintextKey,
  copyApiKey,
} = useApiKeyMutations({ selectedIds, reload: loadApiKeys })

const { allSelected, indeterminate, selectedRowKeys, toggleSelection, toggleAll } = usePageSelection(
  apiKeys,
  selectedIds,
)

const {
  showUseKeyModal,
  selectedUseKey,
  openAiBaseUrl,
  importCreatedKeyToCcs,
  openUseKeyModal,
  importToCcs,
} = useApiKeyUse({
  createdKey,
  createdKeyName,
  revealPlaintextKey,
})

watch(
  showFormModal,
  open => open && void loadGroups(),
)
</script>

<template>
  <div class="cp-mobile-auto-height flex h-full min-h-0 w-full flex-col overflow-hidden">
    <BasePageHeader
      class="h-17"
      title="我的密钥"
      description="创建和管理 API 密钥，并设置每个密钥可以使用的账号"
    />

    <BaseCard
      class="cp-mobile-auto-height cp-mobile-table-host mt-5 flex h-[calc(100dvh-136px)] min-h-125 flex-col"
    >
      <template #header>
        <ApiKeyFilters
          :batch-deleting="batchDeleting"
          :selected-count="selectedIds.size"
          @create="openCreate"
          @delete-selected="showDeleteModal = true"
        />
      </template>

      <template #body>
        <div class="flex h-full min-h-0 flex-col">
          <BaseTable
            class="min-h-0 flex-1"
            :columns="apiKeyColumns"
            :rows="apiKeys"
            :loading="loading"
            :selected-row-keys="selectedRowKeys"
            :sort="sort"
            empty-text="暂无 API Key"
            @sort-change="handleSortChange"
          >
            <template #header-selection>
              <BaseCheckbox
                :model-value="allSelected"
                :indeterminate="indeterminate"
                label="选择当前页密钥"
                @update:model-value="toggleAll"
              />
            </template>
            <template #selection="{ row }">
              <BaseCheckbox
                :model-value="selectedIds.has(row.id)"
                label="选择密钥"
                @update:model-value="toggleSelection(row.id)"
              />
            </template>
            <template #identity="{ row }">
              <ApiKeyIdentityCell :api-key="row" />
            </template>
            <template #prefix="{ row }">
              <ApiKeyPrefixCell
                :prefix="row.prefix"
                :revealing="revealingKeyIds.has(row.id)"
                @copy="copyApiKey(row)"
              />
            </template>
            <template #scope="{ row }">
              <ApiKeyScopeCell :api-key="row" :groups="groups" :loading="loadingGroups" @updated="loadApiKeys" />
            </template>
            <template #budget="{ row }">
              <ApiKeyBudgetCell :api-key="row" />
            </template>
            <template #activeConcurrency="{ row }">
              <span class="font-mono tabular-nums" :class="row.activeConcurrency ? 'text-cp-success' : 'text-cp-text-secondary'" title="该密钥当前正在处理的请求数">{{ row.activeConcurrency ?? '—' }}</span>
            </template>
            <template #enabled="{ row }">
              <ApiKeyStatusBadge :api-key="row" />
            </template>
            <template #lastUsedAt="{ row }">
              <LastUsedAtCell :value="row.lastUsedAt" />
            </template>
            <template #actions="{ row }">
              <ApiKeyActions
                :api-key="row"
                :deleting="deletingKey"
                :revealing="revealingKeyIds.has(row.id)"
                :updating-status="updatingStatusKeyIds.has(row.id)"
                @edit="openEdit"
                @delete="requestDeleteKey"
                @import-ccs="importToCcs"
                @toggle="handleToggleStatus"
                @use="openUseKeyModal"
              />
            </template>
          </BaseTable>
          <BaseTablePagination
            :pagination="apiKeyPagination"
            :loading="loading"
            @page-change="handlePageChange"
            @page-size-change="handlePageSizeChange"
          />
        </div>
      </template>
    </BaseCard>

    <ApiKeyCreateModal
      v-model="showFormModal"
      v-model:created-open="showKeyModal"
      v-model:form="form"
      :groups="groups"
      :group-loading="loadingGroups"
      :editing="Boolean(editingKey)"
      :created-key="createdKey"
      :saving="savingKey"
      @copy="copyToClipboard"
      @save="requestSave"
      @import-ccs="importCreatedKeyToCcs"
    />

    <ApiKeyUseModal
      v-model="showUseKeyModal"
      :api-key="selectedUseKey"
      :api-base-url="openAiBaseUrl"
      @copy="copyToClipboard"
    />

    <BaseConfirmModal
      v-model="showDeleteModal"
      title="确认删除"
      description="删除后这些 API Key 将立即失效，此操作不可撤销"
      destructive
      confirm-text="确认删除"
      :loading="batchDeleting"
      @confirm="handleBatchDelete"
    >
      <p class="m-0">
        确定删除选中的 {{ selectedIds.size }} 个 API Key 吗？
      </p>
    </BaseConfirmModal>

    <BaseConfirmModal
      v-model="showSingleDeleteModal"
      title="删除 API Key"
      description="删除后该 API Key 将立即失效，此操作不可撤销"
      destructive
      confirm-text="确认删除"
      :loading="deletingKey"
      @confirm="handleDelete"
    >
      <p class="m-0">
        确定删除 {{ pendingDeleteKey?.name || pendingDeleteKey?.prefix || '该 API Key' }} 吗？
      </p>
    </BaseConfirmModal>
  </div>
</template>
