<script setup lang="ts">
import type { Ref } from 'vue'
import type { UsageDisplayRecord } from '../utils/records'
import type { BaseTableColumn } from '@/components/base/BaseTable/columns'
import { Minimize2 } from '@lucide/vue'
import { inject } from 'vue'
import BaseTable from '@/components/base/BaseTable/index.vue'
import ProviderIconGroup from '@/components/ProviderIconGroup.vue'
import {
  usageAccountText,
  usageAuthenticationKind,
  usageIsCompact,
  usageUserAgent,
} from '../utils/records'
import UsageBillingCell from './UsageBillingCell.vue'
import UsageClientIpCell from './UsageClientIpCell.vue'
import UsageLatencyCell from './UsageLatencyCell.vue'
import UsageModelCell from './UsageModelCell.vue'
import UsageReasoningEffortCell from './UsageReasoningEffortCell.vue'
import UsageTokenCell from './UsageTokenCell.vue'
import UsageTransportBadge from './UsageTransportBadge.vue'
import UsageTurnStateCell from './UsageTurnStateCell.vue'

withDefaults(
  defineProps<{
    columns: BaseTableColumn<UsageDisplayRecord>[]
    rows: UsageDisplayRecord[]
    loading?: boolean
    emptyText?: string
  }>(),
  {
    loading: false,
    emptyText: '暂无使用记录',
  },
)
// 使用记录表只负责该领域的单元格呈现；筛选与分页由页面组合。
const personal = inject<Readonly<Ref<boolean>>>('personalUsage')
</script>

<template>
  <BaseTable
    :columns="columns"
    :rows="rows"
    :loading="loading"
    :empty-text="emptyText"
  >
    <template #userEmail="{ row }">
      <div class="grid min-w-0 gap-1">
        <span class="truncate text-cp-sm font-bold" :title="row.username || row.userEmail || row.userId || '未关联用户'">
          {{ row.username || row.userEmail || row.userId || '未关联用户' }}
        </span>
        <span v-if="row.username && row.userEmail" class="truncate text-cp-xs text-cp-text-secondary" :title="row.userEmail">
          {{ row.userEmail }}
        </span>
      </div>
    </template>
    <template #provider="{ row }">
      <ProviderIconGroup
        :provider="String(row.provider || '')"
        :authentication-kind="personal ? undefined : usageAuthenticationKind(row)"
      />
    </template>

    <template #accountEmail="{ row }">
      <span
        class="block max-w-full truncate font-mono text-cp-sm leading-none font-bold text-cp-text"
        :title="usageAccountText(row)"
      >
        {{ usageAccountText(row) }}
      </span>
    </template>

    <template #clientIp="{ row }">
      <UsageClientIpCell :record="row" />
    </template>

    <template #userAgent="{ row }">
      <span class="block max-w-full wrap-break-word whitespace-normal font-mono text-cp-sm leading-[1.4] font-emphasis text-cp-text-secondary">
        {{ usageUserAgent(row) }}
      </span>
    </template>

    <template #model="{ row }">
      <UsageModelCell :record="row" />
    </template>

    <template #reasoningEffort="{ row }">
      <UsageReasoningEffortCell :record="row" />
    </template>

    <template #route="{ row }">
      <div class="inline-flex max-w-full items-center gap-1.5 whitespace-nowrap">
        <code class="font-mono text-cp-sm font-emphasis">{{ row.route || '—' }}</code>
        <span
          v-if="usageIsCompact(row)"
          class="inline-flex shrink-0 text-cp-orange-text"
          title="压缩请求"
          aria-label="压缩请求"
        >
          <Minimize2 class="size-3.5" stroke-width="2.4" />
        </span>
      </div>
    </template>

    <template #clientTransport="{ row }">
      <UsageTransportBadge :transport="row.clientTransport" />
    </template>

    <template #upstreamTransport="{ row }">
      <UsageTransportBadge :transport="row.upstreamTransport" />
    </template>

    <template #tokenDetails="{ row }">
      <UsageTokenCell :record="row" />
    </template>

    <template #billing="{ row }">
      <UsageBillingCell :record="row" />
    </template>

    <template #latency="{ row }">
      <UsageLatencyCell :record="row" />
    </template>

    <template #turnState="{ row }">
      <UsageTurnStateCell :value="row.turnState" />
    </template>

    <template v-if="$slots.actions" #actions="scope">
      <slot name="actions" v-bind="scope" />
    </template>
  </BaseTable>
</template>
