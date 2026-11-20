<script setup lang="ts">
import type { OperationLog } from '@/api/modules/operations'
import { ShieldCheck } from '@lucide/vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import BaseStatusBadge from '@/components/base/BaseStatusBadge.vue'
import { formatDateTime } from '@/utils/date'

defineProps<{
  detail: OperationLog | null
  authenticationLabel: string
}>()
const open = defineModel<boolean>({ required: true })
</script>

<template>
  <BaseModal v-model="open" title="操作详情" size="lg">
    <template #icon>
      <ShieldCheck class="size-5" aria-hidden="true" />
    </template>
    <div v-if="detail" class="grid gap-5 text-cp-sm">
      <div class="grid min-w-0 gap-2 rounded-cp-lg bg-cp-fill-quaternary p-4">
        <div class="flex flex-wrap items-center gap-2">
          <BaseStatusBadge :tone="detail.status < 400 ? 'success' : 'danger'">
            HTTP {{ detail.status }}
          </BaseStatusBadge>
          <span class="font-mono text-cp-xs text-cp-text-secondary">{{ detail.durationMs }} ms</span>
          <span class="font-mono text-cp-xs text-cp-text-secondary">{{ formatDateTime(detail.occurredAt) }}</span>
        </div>
        <p class="m-0 break-all font-mono text-cp-text">
          {{ detail.method }} {{ detail.path }}
        </p>
      </div>

      <dl class="m-0 grid gap-4 sm:grid-cols-2">
        <div class="grid min-w-0 gap-1 sm:col-span-2">
          <dt class="font-emphasis text-cp-text-secondary">
            请求 ID
          </dt>
          <dd class="m-0 break-all font-mono">
            {{ detail.requestId }}
          </dd>
        </div>
        <div class="grid min-w-0 gap-1">
          <dt class="font-emphasis text-cp-text-secondary">
            操作者
          </dt>
          <dd class="m-0 break-all">
            {{ detail.username || detail.email || authenticationLabel }}
            <span v-if="detail.username && detail.email" class="block text-cp-xs text-cp-text-secondary">{{ detail.email }}</span>
          </dd>
        </div>
        <div class="grid min-w-0 gap-1">
          <dt class="font-emphasis text-cp-text-secondary">
            认证方式
          </dt>
          <dd class="m-0">
            {{ authenticationLabel }}
          </dd>
        </div>
        <div class="grid min-w-0 gap-1">
          <dt class="font-emphasis text-cp-text-secondary">
            连接 IP
          </dt>
          <dd class="m-0 break-all font-mono">
            {{ detail.clientIp || '未记录' }}
          </dd>
          <dd class="m-0 text-cp-xs text-cp-text-tertiary">
            网关直接连接的对端地址，可能是 Docker 网桥或反向代理。
          </dd>
        </div>
        <div class="grid min-w-0 gap-1">
          <dt class="font-emphasis text-cp-text-secondary">
            代理上报 IP
          </dt>
          <dd class="m-0 break-all font-mono">
            {{ detail.forwardedIp || '未上报' }}
          </dd>
          <dd v-if="detail.forwardedIp" class="m-0 text-cp-xs text-cp-text-tertiary">
            来自代理请求头，未经可信代理验证。
          </dd>
        </div>
      </dl>

      <section v-if="detail.changes.length" class="grid gap-2 border-t border-cp-border-secondary pt-4" aria-label="变更记录">
        <h3 class="m-0 font-heavy text-cp-text">
          变更记录
        </h3>
        <div v-for="(change, index) in detail.changes" :key="index" class="grid min-w-0 gap-1 rounded-cp bg-cp-fill-quaternary p-3">
          <p class="m-0 font-emphasis">
            {{ change.action }} · {{ change.entityKind }}
          </p>
          <p class="m-0 break-all font-mono text-cp-xs">
            {{ change.entityRef }}
          </p>
          <p class="m-0 text-cp-xs text-cp-text-secondary">
            变更字段 {{ change.changedFields.join('、') || '无' }}
          </p>
        </div>
      </section>
    </div>
  </BaseModal>
</template>
