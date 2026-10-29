<script setup lang="ts">
import type { AccountExportFormat } from '../utils/codexAuthExport'
import { ref } from 'vue'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import BaseSelect from '@/components/base/BaseSelect.vue'

defineProps<{ selectedCount: number, loading: boolean }>()
const emit = defineEmits<{ export: [format: AccountExportFormat] }>()
const open = defineModel<boolean>({ required: true })
const format = ref('codex')
const formats = [
  { value: 'codex', label: 'Codex 认证文件（auth.json）' },
  { value: 'cpr', label: '系统备份（原导出格式）' },
]
</script>

<template>
  <BaseModal v-model="open" title="导出账号" size="sm" :dismissible="!loading">
    <div class="space-y-4">
      <p class="text-cp-text-secondary">
        已选择 {{ selectedCount }} 个账号
      </p>
      <BaseSelect v-model="format" class="w-full" aria-label="导出格式" :options="formats" :disabled="loading" />
      <p v-if="format === 'codex'" class="text-cp-sm leading-relaxed text-cp-text-secondary">
        导出 OpenAI OAuth 账号的 Codex 认证文件。单个账号下载 auth.json，多个账号下载 ZIP，每个目录对应一个账号。解压后可选择对应的 auth.json 使用或重新导入。
      </p>
      <p v-else class="text-cp-sm leading-relaxed text-cp-text-secondary">
        使用原有系统备份格式，支持 OAuth、API Key 和其他平台账号，保留可导入的账号设置。
      </p>
      <p class="text-cp-sm leading-relaxed text-cp-text-secondary">
        文件包含账号凭据，请妥善保存。Codex 格式不包含系统分组、调度和代理设置；凭据不完整时请使用系统备份。
      </p>
    </div>
    <template #footer>
      <BaseButton variant="secondary" :disabled="loading" @click="open = false">
        取消
      </BaseButton>
      <BaseButton variant="primary" :loading="loading" :disabled="selectedCount === 0" @click="emit('export', format === 'cpr' ? 'cpr' : 'codex')">
        导出 {{ selectedCount }} 个账号
      </BaseButton>
    </template>
  </BaseModal>
</template>
