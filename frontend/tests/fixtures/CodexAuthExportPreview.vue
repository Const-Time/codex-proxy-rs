<script setup lang="ts">
import type { AccountExportFormat } from '../../src/views/accounts/utils/codexAuthExport'
import { ref } from 'vue'
import BaseButton from '../../src/components/base/BaseButton.vue'
import { useDownload } from '../../src/composables/useDownload'
import AccountExportModal from '../../src/views/accounts/components/AccountExportModal.vue'
import { codexAuthDownload } from '../../src/views/accounts/utils/codexAuthExport'

const open = ref(false)
const count = ref(2)
const message = ref('')
const { downloadBlob, downloadJson } = useDownload()
async function exportFixture(format: AccountExportFormat) {
  const accounts = Array.from({ length: count.value }, (_, i) => ({
    id: `fixture-${i}`,
    email: `fixture-${i}@example.test`,
    accountId: `fixture-account-${i}`,
    accessToken: `fixture-access-${i}`,
    refreshToken: `fixture-refresh-${i}`,
    idToken: `fixture-id-${i}`,
  }))
  const payload = { documents: [{ provider: 'openai', document: { sourceFormat: 'cpr', accounts } }] }
  if (format === 'codex') {
    const result = codexAuthDownload(payload, accounts.map(account => account.id))
    await downloadBlob(result.blob, result.fileName)
    message.value = `已下载 ${result.fileName}`
  }
  else {
    await downloadJson(payload, 'fixture-backup.json')
    message.value = '已下载 fixture-backup.json'
  }
  open.value = false
}
</script>

<template>
  <main class="space-y-4 p-8">
    <h1 class="text-cp-2xl font-semibold">
      账号导出验证
    </h1>
    <p>仅使用虚构凭据，无服务器请求。</p>
    <div class="flex gap-2">
      <BaseButton @click="count = 1; open = true">
        导出单个账号
      </BaseButton>
      <BaseButton @click="count = 2; open = true">
        导出两个账号
      </BaseButton>
    </div>
    <p role="status">
      {{ message }}
    </p>
    <AccountExportModal v-model="open" :selected-count="count" :loading="false" @export="exportFixture" />
  </main>
</template>
