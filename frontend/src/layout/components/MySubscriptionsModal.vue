<script setup lang="ts">
import type { UserGroup } from '@/api/modules/users'
import { ref, watch } from 'vue'
import { getUserGroups } from '@/api/modules/users'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import QuotaUsage from '@/components/quota/QuotaUsage.vue'
import { errorMessage } from '@/utils/async'

const open = defineModel<boolean>({ default: false })
const groups = ref<UserGroup[]>([])
const loading = ref(false)
const error = ref('')
async function load() {
  loading.value = true
  error.value = ''
  try {
    groups.value = await getUserGroups()
  }
  catch (cause) { error.value = errorMessage(cause, '订阅加载失败') }
  finally { loading.value = false }
}
watch(open, value => value && void load())
</script>

<template>
  <BaseModal v-model="open" title="我的订阅" size="md">
    <div class="grid gap-4">
      <p v-if="error" role="alert" class="text-cp-error">
        {{ error }}
      </p>
      <p v-if="loading" class="text-cp-text-secondary">
        正在刷新订阅…
      </p>
      <p v-else-if="!groups.length && !error" class="text-cp-text-secondary">
        暂无授权订阅，请联系管理员。
      </p>
      <section v-for="group in groups" :key="group.id" class="grid gap-3 rounded-cp-lg bg-cp-fill-quaternary p-4">
        <div class="flex items-center gap-2 font-semibold">
          <span class="size-3 rounded" :style="{ backgroundColor: group.color }" />{{ group.name }}<span v-if="!group.enabled" class="text-cp-xs text-cp-text-tertiary">已停用</span>
        </div>
        <QuotaUsage :budget="group" :label="group.name" />
      </section>
    </div>
    <template #footer>
      <BaseButton :loading="loading" @click="load">
        刷新
      </BaseButton><BaseButton @click="open = false">
        关闭
      </BaseButton>
    </template>
  </BaseModal>
</template>
