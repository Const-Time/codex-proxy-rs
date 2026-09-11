import type { Ref } from 'vue'
import type { AccountGroup } from '@/api'
import type { SelectOption } from '@/components/base/BaseSelect.vue'
import { computed, ref, watch } from 'vue'
import { getAccountModels, getAccounts } from '@/api'

export function useGroupModelOptions(open: Ref<boolean>, group: () => AccountGroup | null) {
  const loading = ref(false)
  const models = ref<SelectOption[]>([])
  const message = ref('')
  let generation = 0

  async function load() {
    const run = ++generation
    const id = group()?.id
    models.value = []
    message.value = ''
    loading.value = false
    if (!open.value || !id) {
      if (open.value)
        message.value = '请先创建分组并关联账号，再设置模型倍率。'
      return
    }
    loading.value = true
    try {
      const accounts = []
      for (let page = 1; ; page++) {
        const result = await getAccounts({ page, pageSize: 100, groupId: id })
        if (run !== generation)
          return
        accounts.push(...result.items)
        if (!result.items.length || accounts.length >= result.page.total)
          break
      }
      const found = new Map<string, SelectOption>()
      let failures = 0
      for (let start = 0; start < accounts.length; start += 4) {
        const results = await Promise.allSettled(accounts.slice(start, start + 4).map(account => getAccountModels({ accountId: account.id })))
        if (run !== generation)
          return
        for (const result of results) {
          if (result.status === 'rejected') {
            failures++
            continue
          }
          for (const model of result.value.models)
            found.set(model.id, { label: model.label && model.label !== model.id ? `${model.id} · ${model.label}` : model.id, value: model.id })
        }
      }
      models.value = [...found.values()].sort((a, b) => a.value.localeCompare(b.value))
      message.value = failures ? `${failures} 个账号的模型加载失败，可重试；已保留成功加载的选项。` : !accounts.length ? '分组尚未关联账号，请先在账号管理中添加。' : !found.size ? '关联账号未返回模型，可刷新重试。' : ''
    }
    catch {
      if (run === generation)
        message.value = '分组模型加载失败，请重试。'
    }
    finally {
      if (run === generation)
        loading.value = false
    }
  }

  // Preserve saved rules even when an account has been removed or its catalog is unavailable.
  const options = computed(() => {
    const result = [...models.value]
    for (const model of Object.keys(group()?.modelMultipliers ?? {})) {
      if (!result.some(option => option.value === model))
        result.push({ label: `${model}（已配置，当前未获取）`, value: model })
    }
    return result
  })
  watch([open, () => group()?.id], load, { immediate: true })
  return { options, loading, message, reload: load }
}
