import type { AccountGroupRef } from '@/api'

import { onMounted, onScopeDispose, shallowRef } from 'vue'
import { getAccountGroupCatalog } from '@/api'
import { toast } from '@/components/base/BaseToast'
import { errorMessage } from '@/utils/async'

export function useAccountGroupCatalog(options: { immediate?: boolean } = {}) {
  const groups = shallowRef<AccountGroupRef[]>([])
  const loading = shallowRef(false)
  let requestSequence = 0
  let controller: AbortController | undefined

  async function loadGroups() {
    const requestId = ++requestSequence
    controller?.abort()
    const currentController = typeof AbortController !== 'undefined' ? new AbortController() : undefined
    controller = currentController
    loading.value = true
    try {
      const items: AccountGroupRef[] = []
      for (let page = 1; ; page += 1) {
        const batch = await getAccountGroupCatalog({ page, pageSize: 200 }, currentController?.signal)
        if (requestId !== requestSequence)
          return groups.value
        items.push(...batch)
        if (batch.length < 200)
          break
      }
      groups.value = items
      return items
    }
    catch (error: unknown) {
      if (requestId === requestSequence)
        toast.error(errorMessage(error, '账号分组加载失败'))
      return []
    }
    finally {
      if (requestId === requestSequence) {
        if (controller === currentController)
          controller = undefined
        loading.value = false
      }
    }
  }

  if (options.immediate !== false) {
    onMounted(() => {
      void loadGroups()
    })
  }

  onScopeDispose(() => {
    requestSequence += 1
    controller?.abort()
    controller = undefined
  })

  return {
    groups,
    loading,
    loadGroups,
  }
}
