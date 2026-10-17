import type { AccountGroupRef } from '@/api'

import { onMounted, shallowRef } from 'vue'
import { getAccountGroupCatalog } from '@/api'
import { toast } from '@/components/base/BaseToast'
import { errorMessage } from '@/utils/async'

export function useAccountGroupCatalog(options: { immediate?: boolean } = {}) {
  const groups = shallowRef<AccountGroupRef[]>([])
  const loading = shallowRef(false)

  async function loadGroups() {
    loading.value = true
    try {
      const items: AccountGroupRef[] = []
      for (let page = 1; ; page += 1) {
        const batch = await getAccountGroupCatalog({ page, pageSize: 200 })
        items.push(...batch)
        if (batch.length < 200)
          break
      }
      groups.value = items
      return items
    }
    catch (error: unknown) {
      toast.error(errorMessage(error, '账号分组加载失败'))
      return []
    }
    finally {
      loading.value = false
    }
  }

  if (options.immediate !== false) {
    onMounted(() => {
      void loadGroups()
    })
  }

  return {
    groups,
    loading,
    loadGroups,
  }
}
