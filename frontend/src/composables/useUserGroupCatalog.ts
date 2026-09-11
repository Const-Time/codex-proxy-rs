import type { UserGroup } from '@/api/modules/users'
import { onMounted, shallowRef } from 'vue'
import { getUserGroups } from '@/api/modules/users'
import {
  toast,
} from '@/components/base/BaseToast'
import { errorMessage } from '@/utils/async'

export function useUserGroupCatalog(options: { immediate?: boolean } = {}) {
  const groups = shallowRef<UserGroup[]>([])
  const loading = shallowRef(false)
  async function loadGroups() {
    loading.value = true
    try {
      groups.value = await getUserGroups()
    }
    catch (error) {
      groups.value = []
      toast.error(errorMessage(error, '分组加载失败'))
    }
    finally {
      loading.value = false
    }
  }
  if (options.immediate !== false)
    onMounted(loadGroups)
  return {
    groups,
    loading,
    loadGroups,
  }
}
