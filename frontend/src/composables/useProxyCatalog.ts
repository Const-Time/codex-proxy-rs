import type { OutboundProxyRecord } from '@/api'
import { onMounted, onScopeDispose, shallowRef } from 'vue'
import { getProxies } from '@/api'
import { toast } from '@/components/base/BaseToast'
import { errorMessage } from '@/utils/async'

const PROXY_CATALOG_TTL_MS = 15_000

let cachedProxies: { expiresAt: number, items: OutboundProxyRecord[] } | undefined
let inFlightCatalog: Promise<OutboundProxyRecord[]> | undefined

async function fetchAllProxies(): Promise<OutboundProxyRecord[]> {
  const first = await getProxies({ page: 1, pageSize: 200 })
  if (first.page.totalPages <= 1)
    return first.items

  const pages = Array.from({ length: first.page.totalPages - 1 }, (_, index) => index + 2)
  const rest = await Promise.all(pages.map(page => getProxies({ page, pageSize: 200 })))
  return [...first.items, ...rest.flatMap(batch => batch.items)]
}

export function invalidateProxyCatalogCache() {
  cachedProxies = undefined
}

export function useProxyCatalog() {
  const proxies = shallowRef<OutboundProxyRecord[]>(cachedProxies && cachedProxies.expiresAt > Date.now() ? cachedProxies.items : [])
  const loading = shallowRef(false)
  let disposed = false

  async function loadProxies(force = false) {
    if (loading.value)
      return
    if (!force && cachedProxies && cachedProxies.expiresAt > Date.now()) {
      proxies.value = cachedProxies.items
      return
    }
    loading.value = true
    try {
      if (force || !inFlightCatalog) {
        const pending = fetchAllProxies().then((items) => {
          cachedProxies = { expiresAt: Date.now() + PROXY_CATALOG_TTL_MS, items }
          return items
        }).finally(() => {
          if (inFlightCatalog === pending)
            inFlightCatalog = undefined
        })
        inFlightCatalog = pending
      }
      const items = await inFlightCatalog
      if (!disposed)
        proxies.value = items
    }
    catch (error) {
      if (!disposed)
        toast.error(errorMessage(error, '代理列表加载失败'))
    }
    finally {
      if (!disposed)
        loading.value = false
    }
  }

  onMounted(() => void loadProxies())
  onScopeDispose(() => {
    disposed = true
  })

  return { proxies, loading, loadProxies }
}
