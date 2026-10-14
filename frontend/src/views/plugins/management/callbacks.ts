interface Target { instanceId: string, revision: number }
export function callbackTicketInput(value: unknown): { path: string, ttlSeconds: number } | null {
  if (!value || typeof value !== 'object')
    return null
  const input = value as Record<string, unknown>
  if (typeof input.path !== 'string' || input.path.length > 512
    || !input.path.split('/').every(part => /^[\w.-]+$/.test(part) && part !== '.' && part !== '..')
    || typeof input.ttlSeconds !== 'number' || !Number.isInteger(input.ttlSeconds)
    || input.ttlSeconds < 1 || input.ttlSeconds > 600) {
    return null
  }
  return { path: input.path, ttlSeconds: input.ttlSeconds }
}

export function pluginCallbackUrl(apiBase: string, origin: string, target: Target, path: string, state: string) {
  if (!/^[\da-f]{32}\.[\da-f]{64}$/i.test(state))
    throw new Error('宿主登录票据无效')
  const url = new URL(`${apiBase}/api/plugins/callback`, origin)
  url.searchParams.set('id', target.instanceId)
  url.searchParams.set('revision', String(target.revision))
  url.searchParams.set('path', path)
  url.searchParams.set('state', state)
  return url.href
}
