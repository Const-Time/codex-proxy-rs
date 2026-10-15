interface ImportDocument {
  provider: 'openai' | 'xai'
  document: Record<string, unknown>
}

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

/** OpenAI entries are independent; xAI retains document-wide refresh-token deduplication. */
export function splitAccountDocument(entry: ImportDocument): ImportDocument[] {
  if (entry.provider !== 'openai')
    return [entry]
  const document = entry.document
  const data = record(document.data) && Array.isArray(document.data.accounts) ? document.data : document
  if (!Array.isArray(data.accounts) || !data.accounts.length)
    return [entry]
  return data.accounts.map((account) => {
    const item: Record<string, unknown> = { ...data, accounts: [account] }
    if (Array.isArray(data.proxies)) {
      const key = record(account) ? account.proxy_key : undefined
      item.proxies = data.proxies.filter(proxy => typeof key === 'string' && record(proxy) && proxy.proxy_key === key)
    }
    return { provider: entry.provider, document: data === document ? item : { ...document, data: item } }
  })
}
