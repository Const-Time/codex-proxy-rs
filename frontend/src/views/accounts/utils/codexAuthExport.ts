import { strToU8, zipSync } from 'fflate'

export type AccountExportFormat = 'codex' | 'cpr'

function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new Error('账号导出数据格式异常，请刷新后重试')
  return value as Record<string, unknown>
}

function token(account: Record<string, unknown>, key: string): string {
  const value = account[key]
  if (typeof value !== 'string' || !value.trim())
    throw new Error('所选账号缺少完整 OAuth 凭据，请重新授权后导出，或选择系统备份格式')
  return value
}

/** Convert only explicitly selected OAuth accounts; never guess or fabricate credentials. */
export function codexAuthFiles(payload: unknown, selectedIds: string[]) {
  if (selectedIds.length === 0 || selectedIds.length > 200 || new Set(selectedIds).size !== selectedIds.length)
    throw new Error('请选择 1–200 个不同账号')
  const documents = object(payload).documents
  if (!Array.isArray(documents) || documents.length !== 1)
    throw new Error('Codex 认证文件仅支持 OpenAI OAuth 账号；其他账号请选择系统备份格式')
  const exported = object(documents[0])
  if (exported.provider !== 'openai')
    throw new Error('Codex 认证文件仅支持 OpenAI OAuth 账号；其他账号请选择系统备份格式')
  const document = object(exported.document)
  if (document.sourceFormat !== 'cpr' || !Array.isArray(document.accounts) || document.accounts.length !== selectedIds.length)
    throw new Error('账号导出数量不一致，请刷新后重试')
  const accounts = new Map<string, Record<string, unknown>>()
  for (const value of document.accounts) {
    const account = object(value)
    if (typeof account.id !== 'string' || !selectedIds.includes(account.id) || accounts.has(account.id))
      throw new Error('账号导出列表与选中账号不一致，请刷新后重试')
    accounts.set(account.id, account)
  }
  return selectedIds.map((id, index) => {
    const account = accounts.get(id)!
    if (account.authentication_kind === 'api_key' || 'api_key' in account || String(account.accessToken).startsWith('at-'))
      throw new Error('API Key / 访问令牌账号请使用系统备份格式；Codex 认证文件导出需要完整 OAuth 凭据')
    const auth = {
      auth_mode: 'chatgpt',
      OPENAI_API_KEY: null,
      tokens: {
        id_token: token(account, 'idToken'),
        access_token: token(account, 'accessToken'),
        refresh_token: token(account, 'refreshToken'),
        account_id: token(account, 'accountId'),
      },
    }
    // The account update time also changes with settings; it is not last_refresh.
    const name = typeof account.email === 'string' ? account.email : id
    const safeName = name.replace(/[^\w@.-]/g, '_').slice(0, 80).replace(/[. ]+$/g, '') || 'account'
    return {
      path: `${String(index + 1).padStart(3, '0')}-${safeName}/auth.json`,
      content: `${JSON.stringify(auth, null, 2)}\n`,
    }
  })
}

export function codexAuthDownload(payload: unknown, selectedIds: string[]) {
  const files = codexAuthFiles(payload, selectedIds)
  if (files.length === 1)
    return { fileName: 'auth.json', blob: new Blob([files[0]!.content], { type: 'application/json;charset=utf-8' }) }
  const archive = zipSync(Object.fromEntries(files.map(file => [file.path, strToU8(file.content)])), { level: 0 })
  return {
    fileName: `codex-auth-${files.length}-accounts.zip`,
    blob: new Blob([new Uint8Array(archive)], { type: 'application/zip' }),
  }
}
