import type { getAccounts } from '@/api'

import { computed, ref, shallowRef, watch } from 'vue'
import {
  completeAccountOAuth,
  importAccounts,
  startAccountOAuth,
} from '@/api'
import { toast } from '@/components/base/BaseToast'
import { useAsyncAction } from '@/composables/useAsyncAction'
import { errorMessage } from '@/utils/async'
import { isRecord } from '@/utils/object'
import { formatProviderLabel, isSupportedProvider } from '@/utils/providers'
import { accountImportSettings, accountProxyError, emptyAccountCreateForm } from '../components/AccountCreateModal/model'
import { splitAccountDocument } from '../utils/accountImport'
import { apiKeyAccountError, emptyApiKeyAccountForm } from '../utils/upstreamApiKey'

type AccountRow = Awaited<ReturnType<typeof getAccounts>>['items'][number]
type ImportProvider = 'openai' | 'xai'

interface MixedImportDocument {
  provider: ImportProvider
  document: Record<string, unknown>
}

type OpenAiTokenImportMode = 'access_token' | 'refresh_token'

const MAX_TOKEN_IMPORT_COUNT = 200

export function useAccountOnboarding(options: {
  reload: () => Promise<unknown>
}) {
  const createModalOpen = shallowRef(false)
  const reauthorizingAccount = shallowRef<AccountRow | null>(null)
  const creatingAccountAction = useAsyncAction()
  const authorizingOAuthAction = useAsyncAction()
  const creatingAccount = creatingAccountAction.loading
  const authorizingOAuth = authorizingOAuthAction.loading
  const createForm = ref(emptyAccountCreateForm())
  const completedImports = new Map<string, number>()
  watch(createForm, () => completedImports.clear(), { deep: true, flush: 'sync' })

  const showCreateModal = computed({
    get: () => createModalOpen.value,
    set: (value: boolean) => {
      createModalOpen.value = value
      if (!value) {
        reauthorizingAccount.value = null
        createForm.value = emptyAccountCreateForm()
      }
    },
  })

  async function handleCreate() {
    if (createForm.value.mode === 'oauth') {
      await completeOAuth()
      return
    }
    if (creatingAccount.value)
      return

    await creatingAccountAction.run(
      async () => {
        const proxyError = accountProxyError(createForm.value)
        if (!reauthorizingAccount.value && proxyError)
          throw new Error(proxyError)
        const message = createForm.value.provider === 'batch'
          ? await importMixedAccountDocument()
          : await importAccountDocument()
        await finishCreate(message)
      },
      { errorText: reauthorizingAccount.value ? '文件重新授权失败' : '导入失败' },
    )
  }

  async function handleAuthorizeOAuth() {
    if (authorizingOAuth.value)
      return

    await authorizingOAuthAction.run(
      async () => {
        const input = newAccountInput()
        const account = reauthorizingAccount.value
        const proxyError = accountProxyError(createForm.value)
        if (!account && proxyError)
          throw new Error(proxyError)
        const result = await startAccountOAuth({
          ...input,
          outboundProxyId: !account && createForm.value.proxyMode === 'proxy' ? createForm.value.proxyId.trim() : undefined,
          ...(account
            ? {
                accountId: account.id,
              }
            : {}),
        })

        createForm.value = {
          ...createForm.value,
          oauthFlowId: result.flowId,
          oauthAuthUrl: result.authorizationUrl,
          oauthCallback: '',
        }
        toast.success('授权链接已生成')
      },
      { errorText: '授权链接生成失败' },
    )
  }

  async function completeOAuth() {
    if (creatingAccount.value)
      return

    await creatingAccountAction.run(
      async () => {
        if (!createForm.value.oauthFlowId)
          throw new Error('请先生成授权链接')

        const callbackUrl = createForm.value.oauthCallback.trim()
        if (!callbackUrl) {
          throw new Error(createForm.value.provider === 'xai'
            ? '请粘贴 OAuth 回调地址、含 code 和 state 的查询字符串或授权码'
            : '请粘贴 OAuth 回调地址')
        }
        await completeAccountOAuth({
          provider: createForm.value.provider,
          flowId: createForm.value.oauthFlowId,
          callbackUrl,
          settings: reauthorizingAccount.value ? undefined : accountImportSettings(createForm.value),
        })
        await finishCreate(
          reauthorizingAccount.value
            ? '账号重新授权成功'
            : createForm.value.provider === 'xai'
              ? 'xAI OAuth 账号已添加'
              : 'OpenAI OAuth 账号已添加',
        )
      },
      {
        errorText: reauthorizingAccount.value ? '重新授权失败' : 'OAuth 授权导入失败',
      },
    )
  }

  function openCreateAccount() {
    reauthorizingAccount.value = null
    createForm.value = emptyAccountCreateForm()
    showCreateModal.value = true
  }

  function openReauthorizeAccount(account: AccountRow) {
    if (account.authenticationKind !== 'oauth' || (account.provider !== 'openai' && account.provider !== 'xai'))
      return
    reauthorizingAccount.value = account
    createForm.value = {
      ...emptyAccountCreateForm(),
      provider: account.provider,
      step: 'import',
      mode: 'oauth',
    }
    showCreateModal.value = true
  }

  function newAccountInput() {
    const account = reauthorizingAccount.value
    return {
      provider: createForm.value.provider,
      name: account?.name || account?.email || `${createForm.value.provider} OAuth`,
    }
  }

  async function importAccountDocument() {
    const provider = requireImportProvider(createForm.value.provider)
    const mode = createForm.value.mode
    if (mode === 'oauth')
      throw new Error('请选择凭据导入方式')
    if (mode === 'api_key') {
      if (provider !== 'openai')
        throw new Error('当前平台不支持 API Key 账号')
      const form = createForm.value.apiKey
      const error = apiKeyAccountError(form)
      if (error)
        throw new Error(error)
      await importAccounts({
        provider,
        settings: accountImportSettings(createForm.value),
        outboundProxyId: createForm.value.proxyMode === 'proxy' ? createForm.value.proxyId.trim() : undefined,
        data: { provider, authentication_kind: 'api_key', name: form.name.trim(), base_url: form.base_url.trim(), api_key: form.apiKey, transport: form.transport },
      })
      return 'API Key 账号已添加'
    }
    const documents = accountImportDocuments(
      provider,
      mode,
      createForm.value.importTexts[mode],
      reauthorizingAccount.value !== null,
    )
    const account = reauthorizingAccount.value
    if (account) {
      if (mode !== 'json' || provider !== account.provider || documents.length !== 1)
        throw new Error('请选择当前账号所属平台的单账号文件')
      const result = await importAccounts({
        provider,
        accountId: account.id,
        data: documents[0]!.document,
      })
      if (result.accountIds.length !== 1 || result.accountIds[0] !== account.id)
        throw new Error('重新授权结果与目标账号不一致，请刷新账号列表检查')
      return '账号文件重新授权成功'
    }
    return importDocuments(documents)
  }

  async function importMixedAccountDocument() {
    return importDocuments(parseMixedImportDocuments(parseImportJson(createForm.value.importTexts.json)))
  }

  async function importDocuments(documents: MixedImportDocument[]) {
    if (documents.length > MAX_TOKEN_IMPORT_COUNT)
      throw new Error(`一次最多导入 ${MAX_TOKEN_IMPORT_COUNT} 项，请拆分后重试`)
    let importedCount = 0
    const failures: string[] = []
    const seen = new Set<string>()
    for (const [index, entry] of documents.entries()) {
      const input = {
        provider: entry.provider,
        settings: accountImportSettings(createForm.value),
        outboundProxyId: createForm.value.proxyMode === 'proxy' ? createForm.value.proxyId.trim() : undefined,
        data: entry.document,
      }
      const identity = JSON.stringify(input)
      if (seen.has(identity))
        continue
      seen.add(identity)
      const completed = completedImports.get(identity)
      if (completed !== undefined) {
        importedCount += completed
        continue
      }
      try {
        const maximumMiB = entry.provider === 'xai' ? 16 : 4
        if (new TextEncoder().encode(identity).length > maximumMiB * 1024 * 1024)
          throw new Error(`单条文档超过 ${maximumMiB} MiB，请拆分后重试`)
        const result = await importAccounts(input)
        completedImports.set(identity, result.importedCount)
        importedCount += result.importedCount
      }
      catch (error) {
        failures.push(`第 ${index + 1} 项 ${formatProviderLabel(entry.provider)}：${errorMessage(error, '导入失败')}`)
      }
    }
    if (failures.length) {
      await options.reload()
      throw new Error(`已导入 ${importedCount} 个账号，${failures.length} 项失败：${failures.join('、')}。保持输入不变重试会跳过已确认成功的条目；网络中断的条目请先检查账号列表。`)
    }
    return `已导入 ${importedCount} 个账号`
  }

  async function finishCreate(message: string) {
    showCreateModal.value = false
    await options.reload()
    toast.success(message)
  }

  watch(
    () => createForm.value.provider,
    () => {
      createForm.value = {
        ...createForm.value,
        mode: createForm.value.provider === 'batch' ? 'json' : 'oauth',
        apiKey: emptyApiKeyAccountForm(),
        importTexts: { access_token: '', refresh_token: '', json: '' },
        oauthFlowId: '',
        oauthAuthUrl: '',
        oauthCallback: '',
      }
    },
    { flush: 'sync' },
  )

  watch(
    [
      () => createForm.value.proxyMode,
      () => createForm.value.proxyMode === 'proxy' ? createForm.value.proxyId.trim() : '',
    ],
    () => {
      createForm.value.oauthFlowId = ''
      createForm.value.oauthAuthUrl = ''
      createForm.value.oauthCallback = ''
    },
    { flush: 'sync' },
  )

  return {
    showCreateModal,
    reauthorizingAccount,
    creatingAccount,
    authorizingOAuth,
    createForm,
    handleCreate,
    handleAuthorizeOAuth,
    openCreateAccount,
    openReauthorizeAccount,
  }
}

function parseImportJson(value: string) {
  try {
    return JSON.parse(value)
  }
  catch {
    throw new Error('JSON 格式不正确')
  }
}

function requireImportProvider(value: string): ImportProvider {
  if (isSupportedProvider(value))
    return value
  throw new Error('请选择要导入的账号平台')
}

function accountImportDocuments(
  provider: ImportProvider,
  mode: string,
  value: string,
  reauthorizing = false,
): MixedImportDocument[] {
  if (provider === 'openai' && isOpenAiTokenImportMode(mode)) {
    return splitAccountDocument({ provider, document: parseOpenAiTokenImport(value, mode) })
  }
  const parsed: unknown = parseImportJson(value)
  if (reauthorizing && isRecord(parsed) && Array.isArray(parsed.documents)) {
    const documents = parseMixedImportDocuments(parsed)
    if (documents.length !== 1 || documents[0]?.provider !== provider)
      throw new Error('重新授权只接受当前平台的单账号文件，不支持批量账号包')
  }
  return providerImportDocuments(parsed, provider)
}

function parseOpenAiTokenImport(value: string, mode: OpenAiTokenImportMode) {
  const tokens = value
    .split(/\r?\n/)
    .map(token => token.trim())
    .filter(Boolean)
  const label = mode === 'access_token' ? 'Access Token' : 'Refresh Token'

  if (tokens.length === 0)
    throw new Error(`请至少粘贴一个 ${label}`)
  if (tokens.length > MAX_TOKEN_IMPORT_COUNT)
    throw new Error(`单次最多导入 ${MAX_TOKEN_IMPORT_COUNT} 个 ${label}`)

  const credentialKey = mode === 'access_token' ? 'accessToken' : 'refreshToken'
  return {
    accounts: tokens.map(token => ({ [credentialKey]: token })),
  }
}

function isOpenAiTokenImportMode(value: string): value is OpenAiTokenImportMode {
  return value === 'access_token' || value === 'refresh_token'
}

function providerImportDocuments(value: unknown, provider: ImportProvider): MixedImportDocument[] {
  if (isRecord(value) && Array.isArray(value.documents)) {
    const documents = parseMixedImportDocuments(value)
      .filter(entry => entry.provider === provider)
    if (documents.length === 0) {
      const label = formatProviderLabel(provider)
      throw new Error(`批量导入文件不包含 ${label} 账号文档`)
    }
    return documents
  }
  if (!isRecord(value))
    throw new Error('导入文件必须是 JSON object')
  return splitAccountDocument({ provider, document: value })
}

function parseMixedImportDocuments(value: unknown): MixedImportDocument[] {
  if (!isRecord(value) || !Array.isArray(value.documents))
    throw new Error('批量导入文件必须是 CPR 多平台导出文件')

  const documents: MixedImportDocument[] = []
  for (const entry of value.documents) {
    if (!isRecord(entry))
      throw new Error('批量导入文件包含无效的 Provider 文档')
    const provider = entry.provider
    if (!isSupportedProvider(provider))
      throw new Error('批量导入文件包含无效的 Provider 文档')
    if (!isRecord(entry.document))
      throw new Error('批量导入文件包含无效的 Provider 文档')
    documents.push(...splitAccountDocument({ provider, document: entry.document }))
  }

  if (documents.length === 0)
    throw new Error('批量文件没有可导入的账号文档')
  return documents
}
