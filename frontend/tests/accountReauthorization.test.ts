import type { AccountCreateForm } from '../src/views/accounts/components/AccountCreateModal/model.ts'
import assert from 'node:assert/strict'
// eslint-disable-next-line test/no-import-node-test -- Use the existing native Node test runner.
import test from 'node:test'
import { resolveAccountCreatePresentation } from '../src/views/accounts/components/AccountCreateModal/presenter.ts'

test('reauthorization offers OAuth and account files for OpenAI and xAI', () => {
  for (const provider of ['openai', 'xai'] as const) {
    const form = {
      provider,
      mode: 'json',
      step: 'import',
      importTexts: { json: '{"tokens":{}}', access_token: '', refresh_token: '' },
    } as AccountCreateForm
    const input = { form, account: null, saving: false, oauthLoading: false, reauthorizing: true }
    const view = resolveAccountCreatePresentation(input)
    assert.deepEqual(view.modeOptions.map(option => option.value), ['oauth', 'json'])
    assert.equal(view.importInput.uploadable, true)
    assert.equal(view.canSubmit, true)
    assert.equal(view.submitLabel, '完成重新授权')
    assert.match(view.modal.description, /保留原有配置/)
    assert.equal(resolveAccountCreatePresentation({ ...input, saving: true }).canSubmit, false)
    form.importTexts.json = ''
    assert.equal(resolveAccountCreatePresentation(input).canSubmit, false)
  }
})

test('Antigravity create and reauthorization only expose OAuth with full callback guidance', () => {
  const createForm = {
    provider: 'antigravity',
    mode: 'oauth',
    step: 'import',
    oauthFlowId: 'flow-1',
    oauthAuthUrl: 'https://accounts.google.com/o/oauth2/auth?state=s',
    oauthCallback: 'http://localhost:8085/auth/callback?code=c&state=s',
    importTexts: { json: '', access_token: '', refresh_token: '' },
  } as AccountCreateForm

  const createView = resolveAccountCreatePresentation({
    form: createForm,
    account: null,
    saving: false,
    oauthLoading: false,
    reauthorizing: false,
  })
  assert.equal(createView.isAntigravity, true)
  assert.deepEqual(createView.modeOptions.map(option => option.value), ['oauth'])
  assert.equal(createView.oauth.panelTitle, 'Antigravity OAuth 授权')
  assert.equal(createView.oauth.callbackLabel, '回调地址')
  assert.match(createView.oauth.callbackPlaceholder, /code.*state/)
  assert.equal(createView.canSubmit, true)
  assert.equal(createView.submitLabel, '完成导入')

  const apiKeyAttempt = resolveAccountCreatePresentation({
    form: { ...createForm, mode: 'api_key' },
    account: null,
    saving: false,
    oauthLoading: false,
    reauthorizing: false,
  })
  assert.equal(apiKeyAttempt.canSubmit, false)

  const reauthView = resolveAccountCreatePresentation({
    form: createForm,
    account: null,
    saving: false,
    oauthLoading: false,
    reauthorizing: true,
  })
  assert.deepEqual(reauthView.modeOptions.map(option => option.value), ['oauth'])
  assert.equal(reauthView.oauth.panelTitle, 'Antigravity OAuth 重新授权')
  assert.equal(reauthView.submitLabel, '完成重新授权')
  assert.match(reauthView.modal.description, /Antigravity/)
  assert.equal(reauthView.canSubmit, true)
})
