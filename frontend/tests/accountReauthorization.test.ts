import type { AccountCreateForm } from '../src/views/accounts/components/AccountCreateModal/model.ts'
import assert from 'node:assert/strict'
// eslint-disable-next-line test/no-import-node-test -- Use the existing native Node test runner.
import test from 'node:test'
import { resolveAccountCreatePresentation } from '../src/views/accounts/components/AccountCreateModal/presenter.ts'

test('reauthorization offers OAuth and account files for both providers', () => {
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
