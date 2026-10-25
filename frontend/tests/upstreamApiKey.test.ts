import assert from 'node:assert/strict'
// eslint-disable-next-line test/no-import-node-test -- Use the existing native Node test runner.
import test from 'node:test'
import { apiKeyAccountError, emptyApiKeyAccountForm } from '../src/views/accounts/utils/upstreamApiKey.ts'

test('third-party accounts accept HTTP and HTTPS and preserve an existing key on edit', () => {
  for (const base_url of ['https://api.example.com/v1', 'http://gateway.example.com:8080/v1']) {
    const form = { ...emptyApiKeyAccountForm(), name: 'Gateway', base_url, apiKey: 'test-secret' }
    assert.equal(apiKeyAccountError(form), undefined)
    form.apiKey = ''
    assert.ok(apiKeyAccountError(form))
    assert.equal(apiKeyAccountError(form, true), undefined)
  }
})

test('third-party credentials reject ambiguous URL auth, queries and unsafe key characters', () => {
  const form = { ...emptyApiKeyAccountForm(), name: 'Gateway', base_url: 'https://api.example.com/v1', apiKey: 'test-secret' }
  for (const base_url of ['ftp://api.example.com', 'https://user:pass@api.example.com', 'https://api.example.com?token=x', 'https://api.example.com#key'])
    assert.ok(apiKeyAccountError({ ...form, base_url }))
  for (const apiKey of ['has space', 'line\nbreak', '汉字'])
    assert.ok(apiKeyAccountError({ ...form, apiKey }))
})
