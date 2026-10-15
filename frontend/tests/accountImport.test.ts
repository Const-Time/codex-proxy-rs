/* eslint-disable test/no-import-node-test */
import assert from 'node:assert/strict'
import { test } from 'node:test'
import { splitAccountDocument } from '../src/views/accounts/utils/accountImport.ts'

test('OpenAI entries split independently while preserving envelope and proxy references', () => {
  const account = { access_token: 'fixture', proxy_key: 'selected' }
  const proxy = { proxy_key: 'selected', url: 'http://example.invalid' }
  const original = { provider: 'openai' as const, document: { version: 1, data: {
    accounts: [account, { refresh_token: 'fixture-two' }, null],
    proxies: [proxy, { proxy_key: 'other' }],
    format: 'oauth',
  } } }
  const parts = splitAccountDocument(original)
  assert.equal(parts.length, 3)
  assert.deepEqual(parts[0]?.document, { version: 1, data: { accounts: [account], proxies: [proxy], format: 'oauth' } })
  assert.deepEqual((parts[1]?.document.data as Record<string, unknown>).proxies, [])
  assert.deepEqual((parts[2]?.document.data as Record<string, unknown>).accounts, [null])
  assert.equal(original.document.data.accounts.length, 3)
})

test('single OpenAI and xAI documents keep their provider contract', () => {
  for (const entry of [
    { provider: 'openai' as const, document: { access_token: 'fixture' } },
    { provider: 'xai' as const, document: { accounts: [{ refresh_token: 'fixture' }, { refresh_token: 'fixture' }] } },
  ]) assert.deepEqual(splitAccountDocument(entry), [entry])
})
