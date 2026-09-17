import assert from 'node:assert/strict'
// eslint-disable-next-line test/no-import-node-test -- Use the existing native Node test runner.
import test from 'node:test'
import { accountModelAccessError, accountModelIdError } from '../src/views/accounts/utils/modelAccess.ts'

test('model policy can preserve existing settings or explicitly allow all', () => {
  assert.equal(accountModelAccessError(undefined), undefined)
  assert.equal(accountModelAccessError({ mode: 'all', models: [] }), undefined)
  for (const mode of ['allowlist', 'denylist'] as const) {
    assert.ok(accountModelAccessError({ mode, models: [] }))
    assert.equal(accountModelAccessError({ mode, models: ['test-luna', 'Test-Luna'] }), undefined)
  }
})

test('model policies require bounded exact IDs and reject wildcards and control characters', () => {
  for (const id of ['', ' test-luna', 'test-luna ', '__internal', 'gpt-*', 'a\nb', 'a\u0000b', 'x'.repeat(257), '模'.repeat(86)])
    assert.ok(accountModelIdError(id), JSON.stringify(id))
  for (const id of ['test-luna', 'vendor/model:v1', 'x'.repeat(256), '模'.repeat(85)])
    assert.equal(accountModelIdError(id), undefined)
  assert.ok(accountModelAccessError({ mode: 'allowlist', models: Array.from({ length: 257 }, (_, i) => `model-${i}`) }))
  assert.equal(accountModelAccessError({ mode: 'allowlist', models: Array.from({ length: 256 }, (_, i) => `model-${i}`) }), undefined)
})
