import assert from 'node:assert/strict'
// eslint-disable-next-line test/no-import-node-test -- Native Node test runner.
import test from 'node:test'
import { createPluginFormState, parseBindingOrder, parseBindingScope, parseJsonObject, parsePluginPayload } from '../src/views/plugins/bindings.ts'

test('plugin binding parsers validate order bounds, scope uniqueness and JSON objects', () => {
  assert.equal(parseBindingOrder('0', '顺序'), 0)
  assert.equal(parseBindingOrder('-10000', '顺序'), -10_000)
  assert.equal(parseBindingOrder('10000', '顺序'), 10_000)
  assert.throws(() => parseBindingOrder('10001', '顺序'), /-10000 到 10000/)
  assert.throws(() => parseBindingOrder('1.5', '顺序'), /-10000 到 10000/)

  assert.deepEqual(parseBindingScope('all', '', '范围 ID'), { kind: 'all' })
  assert.deepEqual(parseBindingScope('keys', 'k1\nk2  k3', '范围 ID'), { kind: 'keys', ids: ['k1', 'k2', 'k3'] })
  assert.throws(() => parseBindingScope('keys', '   ', '范围 ID'), /不重复/)
  assert.throws(() => parseBindingScope('groups', 'g1\ng1', '范围 ID'), /不重复/)

  assert.deepEqual(parseJsonObject('{"a": 1}', '配置'), { a: 1 })
  assert.throws(() => parseJsonObject('{invalid', '配置'), /有效的 JSON/)
  assert.throws(() => parseJsonObject('[]', '配置'), /JSON 对象/)
})

test('parsePluginPayload enforces middleware mount scope and builds secrets cleanly', () => {
  const form = createPluginFormState()
  form.name = 'demo'
  form.digest = 'abc'
  form.trust = true
  form.secrets = { token: 'secret-value' }
  form.removeSecrets = { legacy: true }
  form.middleware.enabled = true
  form.middleware.http = true
  form.middleware.scope = 'keys'
  form.middleware.ids = 'k1'
  assert.throws(() => parsePluginPayload(form, ['token', 'legacy'], 1, true), /全部请求/)

  form.middleware.scope = 'all'
  const payload = parsePluginPayload(form, ['token', 'legacy'], 1, true)
  assert.deepEqual(payload.secrets, { token: 'secret-value', legacy: null })
  assert.deepEqual(payload.middleware, { order: 0, mounts: ['http', 'request'], scope: { kind: 'all' } })
})
