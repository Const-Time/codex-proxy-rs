import assert from 'node:assert/strict'
// eslint-disable-next-line test/no-import-node-test -- Native Node test runner.
import test from 'node:test'
import { callbackTicketInput, pluginCallbackUrl } from '../src/views/plugins/management/callbacks.ts'

test('login tickets accept registered relative paths and bounded integer lifetimes only', () => {
  for (const path of ['', '../oauth', 'oauth/../other', '/oauth', 'https://other.test', 'oauth?instance=other', 'oauth\\other', 'oauth\n', 'x'.repeat(513)])
    assert.equal(callbackTicketInput({ path, ttlSeconds: 60 }), null)
  for (const ttlSeconds of [0, 601, 1.5, '60', Infinity, Number.NaN])
    assert.equal(callbackTicketInput({ path: 'oauth', ttlSeconds }), null)
  assert.deepEqual(callbackTicketInput({ path: 'login/oauth', ttlSeconds: 600, instanceId: 'forged', revision: 7 }), { path: 'login/oauth', ttlSeconds: 600 })
})

test('callback addresses retain the parent target and deployment prefix and cannot inject queries', () => {
  const state = `${'a'.repeat(32)}.${'b'.repeat(64)}`
  const address = new URL(pluginCallbackUrl('/gateway', 'https://host.test', { instanceId: 'plugin&forged=1', revision: 3 }, 'login/oauth', state))
  assert.equal(address.origin, 'https://host.test')
  assert.equal(address.pathname, '/gateway/api/plugins/callback')
  assert.deepEqual([...address.searchParams], [['id', 'plugin&forged=1'], ['revision', '3'], ['path', 'login/oauth'], ['state', state]])
  assert.throws(() => pluginCallbackUrl('', 'https://host.test', { instanceId: 'plugin', revision: 3 }, 'oauth', 'state&forged=1'))
})
