import assert from 'node:assert/strict'
// eslint-disable-next-line test/no-import-node-test -- Use the existing native Node test runner.
import test from 'node:test'
import { UNOWNED_USAGE_USER_LABEL, usageUserDisplay } from '../src/views/usage/utils/user.ts'

test('usage user display identifies maintenance from explicit internal markers', () => {
  for (const record of [{ clientTransport: 'maintenance' }, { requestKind: 'state_probe' }]) {
    const display = usageUserDisplay(record)
    assert.equal(display.label, '系统维护')
    assert.match(display.description, /消耗上游额度/)
    assert.match(display.description, /不扣用户额度/)
  }
})

test('unowned business and legacy records are not mislabeled as system maintenance', () => {
  for (const record of [{}, { clientTransport: 'websocket' }, { clientTransport: 'http_sse' }, { username: ' ', userEmail: null, userId: null }]) {
    assert.equal(usageUserDisplay(record).label, UNOWNED_USAGE_USER_LABEL)
  }
  assert.equal(UNOWNED_USAGE_USER_LABEL, '无用户归属')
})

test('known user identity retains username, email and id fallback order', () => {
  const record = { username: '用户一', userEmail: 'user@example.test', userId: 'user-1' }
  assert.equal(usageUserDisplay(record).label, '用户一')
  assert.equal(usageUserDisplay({ ...record, username: '' }).label, 'user@example.test')
  assert.equal(usageUserDisplay({ userId: record.userId }).label, 'user-1')
  assert.equal(usageUserDisplay({ ...record, clientTransport: 'maintenance' }).label, '用户一')
})
