import assert from 'node:assert/strict'
// eslint-disable-next-line test/no-import-node-test -- Use the existing native Node test runner.
import test from 'node:test'
import { turnStateDisplay, turnStateSourceDisplay } from '../src/views/usage/utils/turnState.ts'

test('missing and legacy turn-state values render an empty cell', () => {
  for (const value of [undefined, null, '', '  ', 292, {}]) {
    assert.deepEqual(turnStateDisplay(value), { token: '', length: 0 })
  }
})

test('character counts use the complete token, which remains available for copying', () => {
  for (const length of [292, 312, 332, 356]) {
    const token = `gAAAA${'a'.repeat(length - 5)}`
    assert.deepEqual(turnStateDisplay(token), { token, length })
    assert.deepEqual(turnStateDisplay(` ${token} `), { token, length })
  }
  assert.equal(turnStateDisplay('a😀').length, 2)
})

test('request fallback is distinguished from returned and historical response values', () => {
  assert.equal(turnStateSourceDisplay('request').label, '请求')
  for (const source of ['response', null, undefined]) {
    assert.equal(turnStateSourceDisplay(source).label, '返回')
  }
  assert.equal(turnStateSourceDisplay('future-source').label, '未知')
})
