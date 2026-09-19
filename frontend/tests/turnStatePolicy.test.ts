import assert from 'node:assert/strict'
// eslint-disable-next-line test/no-import-node-test -- Use the existing native Node test runner.
import test from 'node:test'
import { exactModels, remainingLabel, shapeLabel, takeoverPolicyLabel, waitReasonLabel } from '../src/views/turn-state/presenter.ts'

test('takeover policy labels describe HTTP and WS eligibility rather than per-request injection', () => {
  assert.equal(takeoverPolicyLabel(true, true), 'HTTP / WS 接管已开启')
  assert.equal(takeoverPolicyLabel(true, false), 'HTTP / WS 接管待全局启用')
  assert.equal(takeoverPolicyLabel(false, true), 'HTTP / WS 接管已关闭')
  assert.equal(takeoverPolicyLabel(false, false), 'HTTP / WS 接管已关闭')
})

test('state maintenance accepts personal and team candidate shapes without capability labels', () => {
  for (const [headerLength, blocks] of [[292, 10], [332, 12]]) {
    assert.equal(shapeLabel({ headerLength, blocks, issuedAt: 1, totalBytes: 1, cipherBytes: 1 }, { headerLength: 0, cipherBlocks: 0 }), '符合候选形态')
  }
  assert.equal(shapeLabel({ headerLength: 312, blocks: 11, issuedAt: 1, totalBytes: 1, cipherBytes: 1 }, { headerLength: 0, cipherBlocks: 0 }), '观测形态（不判定能力）')
  assert.equal(remainingLabel(100, 101), '已过期 · 不再注入')
  assert.deepEqual(exactModels('gpt-5.4,gpt-5.4 gpt-5.5'), ['gpt-5.4', 'gpt-5.5'])
  assert.throws(() => exactModels('gpt-*'))
})

test('state maintenance distinguishes idle, budget and upstream cooldown', () => {
  assert.equal(waitReasonLabel('idle'), '无近期业务，等待流量')
  assert.equal(waitReasonLabel('budget'), '小时预算已用完')
  assert.equal(waitReasonLabel('upstream_cooldown'), '等待上游冷却')
  assert.notEqual(waitReasonLabel('account_busy'), waitReasonLabel('paused'))
  assert.equal(waitReasonLabel('future_reason'), 'future_reason')
})
