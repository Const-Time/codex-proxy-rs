import assert from 'node:assert/strict'
// eslint-disable-next-line test/no-import-node-test -- Use the existing native Node test runner.
import test from 'node:test'
import { exactModels, maintenanceModels, remainingLabel, shapeLabel, takeoverPolicyLabel, waitReasonLabel } from '../src/views/turn-state/presenter.ts'

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

test('state maintenance explains individual candidate rejection reasons', () => {
  assert.equal(waitReasonLabel('state_missing'), '上游未返回 State')
  assert.equal(waitReasonLabel('state_shape_mismatch'), '候选长度规则不匹配')
  assert.equal(waitReasonLabel('state_ttl'), '候选剩余时间不足')
  assert.equal(waitReasonLabel('state_structure'), 'State 结构无法解析')
  assert.equal(waitReasonLabel('state_future'), '候选签发时间超前')
  assert.equal(waitReasonLabel('state_duplicate'), '未获取到新的 State')
  assert.notEqual(waitReasonLabel('account_busy'), waitReasonLabel('storage_unavailable'))
})

test('maintenance model choices use exact account catalog IDs and account access rules', () => {
  const catalog = [
    { id: 'gpt-5.6-sol', label: 'Sol' },
    { id: 'gpt-6-astra', label: 'gpt-6-astra' },
    { id: 'gpt-5.6-sol', label: 'Duplicate' },
    { id: 'gpt-image-2', label: 'Image' },
    { id: 'gpt-*', label: 'Wildcard' },
    { id: 'invalid model', label: 'Invalid' },
    { id: '', label: 'Empty' },
    { id: 'x'.repeat(129), label: 'Too long' },
  ]
  assert.deepEqual(maintenanceModels(catalog, { mode: 'all', models: [] }), [
    { value: 'gpt-5.6-sol', label: 'Sol · gpt-5.6-sol' },
    { value: 'gpt-6-astra', label: 'gpt-6-astra' },
  ])
  assert.deepEqual(maintenanceModels(catalog, { mode: 'allowlist', models: ['gpt-6-astra', 'not-in-catalog'] }), [
    { value: 'gpt-6-astra', label: 'gpt-6-astra' },
  ])
  assert.deepEqual(maintenanceModels(catalog, { mode: 'denylist', models: ['gpt-5.6-sol'] }), [
    { value: 'gpt-6-astra', label: 'gpt-6-astra' },
  ])
  assert.deepEqual(maintenanceModels([], { mode: 'all', models: [] }), [])
})
