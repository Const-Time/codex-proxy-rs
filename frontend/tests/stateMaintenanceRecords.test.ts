import assert from 'node:assert/strict'
// eslint-disable-next-line test/no-import-node-test -- Existing native Node test runner.
import test from 'node:test'
import { decisionLabel, egressLabel, egressRegion, ratio, recordRange } from '../src/views/turn-state/records.ts'

test('egress snapshots never imply that a request used the detected IP', () => {
  const egress = { ip: '203.0.113.1', detectedAt: 1, source: 'independent_proxy_test', rotating: false, location: null }
  assert.equal(egressLabel(egress), '独立检测快照')
  assert.equal(egressLabel({ ...egress, rotating: true }), '轮换出口参考')
  assert.equal(egressLabel({ ...egress, ip: null }), '检测未确认')
  assert.equal(egressLabel(null), '出口未记录')
  assert.equal(egressRegion(egress), '归属地区未知')
  assert.equal(egressRegion({ ...egress, location: { country: 'SG', region: 'Singapore', city: 'Singapore', timezone: 'Asia/Singapore' } }), 'SG · Singapore')
})

test('probe decisions distinguish stream success, acceptance and verification, never promise publication', () => {
  assert.equal(decisionLabel('accepted'), '候选入选')
  assert.equal(decisionLabel('verified'), '验证通过')
  assert.equal(decisionLabel('interrupted'), '结果未确认')
  assert.equal(decisionLabel('legacy'), '历史结果未记录')
  assert.equal(decisionLabel('unknown'), '结果未知')
  assert.equal(ratio(0, 0), '—')
  assert.equal(ratio(2, 3), '66.7%')
  const range = recordRange(24, Date.parse('2026-09-19T00:00:00Z'))
  assert.deepEqual(range, { from: '2026-09-18T00:00:00.000Z', to: '2026-09-19T00:00:00.000Z' })
})
