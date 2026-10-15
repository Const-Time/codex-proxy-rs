import assert from 'node:assert/strict'
// eslint-disable-next-line test/no-import-node-test -- Pure presentation regressions use the native Node runner.
import test from 'node:test'
import { serviceTierLabel, serviceTierSummary } from '../src/views/usage/utils/serviceTier.ts'

test('requested Fast does not overwrite upstream Standard', () => {
  assert.equal(serviceTierSummary({ requestedServiceTier: 'priority', upstreamServiceTier: 'default' }), '请求 Fast · 上游 Standard')
  assert.equal(serviceTierSummary({ requestedServiceTier: 'priority', upstreamServiceTier: 'priority' }), '请求 Fast · 上游 Fast')
})

test('missing and auto responses are not invented as confirmed tiers', () => {
  assert.equal(serviceTierSummary({}), null)
  assert.equal(serviceTierSummary({ requestedServiceTier: 'priority' }), '请求 Fast · 上游 未记录')
  assert.equal(serviceTierSummary({ requestedServiceTier: 'priority', upstreamServiceTier: 'auto' }), '请求 Fast · 上游 Auto（未确认）')
  assert.equal(serviceTierLabel('new-tier'), 'new-tier')
})
