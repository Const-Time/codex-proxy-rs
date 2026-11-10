import assert from 'node:assert/strict'
// eslint-disable-next-line test/no-import-node-test -- Use the existing native Node test runner.
import test from 'node:test'
import { parseQuotaUsageLimits } from '../src/views/accounts/utils/quotaUsageLimits.ts'

test('quota usage limits accept independent integer caps and clear to defaults', () => {
  assert.deepEqual(parseQuotaUsageLimits('90', '80'), { shortTermPercent: 90, weeklyPercent: 80 })
  assert.deepEqual(parseQuotaUsageLimits('', ' '), { shortTermPercent: 100, weeklyPercent: 100 })
  for (const invalid of ['0', '-1', '101', '90.5', 'NaN', 'Infinity']) {
    assert.equal(parseQuotaUsageLimits(invalid, '80'), null)
    assert.equal(parseQuotaUsageLimits('90', invalid), null)
  }
})
