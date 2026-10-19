import assert from 'node:assert/strict'
// eslint-disable-next-line test/no-import-node-test -- Native Node test runner.
import { test } from 'node:test'
import { customUsageRange, usageDateBounds } from '../src/views/usage/utils/dateRange.ts'

test('custom usage dates include the full end day and clamp today to now', () => {
  const now = new Date(2026, 9, 19, 12, 30)
  assert.deepEqual(usageDateBounds(now), { min: '2026-09-19', max: '2026-10-19' })
  assert.deepEqual(customUsageRange('2026-10-10', '2026-10-15', now), {
    startTime: new Date(2026, 9, 10).toISOString(),
    endTime: new Date(2026, 9, 16).toISOString(),
  })
  assert.equal(customUsageRange('2026-10-19', '2026-10-19', now)?.endTime, now.toISOString())
})

test('invalid, reversed, expired and future dates do not produce queries', () => {
  const now = new Date(2026, 2, 10, 12)
  for (const [start, end] of [['2026-02-30', '2026-03-01'], ['2026-03-02', '2026-03-01'], ['2026-01-01', '2026-03-01'], ['2026-03-01', '2026-03-11'], ['', '2026-03-01']])
    assert.equal(customUsageRange(start!, end!, now), null)
})

test('calendar end includes a DST transition day without adding a fixed 24 hours', () => {
  const previous = process.env.TZ
  process.env.TZ = 'America/Los_Angeles'
  try {
    const range = customUsageRange('2026-03-08', '2026-03-08', new Date(2026, 2, 10, 12))!
    assert.equal(Date.parse(range.endTime) - Date.parse(range.startTime), 23 * 60 * 60 * 1000)
  }
  finally {
    if (previous === undefined)
      delete process.env.TZ
    else process.env.TZ = previous
  }
})
