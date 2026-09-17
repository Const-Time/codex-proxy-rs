import assert from 'node:assert/strict'
// eslint-disable-next-line test/no-import-node-test -- Use the existing native Node test runner.
import test from 'node:test'
import { usageModelDisplay } from '../src/views/usage/utils/modelDisplay.ts'

test('request, sent and returned models remain distinct without inventing reports', () => {
  const cases = [
    { sent: 'A', returned: 'B', routes: [['returned', 'B']] },
    { sent: 'A', returned: 'A', routes: [] },
    { sent: 'A', returned: null, routes: [] },
    { sent: 'B', returned: null, routes: [['mapped', 'B']] },
    { sent: 'B', returned: 'C', routes: [['mapped', 'B'], ['returned', 'C']] },
    { sent: 'B', returned: 'B', routes: [['returned', 'B']] },
    { sent: 'B', returned: 'A', routes: [['mapped', 'B']] },
  ]
  for (const { sent, returned, routes } of cases) {
    const record = { requestedModel: 'A', model: 'A', upstreamModel: sent, upstreamResponseModel: returned }
    const display = usageModelDisplay(record)
    assert.equal(display.primary, 'A')
    assert.deepEqual(display.routes.map(route => [route.kind, route.model]), routes)
    assert.equal(record.upstreamResponseModel, returned, 'display must not mutate stored facts')
  }
})

test('mapping and response equality is explained while missing models stay unknown', () => {
  const display = usageModelDisplay({ requestedModel: 'A', model: 'A', upstreamModel: 'B', upstreamResponseModel: 'B' })
  assert.match(display.routes[0]!.description, /与网关映射后发送的模型一致/)
  assert.deepEqual(
    usageModelDisplay({ requestedModel: null, model: null, upstreamModel: null, upstreamResponseModel: null }),
    { primary: '—', secondary: '', routes: [] },
  )
})
