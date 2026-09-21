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

test('personal usage shows the routed model beneath the requested model', () => {
  for (const upstreamResponseModel of [null, 'gpt-5.6-luna']) {
    const display = usageModelDisplay({
      requestedModel: 'codex-auto-review',
      model: 'gpt-5.6-luna',
      upstreamModel: 'gpt-5.6-luna',
      upstreamResponseModel,
    })
    assert.equal(display.primary, 'codex-auto-review')
    assert.equal(display.secondary, 'gpt-5.6-luna')
    assert.deepEqual(display.routes.map(route => [route.kind, route.model]), [
      [upstreamResponseModel ? 'returned' : 'mapped', 'gpt-5.6-luna'],
    ])
  }
})

test('personal usage does not show a routing arrow when the model is unchanged', () => {
  const display = usageModelDisplay({
    requestedModel: 'gpt-5.6-sol',
    model: 'gpt-5.6-sol',
    upstreamModel: 'gpt-5.6-sol',
    upstreamResponseModel: 'gpt-5.6-sol',
  })
  assert.equal(display.primary, 'gpt-5.6-sol')
  assert.equal(display.secondary, '')
  assert.deepEqual(display.routes, [])
})
