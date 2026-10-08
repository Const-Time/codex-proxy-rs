import type { ModelPacket } from '../src/views/plugins/management/models.ts'
import assert from 'node:assert/strict'
import { Buffer } from 'node:buffer'
// eslint-disable-next-line test/no-import-node-test -- Native Node test runner.
import test from 'node:test'
import { PluginModelBridge } from '../src/views/plugins/management/models.ts'

const input = { clientKeyId: 'own-key', body: '{"model":"model-a","input":"hello"}' }

test('only explicitly selected Key may reach the host, and invalid bodies fail before fetch', async () => {
  const packets: ModelPacket[] = []
  let calls = 0
  const bridge = new PluginModelBridge('own-key', async () => {
    calls++
    return new Response()
  }, packet => packets.push(packet))
  try {
    for (const payload of [{ ...input, clientKeyId: 'other' }, { ...input, body: '[]' }, { ...input, body: '{' }, { ...input, body: 'x'.repeat(8 * 1024 * 1024 + 1) }])
      await bridge.request(packets.length + 1, payload)
    assert.equal(calls, 0)
    assert.equal(packets.length, 4)
    assert(packets.every(packet => packet.type === 'models-error'))
  }
  finally { bridge.dispose() }
})

test('SDK credits bound chunks and preserve the complete response without reading ahead', async () => {
  const packets: ModelPacket[] = []
  let reads = 0
  const expected = new Uint8Array(150_000).map((_, index) => index % 251)
  const bridge = new PluginModelBridge('own-key', async () => new Response(new ReadableStream({
    pull(controller) {
      reads++
      controller.enqueue(expected)
      controller.close()
    },
  }, { highWaterMark: 0 }), { headers: { 'content-type': 'text/event-stream' } }), packet => packets.push(packet))
  try {
    await bridge.request(1, input)
    assert.equal(reads, 0)
    assert.equal(packets[0]?.type, 'models-response')
    for (let pullId = 1; pullId <= 4; pullId++)
      await bridge.pull(1, pullId)
    const chunks = packets.filter(packet => packet.type === 'models-chunk').map(packet => (packet.payload as { body: ArrayBuffer }).body)
    assert(chunks.every(chunk => chunk.byteLength <= 65_536))
    assert.deepEqual(Buffer.concat(chunks.map(chunk => Buffer.from(chunk))), Buffer.from(expected))
    assert.equal(packets.at(-1)?.type, 'models-complete')
    assert.equal(reads, 1)
  }
  finally { bridge.dispose() }
})

test('cancel before headers aborts fetch and suppresses late delivery', async () => {
  const packets: ModelPacket[] = []
  let signal: AbortSignal | undefined
  let finish!: (response: Response) => void
  let bodyCancelled = false
  const bridge = new PluginModelBridge('own-key', async (_, current) => {
    signal = current
    return new Promise((resolve) => {
      finish = resolve
    })
  }, packet => packets.push(packet))
  const pending = bridge.request(1, input)
  bridge.cancel(1)
  assert.equal(signal?.aborted, true)
  finish(new Response(new ReadableStream({ cancel() {
    bodyCancelled = true
  } })))
  await pending
  assert.deepEqual(packets, [])
  assert.equal(bodyCancelled, true)
  bridge.dispose()
})

test('duplicate pull while reading cancels upstream and returns a single error', async () => {
  const packets: ModelPacket[] = []
  let cancelled = false
  const bridge = new PluginModelBridge('own-key', async () => new Response(new ReadableStream({ cancel() {
    cancelled = true
  } })), packet => packets.push(packet))
  await bridge.request(1, input)
  const pending = bridge.pull(1, 1)
  await bridge.pull(1, 1)
  await pending
  assert.equal(cancelled, true)
  assert.deepEqual(packets.map(packet => packet.type), ['models-response', 'models-error'])
  bridge.dispose()
})

test('closing a page aborts all admitted requests and prevents subsequent work', async () => {
  const signals: AbortSignal[] = []
  const packets: ModelPacket[] = []
  const bridge = new PluginModelBridge('own-key', async (_, signal) => {
    signals.push(signal)
    return new Response(new ReadableStream())
  }, packet => packets.push(packet))
  for (let id = 1; id <= 5; id++)
    await bridge.request(id, input)
  assert.equal(signals.length, 4)
  assert.equal(packets.at(-1)?.type, 'models-error')
  bridge.dispose()
  assert(signals.every(signal => signal.aborted))
  await bridge.request(6, input)
  assert.equal(signals.length, 4)
})
