/* eslint-disable test/no-import-node-test */
import assert from 'node:assert/strict'
import { test } from 'node:test'
import { buildCodexConfigFiles } from '../src/views/api-keys/utils/codexConfig.ts'
import { deliveryStateText, errorOriginText, responseStatusText, usageDisplayOutcome } from '../src/views/usage/utils/opsErrorPresentation.ts'

test('stream diagnostics distinguish handshake, delivery and logical outcome', () => {
  assert.equal(responseStatusText(101, 'websocket'), 'WebSocket · 握手 101')
  assert.equal(responseStatusText(null, 'websocket'), 'WebSocket · HTTP 状态未记录')
  assert.equal(responseStatusText(200, 'sse'), 'HTTP 200')
  assert.equal(deliveryStateText(null), '未记录')
  assert.equal(deliveryStateText(false), '未开始交付')
  assert.equal(deliveryStateText(true), '已开始交付')
  assert.equal(usageDisplayOutcome('incomplete', 'invalid_request', 'invalid_request'), 'failed')
  assert.equal(usageDisplayOutcome('incomplete', null, 'timeout'), 'failed')
  assert.equal(usageDisplayOutcome('incomplete', null, 'process_interrupted'), 'incomplete')
  assert.equal(usageDisplayOutcome('incomplete', null, null), 'incomplete')
  assert.equal(usageDisplayOutcome('succeeded', null, null), 'succeeded')
  assert.equal(errorOriginText(undefined), '来源未确定')
  assert.equal(errorOriginText('proxy'), '代理内部处理')
})

test('generated Codex configuration enables discovery while preserving authentication and transport choices', () => {
  const files = buildCodexConfigFiles({ apiKey: 'test-key', baseUrl: 'https://example.test/v1/' })
  assert.match(files.configToml, /api_key_model_discovery = true/)
  assert.match(files.configToml, /supports_websockets = false/)
  assert.match(files.configToml, /experimental_bearer_token = "test-key"/)
  assert.deepEqual(files.auth, { OPENAI_API_KEY: 'test-key' })
  assert.match(buildCodexConfigFiles({ apiKey: 'test-key', baseUrl: 'https://example.test/v1', websocketEnabled: true }).configToml, /supports_websockets = true/)
})
