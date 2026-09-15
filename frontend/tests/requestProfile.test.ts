import type { RequestTraceEvent } from '../src/api/modules/usage.ts'
import assert from 'node:assert/strict'
// eslint-disable-next-line test/no-import-node-test -- Use the existing native Node test runner.
import test from 'node:test'
import { requestProfiles } from '../src/views/usage/utils/requestProfile.ts'

function event(sequence: number, attemptIndex: number, stage: string, data: Record<string, unknown>, exchangeId: number | null = null): RequestTraceEvent {
  return { sequence, lastSequence: sequence, elapsedMs: sequence * 10, lastElapsedMs: sequence * 10, attemptIndex, exchangeId, stage, count: 1, data }
}
const profile = { transport: 'websocket', viaProxy: true, proxy: { proxyId: 'p_one', mode: 'auto', detectedIp: '203.0.113.5' }, effectiveLocation: { timezone: 'America/Los_Angeles' }, headers: { 'user-agent': 'codex/0.154.0' } }

test('profiles keep attempts and fallback send evidence separate', () => {
  const rows = requestProfiles([
    event(1, 1, 'upstream.location.applied', { changes: { environmentChanged: 1, searchChanged: 2 } }),
    event(2, 1, 'upstream.request.profile', profile),
    event(3, 1, 'upstream.request.profile', { ...profile, transport: 'http_sse' }, 1),
    event(4, 1, 'upstream.response.headers', {}, 1),
    event(5, 2, 'upstream.request.profile', { ...profile, proxy: { mode: 'manual' } }),
    event(6, 2, 'upstream.connection', { reused: true, connectionId: 'c_2' }, 2),
    event(7, 2, 'upstream.payload.sent', {}, 2),
  ])
  const field = (index: number, label: string) => rows[index]!.items.find(item => item.label === label)?.value
  assert.equal(field(0, '发送观测'), '仅记录发送前参数；未确认发送成功')
  assert.equal(field(1, '发送观测'), '已收到上游响应头')
  assert.equal(field(2, '发送观测'), '业务报文已发送')
  assert.equal(field(2, 'WebSocket 连接'), '复用已有连接')
  assert.equal(field(2, '实际改写'), '此调用未记录地区改写')
  assert.equal(field(0, '实际改写'), '环境信息 1 处；搜索位置 2 处')
  assert.equal(field(0, '地区策略'), '自动检测')
  assert.equal(field(2, '地区策略'), '手动指定')
})

test('old and truncated traces cannot masquerade as confirmed configuration', () => {
  assert.deepEqual(requestProfiles([]), [])
  const [row] = requestProfiles([event(1, 1, 'upstream.request.profile', { truncated: true })])
  assert.equal(row!.items[0]!.value, '快照超过保存上限，仅保留摘要')
  assert.equal(row!.items.find(item => item.label === '出口方式')!.value, '未记录')
})
