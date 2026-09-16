import type { RequestTraceEvent } from '../src/api/modules/usage.ts'
import assert from 'node:assert/strict'
// eslint-disable-next-line test/no-import-node-test -- Use the existing native Node test runner.
import test from 'node:test'
import { proxyLocalTime, requestProfiles } from '../src/views/usage/utils/requestProfile.ts'

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

test('proxy detection time uses the recorded timezone across midnight without mutating the snapshot', () => {
  const data = { ...profile, proxy: { ...profile.proxy, detectedAt: '2026-09-15 15:34:13.541689+00' }, effectiveLocation: { timezone: 'Asia/Tokyo' } }
  const original = JSON.stringify(data)
  const [row] = requestProfiles([event(1, 1, 'upstream.request.profile', data)])
  assert.equal(row!.items.find(item => item.label === '上次出口检测时间')!.value, '2026-09-16 00:34:13（Asia/Tokyo，UTC+09:00）')
  assert.equal(JSON.stringify(data), original)
  assert.equal(proxyLocalTime('2026-09-16T00:34:13+09:00', 'Asia/Tokyo'), '2026-09-16 00:34:13（Asia/Tokyo，UTC+09:00）')
})

test('offset labels follow daylight saving and fractional timezone offsets', () => {
  assert.equal(proxyLocalTime('2026-07-15T12:00:00Z', 'America/New_York'), '2026-07-15 08:00:00（America/New_York，UTC-04:00）')
  assert.equal(proxyLocalTime('2026-01-15T12:00:00Z', 'America/New_York'), '2026-01-15 07:00:00（America/New_York，UTC-05:00）')
  assert.equal(proxyLocalTime('2026-09-15T12:00:00Z', 'Asia/Kolkata'), '2026-09-15 17:30:00（Asia/Kolkata，UTC+05:30）')
  assert.equal(proxyLocalTime('2026-09-15T12:00:00Z', 'UTC'), '2026-09-15 12:00:00（UTC，UTC+00:00）')
})

test('request time is distinct from detection time and follows each attempt snapshot', () => {
  const rows = requestProfiles([
    event(1, 1, 'upstream.request.profile', {
      ...profile,
      proxy: { ...profile.proxy, detectedAt: '2026-09-15 15:34:13.541689+00' },
      effectiveLocation: { timezone: 'Asia/Tokyo' },
    }),
    event(2, 2, 'upstream.request.profile', profile),
  ], '2026-09-16T01:17:23Z')
  const field = (index: number, label: string) => rows[index]!.items.find(item => item.label === label)?.value
  assert.equal(field(0, '请求时间（代理时区）'), '2026-09-16 10:17:23（Asia/Tokyo，UTC+09:00）')
  assert.equal(field(0, '上次出口检测时间'), '2026-09-16 00:34:13（Asia/Tokyo，UTC+09:00）')
  assert.equal(field(1, '请求时间（代理时区）'), '2026-09-15 18:17:23（America/Los_Angeles，UTC-07:00）')
  const [missing] = requestProfiles([event(1, 1, 'upstream.request.profile', profile)])
  assert.equal(missing!.items.find(item => item.label === '请求时间（代理时区）')!.value, '—')
})

test('missing or invalid timestamps and timezones never invent a local time', () => {
  assert.equal(proxyLocalTime(null, 'Asia/Tokyo'), '—')
  assert.equal(proxyLocalTime('2026-09-15 15:34:13+00', null), '2026-09-15 15:34:13+00（原始时间；未记录代理时区）')
  assert.equal(proxyLocalTime('2026-09-15 15:34:13+00', 'invalid/zone'), '2026-09-15 15:34:13+00（原始时间；代理时区不可识别）')
  assert.equal(proxyLocalTime('2026-09-15 15:34:13', 'Asia/Tokyo'), '2026-09-15 15:34:13（原始时间；缺少 UTC 偏移）')
  assert.match(proxyLocalTime('invalidZ', 'Asia/Tokyo'), /时间格式不可识别/)
})
