import type { RequestTraceEvent } from '@/api'

function record(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : {}
}

function text(value: unknown) {
  return typeof value === 'string' && value.length > 0 ? value : '—'
}

const modeLabels: Record<string, string> = { auto: '自动检测', manual: '手动指定', passthrough: '透传客户端' }
const transportLabels: Record<string, string> = { http_sse: 'HTTP SSE', http_json: 'HTTP JSON', websocket: 'WebSocket' }
const headerLabels: Record<string, string> = {
  'user-agent': '上游 User-Agent',
  'version': '上游客户端版本',
  'originator': '客户端来源',
  'openai-beta': 'OpenAI Beta',
  'sec-websocket-extensions': '请求压缩协商',
  'content-type': '内容类型',
  'content-encoding': '内容编码',
}

export function requestProfiles(events: RequestTraceEvent[]) {
  const profiles = events.filter(event => event.stage === 'upstream.request.profile')
  return profiles.map((event, index) => {
    const data = event.data
    const proxy = record(data.proxy)
    const location = record(data.effectiveLocation)
    const applied = [...events].reverse().find(item => item.stage === 'upstream.location.applied'
      && item.attemptIndex === event.attemptIndex && item.sequence < event.sequence)
    const changes = record(applied?.data.changes)
    const subsequent = events.filter(item => item.attemptIndex === event.attemptIndex
      && item.sequence > event.sequence && item.sequence < (profiles[index + 1]?.sequence ?? Infinity)
      && (event.exchangeId == null || event.exchangeId === item.exchangeId))
    const connection = subsequent.find(item => item.stage === 'upstream.connection')
    const sent = subsequent.some(item => item.stage === 'upstream.payload.sent')
    const response = subsequent.some(item => item.stage === 'upstream.response.headers')
    const items: Array<{ label: string, value: string, mono?: boolean, fullWidth?: boolean }> = [
      { label: '上游传输', value: transportLabels[text(data.transport)] ?? text(data.transport) },
      { label: '发送观测', value: sent ? '业务报文已发送' : response ? '已收到上游响应头' : '仅记录发送前参数；未确认发送成功' },
      { label: '出口方式', value: data.viaProxy === true ? '绑定代理' : data.viaProxy === false ? '直连' : '未记录' },
      { label: '代理 ID', value: text(proxy.proxyId), mono: true },
      { label: '地区策略', value: modeLabels[text(proxy.mode)] ?? (data.viaProxy ? '未记录来源' : '保留客户端') },
      { label: '上次检测出口 IP', value: text(proxy.detectedIp), mono: true },
      { label: '出口检测时间', value: text(proxy.detectedAt), mono: true },
      { label: '代理提供的地区', value: [location.country, location.region, location.city].filter(value => typeof value === 'string').join(' / ') || '无覆盖值' },
      { label: '代理提供的时区', value: text(location.timezone), mono: true },
      { label: '实际改写', value: applied ? `环境信息 ${Number(changes.environmentChanged) || 0} 处；搜索位置 ${Number(changes.searchChanged) || 0} 处` : '此调用未记录地区改写' },
      { label: '请求压缩方式', value: text(data.compression), mono: true },
    ]
    if (data.truncated === true)
      items.unshift({ label: '采集状态', value: '快照超过保存上限，仅保留摘要', fullWidth: true })
    if (connection) {
      items.push({ label: 'WebSocket 连接', value: connection.data.reused === true ? '复用已有连接' : connection.data.reused === false ? '新连接' : '未记录' })
      items.push({ label: '连接 ID', value: text(connection.data.connectionId), mono: true })
    }
    for (const [name, label] of Object.entries(headerLabels)) {
      const value = record(data.headers)[name]
      if (typeof value === 'string')
        items.push({ label, value, mono: true, fullWidth: name === 'user-agent' })
    }
    const structured = record(data.structuredLocation)
    for (const [key, label] of [['environments', '报文环境'], ['searches', '报文搜索位置']] as const) {
      const values = structured[key]
      if (Array.isArray(values)) {
        values.forEach((value, i) => {
          const fields = record(value)
          items.push({
            label: `${label} ${i + 1}`,
            value: Object.entries(fields).map(([name, value]) => `${name}: ${text(value)}`).join(' · ') || '未携带位置字段',
            mono: true,
            fullWidth: true,
          })
        })
      }
    }
    for (const [name, value] of Object.entries(record(data.identifierDigests))) {
      const digest = text(record(value).sha256)
      items.push({ label: `${name} · SHA-256`, value: digest, mono: true, fullWidth: true })
    }
    for (const [name, value] of Object.entries(record(data.sensitiveFieldsPresent)))
      items.push({ label: `${name} · 仅记录存在性`, value: value === true ? '已携带（不保存原值）' : '未携带' })
    return { sequence: event.sequence, attemptIndex: event.attemptIndex, elapsedMs: event.elapsedMs, items }
  })
}
