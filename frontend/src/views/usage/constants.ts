import type { UsageDisplayRecord } from './utils/records'
import type { OpsError } from '@/api'
import { defineTableColumns } from '@/components/base/BaseTable/columns'
import { formatProvider } from './utils/format'

type OpsErrorRow = OpsError

export const usageRecordColumns = defineTableColumns<UsageDisplayRecord>([
  { key: 'userEmail', label: '用户', kind: 'custom', size: '3xl' },
  {
    key: 'accountEmail',
    label: '账号',
    kind: 'identity',
    size: '3xl',
  },
  {
    key: 'provider',
    label: '平台/类型',
    kind: 'status',
    size: 'sm',
    format: (value: unknown) => formatProvider(typeof value === 'string' ? value : null),
  },
  { key: 'model', label: '模型', kind: 'custom', size: 'xl' },
  { key: 'reasoningEffort', label: '推理强度', kind: 'status', size: 'lg' },
  { key: 'route', label: '端点', kind: 'mono' },
  { key: 'upstreamTransport', label: '上游', kind: 'status', size: 'md' },
  { key: 'clientTransport', label: '接入', kind: 'status', size: 'md' },
  { key: 'tokenDetails', label: 'TOKEN', kind: 'numeric', size: 'xl' },
  { key: 'billing', label: '费用', kind: 'numeric', size: 'lg' },
  { key: 'latency', label: '延迟', kind: 'numeric', size: 'xl' },
  { key: 'generationRate', label: '速率', kind: 'numeric', size: 'lg' },
  { key: 'createdAtDisplay', label: '时间', kind: 'datetime' },
  { key: 'turnState', label: 'TURN-STATE', kind: 'custom', size: '4xl' },
  { key: 'clientIp', label: 'IP', kind: 'custom', size: '3xl' },
  { key: 'userAgent', label: 'User-Agent', kind: 'custom', size: '4xl' },
  { key: 'actions', label: '操作', kind: 'actions', size: 'sm' },
])

export const opsErrorColumns = defineTableColumns<OpsErrorRow>([
  { key: 'message', label: '错误', kind: 'custom', size: '2xl', mobileSize: 'md', sticky: 'left', fill: true, hideable: false },
  { key: 'createdAtDisplay', label: '时间', kind: 'datetime', size: 'md', sticky: 'left', hideable: false },
  { key: 'clientApiKeyId', label: '密钥 ID', kind: 'mono', size: 'lg', defaultHidden: true },
  { key: 'userEmail', label: '用户', kind: 'custom', size: 'lg', defaultHidden: true },
  { key: 'accountId', label: '账号', kind: 'identity', size: 'xl', emptyText: '未知账号' },
  { key: 'provider', label: '平台/类型', kind: 'custom', size: 'sm' },
  { key: 'model', label: '模型', kind: 'custom', size: 'lg', emptyText: '未记录模型' },
  { key: 'reasoningEffort', label: '推理强度', kind: 'mono', size: 'md' },
  { key: 'upstreamSendState', label: '发送状态', kind: 'custom', size: 'lg', defaultHidden: true },
  { key: 'route', label: '端点', kind: 'mono', size: 'xl', defaultHidden: true },
  { key: 'requestId', label: '请求 ID', kind: 'mono', size: '2xl', defaultHidden: true },
  { key: 'clientIp', label: 'IP', kind: 'custom', size: '3xl', defaultHidden: true },
  { key: 'userAgent', label: 'User-Agent', kind: 'custom', size: '4xl', defaultHidden: true },
  { key: 'actions', label: '操作', kind: 'actions', size: 'xs', hideable: false },
])

export const usageTimeRangeOptions = [
  { label: '今天', value: 'today' },
  { label: '最近 7 天', value: '7d' },
  { label: '最近 30 天', value: '30d' },
  { label: '自定义范围', value: 'custom' },
]
