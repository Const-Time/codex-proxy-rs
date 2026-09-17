import type { UsageListRecord } from '@/api'

export function usageModelDisplay(record: Pick<UsageListRecord, 'model' | 'requestedModel' | 'upstreamModel' | 'upstreamResponseModel'>) {
  const requestedModel = record.requestedModel || ''
  const upstreamModel = record.upstreamModel || ''
  const storedModel = record.model || ''
  const primary = requestedModel || storedModel || upstreamModel || '—'
  const secondary
    = upstreamModel && upstreamModel !== primary
      ? upstreamModel
      : requestedModel && storedModel && storedModel !== requestedModel
        ? storedModel
        : ''

  const responseModel = record.upstreamResponseModel || ''
  const returned = responseModel && responseModel !== primary ? responseModel : ''
  const routes = []
  if (secondary && secondary !== returned) {
    routes.push({
      model: secondary,
      kind: 'mapped' as const,
      description: `网关映射后发送给上游的模型：${secondary}`,
    })
  }
  if (returned) {
    routes.push({
      model: returned,
      kind: 'returned' as const,
      description: returned === secondary
        ? `上游返回模型：${returned}（与网关映射后发送的模型一致）`
        : `上游返回模型：${returned}`,
    })
  }

  return { primary, secondary, routes }
}
