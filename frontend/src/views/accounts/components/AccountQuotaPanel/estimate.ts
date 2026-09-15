interface Estimate {
  cycleBased?: boolean
  missingCostCount?: number
  requestCount?: number
  usedUsd: string | null
  billedUsedUsd?: string | null
  blockCount?: number
  lowSample?: boolean
  incompleteTokens?: boolean
  percentDelta: number
  sampleStart: string
  sampleEnd: string
}

export function quotaEstimatePresentation(estimate?: Estimate | null, hint?: string | null) {
  if (!estimate) {
    return {
      label: '待估算',
      basis: hint ?? '等待有效额度快照及消费记录',
      warning: '',
      title: '按本服务的历史请求和额度观测配对估算。优先使用最近三个完整的 5 个百分点分段及尾段；没有足够分段时，仅对周期开始前接入的账号使用周期累计数据。不会查询上游或改变账单。',
    }
  }
  const method = estimate.cycleBased ? '周期估算' : '区间估算'
  const partial = (estimate.missingCostCount ?? 0) > 0
  const stale = hint?.startsWith('保留上次估算') ?? false
  const lowSample = estimate.lowSample ?? estimate.percentDelta < 10
  const warnings = [
    stale ? `${hint}；估算截至 ${estimate.sampleEnd}` : '',
    partial ? `记录不完整：${estimate.missingCostCount} / ${estimate.requestCount ?? '—'} 笔费用缺失，费用估算等待补齐` : '',
    lowSample ? '样本较少，继续积累至 10 个百分点及两个完整分段后再参考' : '',
    estimate.incompleteTokens ? '部分 Token 缺失，Token 估值可能偏低' : '',
    hint && !stale ? hint : '',
  ].filter(Boolean)
  return {
    label: `${method} · ${stale ? '上次结果' : partial ? '记录不完整' : lowSample ? '初步估算' : '参考估算'}`,
    basis: `${estimate.cycleBased ? '累计' : '采样'}原始 ${estimate.usedUsd != null ? `$${estimate.usedUsd}` : '待补算'} · 计费 ${estimate.billedUsedUsd != null ? `$${estimate.billedUsedUsd}` : '待补算'} / ${estimate.cycleBased ? '已用 ' : '上涨 '}${Number(estimate.percentDelta.toFixed(2))}${estimate.cycleBased ? '%' : ' 个百分点'}${estimate.blockCount ? ` · ${estimate.blockCount} 个完整分段` : ''}`,
    warning: warnings.join('；'),
    title: `${method}范围 ${estimate.sampleStart} 至 ${estimate.sampleEnd}。对应原始费用或倍率后计费 ÷ (使用比例 / 100)。倍率后使用每笔请求的历史倍率。周期估算以账号在周期开始前已接入为前提，无法证明记录完整或没有站外使用；日志缺失、跨边界请求、上游比例延迟及模型组合变化都会影响估值。仅为本地费用折算，非上游承诺额度。`,
  }
}
