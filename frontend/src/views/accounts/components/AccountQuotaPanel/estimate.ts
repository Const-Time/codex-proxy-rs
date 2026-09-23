interface Estimate {
  cycleMissingCostCount?: number
  cycleMissingBilledCount?: number
  costIncomplete?: boolean
  billedCostIncomplete?: boolean
  sampleUsd?: string | null
  sampleBilledUsd?: string | null
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

export function estimateCostCoverage(estimate?: Partial<Estimate> | null) {
  const raw = Boolean(estimate?.costIncomplete || (estimate?.missingCostCount ?? 0) > 0 || (estimate?.cycleMissingCostCount ?? 0) > 0)
  const billed = estimate?.billedCostIncomplete ?? Boolean((estimate?.cycleMissingBilledCount ?? 0) > 0 || raw)
  return { raw, billed }
}

export function estimateCostDisplay(value: string | null | undefined, partial: boolean) {
  if (value == null || value === '' || !Number.isFinite(Number(value)) || Number(value) < 0)
    return '待估算'
  return `${partial ? '部分估算 ' : ''}${Number(value).toLocaleString('en-US', { style: 'currency', currency: 'USD', minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
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
  const coverage = estimateCostCoverage(estimate)
  const partial = coverage.raw || coverage.billed
  const sampleUsd = 'sampleUsd' in estimate ? estimate.sampleUsd : estimate.usedUsd
  const sampleBilledUsd = 'sampleBilledUsd' in estimate ? estimate.sampleBilledUsd : estimate.billedUsedUsd
  const stale = hint?.startsWith('保留上次估算') ?? false
  const lowSample = estimate.lowSample ?? estimate.percentDelta < 10
  const warnings = [
    stale ? `${hint}；估算截至 ${estimate.sampleEnd}` : '',
    (estimate.missingCostCount ?? 0) > 0 ? `采样区间 ${estimate.missingCostCount} / ${estimate.requestCount ?? '—'} 笔费用缺失` : '',
    (estimate.cycleMissingCostCount ?? 0) > 0 ? `本周期累计 ${estimate.cycleMissingCostCount} 笔原始费用缺失` : '',
    (estimate.cycleMissingBilledCount ?? 0) > 0 ? `本周期累计 ${estimate.cycleMissingBilledCount} 笔倍率后费用缺失` : '',
    partial ? '部分费用仅按已知金额外推，不代表完整额度或保证下限；缺失费用未按零计价，不修改历史账单' : '',
    lowSample ? '样本较少，继续积累至 10 个百分点及两个完整分段后再参考' : '',
    estimate.incompleteTokens ? '部分 Token 缺失，Token 估值可能偏低' : '',
    hint && !stale ? hint : '',
  ].filter(Boolean)
  return {
    label: `${method} · ${stale ? '上次结果' : partial ? '部分费用估算' : lowSample ? '初步估算' : '参考估算'}`,
    basis: `${estimate.cycleBased ? '累计' : '采样'}原始 ${sampleUsd != null ? `$${sampleUsd}` : '待补算'} · 计费 ${sampleBilledUsd != null ? `$${sampleBilledUsd}` : '待补算'} / ${estimate.cycleBased ? '已用 ' : '上涨 '}${Number(estimate.percentDelta.toFixed(2))}${estimate.cycleBased ? '%' : ' 个百分点'}${estimate.blockCount ? ` · ${estimate.blockCount} 个完整分段` : ''}`,
    warning: warnings.join('；'),
    title: `${method}范围 ${estimate.sampleStart} 至 ${estimate.sampleEnd}。预计总额 = 本周期已知累计费用 + 采样已知费用 × 剩余百分点 / 采样上涨百分点。倍率后使用每笔请求的历史倍率。周期估算以账号在周期开始前已接入为前提，无法证明记录完整或没有站外使用；日志缺失、跨边界请求、上游比例延迟及模型组合变化都会影响估值。仅为本地费用折算，非上游承诺额度。`,
  }
}
