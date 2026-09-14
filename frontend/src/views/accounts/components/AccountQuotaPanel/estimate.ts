interface Estimate {
  cycleBased?: boolean
  missingCostCount?: number
  requestCount?: number
  usedUsd: string
  billedUsedUsd?: string | null
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
      title: '按本服务记录估算。周期开始前已接入的账号使用周期累计费用，中途接入的账号使用采样区间；后台每轮间隔 60 秒读取已保存快照。',
    }
  }
  const method = estimate.cycleBased ? '周期估算' : '区间估算'
  const partial = (estimate.missingCostCount ?? 0) > 0
  const stale = hint?.startsWith('保留上次估算') ?? false
  return {
    label: `${method} · ${stale ? '上次结果' : partial ? '记录不完整' : estimate.percentDelta < 5 ? '初步估算' : '参考估算'}`,
    basis: `${estimate.cycleBased ? '累计' : '采样'}原始 $${estimate.usedUsd} · 计费 ${estimate.billedUsedUsd != null ? `$${estimate.billedUsedUsd}` : '待补算'} / ${estimate.cycleBased ? '已用 ' : '上涨 '}${Number(estimate.percentDelta.toFixed(2))}${estimate.cycleBased ? '%' : ' 个百分点'}`,
    warning: stale
      ? `${hint}；估算截至 ${estimate.sampleEnd}`
      : partial
        ? `记录不完整：${estimate.missingCostCount} / ${estimate.requestCount ?? '—'} 笔费用缺失，估值可能偏低`
        : estimate.percentDelta < 5
          ? '样本较少，继续积累至 5 个百分点后再参考'
          : '',
    title: `${method}范围 ${estimate.sampleStart} 至 ${estimate.sampleEnd}。对应原始费用或倍率后计费 ÷ (使用比例 / 100)。倍率后使用每笔请求的历史倍率。周期估算以账号在周期开始前已接入为前提，无法证明记录完整或没有站外使用；日志缺失、跨边界请求、上游比例延迟及模型组合变化都会影响估值。仅为本地费用折算，非上游承诺额度。`,
  }
}
