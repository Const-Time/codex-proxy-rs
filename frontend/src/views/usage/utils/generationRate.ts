interface GenerationTiming {
  outputTokens: number | null
  latencyMs: number | null
  firstTokenMs: number | null
}

/** 与统计吞吐一致：输出 token 数 / 首个输出到完成的时间。 */
export function generationRate({ outputTokens, latencyMs, firstTokenMs }: GenerationTiming): number | null {
  if (outputTokens === null || latencyMs === null || firstTokenMs === null
    || !Number.isFinite(outputTokens) || !Number.isFinite(latencyMs) || !Number.isFinite(firstTokenMs)
    || outputTokens <= 0 || firstTokenMs < 0 || latencyMs <= firstTokenMs) {
    return null
  }
  const rate = outputTokens * 1000 / (latencyMs - firstTokenMs)
  return Number.isFinite(rate) ? rate : null
}
