import type { TurnStatePolicy, TurnStateShape } from '../../api/modules/turn-state'

export function takeoverPolicyLabel(accountEnabled: boolean, maintenanceEnabled: boolean): string {
  if (!accountEnabled)
    return 'HTTP / WS 接管已关闭'
  return maintenanceEnabled ? 'HTTP / WS 接管已开启' : 'HTTP / WS 接管待全局启用'
}

export function remainingLabel(expiresAt: number | null, now: number): string {
  if (expiresAt === null)
    return '暂无可用值'
  const remaining = Math.floor(expiresAt - now)
  if (remaining <= 0)
    return '已过期 · 不再注入'
  return `${Math.floor(remaining / 60)}:${String(remaining % 60).padStart(2, '0')}`
}

export function shapeLabel(shape: TurnStateShape | null, policy: Pick<TurnStatePolicy, 'headerLength' | 'cipherBlocks'>): string {
  if (!shape)
    return '无法解析'
  if (policy.headerLength === 0 && policy.cipherBlocks === 0) {
    return (shape.headerLength === 292 && shape.blocks === 10) || (shape.headerLength === 332 && shape.blocks === 12)
      ? '符合候选形态'
      : '观测形态（不判定能力）'
  }
  return shape.headerLength === policy.headerLength && shape.blocks === policy.cipherBlocks
    ? '符合长度规则'
    : '不符合长度规则'
}

export function exactModels(value: string): string[] {
  const models = [...new Set(value.split(/[,，\s]+/).filter(Boolean))]
  if (!models.length || models.some(model => model.includes('*') || model.length > 128))
    throw new Error('请填写精确上游模型名，不能使用通配符')
  return models
}

export function waitReasonLabel(reason: string): string {
  const labels: Record<string, string> = {
    paused: '维护已暂停',
    fresh: '候选充足',
    idle: '无近期业务，等待流量',
    budget: '小时预算已用完',
    upstream_cooldown: '等待上游冷却',
    account_busy: '账号忙',
    account_unavailable: '账号或模型不可用',
    proxy_unavailable: '代理不可用',
    storage_unavailable: '存储不可用',
    upstream_failure: '上游拒绝或失败',
    retry: '等待重试',
    network: '网络异常',
    protocol: '响应异常',
    cancelled: '任务已取消',
    queued: '等待调度',
  }
  return labels[reason] ?? reason
}
