/** 仅展示不透明上游状态，不根据字符长度推断模型能力。 */
export function turnStateDisplay(value: unknown) {
  const token = typeof value === 'string' ? value.trim() : ''
  return {
    token,
    length: Array.from(token).length,
  }
}

/** v3.9.4 记录只有返回值、没有来源标记；未知来源不误标成请求。 */
export function turnStateSourceDisplay(source: unknown) {
  if (source === 'maintenance') {
    return { label: '维护', description: '系统维护探测在业务出口验证时发送的候选状态，不是用户请求' }
  }
  if (source === 'managed') {
    return { label: '接管', description: '本次 HTTP 请求由账号级接管策略注入的候选状态；上游未返回有效的新值' }
  }
  if (source === 'request') {
    return { label: '请求', description: '本次出站请求携带的状态；上游未返回有效的新值' }
  }
  if (source == null || source === 'response') {
    return { label: '返回', description: '本次上游响应返回的状态' }
  }
  return { label: '未知', description: '此记录未识别的 turn-state 来源' }
}
