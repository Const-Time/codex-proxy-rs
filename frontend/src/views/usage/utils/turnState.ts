/** 仅展示不透明上游状态，不根据字符长度推断模型能力。 */
export function turnStateDisplay(value: unknown) {
  const token = typeof value === 'string' ? value.trim() : ''
  return {
    token,
    length: Array.from(token).length,
  }
}
