export const UNOWNED_USAGE_USER_LABEL = '无用户归属'

interface UsageUserRecord {
  username?: string | null
  userEmail?: string | null
  userId?: string | null
  clientTransport?: string | null
  requestKind?: string | null
}

/** 无用户不等于系统请求：只有明确的内部探测标记才能显示“系统维护”。 */
export function usageUserDisplay(record: UsageUserRecord) {
  const user = record.username?.trim() || record.userEmail?.trim() || record.userId?.trim()
  if (user)
    return { label: user, description: user }

  if (record.clientTransport === 'maintenance' || record.requestKind === 'state_probe') {
    return {
      label: '系统维护',
      description: 'Turn-state 维护探测（采集或验证），非用户请求；消耗上游额度，不扣用户额度',
    }
  }

  return {
    label: UNOWNED_USAGE_USER_LABEL,
    description: '该记录没有关联的用户身份；不代表系统维护请求',
  }
}
