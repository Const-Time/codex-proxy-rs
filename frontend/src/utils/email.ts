export function isLoginEmail(value: string): boolean {
  const parts = value.split('@')
  if (parts.length !== 2 || value.length > 128)
    return false
  const [local, domain] = parts
  return local.length > 0 && local.length <= 64
    && !local.startsWith('.') && !local.endsWith('.') && !local.includes('..')
    && /^[\w.!#$%&'*+/=?^`{|}~-]+$/.test(local)
    && domain.includes('.')
    && domain.split('.').every(label => label.length > 0 && label.length <= 63
      && !label.startsWith('-') && !label.endsWith('-') && /^[a-z0-9-]+$/i.test(label))
}
