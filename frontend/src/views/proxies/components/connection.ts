export const proxyProtocols = [
  { label: 'HTTP', value: 'http' },
  { label: 'HTTPS', value: 'https' },
  { label: 'SOCKS5', value: 'socks5' },
  { label: 'SOCKS5H（代理解析 DNS）', value: 'socks5h' },
]

export interface ProxyConnectionFields {
  protocol: string
  host: string
  port: string
  authentication: string
  username: string
  password: string
}

export function proxyConnectionFields(endpoint?: string, hasAuthentication = false): ProxyConnectionFields {
  const fields = { protocol: 'http', host: '', port: '', authentication: hasAuthentication ? 'password' : 'none', username: '', password: '' }
  if (!endpoint)
    return fields
  try {
    const url = new URL(endpoint)
    if (!proxyProtocols.some(option => `${option.value}:` === url.protocol))
      return fields
    fields.protocol = url.protocol.slice(0, -1)
    fields.host = url.hostname.replace(/^\[|\]$/g, '')
    fields.port = url.port || (url.protocol === 'https:' ? '443' : url.protocol === 'http:' ? '80' : '')
    // The admin API exposes only a redacted endpoint. Never invent saved credentials.
  }
  catch {
    // Leave invalid legacy endpoints empty so editing requires explicit correction.
  }
  return fields
}

export function buildProxyConnection(fields: ProxyConnectionFields): string {
  if (!proxyProtocols.some(option => option.value === fields.protocol))
    throw new Error('请选择支持的代理类型')
  const host = fields.host.trim()
  if (!host || /[\s/@\\?#%]/.test(host))
    throw new Error('请填写 IP 或主机名，不要包含协议、端口或路径')
  if (!/^\d+$/.test(fields.port) || Number(fields.port) < 1 || Number(fields.port) > 65535)
    throw new Error('端口必须是 1–65535 的整数')
  const address = host.includes(':') && !host.startsWith('[') ? `[${host}]` : host
  let hostname: string
  try {
    // Validate IPv4, bracketed/unbracketed IPv6 and DNS uniformly for all protocols.
    const url = new URL(`http://${address}:${Number(fields.port)}`)
    if (!url.hostname || url.username || url.password || url.pathname !== '/' || url.search || url.hash)
      throw new Error('invalid host')
    hostname = url.hostname
  }
  catch {
    throw new Error('IP 或主机名格式不正确，端口请填写在单独的端口栏')
  }
  let credentials = ''
  if (fields.authentication === 'password') {
    if (!fields.username.trim())
      throw new Error('使用账号认证时请填写账号；不需要认证请选择“无需认证”')
    if ([...fields.username + fields.password].some(char => char.charCodeAt(0) < 32 || char.charCodeAt(0) === 127))
      throw new Error('账号和密码不能包含换行或控制字符')
    credentials = `${encodeURIComponent(fields.username)}${fields.password ? `:${encodeURIComponent(fields.password)}` : ''}@`
  }
  else if (fields.authentication !== 'none') {
    throw new Error('请选择认证方式')
  }
  const result = `${fields.protocol}://${credentials}${hostname}:${Number(fields.port)}`
  if (result.length > 4096)
    throw new Error('代理连接信息过长')
  return result
}

export function proxyConnectionUpdate(fields: ProxyConnectionFields, existing: boolean, changeConnection: boolean): string {
  // Empty means omit proxyUrl on update, preserving server-side secrets verbatim.
  return existing && !changeConnection ? '' : buildProxyConnection(fields)
}
