import assert from 'node:assert/strict'
// eslint-disable-next-line test/no-import-node-test -- Use the existing native Node test runner.
import test from 'node:test'
import { buildProxyConnection, proxyConnectionFields, proxyConnectionUpdate, proxyProtocols } from '../src/views/proxies/components/connection.ts'

test('all supported proxy types accept separate host and port without authentication', () => {
  for (const { value: protocol } of proxyProtocols) {
    assert.equal(buildProxyConnection({ ...proxyConnectionFields(), protocol, host: '127.0.0.1', port: '1080' }), `${protocol}://127.0.0.1:1080`)
  }
})

test('credentials are percent encoded without trimming or leaking into host fields', () => {
  const fields = { ...proxyConnectionFields(), protocol: 'socks5h', host: 'proxy.example.com', port: '1080', authentication: 'password', username: 'user@a:b', password: ' p@ss:/?#%密 ' }
  const url = new URL(buildProxyConnection(fields))
  assert.equal(decodeURIComponent(url.username), fields.username)
  assert.equal(decodeURIComponent(url.password), fields.password)
  assert.equal(url.hostname, fields.host)
  assert.equal(url.search, '')
  assert.equal(url.hash, '')
})

test('password is optional and choosing no authentication omits stale credentials', () => {
  const fields = { ...proxyConnectionFields(), host: 'proxy.example.com', port: '80', authentication: 'password', username: 'user' }
  assert.equal(buildProxyConnection(fields), 'http://user@proxy.example.com:80')
  assert.equal(buildProxyConnection({ ...fields, authentication: 'none', password: 'stale' }), 'http://proxy.example.com:80')
})

test('IPv6 with or without brackets and DNS names work with separate ports', () => {
  for (const host of ['2001:db8::1', '[2001:db8::1]'])
    assert.equal(buildProxyConnection({ ...proxyConnectionFields(), host, port: '443', protocol: 'https' }), 'https://[2001:db8::1]:443')
  assert.equal(buildProxyConnection({ ...proxyConnectionFields(), host: ' localhost ', port: '8080' }), 'http://localhost:8080')
})

test('invalid hosts, protocols, ports and incomplete authentication are rejected', () => {
  const valid = { ...proxyConnectionFields(), host: 'proxy.example.com', port: '1080' }
  for (const host of ['', 'http://localhost', 'localhost:8080', 'a/b', 'a?b', 'a#b', 'u@host', 'bad host', 'bad\\host', '[::gg]', '[::1]:1080', '999.999.999.999'])
    assert.throws(() => buildProxyConnection({ ...valid, host }), undefined, host)
  for (const port of ['', '0', '65536', '-1', '1.5', '1e3', 'abc', ' 80'])
    assert.throws(() => buildProxyConnection({ ...valid, port }), /端口/)
  assert.throws(() => buildProxyConnection({ ...valid, protocol: 'ftp' }), /代理类型/)
  assert.throws(() => buildProxyConnection({ ...valid, authentication: 'password' }), /账号/)
  assert.throws(() => buildProxyConnection({ ...valid, authentication: 'password', username: 'u\nx' }), /控制字符/)
})

test('editing redacted endpoints preserves credentials unless connection replacement is explicit', () => {
  const fields = proxyConnectionFields('socks5h://[2001:db8::1]:1080', true)
  assert.equal(fields.host, '2001:db8::1')
  assert.equal(fields.port, '1080')
  assert.equal(fields.username, '')
  assert.equal(fields.password, '')
  assert.equal(proxyConnectionUpdate(fields, true, false), '')
  assert.throws(() => proxyConnectionUpdate(fields, true, true), /账号/)
  assert.equal(proxyConnectionUpdate({ ...fields, authentication: 'none' }, true, true), 'socks5h://[2001:db8::1]:1080')
  assert.equal(proxyConnectionFields('http://localhost/').port, '80')
  assert.equal(proxyConnectionFields('https://localhost/').port, '443')
  assert.throws(() => proxyConnectionUpdate(proxyConnectionFields(), false, false))
})
