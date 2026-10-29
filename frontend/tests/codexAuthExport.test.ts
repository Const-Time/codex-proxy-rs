import assert from 'node:assert/strict'
import { Buffer } from 'node:buffer'
// eslint-disable-next-line test/no-import-node-test -- Use the existing native Node test runner.
import test from 'node:test'
import { strFromU8, unzipSync } from 'fflate'
import { codexAuthDownload, codexAuthFiles } from '../src/views/accounts/utils/codexAuthExport.ts'

function account(id: string, email = 'same@example.test') {
  return {
    id,
    email,
    accountId: `upstream-${id}`,
    accessToken: `access-${id}`,
    refreshToken: `refresh-${id}`,
    idToken: `id-${id}`,
    label: 'backup-only-label',
    updatedAt: '2026-10-28T10:00:00Z',
    outboundProxyUrl: 'http://user:secret@proxy.test',
  }
}
function payload(accounts: unknown[], provider = 'openai') {
  return { exportedAt: '2026-10-28T11:00:00Z', documents: [{ provider, document: { sourceFormat: 'cpr', accounts } }] }
}

test('single account downloads Codex tokens without backup metadata or fabricated refresh time', async () => {
  const input = payload([account('a')])
  const original = JSON.stringify(input)
  const file = codexAuthDownload(input, ['a'])
  assert.equal(file.fileName, 'auth.json')
  assert.match(file.blob.type, /application\/json/)
  assert.deepEqual(JSON.parse(await file.blob.text()), {
    auth_mode: 'chatgpt',
    OPENAI_API_KEY: null,
    tokens: { id_token: 'id-a', access_token: 'access-a', refresh_token: 'refresh-a', account_id: 'upstream-a' },
  })
  assert.equal(JSON.stringify(input), original)
})

test('batch ZIP preserves selected order, separate credentials and safe unique filenames', async () => {
  const file = codexAuthDownload(payload([account('b', '../../evil\\name'), account('a', '../../evil\\name')]), ['a', 'b'])
  assert.equal(file.fileName, 'codex-auth-2-accounts.zip')
  const archive = unzipSync(Buffer.from(await file.blob.arrayBuffer()))
  const paths = Object.keys(archive)
  assert.equal(paths.length, 2)
  for (const [i, path] of paths.entries()) {
    assert.match(path, /^00[12]-[^/\\]+\/auth\.json$/)
    assert.ok(!path.split('/').includes('..'))
    const auth = JSON.parse(strFromU8(archive[path]!))
    assert.equal(auth.tokens.access_token, i === 0 ? 'access-a' : 'access-b')
  }
  const duplicates = codexAuthFiles(payload([account('a'), account('b')]), ['a', 'b'])
  assert.notEqual(duplicates[0]!.path, duplicates[1]!.path)
})

test('incomplete or unsupported credentials abort the whole export without exposing tokens in errors', () => {
  for (const field of ['accessToken', 'refreshToken', 'idToken', 'accountId']) {
    for (const missing of [undefined, null, '', '   ']) {
      assert.throws(() => codexAuthFiles(payload([account('a'), { ...account('b'), [field]: missing }]), ['a', 'b']), /缺少完整 OAuth/)
    }
  }
  assert.throws(() => codexAuthFiles(payload([account('a')], 'xai'), ['a']), /仅支持 OpenAI OAuth/)
  assert.throws(() => codexAuthFiles(payload([{ ...account('a'), authentication_kind: 'api_key', api_key: 'secret-never-log' }]), ['a']), /系统备份/)
  assert.throws(() => codexAuthFiles(payload([{ ...account('a'), accessToken: 'at-secret-never-log' }]), ['a']), /系统备份/)
})

test('mismatched, duplicate and empty selections never silently export different accounts', () => {
  assert.throws(() => codexAuthFiles(payload([]), []))
  assert.throws(() => codexAuthFiles(payload([account('a')]), ['a', 'a']))
  assert.throws(() => codexAuthFiles(payload([account('a'), account('a')]), ['a', 'b']))
  assert.throws(() => codexAuthFiles(payload([account('b')]), ['a']))
  assert.throws(() => codexAuthFiles(payload([account('a')]), ['a', 'b']))
  assert.throws(() => codexAuthFiles({ documents: [] }, ['a']))
  assert.throws(() => codexAuthFiles(null, ['a']))
})
