import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
// eslint-disable-next-line test/no-import-node-test -- Native Node test runner.
import test from 'node:test'
import { runInNewContext } from 'node:vm'
import ts from 'typescript'

function filters(personal: boolean, api: Record<string, unknown>) {
  const source = readFileSync(new URL('../src/views/usage/components/UsageRecordFiltersBar.vue', import.meta.url), 'utf8').split('<script setup lang="ts">')[1]!.split('</script>')[0]!
  const modules: Record<string, unknown> = {
    'vue': {
      shallowRef: (value: unknown) => ({ value }),
      reactive: (value: unknown) => value,
      computed: (read: () => unknown) => ({ get value() { return read() } }),
      inject: () => ({ value: personal }),
      onMounted: () => {},
    },
    '@/api/modules/operations': api,
    '@/api/modules/users': api,
    '@/utils/async': { errorMessage: String },
  }
  const context = { exports: {}, defineModel: () => ({ value: {} }), require: (name: string) => {
    assert(name in modules, `unexpected import: ${name}`)
    return modules[name]
  } }
  runInNewContext(ts.transpileModule(`${source}\nexport { load, pending, groups, accounts, keys, error }`, { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } }).outputText, context)
  return context.exports as {
    load: () => Promise<void>
    pending: Record<string, boolean>
    groups: { value: { value: string }[] }
    accounts: { value: { value: string }[] }
    keys: { value: { value: string }[] }
    error: { value: string }
  }
}

test('usage catalogs publish independently and retry failures without full account queries', async () => {
  let releaseKeys: (value: unknown[]) => void = () => {}
  let failing = true
  let keyCalls = 0
  const view = filters(false, {
    getUsageKeyOptions: () => {
      keyCalls++
      return new Promise((resolve) => {
        releaseKeys = resolve
      })
    },
    getUsageCatalogOptions: async (kind: string) => {
      if (kind === 'accounts' && failing)
        throw new Error('catalog unavailable')
      return [{ id: kind, name: kind }]
    },
    getUsers: async () => [],
  })
  const first = view.load()
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(view.groups.value[0]?.value, 'groups')
  assert.equal(view.pending.groups, false)
  assert.equal(view.pending.accounts, false)
  assert.equal(view.pending.keys, true)
  assert.match(view.error.value, /catalog unavailable/)
  await view.load()
  assert.equal(keyCalls, 1, 'duplicate load is suppressed')
  releaseKeys([])
  await first
  failing = false
  const retry = view.load()
  releaseKeys([{ id: 'key', name: 'key' }])
  await retry
  assert.equal(view.accounts.value[0]?.value, 'accounts')
  assert.equal(view.keys.value[0]?.value, 'key')
  assert.equal(view.error.value, '')
})

test('personal filters never request administrator account or user catalogs', async () => {
  const calls: string[] = []
  const view = filters(true, {
    getUsageKeyOptions: async (personal: boolean) => {
      assert(personal)
      return []
    },
    getUsageCatalogOptions: async (kind: string, personal: boolean) => {
      assert(personal)
      calls.push(kind)
      return []
    },
    getUsers: () => { throw new Error('must not load users') },
  })
  await view.load()
  assert.deepEqual(calls, ['groups'])
  assert.equal(view.error.value, '')
})
