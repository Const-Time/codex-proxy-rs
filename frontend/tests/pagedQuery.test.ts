import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
// eslint-disable-next-line test/no-import-node-test -- Native Node test runner.
import test from 'node:test'
import { runInNewContext } from 'node:vm'
import ts from 'typescript'

function query(load: (pagination: { page: number, pageSize: number }) => Promise<unknown>) {
  const modules: Record<string, unknown> = {
    'vue': { shallowRef: (value: unknown) => ({ value }), onScopeDispose: () => {} },
    'es-toolkit': { clamp: (value: number, min: number, max: number) => Math.min(max, Math.max(min, value)) },
    '@/utils/async': { errorMessage: String },
  }
  function compile(file: string) {
    const context = { exports: {}, require: (name: string) => {
      assert(name in modules, `unexpected import: ${name}`)
      return modules[name]
    } }
    const source = readFileSync(new URL(`../src/composables/${file}.ts`, import.meta.url), 'utf8')
    runInNewContext(ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } }).outputText, context)
    return context.exports
  }
  modules['./useRequestState'] = compile('useRequestState')
  const module = compile('usePagedQuery') as { usePagedQuery: (options: unknown) => {
    page: { value: number }
    pageSize: { value: number }
    items: { value: unknown[] }
    execute: () => Promise<boolean>
    reloadFromStart: () => Promise<boolean>
    invalidate: () => void
  } }
  return module.usePagedQuery({ initialPageSize: 20, load })
}

test('failed navigation restores the page and size of the displayed result', async () => {
  let fail = false
  const view = query(async ({ page, pageSize }) => {
    if (fail)
      throw new Error('offline')
    return { items: ['first'], page: { page, pageSize, total: 60, totalPages: 3 } }
  })
  assert.equal(await view.execute(), true)
  fail = true
  view.page.value = 2
  view.pageSize.value = 50
  assert.equal(await view.execute(), false)
  assert.equal(view.page.value, 1)
  assert.equal(view.pageSize.value, 20)
  assert.equal(view.items.value[0], 'first')
  assert.equal(await view.reloadFromStart(), false)
  assert.equal(view.items.value.length, 0)
  assert.equal(view.page.value, 1)
})

test('a stale failed page cannot roll back a newer successful request', async () => {
  let reject: (error: Error) => void = () => {}
  const view = query(({ page, pageSize }) => page === 2
    ? new Promise((_, fail) => { reject = fail })
    : Promise.resolve({ items: [page], page: { page, pageSize, total: 60, totalPages: 3 } }))
  await view.execute()
  view.page.value = 2
  const stale = view.execute()
  view.page.value = 3
  await view.execute()
  reject(new Error('late failure'))
  await stale
  assert.equal(view.page.value, 3)
  assert.equal(view.items.value[0], 3)
})
