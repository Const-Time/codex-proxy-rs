import assert from 'node:assert/strict'
// eslint-disable-next-line test/no-import-node-test -- Component rendering uses the native Node runner.
import test from 'node:test'
import { fileURLToPath } from 'node:url'
import { createServer } from 'vite'
import { createSSRApp, h, ref } from 'vue'
import { renderToString } from 'vue/server-renderer'

test('tables and usage wrapper keep actions and column settings in one toolbar, including empty results', async () => {
  const server = await createServer({
    root: fileURLToPath(new URL('..', import.meta.url)),
    server: { middlewareMode: true, hmr: false, watch: null },
    appType: 'custom',
  })
  try {
    for (const path of ['/src/components/base/BaseTable/index.vue', '/src/views/usage/components/UsageRecordsTable.vue']) {
      const { default: Table } = await server.ssrLoadModule(path)
      const app = createSSRApp({
        render: () => h(Table, { tableId: 'toolbar-regression', columns: [{ key: 'name', label: '名称' }], rows: [] }, {
          'toolbar': () => h('input', { 'aria-label': '筛选测试' }),
          'toolbar-actions': () => h('button', { 'aria-label': '刷新测试' }, '刷新'),
        }),
      })
      app.provide('personalUsage', ref(false))
      app.directive('loading', {})
      const html = await renderToString(app)
      const start = html.indexOf('cp-table-toolbar')
      const body = html.indexOf('cp-data-table-body')
      assert.ok(start >= 0 && body > start)
      const toolbar = html.slice(start, body)
      for (const label of ['筛选测试', '刷新测试', '显示列'])
        assert.ok(toolbar.includes(`aria-label="${label}"`), `${path}: ${label} must share the toolbar`)
      assert.equal((html.match(/cp-table-toolbar/g) || []).length, 1)
    }
    const { default: Table } = await server.ssrLoadModule('/src/components/base/BaseTable/index.vue')
    for (const withActions of [false, true]) {
      const app = createSSRApp({ render: () => h(Table, { columns: [], rows: [] }, withActions ? { 'toolbar-actions': () => h('button', '刷新') } : {}) })
      app.provide('personalUsage', ref(false))
      app.directive('loading', {})
      const html = await renderToString(app)
      assert.equal(html.includes('cp-table-toolbar'), withActions, 'no toolbar means no reserved blank row')
      assert.equal(html.includes('aria-label="显示列"'), false, 'transient tables must not gain column settings')
    }
  }
  finally {
    await server.close()
  }
})
