import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
// eslint-disable-next-line test/no-import-node-test -- Native Node test runner.
import test from 'node:test'
import { runInNewContext } from 'node:vm'
import ts from 'typescript'

function load<T>(path: string, overrides: Record<string, unknown>) {
  const disposals: Array<() => void> = []
  const errors: unknown[] = []
  const modules: Record<string, unknown> = {
    'vue': {
      shallowRef: (value: unknown) => ({ value }),
      computed: (read: () => unknown) => ({ get value() { return read() } }),
      onMounted: () => {},
      onScopeDispose: (dispose: () => void) => disposals.push(dispose),
      watch: () => {},
    },
    '@vueuse/core': { watchDebounced: () => {} },
    '@/components/base/BaseToast': { toast: { error: (error: unknown) => errors.push(error) } },
    '@/utils/async': { errorMessage: String, withMinimumDuration: (promise: unknown) => promise },
    '@/utils/date': { formatDateTime: String },
    ...overrides,
  }
  const context = { exports: {}, AbortController, document: { hidden: false }, require: (name: string) => {
    assert(name in modules, `unexpected import: ${name}`)
    return modules[name]
  } }
  const source = readFileSync(new URL(path, import.meta.url), 'utf8')
  runInNewContext(ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } }).outputText, context)
  return { module: context.exports as T, dispose: () => disposals.forEach(dispose => dispose()), errors, document: context.document }
}

test('API Key concurrency refresh retains the active search', async () => {
  let refresh: (() => Promise<void>) | undefined
  const queries: Array<{ search?: string, sortBy?: string }> = []
  const fixture = load<{ useApiKeysQuery: typeof import('../src/views/api-keys/composables/useApiKeysQuery.ts').useApiKeysQuery }>('../src/views/api-keys/composables/useApiKeysQuery.ts', {
    '@vueuse/core': { watchDebounced: () => {}, useIntervalFn: (callback: () => Promise<void>) => { refresh = callback } },
    '@/api': { getApiKeys: async (query: { search?: string, sortBy?: string }) => {
      queries.push(query)
      return { items: [{ id: query.search === 'needle' ? 'visible' : 'other', createdAt: '2026-10-16', activeConcurrency: 3 }], total: 1 }
    } },
  })
  const view = fixture.module.useApiKeysQuery()
  view.searchQuery.value = 'needle'
  await view.loadApiKeys()
  assert(refresh)
  await refresh()
  assert.equal(queries[1]?.search, 'needle')
  assert.equal(view.apiKeys.value[0]?.activeConcurrency, 3)
  fixture.dispose()
})

test('group selectors page through the lightweight catalog and retain data on failed reload', async () => {
  const pages: number[] = []
  let fail = false
  const fixture = load<{ useAccountGroupCatalog: typeof import('../src/composables/useAccountGroupCatalog.ts').useAccountGroupCatalog }>('../src/composables/useAccountGroupCatalog.ts', {
    '@/api': {
      getAccountGroupCatalog: async ({ page, pageSize }: { page: number, pageSize: number }) => {
        pages.push(page)
        assert.equal(pageSize, 200)
        if (fail)
          throw new Error('unavailable')
        return Array.from({ length: page === 1 ? 200 : 1 }, (_, i) => ({ id: `${page}-${i}`, name: 'Group', color: '#64748BFF', enabled: true }))
      },
    },
  })
  const view = fixture.module.useAccountGroupCatalog({ immediate: false })
  await view.loadGroups()
  assert.deepEqual(pages, [1, 2])
  assert.equal(view.groups.value.length, 201)
  assert.equal(view.loading.value, false)
  fail = true
  await view.loadGroups()
  assert.equal(view.groups.value.length, 201)
  assert.equal(view.loading.value, false)
  assert.equal(fixture.errors.length, 1)
})

test('superseded usage requests and component disposal abort network work without error toasts', async () => {
  const signals: AbortSignal[] = []
  const pending = (_query: unknown, signal: AbortSignal) => {
    signals.push(signal)
    return new Promise((_resolve, reject) => signal.addEventListener('abort', () => reject(new Error('aborted')), { once: true }))
  }
  const fixture = load<{ useUsageRecordsTable: typeof import('../src/views/usage/composables/useUsageRecordsTable.ts').useUsageRecordsTable }>('../src/views/usage/composables/useUsageRecordsTable.ts', {
    '@/api': { getUsageRecords: pending, getUsageRecordSummary: pending, getUsageRecordInsightCard: pending, getUsageRecordInsightsDiagnostics: pending },
  })
  const view = fixture.module.useUsageRecordsTable({ timeRangeParams: { value: { startTime: '2026-10-15', endTime: '2026-10-16' } }, refreshTimeRangeEnd: () => {} })
  const first = view.loadUsageRecords()
  assert.equal(signals.length, 6)
  const second = view.loadUsageRecords()
  assert(signals.slice(0, 6).every(signal => signal.aborted))
  assert(signals.slice(6).every(signal => !signal.aborted))
  fixture.dispose()
  await Promise.all([first, second])
  assert(signals.every(signal => signal.aborted))
  assert.equal(fixture.errors.length, 0)
})

test('slow or failed diagnostics do not delay or discard successful usage panels', async () => {
  let rejectDiagnostics: (error: Error) => void = () => {}
  const fixture = load<{ useUsageRecordsTable: typeof import('../src/views/usage/composables/useUsageRecordsTable.ts').useUsageRecordsTable }>('../src/views/usage/composables/useUsageRecordsTable.ts', {
    '@/api': {
      getUsageRecords: async () => ({ items: [], pageSize: 10, total: 0, currentPage: 1 }),
      getUsageRecordSummary: async () => ({ totalRequests: '42' }),
      getUsageRecordInsightCard: async ({ card }: { card: string }) => ({ [card]: { marker: 'ready' } }),
      getUsageRecordInsightsDiagnostics: () => new Promise((_resolve, reject) => { rejectDiagnostics = reject }),
    },
  })
  const view = fixture.module.useUsageRecordsTable({ timeRangeParams: { value: { startTime: '2026-10-15', endTime: '2026-10-16' } }, refreshTimeRangeEnd: () => {} })
  const request = view.loadUsageRecords()
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(view.summary.value.totalRequests, '42')
  assert.equal(view.analyticsLoading.value, false)
  assert.equal(view.diagnosticsLoading.value, true)
  rejectDiagnostics(new Error('diagnostics unavailable'))
  await request
  assert.equal(view.summary.value.totalRequests, '42')
  assert.equal((view.insights.value.overview.health as unknown as { marker: string }).marker, 'ready')
  assert.equal(view.diagnosticsLoading.value, false)
  assert.equal(fixture.errors.length, 1)
  fixture.dispose()
})

test('dashboard cancels superseded trends and disposed requests, and skips hidden-tab polling', async () => {
  let poll: (() => void) | undefined
  const signals: AbortSignal[] = []
  const pending = (_query: unknown, signal: AbortSignal) => {
    signals.push(signal)
    return new Promise((_resolve, reject) => signal.addEventListener('abort', () => reject(new Error('aborted')), { once: true }))
  }
  const fixture = load<{ useDashboard: typeof import('../src/views/dashboard/composables/useDashboard.ts').useDashboard }>('../src/views/dashboard/composables/useDashboard.ts', {
    '@lucide/vue': { Activity: {}, FileText: {}, Timer: {}, Users: {} },
    '@vueuse/core': { useIntervalFn: (callback: () => void) => {
      poll = callback
      return { resume() {} }
    } },
    '@/utils/number': { formatCompactNumber: String, formatInteger: String },
    '@/api': { getDashboardSummary: pending, getDashboardTrend: pending },
  })
  const view = fixture.module.useDashboard()
  const first = view.loadTrend('latency')
  const second = view.loadTrend('errors')
  assert.equal(signals[0]?.aborted, true)
  assert.equal(signals[1]?.aborted, false)
  assert(poll)
  fixture.document.hidden = true
  poll()
  assert.equal(signals.length, 2)
  fixture.document.hidden = false
  const summary = view.refresh()
  assert.equal(signals[1]?.aborted, true)
  assert.equal(signals.length, 3)
  fixture.dispose()
  await Promise.all([first, second, summary])
  assert(signals.every(signal => signal.aborted))
  assert.equal(view.trendError.value, '')
})

test('usage cards cache selected metrics independently and never share another filter scope', async () => {
  const calls: Array<{ card: string, metric?: string, userId?: string }> = []
  const fixture = load<{ useUsageRecordsTable: typeof import('../src/views/usage/composables/useUsageRecordsTable.ts').useUsageRecordsTable }>('../src/views/usage/composables/useUsageRecordsTable.ts', {
    '@/api': {
      getUsageRecords: async () => ({ items: [], pageSize: 10, total: 0, currentPage: 1 }),
      getUsageRecordSummary: async () => ({ totalRequests: '42' }),
      getUsageRecordInsightCard: async (query: { card: string, metric?: string, userId?: string }) => {
        calls.push(query)
        return { [query.card]: { marker: query.metric || query.card } }
      },
      getUsageRecordInsightsDiagnostics: async () => ({ items: [] }),
    },
  })
  const filters = { value: { userId: 'first' } }
  const view = fixture.module.useUsageRecordsTable({ timeRangeParams: { value: { startTime: '2026-10-15', endTime: '2026-10-16' } }, refreshTimeRangeEnd: () => {}, recordFilters: filters })
  await view.loadUsageRecords()
  assert.deepEqual(calls.map(c => c.card), ['health', 'cost', 'performance'])
  assert.equal(calls[2]?.metric, 'total')
  view.performanceMetric.value = 'concurrency'
  await view.loadUsageRecords()
  assert.equal(calls.length, 4)
  assert.equal(calls[3]?.metric, 'concurrency')
  view.performanceMetric.value = 'total'
  await view.loadUsageRecords()
  assert.equal(calls.length, 4)
  filters.value = { userId: 'second' }
  await view.loadUsageRecords()
  assert.equal(calls.length, 7)
  assert(calls.slice(4).every(c => c.userId === 'second'))
  await view.refreshUsageRecords()
  assert.equal(calls.length, 10)
  fixture.dispose()
})
