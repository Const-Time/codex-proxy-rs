/* eslint-disable test/no-import-node-test */
import assert from 'node:assert/strict'
import { test } from 'node:test'
import { generationRate } from '../src/views/usage/utils/generationRate.ts'

test('generation rate excludes the first-token wait and includes output tokens', () => {
  assert.equal(generationRate({ outputTokens: 100, latencyMs: 3000, firstTokenMs: 1000 }), 50)
})

test('generation rate does not invent missing or invalid samples', () => {
  for (const input of [
    { outputTokens: null, latencyMs: 3000, firstTokenMs: 1000 },
    { outputTokens: 100, latencyMs: 3000, firstTokenMs: null },
    { outputTokens: 100, latencyMs: null, firstTokenMs: 1000 },
    { outputTokens: 100, latencyMs: 1000, firstTokenMs: 1000 },
    { outputTokens: 100, latencyMs: 900, firstTokenMs: 1000 },
    { outputTokens: 0, latencyMs: 3000, firstTokenMs: 1000 },
    { outputTokens: -1, latencyMs: 3000, firstTokenMs: 1000 },
    { outputTokens: 100, latencyMs: Infinity, firstTokenMs: 1000 },
    { outputTokens: Number.NaN, latencyMs: 3000, firstTokenMs: 1000 },
    { outputTokens: 100, latencyMs: 3000, firstTokenMs: -1 },
  ]) assert.equal(generationRate(input), null)
})
