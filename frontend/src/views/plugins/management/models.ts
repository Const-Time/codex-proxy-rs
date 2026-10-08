import { MAXIMUM_MODEL_BODY_BYTES, MAXIMUM_MODEL_CHUNK_BYTES, MAXIMUM_MODEL_IN_FLIGHT, MODEL_REQUEST_DEADLINE_MS, MODEL_RESPONSE_IDLE_MS } from './bridge-contract.ts'

export interface ModelPacket { id: number, type: string, payload?: unknown, error?: string }
export interface ModelInput { clientKeyId: string, body: Record<string, unknown> }
type FetchModel = (input: ModelInput, signal: AbortSignal) => Promise<Response>
interface Operation {
  controller: AbortController
  reader?: ReadableStreamDefaultReader<Uint8Array>
  remainder?: Uint8Array
  pullId: number
  pulling: boolean
  deadline: ReturnType<typeof setTimeout>
  idle?: ReturnType<typeof setTimeout>
}

/** One fetch per request; each SDK pull receives at most one bounded chunk. */
export class PluginModelBridge {
  private readonly operations = new Map<number, Operation>()
  private disposed = false
  private readonly selectedKey: string
  private readonly fetchModel: FetchModel
  private readonly send: (packet: ModelPacket) => void
  constructor(selectedKey: string, fetchModel: FetchModel, send: (packet: ModelPacket) => void) {
    this.selectedKey = selectedKey
    this.fetchModel = fetchModel
    this.send = send
  }

  async request(id: number, payload: unknown) {
    if (this.disposed)
      return
    if (this.operations.has(id)) {
      this.fail(id, '模型请求编号重复')
      return
    }
    const input = payload as { clientKeyId?: unknown, body?: unknown } | null
    if (!Number.isSafeInteger(id) || id < 1 || !this.selectedKey || !input || input.clientKeyId !== this.selectedKey || typeof input.body !== 'string' || new TextEncoder().encode(input.body).length > MAXIMUM_MODEL_BODY_BYTES || this.operations.size >= MAXIMUM_MODEL_IN_FLIGHT) {
      this.send({ id, type: 'models-error', error: '请选择本人 Key，检查请求格式与并发限制' })
      return
    }
    let body: Record<string, unknown>
    try {
      body = JSON.parse(input.body)
      if (!body || typeof body !== 'object' || Array.isArray(body))
        throw new Error('invalid body')
    }
    catch {
      this.send({ id, type: 'models-error', error: 'Responses 正文必须是 JSON 对象' })
      return
    }
    const operation: Operation = { controller: new AbortController(), pullId: 0, pulling: false, deadline: setTimeout(() => this.fail(id, '模型请求超过 10 分钟'), MODEL_REQUEST_DEADLINE_MS) }
    this.operations.set(id, operation)
    try {
      const response = await this.fetchModel({ clientKeyId: this.selectedKey, body }, operation.controller.signal)
      if (this.operations.get(id) !== operation) {
        await response.body?.cancel()
        return
      }
      operation.reader = response.body?.getReader()
      this.send({ id, type: 'models-response', payload: { status: response.status, statusText: response.statusText, headers: [...response.headers], hasBody: !!operation.reader } })
      if (operation.reader)
        this.armIdle(id, operation)
      else this.cancel(id)
    }
    catch (error) {
      if (this.operations.get(id) === operation)
        this.fail(id, error instanceof Error ? error.message : '模型请求失败')
    }
  }

  async pull(id: number, pullId: unknown) {
    const operation = this.operations.get(id)
    if (!operation)
      return
    if (!operation.reader || operation.pulling || !Number.isSafeInteger(pullId) || pullId !== operation.pullId + 1) {
      this.fail(id, '模型流信用无效')
      return
    }
    operation.pullId++
    operation.pulling = true
    this.armIdle(id, operation)
    try {
      let bytes = operation.remainder
      while (!bytes?.byteLength) {
        const chunk = await operation.reader.read()
        if (this.operations.get(id) !== operation)
          return
        if (chunk.done) {
          this.send({ id, type: 'models-complete', payload: { pullId } })
          this.cancel(id)
          return
        }
        bytes = chunk.value
      }
      const body = bytes.slice(0, MAXIMUM_MODEL_CHUNK_BYTES).buffer
      operation.remainder = bytes.byteLength > MAXIMUM_MODEL_CHUNK_BYTES ? bytes.subarray(MAXIMUM_MODEL_CHUNK_BYTES) : undefined
      operation.pulling = false
      this.armIdle(id, operation)
      this.send({ id, type: 'models-chunk', payload: { pullId, body } })
    }
    catch (error) {
      if (this.operations.get(id) === operation)
        this.fail(id, error instanceof Error ? error.message : '模型流读取失败')
    }
  }

  cancel(id: number) {
    const operation = this.operations.get(id)
    if (!operation)
      return
    this.operations.delete(id)
    clearTimeout(operation.deadline)
    clearTimeout(operation.idle)
    operation.controller.abort()
    void operation.reader?.cancel().catch(() => {})
  }

  dispose() {
    this.disposed = true
    for (const id of this.operations.keys())
      this.cancel(id)
  }

  private armIdle(id: number, operation: Operation) {
    clearTimeout(operation.idle)
    operation.idle = setTimeout(() => this.fail(id, '模型流等待超时'), MODEL_RESPONSE_IDLE_MS)
  }

  private fail(id: number, error: string) {
    this.cancel(id)
    this.send({ id, type: 'models-error', error })
  }
}
