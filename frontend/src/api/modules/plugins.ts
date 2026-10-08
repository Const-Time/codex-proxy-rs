import { API_BASE_URL } from '../constants'
import request from '../request'

export const PLUGIN_MANAGEMENT_TIMEOUT_MS = 35_000
export interface PluginArtifact {
  sha256: string
  pluginId: string
  displayName: string
  version: string
  manifest: { secretFields: string[], configurationSchema: Record<string, unknown> }
  unsupportedCapabilities: string[]
  versionWarning: string | null
}
export interface PluginManagementPage { id: string, title: string, entry: string }
export interface PluginManagementResource { path: string, contentType: string }
export interface PluginTarget { instanceId: string, revision: number }
export interface PluginManagementView {
  name: string
  target: PluginTarget
  pages: PluginManagementPage[]
  resources: PluginManagementResource[]
}
export interface PluginAuthenticationBinding { failurePolicy: 'reject' | 'delegate', identities: Record<string, string> }
export interface PluginInstance {
  frontendAuthentication: PluginAuthenticationBinding | null
  id: string
  revision: number
  name: string
  artifact: PluginArtifact
  enabled: boolean
  status: string
  configuration: Record<string, unknown>
  configuredSecrets: string[]
  pages: PluginManagementPage[]
  resources: PluginManagementResource[]
}
export interface PluginUpdate {
  frontendAuthentication: PluginAuthenticationBinding | null
  revision: number
  name: string
  artifactSha256: string
  enabled: boolean
  trustedProcess: boolean
  configuration: Record<string, unknown>
  secrets: Record<string, string | null>
}
interface WireReply { status: number, contentType: string, body: string }
export function decodeReply(reply: WireReply) {
  return { ...reply, body: Uint8Array.from(atob(reply.body), c => c.charCodeAt(0)).buffer }
}
export const getPlugins = () => request<PluginInstance[]>({ url: '/api/admin/plugins' })
export const getPluginArtifacts = () => request<PluginArtifact[]>({ url: '/api/admin/plugins/artifacts' })
export const removePluginArtifact = (sha256: string) => request({ url: '/api/admin/plugins/artifacts/remove', method: 'POST', data: { sha256 } })
export const uploadPlugin = (body: File) => request<PluginArtifact>({ url: '/api/admin/plugins/artifacts', method: 'POST', data: body, headers: { 'Content-Type': 'application/gzip' }, timeout: 120_000 })
export const savePlugin = (id: string | undefined, data: PluginUpdate) => request<PluginInstance>({ url: id ? '/api/admin/plugins/update' : '/api/admin/plugins', method: 'POST', data: id ? { id, ...data } : data, timeout: 120_000 })
export const uninstallPlugin = (plugin: PluginInstance) => request({ url: '/api/admin/plugins/uninstall', method: 'POST', data: { id: plugin.id, revision: plugin.revision } })
export async function getPluginManagementResource(target: PluginTarget, path: string, options: { signal: AbortSignal, silent?: boolean }) {
  const reply = await request<WireReply>({ url: '/api/admin/plugins/resource', params: { id: target.instanceId, revision: target.revision, path }, signal: options.signal })
  return decodeReply(reply)
}
export async function callPluginRoute(target: PluginTarget, payload: { method: string, path: string, query?: string, contentType?: string, body?: ArrayBuffer }, signal: AbortSignal) {
  const bytes = new Uint8Array(payload.body ?? new ArrayBuffer(0))
  let binary = ''
  for (let offset = 0; offset < bytes.length; offset += 8192)
    binary += String.fromCharCode(...bytes.subarray(offset, offset + 8192))
  return decodeReply(await request<WireReply>({ url: '/api/admin/plugins/invoke', method: 'POST', data: { id: target.instanceId, revision: target.revision, ...payload, body: btoa(binary) }, signal, timeout: PLUGIN_MANAGEMENT_TIMEOUT_MS }))
}

export function callPluginModel(target: PluginTarget, input: { clientKeyId: string, body: Record<string, unknown> }, signal: AbortSignal) {
  return fetch(`${API_BASE_URL}/api/admin/plugins/models/responses`, { method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id: target.instanceId, revision: target.revision, ...input }), signal })
}
