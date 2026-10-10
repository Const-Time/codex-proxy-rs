import type { CostComponent, MultiplierScope } from '@/api'

export const costComponents: { value: CostComponent, label: string }[] = [
  { value: 'input', label: '输入' },
  { value: 'output', label: '输出' },
  { value: 'cacheRead', label: '缓存读' },
  { value: 'cacheWrite', label: '缓存写' },
]
export function scopeIncludes(scope: MultiplierScope | undefined, component: CostComponent) {
  return !scope || scope === 'all' || scope.components.includes(component)
}
export function validScope(scope: MultiplierScope) {
  return scope === 'all' || (scope.components.length > 0 && new Set(scope.components).size === scope.components.length && scope.components.every(c => costComponents.some(item => item.value === c)))
}
export function scopeLabel(scope: MultiplierScope | undefined) {
  return !scope || scope === 'all' ? '全部费用' : costComponents.filter(c => scope.components.includes(c.value)).map(c => c.label).join('、') || '请选择费用项'
}
