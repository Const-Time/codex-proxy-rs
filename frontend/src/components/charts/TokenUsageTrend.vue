<script setup lang="ts">
import type { EChartsOption, LineSeriesOption } from 'echarts'
import type { TokenUsagePoint } from './tokenUsage'
import { usePreferredReducedMotion } from '@vueuse/core'
import { computed, ref } from 'vue'
import BaseEmpty from '@/components/base/BaseEmpty.vue'
import { useChartPalette } from '@/composables/useChartPalette'
import { formatCompactNumber as compact } from '@/utils/number'
import BaseChart from './BaseChart.vue'
import { tokenCacheHitRate, tokenUsageTotals } from './tokenUsage'
import { chartTooltipStyle, tooltipIndex } from './tooltip'

const props = withDefaults(defineProps<{ points: TokenUsagePoint[], loading?: boolean }>(), { loading: false })
const { palette } = useChartPalette()
const reducedMotion = usePreferredReducedMotion()
const selected = ref('')
const totals = computed(() => tokenUsageTotals(props.points))
const percent = (value: number | null) => value == null ? '—' : `${(value * 100).toFixed(1)}%`
const escape = (value: string) => value.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;').replaceAll('\'', '&#39;')
const metrics = computed(() => [
  { name: '输入', value: compact(totals.value.input), color: palette.value.info, key: 'inputTokens' as const },
  { name: '输出', value: compact(totals.value.output), color: palette.value.success, key: 'outputTokens' as const },
  { name: '缓存读取', value: compact(totals.value.read), color: palette.value.reasoning, key: 'cachedTokens' as const },
  { name: '缓存写入', value: compact(totals.value.write), color: palette.value.warning, key: 'cacheWriteTokens' as const },
  { name: '缓存命中率', value: percent(totals.value.hitRate), color: palette.value.normal, key: 'rate' as const },
])
const hasData = computed(() => props.points.some(point => point.requests > 0 || point.totalTokens > 0 || point.cacheWriteTokens > 0))
const option = computed<EChartsOption>(() => ({
  animationDuration: reducedMotion.value === 'reduce' ? 0 : 240,
  grid: { left: 0, right: 0, top: 24, bottom: 0, outerBoundsMode: 'same', outerBoundsContain: 'axisLabel' },
  xAxis: {
    type: 'category',
    data: props.points.map(point => point.label),
    boundaryGap: false,
    axisLabel: { color: palette.value.textMuted, fontSize: 10, hideOverlap: true },
    axisLine: { lineStyle: { color: palette.value.border } },
    axisTick: { show: false },
  },
  yAxis: [
    {
      type: 'value',
      min: 0,
      name: 'Token',
      splitNumber: 3,
      nameTextStyle: { color: palette.value.textMuted, fontSize: 10 },
      axisLabel: { color: palette.value.textMuted, fontSize: 10, formatter: (value: number) => compact(value) },
      splitLine: { lineStyle: { color: palette.value.grid } },
    },
    {
      type: 'value',
      min: 0,
      max: 1,
      interval: 0.25,
      name: '命中率',
      nameTextStyle: { color: palette.value.textMuted, fontSize: 10 },
      axisLabel: { color: palette.value.textMuted, fontSize: 10, formatter: (value: number) => `${value * 100}%` },
      splitLine: { show: false },
    },
  ],
  tooltip: {
    trigger: 'axis',
    ...chartTooltipStyle(palette.value, { axisPointer: true, confine: true }),
    formatter: (params: unknown) => {
      const point = props.points[tooltipIndex(Array.isArray(params) ? params[0] : params)]
      if (!point)
        return ''
      return [
        escape(point.label),
        ...metrics.value.map(metric => `${escape(metric.name)}：${metric.key === 'rate' ? percent(tokenCacheHitRate(point.inputTokens, point.cachedTokens)) : point[metric.key].toLocaleString('zh-CN')}`),
        `总 Token：${point.totalTokens.toLocaleString('zh-CN')}`,
        `请求数：${point.requests.toLocaleString('zh-CN')}`,
        ...(point.cost != null ? [`实际费用：$${escape(point.cost)}`] : []),
      ].join('<br/>')
    },
  },
  series: metrics.value.filter(metric => !selected.value || selected.value === metric.name).map((metric): LineSeriesOption => ({
    name: metric.name,
    type: 'line',
    yAxisIndex: metric.key === 'rate' ? 1 : 0,
    data: props.points.map(point => metric.key === 'rate' ? tokenCacheHitRate(point.inputTokens, point.cachedTokens) : point[metric.key]),
    smooth: false,
    connectNulls: false,
    showSymbol: metric.key === 'rate'
      ? props.points.filter(point => point.inputTokens > 0).length <= 12
      : props.points.length <= 12,
    showAllSymbol: metric.key === 'rate',
    symbol: 'circle',
    symbolSize: 5,
    lineStyle: { color: metric.color, width: 2, type: metric.key === 'rate' ? 'dashed' : 'solid' },
    itemStyle: { color: metric.color },
  })),
}))
</script>

<template>
  <div class="grid min-w-0 gap-3">
    <div class="grid grid-cols-3 gap-1 rounded-xl bg-cp-fill-quaternary/45 p-1.5 sm:grid-cols-5">
      <button
        v-for="metric in metrics" :key="metric.name" type="button"
        class="grid min-w-0 gap-1.5 rounded-lg px-1.5 py-2 text-left transition-colors hover:bg-cp-fill-tertiary focus-visible:outline-2 focus-visible:outline-cp-primary"
        :class="selected === metric.name ? 'bg-cp-fill-tertiary' : ''"
        :aria-label="`单独查看${metric.name}趋势`" :aria-pressed="selected === metric.name"
        :title="metric.key === 'rate' ? '按 Token 计算：总缓存读取 ÷ 总输入；无输入时不计算。点击单独查看，再次点击恢复全部。' : `${metric.name}：${metric.value}。各项独立展示，不叠加计入总 Token。点击单独查看，再次点击恢复全部。`"
        @click="selected = selected === metric.name ? '' : metric.name"
      >
        <span class="flex items-center gap-1 whitespace-nowrap text-[10px] font-bold text-cp-text-secondary">
          <i class="size-1.5 shrink-0 rounded-full" :style="{ backgroundColor: metric.color }" />{{ metric.name }}
        </span>
        <strong class="truncate font-mono text-cp-base font-heavy tabular-nums text-cp-text">{{ metric.value }}</strong>
      </button>
    </div>
    <BaseChart v-if="hasData" :option="option" :height="210" />
    <BaseEmpty v-else surface="none" size="sm" :title="loading ? '正在加载用量趋势' : '暂无用量趋势'" class="h-52.5 place-content-center" />
  </div>
</template>
