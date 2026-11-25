<script setup lang="ts">
import type { MetricCardView, MetricTone } from '../composables/useDashboard'

import { computed, useId } from 'vue'
import BaseCard from '@/components/base/BaseCard.vue'
import BaseMotionIcon from '@/components/base/BaseMotionIcon.vue'

import { useThemeColor } from '@/composables/useThemeColor'
import { colorWithAlpha } from '@/utils/color'
import { metricToneIconClasses, metricToneValueClasses } from '../constants'
import AnimatedMetricValue from './AnimatedMetricValue.vue'

const props = defineProps<{
  metric: MetricCardView
}>()

const sparklineGradientId = `cp-sparkline-${useId()}`

const trendToneClasses: Record<MetricTone, string> = {
  normal: 'bg-cp-text-tertiary',
  info: 'bg-cp-info-text',
  success: 'bg-cp-success-text',
  warning: 'bg-cp-warning-text',
  danger: 'bg-cp-error-text',
}

const themeColor = useThemeColor()
const sparklineColorTokens: Record<MetricTone, { token: string, fallback: string }> = {
  normal: { token: '--cp-color-blue-solid', fallback: '#5983F4' },
  info: { token: '--cp-color-blue-solid', fallback: '#5983F4' },
  success: { token: '--cp-color-green-solid', fallback: '#12B981' },
  warning: { token: '--cp-color-orange-solid', fallback: '#F59E0B' },
  danger: { token: '--cp-color-error', fallback: '#EF4444' },
}

const SPARKLINE_WIDTH = 160
const SPARKLINE_HEIGHT = 64
const SPARKLINE_PAD_Y = 5

const sparklineSvg = computed(() => {
  const values = props.metric.sparkline?.values ?? []
  if (values.length < 2)
    return null

  const colorToken = sparklineColorTokens[props.metric.sparkline?.tone ?? 'normal']
  const stroke = themeColor(colorToken.token, colorToken.fallback)
  const min = Math.min(...values)
  const max = Math.max(...values)
  const span = max - min
  const usableHeight = SPARKLINE_HEIGHT - SPARKLINE_PAD_Y * 2
  const stepX = SPARKLINE_WIDTH / (values.length - 1)

  const points = values.map((value, index) => {
    const normalized = span > 0 ? (value - min) / span : 0.5
    return {
      x: index * stepX,
      y: SPARKLINE_PAD_Y + usableHeight * (1 - normalized),
    }
  })

  const linePath = smoothLinePath(points)
  const first = points[0]!
  const last = points[points.length - 1]!
  const areaPath = `${linePath} L ${last.x.toFixed(2)} ${SPARKLINE_HEIGHT} L ${first.x.toFixed(2)} ${SPARKLINE_HEIGHT} Z`

  return {
    stroke,
    fillStart: colorWithAlpha(stroke, 0.12),
    fillEnd: colorWithAlpha(stroke, 0),
    linePath,
    areaPath,
  }
})

function smoothLinePath(points: Array<{ x: number, y: number }>) {
  const start = points[0]!
  let path = `M ${start.x.toFixed(2)} ${start.y.toFixed(2)}`
  for (let i = 0; i < points.length - 1; i += 1) {
    const p0 = points[Math.max(0, i - 1)]!
    const p1 = points[i]!
    const p2 = points[i + 1]!
    const p3 = points[Math.min(points.length - 1, i + 2)]!
    const cp1x = p1.x + (p2.x - p0.x) / 6
    const cp1y = p1.y + (p2.y - p0.y) / 6
    const cp2x = p2.x - (p3.x - p1.x) / 6
    const cp2y = p2.y - (p3.y - p1.y) / 6
    path += ` C ${cp1x.toFixed(2)} ${cp1y.toFixed(2)}, ${cp2x.toFixed(2)} ${cp2y.toFixed(2)}, ${p2.x.toFixed(2)} ${p2.y.toFixed(2)}`
  }
  return path
}
</script>

<template>
  <BaseCard as="article" padding="compact" class="cp-dashboard-metric relative h-38.5 min-w-0 w-full">
    <div class="flex items-start gap-3">
      <BaseMotionIcon
        class="inline-flex size-8.5 shrink-0 items-center justify-center rounded-cp-lg"
        :class="metricToneIconClasses[metric.tone]"
      >
        <component :is="metric.icon" :size="18" />
      </BaseMotionIcon>
      <span class="mt-1 text-cp leading-[1.15] font-emphasis text-cp-text-secondary">{{ metric.title }}</span>
    </div>

    <div class="mt-3.25 flex h-7.75 items-end gap-2">
      <strong class="max-w-full truncate font-mono text-[clamp(21px,2.4vw,28px)] leading-[1.05] font-heavy tabular-nums text-cp-text">
        <AnimatedMetricValue :value="metric.value" :raw-value="metric.valueRaw" :formatter="metric.valueFormatter" />
      </strong>
      <i
        v-if="metric.trend && metric.trend.direction !== 'flat'"
        class="mb-1.25 block size-3"
        :class="[
          trendToneClasses[metric.trend.tone],
          metric.trend.direction === 'up'
            ? '[clip-path:polygon(50%_0,100%_58%,66%_58%,66%_100%,34%_100%,34%_58%,0_58%)]'
            : '[clip-path:polygon(34%_0,66%_0,66%_42%,100%_42%,50%_100%,0_42%,34%_42%)]',
        ]"
      />
    </div>

    <div v-if="sparklineSvg" class="pointer-events-none absolute top-7 right-6 hidden h-16 w-[42%] sm:block" aria-hidden="true">
      <svg
        class="size-full overflow-visible"
        :viewBox="`0 0 ${SPARKLINE_WIDTH} ${SPARKLINE_HEIGHT}`"
        preserveAspectRatio="none"
      >
        <defs>
          <linearGradient :id="sparklineGradientId" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" :stop-color="sparklineSvg.fillStart" />
            <stop offset="100%" :stop-color="sparklineSvg.fillEnd" />
          </linearGradient>
        </defs>
        <path :d="sparklineSvg.areaPath" :fill="`url(#${sparklineGradientId})`" />
        <path
          :d="sparklineSvg.linePath"
          fill="none"
          :stroke="sparklineSvg.stroke"
          stroke-width="1.75"
          stroke-opacity="0.9"
          stroke-linecap="round"
          stroke-linejoin="round"
          vector-effect="non-scaling-stroke"
        />
      </svg>
    </div>

    <div
      class="mt-3 grid h-7.5 w-full grid-cols-[minmax(0,1fr)_minmax(0,1fr)] items-center rounded-cp-lg bg-cp-fill-alter/70 px-3"
    >
      <span class="inline-grid min-w-0 w-full grid-cols-[auto_minmax(0,1fr)] items-baseline gap-2.5">
        <span class="shrink-0 text-cp-xs leading-none font-emphasis text-cp-text-quaternary">
          {{ metric.details[0]?.label }}
        </span>
        <b
          class="min-w-0 truncate font-mono text-xs leading-none font-bold tabular-nums"
          :class="metric.details[0]?.tone ? metricToneValueClasses[metric.details[0].tone] : undefined"
        >
          {{ metric.details[0]?.value }}
        </b>
      </span>
      <span class="inline-grid min-w-0 w-full grid-cols-[auto_minmax(0,auto)] items-baseline justify-end gap-2.5">
        <span class="shrink-0 text-cp-xs leading-none font-emphasis text-cp-text-quaternary">
          {{ metric.details[1]?.label }}
        </span>
        <b
          class="min-w-0 truncate font-mono text-xs leading-none font-bold tabular-nums"
          :class="metric.details[1]?.tone ? metricToneValueClasses[metric.details[1].tone] : undefined"
        >
          {{ metric.details[1]?.value }}
        </b>
      </span>
    </div>
  </BaseCard>
</template>
