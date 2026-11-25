<script setup lang="ts">
import type { HealthTimeline, HealthTimelinePoint } from '../constants'

import { computed, onBeforeUnmount, shallowRef, useTemplateRef, watch } from 'vue'
import BaseCard from '@/components/base/BaseCard.vue'
import BasePopover from '@/components/base/BasePopover.vue'
import { useSharedReducedMotion } from '@/composables/useSharedMedia'
import { formatHealthCount, healthLegend, healthReliabilityValueClass, healthStatusMeta } from '../constants'
import HealthTimelinePointPopover from './HealthTimelinePointPopover.vue'

const props = defineProps<{
  timeline: HealthTimeline
}>()

const healthPopoverArrowSurfaceClasses = {
  top: 'bg-cp-popover-header-bg',
  bottom: 'bg-cp-bg-elevated',
}

const EASE_BACK = 'cubic-bezier(0.34, 1.45, 0.64, 1)'
const EASE_OUT = 'cubic-bezier(0.22, 1, 0.36, 1)'

const timelineGrid = useTemplateRef<HTMLElement>('timelineGrid')
const preferredMotion = useSharedReducedMotion()
const points = computed(() => props.timeline.points)

const activePoint = shallowRef<HealthTimelinePoint>()
const activeAnchor = shallowRef<HTMLElement | null>(null)
const popoverOpen = shallowRef(false)
const highlightedPointTime = shallowRef<string>()
let wavedCellIndexes = new Set<number>()
let cachedButtons: HTMLButtonElement[] = []
let cachedCells: Array<HTMLElement | null> = []
const cellAnimations = new Map<HTMLElement, Animation>()

function observedRequests(point: HealthTimelinePoint) {
  return point.successRequests + point.failedRequests + point.cancelledRequests + point.callerErrorRequests
}

function isInteractivePoint(point: HealthTimelinePoint) {
  return point.status !== 'future' && observedRequests(point) > 0
}

function isActivePoint(point: HealthTimelinePoint) {
  return popoverOpen.value && highlightedPointTime.value === point.time
}

function activatePoint(point: HealthTimelinePoint, pointIndex: number, event: Event) {
  if (!(event.currentTarget instanceof HTMLElement))
    return
  if (!isInteractivePoint(point)) {
    closePointPopover()
    return
  }

  activePoint.value = point
  activeAnchor.value = event.currentTarget
  popoverOpen.value = true
  highlightedPointTime.value = point.time
  animatePointWave(pointIndex)
}

function closePointPopover() {
  popoverOpen.value = false
  resetPointInteraction()
}

function resetPointInteraction() {
  highlightedPointTime.value = undefined
  activePoint.value = undefined
  activeAnchor.value = null
  releasePointWave()
}

function ensureCachedElements() {
  if (cachedButtons.length === points.value.length && cachedButtons[0]?.isConnected)
    return
  cachedButtons = Array.from(timelineGrid.value?.querySelectorAll<HTMLButtonElement>('[data-health-timeline-point]') ?? [])
  cachedCells = cachedButtons.map(button => button.querySelector<HTMLElement>('[data-health-timeline-cell]'))
}

function clearCellAnimation(cell: HTMLElement) {
  cellAnimations.get(cell)?.cancel()
  cellAnimations.delete(cell)
  cell.style.willChange = ''
  cell.style.transform = ''
}

function playCellWave(cell: HTMLElement, y: number, scaleY: number, duration: number, easing: string) {
  const from = getComputedStyle(cell).transform
  cellAnimations.get(cell)?.cancel()
  cell.style.willChange = 'transform'
  const animation = cell.animate(
    [
      { transform: from && from !== 'none' ? from : 'translate3d(0, 0, 0) scaleY(1)' },
      { transform: `translate3d(0, ${y}px, 0) scaleY(${scaleY})` },
    ],
    { duration, easing, fill: 'forwards' },
  )
  cellAnimations.set(cell, animation)
}

function settleCell(cell: HTMLElement) {
  const from = getComputedStyle(cell).transform
  cellAnimations.get(cell)?.cancel()
  const animation = cell.animate(
    [
      { transform: from && from !== 'none' ? from : 'translate3d(0, 0, 0) scaleY(1)' },
      { transform: 'translate3d(0, 0, 0) scaleY(1)' },
    ],
    { duration: 420, easing: EASE_OUT },
  )
  cellAnimations.set(cell, animation)
  void animation.finished
    .then(() => {
      if (cellAnimations.get(cell) === animation) {
        cellAnimations.delete(cell)
        cell.style.willChange = ''
        cell.style.transform = ''
      }
    })
    .catch(() => undefined)
}

function animatePointWave(centerIndex: number) {
  ensureCachedElements()
  const centerButton = cachedButtons[centerIndex]
  if (!centerButton)
    return

  const centerRow = centerButton.offsetTop
  const nextCellIndexes = new Set<number>()
  const cellsByDistance: HTMLElement[][] = [[], [], []]

  for (let distance = 0; distance <= 2; distance += 1) {
    const candidateIndexes = distance === 0 ? [centerIndex] : [centerIndex - distance, centerIndex + distance]

    for (const index of candidateIndexes) {
      const button = cachedButtons[index]
      const point = points.value[index]
      const cell = cachedCells[index]
      if (!button || !point || !cell || button.offsetTop !== centerRow || !isInteractivePoint(point)) {
        continue
      }

      nextCellIndexes.add(index)
      cellsByDistance[distance]?.push(cell)
    }
  }

  const cellsToRelease = [...wavedCellIndexes]
    .filter(index => !nextCellIndexes.has(index))
    .map(index => cachedCells[index])
    .filter((cell): cell is HTMLElement => Boolean(cell))

  if (preferredMotion.value === 'reduce') {
    for (const cell of [...cellsByDistance.flat(), ...cellsToRelease])
      clearCellAnimation(cell)
    wavedCellIndexes = nextCellIndexes
    return
  }

  for (const cell of cellsByDistance[0] ?? [])
    playCellWave(cell, -6, 1.16, 260, EASE_BACK)
  for (const cell of cellsByDistance[1] ?? [])
    playCellWave(cell, -3.25, 1.08, 340, EASE_OUT)
  for (const cell of cellsByDistance[2] ?? [])
    playCellWave(cell, -1.25, 1.03, 420, EASE_OUT)
  for (const cell of cellsToRelease)
    settleCell(cell)
  wavedCellIndexes = nextCellIndexes
}

function releasePointWave() {
  ensureCachedElements()
  const cells = [...wavedCellIndexes]
    .map(index => cachedCells[index])
    .filter((cell): cell is HTMLElement => Boolean(cell))

  if (preferredMotion.value === 'reduce') {
    for (const cell of cells)
      clearCellAnimation(cell)
  }
  else {
    for (const cell of cells)
      settleCell(cell)
  }
  wavedCellIndexes = new Set<number>()
}

function pointAccessibilityLabel(point: HealthTimelinePoint) {
  const eligibleRequests = point.successRequests + point.failedRequests
  return `${point.time}，${healthStatusMeta[point.status].label}，有效请求 ${formatHealthCount(eligibleRequests)}，可用性 ${point.reliabilityDisplay}`
}

watch(points, () => {
  cachedButtons = []
  cachedCells = []
})

watch(popoverOpen, (open) => {
  if (open)
    return

  resetPointInteraction()
})

onBeforeUnmount(() => {
  for (const animation of cellAnimations.values())
    animation.cancel()
  cellAnimations.clear()
})
</script>

<template>
  <BaseCard as="article" :title="timeline.title" :description="timeline.description" class="w-full">
    <template #actions>
      <div class="flex w-full flex-wrap items-center justify-between gap-x-4 gap-y-2">
        <div
          class="flex max-w-full flex-wrap items-center gap-x-2 gap-y-1 text-cp-xs leading-none font-emphasis text-cp-text-quaternary"
        >
          <span
            v-for="item in healthLegend"
            :key="item.status"
            class="inline-flex h-3.5 items-center gap-1 align-middle leading-none"
          >
            <span class="block size-2 shrink-0 rounded-xs" :class="healthStatusMeta[item.status].cellClass" />
            <span class="block leading-none">{{ item.label }}</span>
          </span>
        </div>
        <div class="flex shrink-0 items-center gap-3">
          <strong
            class="font-mono text-sm leading-none font-heavy tabular-nums"
            :class="healthReliabilityValueClass(timeline.successRequests, timeline.failedRequests)"
          >
            {{ timeline.reliabilityDisplay }}
          </strong>
        </div>
      </div>
    </template>

    <template #body>
      <div>
        <BasePopover
          v-model="popoverOpen"
          trigger="hover"
          placement="top"
          :offset="12"
          :anchor-element="activeAnchor"
          :disabled="!activePoint"
          animate-position
          :arrow-surface-class="healthPopoverArrowSurfaceClasses"
          class="min-w-0 w-full"
        >
          <template #trigger>
            <div ref="timelineGrid" class="grid w-full grid-cols-48 items-end gap-x-0.5 gap-y-1 sm:grid-cols-96">
              <button
                v-for="(point, pointIndex) in points"
                :key="point.time"
                data-health-timeline-point
                type="button"
                :aria-disabled="!isInteractivePoint(point)"
                :tabindex="isInteractivePoint(point) ? 0 : -1"
                :aria-expanded="isActivePoint(point)"
                aria-haspopup="dialog"
                :aria-label="pointAccessibilityLabel(point)"
                class="group relative flex h-5 w-full min-w-0.5 items-center border-0 bg-transparent p-0 outline-none"
                :class="isInteractivePoint(point) ? 'cursor-pointer' : 'cursor-default'"
                @mouseenter="activatePoint(point, pointIndex, $event)"
                @focus="activatePoint(point, pointIndex, $event)"
                @blur="closePointPopover"
                @click="activatePoint(point, pointIndex, $event)"
              >
                <span
                  data-health-timeline-cell
                  class="block h-3.5 w-full origin-bottom rounded-xs transition-[filter,box-shadow] duration-250 ease-[cubic-bezier(0.22,1,0.36,1)] motion-reduce:transition-none group-focus-visible:shadow-[0_0_0_2px_var(--cp-color-bg-container),0_0_0_4px_var(--cp-control-outline)]"
                  :class="[
                    healthStatusMeta[point.status].cellClass,
                    isActivePoint(point)
                      ? 'brightness-105 shadow-[0_5px_10px_-5px_var(--cp-color-shadow)]'
                      : isInteractivePoint(point)
                        ? 'group-hover:brightness-95'
                        : undefined,
                  ]"
                />
              </button>
            </div>
          </template>

          <div class="w-72 overflow-hidden rounded-cp-lg">
            <HealthTimelinePointPopover v-if="activePoint" :point="activePoint" />
          </div>
        </BasePopover>
      </div>
    </template>
  </BaseCard>
</template>
