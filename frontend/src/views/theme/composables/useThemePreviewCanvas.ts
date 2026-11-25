import type { ShallowRef } from 'vue'
import { useResizeObserver } from '@vueuse/core'
import { computed, nextTick, onBeforeUnmount, onMounted, readonly, shallowRef } from 'vue'

interface ThemePreviewCanvasOptions {
  viewport: ShallowRef<HTMLElement | null>
  stage: ShallowRef<HTMLElement | null>
  boardWidth: number
  boardHeight: number
}

const minimumScale = 0.25
const maximumScale = 1.5
const zoomStep = 0.1
const scaleGrid = 0.05
const viewportPadding = 48

export function useThemePreviewCanvas(options: ThemePreviewCanvasOptions) {
  const scale = shallowRef(1)
  const x = shallowRef(0)
  const y = shallowRef(0)
  const ready = shallowRef(false)
  const fitted = shallowRef(true)
  const dragging = shallowRef(false)

  let activePointerId: number | null = null
  let dragStartClientX = 0
  let dragStartClientY = 0
  let dragOriginX = 0
  let dragOriginY = 0

  const scalePercent = computed(() => Math.round(scale.value * 100))
  const canZoomOut = computed(() => scale.value > minimumScale)
  const canZoomIn = computed(() => scale.value < maximumScale)

  function clampScale(value: number) {
    const clamped = Math.min(maximumScale, Math.max(minimumScale, value))
    return Math.round(Math.round(clamped / scaleGrid) * scaleGrid * 100) / 100
  }

  function applyPosition() {
    const stage = options.stage.value
    if (!stage)
      return

    stage.style.transform = `translate3d(${Math.round(x.value)}px, ${Math.round(y.value)}px, 0)`
  }

  function viewportCenter() {
    const rect = options.viewport.value?.getBoundingClientRect()
    return {
      x: (rect?.width ?? 0) / 2,
      y: (rect?.height ?? 0) / 2,
    }
  }

  function setScale(nextScale: number, anchor = viewportCenter()) {
    const normalizedScale = clampScale(nextScale)
    if (normalizedScale === scale.value)
      return

    const previousScale = scale.value
    const boardX = (anchor.x - x.value) / previousScale
    const boardY = (anchor.y - y.value) / previousScale

    scale.value = normalizedScale
    x.value = Math.round(anchor.x - boardX * normalizedScale)
    y.value = Math.round(anchor.y - boardY * normalizedScale)
    fitted.value = false
    applyPosition()
  }

  function zoomIn() {
    setScale(scale.value + zoomStep)
  }

  function zoomOut() {
    setScale(scale.value - zoomStep)
  }

  function resetScale() {
    setScale(1)
  }

  function fitToViewport() {
    const viewport = options.viewport.value
    if (!viewport)
      return

    const rect = viewport.getBoundingClientRect()
    const availableWidth = Math.max(0, rect.width - viewportPadding * 2)
    const availableHeight = Math.max(0, rect.height - viewportPadding * 2)
    const rawScale = Math.min(
      availableWidth / options.boardWidth,
      availableHeight / options.boardHeight,
    )
    const nextScale = clampScale(Math.floor(rawScale / scaleGrid) * scaleGrid)

    scale.value = nextScale
    x.value = Math.round((rect.width - options.boardWidth * nextScale) / 2)
    y.value = Math.round((rect.height - options.boardHeight * nextScale) / 2)
    fitted.value = true
    applyPosition()
  }

  function handleWheel(event: WheelEvent) {
    event.preventDefault()
    const viewport = options.viewport.value
    if (!viewport)
      return

    const rect = viewport.getBoundingClientRect()
    const direction = event.deltaY > 0 ? -1 : 1
    setScale(
      scale.value + direction * zoomStep,
      { x: event.clientX - rect.left, y: event.clientY - rect.top },
    )
  }

  function handleKeydown(event: KeyboardEvent) {
    if (event.key === '+' || event.key === '=') {
      event.preventDefault()
      zoomIn()
      return
    }
    if (event.key === '-') {
      event.preventDefault()
      zoomOut()
      return
    }
    if (event.key === '0') {
      event.preventDefault()
      fitToViewport()
    }
  }

  function handleDoubleClick() {
    fitToViewport()
  }

  function handlePointerDown(event: PointerEvent) {
    if (event.button !== 0 || activePointerId !== null)
      return
    const viewport = options.viewport.value
    if (!viewport)
      return
    if (event.target instanceof Element && event.target.closest('[data-canvas-control], button, a, input, select, textarea'))
      return

    activePointerId = event.pointerId
    dragStartClientX = event.clientX
    dragStartClientY = event.clientY
    dragOriginX = x.value
    dragOriginY = y.value
    dragging.value = true
    fitted.value = false
    viewport.setPointerCapture?.(event.pointerId)
  }

  function handlePointerMove(event: PointerEvent) {
    if (activePointerId !== event.pointerId || !dragging.value)
      return

    x.value = Math.round(dragOriginX + (event.clientX - dragStartClientX))
    y.value = Math.round(dragOriginY + (event.clientY - dragStartClientY))
    applyPosition()
  }

  function endDrag(event: PointerEvent) {
    if (activePointerId !== event.pointerId)
      return
    const viewport = options.viewport.value
    if (viewport?.hasPointerCapture?.(event.pointerId))
      viewport.releasePointerCapture(event.pointerId)
    activePointerId = null
    dragging.value = false
  }

  useResizeObserver(options.viewport, () => {
    if (ready.value && fitted.value)
      fitToViewport()
  })

  onMounted(async () => {
    await nextTick()
    const viewport = options.viewport.value
    viewport?.addEventListener('wheel', handleWheel, { passive: false })
    viewport?.addEventListener('keydown', handleKeydown)
    viewport?.addEventListener('dblclick', handleDoubleClick)
    viewport?.addEventListener('pointerdown', handlePointerDown)
    viewport?.addEventListener('pointermove', handlePointerMove)
    viewport?.addEventListener('pointerup', endDrag)
    viewport?.addEventListener('pointercancel', endDrag)
    fitToViewport()
    ready.value = true
  })

  onBeforeUnmount(() => {
    const viewport = options.viewport.value
    viewport?.removeEventListener('wheel', handleWheel)
    viewport?.removeEventListener('keydown', handleKeydown)
    viewport?.removeEventListener('dblclick', handleDoubleClick)
    viewport?.removeEventListener('pointerdown', handlePointerDown)
    viewport?.removeEventListener('pointermove', handlePointerMove)
    viewport?.removeEventListener('pointerup', endDrag)
    viewport?.removeEventListener('pointercancel', endDrag)
    activePointerId = null
    dragging.value = false
  })

  return {
    canZoomIn,
    canZoomOut,
    dragging,
    fitToViewport,
    resetScale,
    scale: readonly(scale),
    scalePercent,
    zoomIn,
    zoomOut,
  }
}
