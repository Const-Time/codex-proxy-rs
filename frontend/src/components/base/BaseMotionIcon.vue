<script setup lang="ts">
import { onBeforeUnmount, useTemplateRef, watch } from 'vue'
import { useSharedReducedMotion } from '@/composables/useSharedMedia'

type MotionIconVariant = 'random' | 'brand'
type MotionPhase = 'enter' | 'leave'
type ReactionBuilder = (root: HTMLElement, glyph: HTMLElement) => Animation[]

interface MotionReaction {
  enter: ReactionBuilder
  leave: ReactionBuilder
}

const props = withDefaults(
  defineProps<{
    as?: keyof HTMLElementTagNameMap
    variant?: MotionIconVariant
  }>(),
  {
    as: 'span',
    variant: 'random',
  },
)

const preferredMotion = useSharedReducedMotion()
const icon = useTemplateRef<HTMLElement>('icon')
const glyph = useTemplateRef<HTMLElement>('glyph')
const EASE_OUT = 'cubic-bezier(0.22, 1, 0.36, 1)'
const EASE_BACK = 'cubic-bezier(0.34, 1.56, 0.64, 1)'
const EASE_IN_OUT = 'cubic-bezier(0.45, 0, 0.55, 1)'

const randomReactions: MotionReaction[] = [
  { enter: liftAndBloomEnter, leave: liftAndBloomLeave },
  { enter: lookAroundEnter, leave: lookAroundLeave },
  { enter: doubleTapEnter, leave: doubleTapLeave },
  { enter: counterSwingEnter, leave: counterSwingLeave },
  { enter: shortOrbitEnter, leave: shortOrbitLeave },
]
const brandReaction: MotionReaction = {
  enter: brandGreetingEnter,
  leave: brandGreetingLeave,
}

let previousReactionIndex = -1
let activeReaction: MotionReaction | undefined
let activeAnimations: Animation[] = []
let animationRunId = 0
let isPointerInside = false

function motionTargets() {
  return [icon.value, glyph.value].filter((target): target is HTMLElement => Boolean(target))
}

function clearMotionStyles(element: HTMLElement) {
  element.style.willChange = ''
  element.style.transform = ''
}

function stopActiveAnimations() {
  animationRunId += 1
  for (const animation of activeAnimations)
    animation.cancel()
  activeAnimations = []
}

function resetMotion() {
  stopActiveAnimations()
  activeReaction = undefined
  for (const target of motionTargets())
    clearMotionStyles(target)
}

function runMotion(
  root: HTMLElement,
  glyphElement: HTMLElement,
  phase: MotionPhase,
  build: () => Animation[],
) {
  const targets = [root, glyphElement]
  stopActiveAnimations()
  const runId = animationRunId
  for (const target of targets)
    target.style.willChange = 'transform'

  const animations = build()
  activeAnimations = animations
  void Promise.allSettled(animations.map(animation => animation.finished)).then(() => {
    if (runId !== animationRunId)
      return
    if (phase === 'leave' && !isPointerInside) {
      activeReaction = undefined
      for (const animation of animations)
        animation.cancel()
      activeAnimations = []
      for (const target of targets)
        clearMotionStyles(target)
      return
    }
    for (const target of targets)
      target.style.willChange = ''
  })
  return animations
}

function currentTransform(element: HTMLElement) {
  const value = getComputedStyle(element).transform
  return value && value !== 'none' ? value : 'translate3d(0, 0, 0) scale(1) rotate(0deg)'
}

function nextRandomReaction() {
  const index
    = previousReactionIndex < 0
      ? Math.floor(Math.random() * randomReactions.length)
      : (previousReactionIndex + 1 + Math.floor(Math.random() * (randomReactions.length - 1)))
        % randomReactions.length
  previousReactionIndex = index
  return randomReactions[index]!
}

function liftAndBloomEnter(root: HTMLElement, glyphElement: HTMLElement) {
  return runMotion(root, glyphElement, 'enter', () => [
    root.animate(
      [
        { transform: currentTransform(root) },
        { transform: 'translate3d(0, -3px, 0) scale(1.055)' },
      ],
      { duration: 220, easing: EASE_OUT, fill: 'forwards' },
    ),
    glyphElement.animate(
      [
        { transform: currentTransform(glyphElement) },
        { transform: 'translate3d(0, -0.5px, 0) rotate(-4deg) scale(1.13)' },
      ],
      { duration: 260, delay: 20, easing: EASE_BACK, fill: 'forwards' },
    ),
  ])
}

function liftAndBloomLeave(root: HTMLElement, glyphElement: HTMLElement) {
  return runMotion(root, glyphElement, 'leave', () => [
    glyphElement.animate(
      [{ transform: currentTransform(glyphElement) }, { transform: 'translate3d(0, 0, 0) rotate(0deg) scale(1)' }],
      { duration: 240, easing: EASE_BACK, fill: 'forwards' },
    ),
    root.animate(
      [{ transform: currentTransform(root) }, { transform: 'translate3d(0, 0, 0) scale(1)' }],
      { duration: 220, delay: 20, easing: EASE_OUT, fill: 'forwards' },
    ),
  ])
}

function lookAroundEnter(root: HTMLElement, glyphElement: HTMLElement) {
  return runMotion(root, glyphElement, 'enter', () => [
    root.animate(
      [
        { transform: currentTransform(root) },
        { transform: 'translate3d(0, -1px, 0) scale(1.025)' },
      ],
      { duration: 200, easing: EASE_OUT, fill: 'forwards' },
    ),
    glyphElement.animate(
      [
        { transform: currentTransform(glyphElement), offset: 0 },
        { transform: 'translate3d(-2.5px, 0, 0) rotate(-7deg)', offset: 0.28 },
        { transform: 'translate3d(2.5px, 0, 0) rotate(7deg)', offset: 0.65 },
        { transform: 'translate3d(1px, 0, 0) rotate(3deg) scale(1.06)', offset: 1 },
      ],
      { duration: 460, easing: EASE_IN_OUT, fill: 'forwards' },
    ),
  ])
}

function lookAroundLeave(root: HTMLElement, glyphElement: HTMLElement) {
  return liftAndBloomLeave(root, glyphElement)
}

function doubleTapEnter(root: HTMLElement, glyphElement: HTMLElement) {
  return runMotion(root, glyphElement, 'enter', () => [
    root.animate(
      [
        { transform: currentTransform(root), offset: 0 },
        { transform: 'translate3d(0, 1px, 0) scale(0.965)', offset: 0.34 },
        { transform: 'translate3d(0, -3px, 0) scale(1.055)', offset: 1 },
      ],
      { duration: 260, easing: EASE_BACK, fill: 'forwards' },
    ),
    glyphElement.animate(
      [
        { transform: currentTransform(glyphElement), offset: 0 },
        { transform: 'translate3d(0, 1.5px, 0) scale(0.92)', offset: 0.32 },
        { transform: 'translate3d(0, -2px, 0) scale(1.12)', offset: 1 },
      ],
      { duration: 280, easing: EASE_BACK, fill: 'forwards' },
    ),
  ])
}

function doubleTapLeave(root: HTMLElement, glyphElement: HTMLElement) {
  return runMotion(root, glyphElement, 'leave', () => [
    root.animate(
      [
        { transform: currentTransform(root), offset: 0 },
        { transform: 'translate3d(0, 0.5px, 0) scale(0.985)', offset: 0.37 },
        { transform: 'translate3d(0, 0, 0) scale(1)', offset: 1 },
      ],
      { duration: 270, easing: EASE_BACK, fill: 'forwards' },
    ),
    glyphElement.animate(
      [
        { transform: currentTransform(glyphElement), offset: 0 },
        { transform: 'translate3d(0, 0.75px, 0) scale(0.97)', offset: 0.35 },
        { transform: 'translate3d(0, 0, 0) scale(1)', offset: 1 },
      ],
      { duration: 280, easing: EASE_BACK, fill: 'forwards' },
    ),
  ])
}

function counterSwingEnter(root: HTMLElement, glyphElement: HTMLElement) {
  return runMotion(root, glyphElement, 'enter', () => [
    root.animate(
      [
        { transform: currentTransform(root), offset: 0 },
        { transform: 'translate3d(-1.5px, -1.5px, 0) rotate(-4deg) scale(1.035)', offset: 0.48 },
        { transform: 'translate3d(1px, -1px, 0) rotate(2deg) scale(1.035)', offset: 1 },
      ],
      { duration: 290, easing: EASE_IN_OUT, fill: 'forwards' },
    ),
    glyphElement.animate(
      [
        { transform: currentTransform(glyphElement), offset: 0 },
        { transform: 'translate3d(2px, 0, 0) rotate(8deg)', offset: 0.48 },
        { transform: 'translate3d(-1px, 0, 0) rotate(-5deg)', offset: 1 },
      ],
      { duration: 290, easing: EASE_IN_OUT, fill: 'forwards' },
    ),
  ])
}

function counterSwingLeave(root: HTMLElement, glyphElement: HTMLElement) {
  return liftAndBloomLeave(root, glyphElement)
}

function shortOrbitEnter(root: HTMLElement, glyphElement: HTMLElement) {
  return runMotion(root, glyphElement, 'enter', () => [
    root.animate(
      [
        { transform: currentTransform(root) },
        { transform: 'translate3d(0, -1.5px, 0) scale(1.025)' },
      ],
      { duration: 180, easing: EASE_OUT, fill: 'forwards' },
    ),
    glyphElement.animate(
      [
        { transform: currentTransform(glyphElement), offset: 0 },
        { transform: 'translate3d(2.5px, -2px, 0) rotate(5deg)', offset: 0.28 },
        { transform: 'translate3d(-2px, -0.5px, 0) rotate(-5deg)', offset: 0.6 },
        { transform: 'translate3d(0.75px, -1px, 0) rotate(2deg) scale(1.08)', offset: 1 },
      ],
      { duration: 460, easing: EASE_OUT, fill: 'forwards' },
    ),
  ])
}

function shortOrbitLeave(root: HTMLElement, glyphElement: HTMLElement) {
  return liftAndBloomLeave(root, glyphElement)
}

function brandGreetingEnter(root: HTMLElement, glyphElement: HTMLElement) {
  return runMotion(root, glyphElement, 'enter', () => [
    root.animate(
      [
        { transform: currentTransform(root), offset: 0 },
        { transform: 'scale(0.965)', offset: 0.33 },
        { transform: 'scale(1)', offset: 1 },
      ],
      { duration: 270, easing: EASE_BACK, fill: 'forwards' },
    ),
    glyphElement.animate(
      [
        { transform: currentTransform(glyphElement), offset: 0 },
        { transform: 'translate3d(0, 1px, 0) scale(0.92)', offset: 0.16 },
        { transform: 'translate3d(0, -1.5px, 0) rotate(-5deg) scale(1.28)', offset: 0.55 },
        { transform: 'translate3d(1px, -1.5px, 0) rotate(4deg) scale(1.2)', offset: 0.8 },
        { transform: 'translate3d(-0.5px, -1.5px, 0) rotate(-2deg) scale(1.24)', offset: 1 },
      ],
      { duration: 540, easing: EASE_IN_OUT, fill: 'forwards' },
    ),
  ])
}

function brandGreetingLeave(root: HTMLElement, glyphElement: HTMLElement) {
  return runMotion(root, glyphElement, 'leave', () => [
    root.animate(
      [
        { transform: currentTransform(root), offset: 0 },
        { transform: 'scale(0.985)', offset: 0.42 },
        { transform: 'scale(1)', offset: 1 },
      ],
      { duration: 320, easing: EASE_BACK, fill: 'forwards' },
    ),
    glyphElement.animate(
      [
        { transform: currentTransform(glyphElement), offset: 0 },
        { transform: 'translate3d(0.35px, -0.5px, 0) rotate(1deg) scale(1.08)', offset: 0.4 },
        { transform: 'translate3d(0, 0, 0) rotate(0deg) scale(1)', offset: 1 },
      ],
      { duration: 320, easing: EASE_BACK, fill: 'forwards' },
    ),
  ])
}

function playEnterReaction() {
  isPointerInside = true
  if (preferredMotion.value === 'reduce') {
    resetMotion()
    return
  }

  const root = icon.value
  const glyphElement = glyph.value
  if (!root || !glyphElement)
    return

  activeReaction = props.variant === 'brand' ? brandReaction : nextRandomReaction()
  activeReaction.enter(root, glyphElement)
}

function playLeaveReaction() {
  isPointerInside = false
  if (preferredMotion.value === 'reduce') {
    resetMotion()
    return
  }

  const root = icon.value
  const glyphElement = glyph.value
  if (!root || !glyphElement || !activeReaction) {
    resetMotion()
    return
  }

  activeReaction.leave(root, glyphElement)
}

watch(preferredMotion, (motion) => {
  if (motion === 'reduce')
    resetMotion()
})

onBeforeUnmount(resetMotion)
</script>

<template>
  <component
    :is="props.as"
    ref="icon"
    @pointerenter="playEnterReaction"
    @pointerleave="playLeaveReaction"
  >
    <span ref="glyph" class="inline-flex items-center justify-center">
      <slot />
    </span>
  </component>
</template>
