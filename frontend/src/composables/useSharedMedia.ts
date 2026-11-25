import { createSharedComposable, useMediaQuery, usePreferredReducedMotion } from '@vueuse/core'

export const useSharedReducedMotion = createSharedComposable(() => usePreferredReducedMotion())
export const useIsMobile = createSharedComposable(() => useMediaQuery('(max-width: 639px)'))
export const useIsDesktop = createSharedComposable(() => useMediaQuery('(min-width: 961px)'))
