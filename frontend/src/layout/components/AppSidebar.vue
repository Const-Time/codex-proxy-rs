<script setup lang="ts">
import {
  ArrowUpCircle,
  ChartNoAxesColumn,
  CreditCard,
  FolderTree,
  Info,
  KeyRound,
  LayoutDashboard,
  LogOut,
  Moon,
  Network,
  Palette,
  PanelLeftClose,
  PanelLeftOpen,
  Puzzle,
  Server,
  Settings,
  ShieldCheck,
  Sun,
  UserRound,
  Users,
} from '@lucide/vue'
import { useResizeObserver, useTimeoutFn } from '@vueuse/core'
import { storeToRefs } from 'pinia'
import { computed, defineAsyncComponent, nextTick, onBeforeUnmount, onMounted, ref, shallowRef, useTemplateRef, watch } from 'vue'
import { RouterLink, useRoute, useRouter } from 'vue-router'

import AppBrandMark from '@/components/AppBrandMark.vue'
import BaseIconButton from '@/components/base/BaseIconButton.vue'
import BaseMotionIcon from '@/components/base/BaseMotionIcon.vue'
import BaseScrollbar from '@/components/base/BaseScrollbar.vue'
import { useSharedReducedMotion } from '@/composables/useSharedMedia'
import { useAuthStore } from '@/stores/modules/auth'
import { useSystemUpdateStore } from '@/stores/modules/system-update'
import { useThemeStore } from '@/stores/modules/theme'

const props = withDefaults(
  defineProps<{
    collapsed?: boolean
    mobile?: boolean
  }>(),
  {
    collapsed: false,
    mobile: false,
  },
)
const emit = defineEmits<{
  close: []
  navigate: []
  openAbout: []
  openSystemUpdate: []
  toggle: []
}>()
const MySubscriptionsModal = defineAsyncComponent(() => import('./MySubscriptionsModal.vue'))
const subscriptionsOpen = ref(false)
const subscriptionsLoaded = shallowRef(false)
const route = useRoute()
const router = useRouter()
const authStore = useAuthStore()
const systemUpdateStore = useSystemUpdateStore()
const themeStore = useThemeStore()
const { version, hasUpdate } = storeToRefs(systemUpdateStore)
const { effectiveTheme } = storeToRefs(themeStore)
const { toggleTheme } = themeStore
const preferredMotion = useSharedReducedMotion()

const adminNavItems = [
  { label: '概览', icon: LayoutDashboard, path: '/' },
  { label: '用户管理', icon: Users, path: '/users' },
  { label: '分组管理', icon: FolderTree, path: '/account-groups' },
  { label: '订阅管理', icon: CreditCard, path: '/subscriptions' },
  { label: '账号管理', icon: Server, path: '/accounts' },
  { label: '代理管理', icon: Network, path: '/proxies' },
  { label: '插件', icon: Puzzle, path: '/plugins' },
  { label: '使用统计', icon: ChartNoAxesColumn, path: '/usage' },
  { label: '操作日志', icon: ShieldCheck, path: '/operation-logs' },
  { label: '系统设置', icon: Settings, path: '/settings' },
]

const personalNavItems = [
  { label: '我的密钥', icon: KeyRound, path: '/api-keys' },
  { label: '使用记录', icon: ChartNoAxesColumn, path: '/my-usage' },
  { label: '个人资料', icon: UserRound, path: '/profile' },
  { label: '主题设置', icon: Palette, path: '/theme' },
]
const navItems = computed(() => [
  ...(authStore.isAdmin ? adminNavItems : []),
  ...personalNavItems,
])
function sectionLabel(index: number) {
  if (authStore.isAdmin && index === 0)
    return '系统管理'
  return index === (authStore.isAdmin ? adminNavItems.length : 0) ? '我的账户' : null
}

function isActive(path: string) {
  if (path === '/')
    return route.path === '/'
  return route.path === path || route.path.startsWith(`${path}/`)
}

const navEl = useTemplateRef<HTMLElement>('nav')
const navSignalEl = useTemplateRef<HTMLElement>('navSignal')
const activeNavIndex = computed(() => {
  const index = navItems.value.findIndex(item => isActive(item.path))
  return Math.max(0, index)
})
const activeIndicatorTop = shallowRef<number | null>(null)
const activeNavIndicatorStyle = computed(() => {
  const fallbackTop = activeNavIndex.value * 48 + (authStore.isAdmin && activeNavIndex.value >= adminNavItems.length ? 72 : 36)
  return {
    transform: `translate3d(0, ${activeIndicatorTop.value ?? fallbackTop}px, 0)`,
  }
})
const navFeedbackMuted = shallowRef(false)
let navSignalAnimation: Animation | undefined

const { start: restoreNavFeedback, stop: stopNavFeedbackRestore } = useTimeoutFn(
  () => {
    navFeedbackMuted.value = false
  },
  300,
  { immediate: false },
)

function muteNavFeedbackDuringMove() {
  navFeedbackMuted.value = true
  stopNavFeedbackRestore()
  restoreNavFeedback()
}

function syncActiveIndicator() {
  const links = navEl.value?.querySelectorAll<HTMLElement>('[data-nav-item]')
  const activeElement = links?.[activeNavIndex.value]
  if (activeElement)
    activeIndicatorTop.value = activeElement.offsetTop
}

function handleNavClick(event: MouseEvent) {
  // 允许用户通过中键或组合键在新标签页打开，仅普通点击收起移动侧边栏。
  if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey || event.button !== 0)
    return
  muteNavFeedbackDuringMove()
  emit('navigate')
}

function openSubscriptions() {
  subscriptionsLoaded.value = true
  subscriptionsOpen.value = true
}

function openSystemUpdate() {
  if (!authStore.isAdmin)
    return
  emit('openSystemUpdate')
}

async function handleLogout() {
  await authStore.logout()
  await router.push('/login')
  emit('navigate')
}

const isCollapsed = computed(() => !props.mobile && Boolean(props.collapsed))
const themeToggleLabel = computed(() => (effectiveTheme.value === 'dark' ? '切换浅色模式' : '切换暗黑模式'))
const versionText = computed(() => version.value?.version.trim() ?? '')
const hasVersionLabel = computed(() => versionText.value.length > 0)
const versionLabel = computed(() => `v${versionText.value}`)
const updateButtonLabel = computed(() => (hasUpdate.value ? '发现新版本，打开系统更新' : '打开系统更新'))

function animateNavSignal() {
  const signal = navSignalEl.value
  if (!signal)
    return

  navSignalAnimation?.cancel()
  if (preferredMotion.value === 'reduce') {
    signal.style.opacity = '0'
    return
  }

  navSignalAnimation = signal.animate(
    [
      { opacity: 0.48, transform: 'translate3d(-70%, 0, 0)' },
      { opacity: 0, transform: 'translate3d(120%, 0, 0)' },
    ],
    {
      duration: 520,
      easing: 'cubic-bezier(0.22, 1, 0.36, 1)',
    },
  )
}

useResizeObserver(navEl, syncActiveIndicator)

onMounted(async () => {
  await nextTick()
  syncActiveIndicator()
})

watch(
  [activeNavIndex, isCollapsed, () => authStore.isAdmin],
  async () => {
    await nextTick()
    syncActiveIndicator()
  },
  { flush: 'post' },
)

watch(
  () => route.path,
  async (path, previousPath) => {
    if (path === previousPath)
      return
    muteNavFeedbackDuringMove()
    await nextTick()
    syncActiveIndicator()
    animateNavSignal()
  },
  { flush: 'post' },
)

onBeforeUnmount(() => {
  stopNavFeedbackRestore()
  navSignalAnimation?.cancel()
})
</script>

<template>
  <aside
    class="z-20 h-dvh shrink-0 flex-col overflow-hidden bg-(--cp-layout-sider-bg) shadow-cp-layout-sider transition-[width,flex-basis] duration-300 ease-[cubic-bezier(0.22,1,0.36,1)] motion-reduce:transition-none"
    :class="[
      mobile ? 'flex' : 'hidden min-[961px]:flex',
      isCollapsed ? 'w-19 basis-19 items-center' : 'w-58 basis-58',
    ]"
  >
    <div
      class="mx-4 mt-6 grid h-12 shrink-0 grid-cols-[44px_minmax(0,1fr)] items-center"
      :class="isCollapsed ? 'w-11 justify-start' : 'self-stretch gap-3'"
    >
      <BaseMotionIcon
        variant="brand"
        class="inline-flex size-11 items-center justify-center relative -top-0.5 rounded-cp"
      >
        <AppBrandMark class="block size-11 select-none" />
      </BaseMotionIcon>
      <span
        v-show="!isCollapsed"
        class="grid min-w-33 content-center overflow-hidden transition-[opacity,transform] duration-200 ease-[cubic-bezier(0.22,1,0.36,1)] motion-reduce:transition-none"
        :class="isCollapsed ? 'pointer-events-none -translate-x-1.5 opacity-0' : 'translate-x-0 opacity-100'"
      >
        <strong class="text-base leading-[1.1] font-heavy text-cp-text"> Codex Proxy </strong>
        <span class="mt-1.5 flex h-4.5 min-w-0 items-center gap-2">
          <span class="shrink-0 text-xs leading-none font-emphasis text-cp-text-secondary"> Rust build </span>
          <button
            v-if="authStore.isAdmin && hasVersionLabel"
            type="button"
            class="inline-flex h-4.5 min-w-0 cursor-pointer items-center gap-1 rounded-cp-sm border-0 px-1.5 font-mono text-[10px] leading-none font-bold transition-colors outline-none focus-visible:ring-2 focus-visible:ring-cp-control-outline focus-visible:ring-offset-2 focus-visible:ring-offset-cp-bg-container"
            :class="[
              hasUpdate
                ? 'bg-cp-success-container text-cp-success-on-container hover:bg-cp-success-container-hover'
                : 'bg-cp-fill-quaternary text-cp-text-quaternary hover:bg-cp-fill-tertiary hover:text-cp-text-secondary',
            ]"
            :title="updateButtonLabel"
            @click="openSystemUpdate"
          >
            <span>{{ versionLabel }}</span>
            <ArrowUpCircle v-if="hasUpdate" class="size-3 shrink-0 text-cp-success" />
          </button>
        </span>
      </span>
    </div>

    <BaseScrollbar class="my-4 w-full flex-1">
      <div class="px-4">
        <nav ref="nav" class="relative grid gap-2" :class="isCollapsed ? 'mx-auto w-10' : 'w-full'" aria-label="主导航">
          <span
            class="pointer-events-none absolute inset-x-0 top-0 h-10 overflow-hidden rounded-cp bg-cp-menu-item-selected-bg transition-transform duration-260 ease-[cubic-bezier(0.22,1,0.36,1)] motion-reduce:transition-none"
            :style="activeNavIndicatorStyle"
          >
            <span
              ref="navSignal"
              class="absolute inset-y-0 left-0 w-2/3 opacity-0 [background:linear-gradient(90deg,transparent,color-mix(in_srgb,var(--cp-color-info)_9%,transparent),transparent)]"
            />
          </span>
          <template v-for="(item, index) in navItems" :key="item.path">
            <div v-if="sectionLabel(index)" class="flex h-7 items-end px-3 pb-1 text-xs font-semibold text-cp-text-quaternary">
              <span :class="isCollapsed ? 'sr-only' : 'whitespace-nowrap'">{{ sectionLabel(index) }}</span>
              <span v-if="isCollapsed" class="h-px w-full bg-cp-fill-tertiary" aria-hidden="true" />
            </div>
            <RouterLink
              :to="item.path"
              data-nav-item
              :aria-current="isActive(item.path) ? 'page' : undefined"
              :aria-label="isCollapsed ? item.label : undefined"
              :title="isCollapsed ? item.label : undefined"
              class="relative z-10 inline-flex h-10 cursor-pointer items-center rounded-cp border-0 text-sm leading-[1.15] no-underline outline-none focus-visible:ring-2 focus-visible:ring-cp-control-outline focus-visible:ring-offset-2 focus-visible:ring-offset-cp-bg-container"
              :class="[
                isCollapsed ? 'w-10 justify-center' : 'w-full gap-2.5 px-3',
                isActive(item.path)
                  ? navFeedbackMuted
                    ? 'bg-transparent font-bold text-cp-text transition-none'
                    : 'bg-transparent font-bold text-cp-text transition-colors duration-200'
                  : navFeedbackMuted
                    ? 'bg-transparent font-semibold text-cp-text-secondary transition-none'
                    : 'bg-transparent font-semibold text-cp-text-secondary transition-colors duration-200 hover:bg-cp-fill-quaternary hover:text-cp-text',
              ]"
              @click="handleNavClick"
            >
              <component :is="item.icon" class="shrink-0" :size="18" aria-hidden="true" />
              <span
                class="overflow-hidden whitespace-nowrap transition-[opacity,transform] duration-200 motion-reduce:transition-none"
                :class="isCollapsed ? 'pointer-events-none w-0 -translate-x-1.5 opacity-0' : 'w-auto translate-x-0 opacity-100'"
              >
                {{ item.label }}
              </span>
            </RouterLink>
          </template>
        </nav>
      </div>
    </BaseScrollbar>

    <MySubscriptionsModal v-if="subscriptionsLoaded" v-model="subscriptionsOpen" />
    <div class="mb-6 max-w-[calc(100%-2rem)] shrink-0 self-center" :class="isCollapsed ? 'w-11' : 'w-50'">
      <div
        class="bg-cp-fill-quaternary"
        :class="isCollapsed ? 'grid gap-1 rounded-cp p-1' : 'grid gap-1 rounded-cp-lg px-2 py-2'"
      >
        <div
          v-if="!isCollapsed"
          class="flex min-w-0 items-center gap-1 text-cp-sm font-emphasis text-cp-text-secondary"
        >
          <div class="flex min-w-0 flex-1 items-center gap-1" :title="`在线 · ${authStore.user?.email || ''}`">
            <span class="inline-flex size-cp-control-sm shrink-0 items-center justify-center" aria-label="在线">
              <span class="size-2 rounded-full bg-cp-success" />
            </span>
            <span class="truncate">{{ authStore.user?.username || authStore.user?.email }}</span>
          </div>
          <BaseIconButton size="sm" class="shrink-0" label="退出登录" variant="destructive" @click="handleLogout">
            <LogOut :size="18" />
          </BaseIconButton>
        </div>

        <div class="flex items-center" :class="isCollapsed ? 'grid gap-1' : 'w-full justify-between'">
          <BaseIconButton
            v-if="authStore.isAdmin && isCollapsed && hasUpdate"
            variant="success"
            size="md"
            :label="updateButtonLabel"
            @click="openSystemUpdate"
          >
            <ArrowUpCircle :size="19" />
          </BaseIconButton>

          <BaseIconButton
            v-if="isCollapsed"
            size="md"
            label="退出登录"
            variant="destructive"
            @click="handleLogout"
          >
            <LogOut :size="19" />
          </BaseIconButton>

          <BaseIconButton variant="ghost" :size="isCollapsed ? 'md' : 'sm'" label="我的订阅" :pressed="subscriptionsOpen" @click="openSubscriptions">
            <CreditCard :size="isCollapsed ? 19 : 18" />
          </BaseIconButton>

          <BaseIconButton
            variant="ghost"
            :size="isCollapsed ? 'md' : 'sm'"
            :label="themeToggleLabel"
            @click="toggleTheme($event)"
          >
            <Sun v-if="effectiveTheme === 'dark'" :size="isCollapsed ? 19 : 18" />
            <Moon v-else :size="isCollapsed ? 19 : 18" />
          </BaseIconButton>

          <BaseIconButton variant="ghost" :size="isCollapsed ? 'md' : 'sm'" label="关于" @click="emit('openAbout')">
            <Info :size="isCollapsed ? 19 : 18" />
          </BaseIconButton>

          <BaseIconButton v-if="mobile" variant="ghost" size="sm" label="关闭侧边栏" data-sidebar-close @click="emit('close')">
            <PanelLeftClose :size="18" />
          </BaseIconButton>

          <BaseIconButton
            v-else
            variant="ghost"
            :size="isCollapsed ? 'md' : 'sm'"
            data-sidebar-toggle
            :label="isCollapsed ? '展开侧边栏' : '收缩侧边栏'"
            @click="emit('toggle')"
          >
            <PanelLeftOpen v-if="isCollapsed" :size="19" />
            <PanelLeftClose v-else :size="18" />
          </BaseIconButton>
        </div>
      </div>
    </div>
  </aside>
</template>
