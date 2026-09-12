<script setup lang="ts">
import { onKeyStroke, useMediaQuery } from '@vueuse/core'
import { storeToRefs } from 'pinia'
import { nextTick, onBeforeUnmount, onMounted, provide, ref, shallowRef, watch } from 'vue'
import { RouterView, useRoute } from 'vue-router'
import BaseScrollbar from '@/components/base/BaseScrollbar.vue'

import { useAuthStore } from '@/stores/modules/auth'
import { useSystemUpdateStore } from '@/stores/modules/system-update'
import { useUiStore } from '@/stores/modules/ui'

import AppAboutModal from './components/AppAboutModal.vue'
import AppSidebar from './components/AppSidebar.vue'
import SystemUpdateModal from './components/SystemUpdateModal/index.vue'

const uiStore = useUiStore()
const systemUpdateStore = useSystemUpdateStore()
const { sidebarCollapsed } = storeToRefs(uiStore)
const { loadedOnce } = storeToRefs(systemUpdateStore)
const { toggleSidebar } = uiStore
const route = useRoute()
const pageScrollbarRef = ref<InstanceType<typeof BaseScrollbar> | null>(null)
const mobileSidebarOpen = shallowRef(false)
const mobileSidebar = ref<HTMLElement | null>(null)
const desktop = useMediaQuery('(min-width: 961px)')
let sidebarTrigger: HTMLElement | null = null
const aboutOpen = shallowRef(false)
const systemUpdateOpen = shallowRef(false)
const systemUpdateOpening = shallowRef(false)

async function openMobileSidebar() {
  sidebarTrigger = document.activeElement instanceof HTMLElement ? document.activeElement : null
  mobileSidebarOpen.value = true
  await nextTick()
  mobileSidebar.value?.querySelector<HTMLButtonElement>('button')?.focus()
}

async function closeMobileSidebar() {
  mobileSidebarOpen.value = false
  await nextTick()
  if (sidebarTrigger?.isConnected)
    sidebarTrigger.focus()
}
provide('openMobileSidebar', openMobileSidebar)
watch(desktop, value => value && closeMobileSidebar())
onKeyStroke('Escape', (event) => {
  if (!event.defaultPrevented && mobileSidebarOpen.value)
    closeMobileSidebar()
})
onKeyStroke('Tab', (event) => {
  if (!mobileSidebarOpen.value || event.defaultPrevented)
    return
  const controls = Array.from(mobileSidebar.value?.querySelectorAll<HTMLElement>('button:not(:disabled), a[href], input:not(:disabled), [tabindex="0"]') ?? [])
    .filter(element => element.getClientRects().length > 0)
  const first = controls[0]
  const last = controls.at(-1)
  if (event.shiftKey && document.activeElement === first) {
    event.preventDefault()
    last?.focus()
  }
  else if (!event.shiftKey && document.activeElement === last) {
    event.preventDefault()
    first?.focus()
  }
})

async function openSystemUpdate() {
  if (systemUpdateOpen.value || systemUpdateOpening.value)
    return

  systemUpdateOpening.value = true
  try {
    if (!loadedOnce.value)
      await systemUpdateStore.loadSystem(false)
  }
  catch {
    // 弹窗打开后由弹窗内的加载逻辑提示失败原因。
  }
  finally {
    systemUpdateOpening.value = false
    systemUpdateOpen.value = true
  }
}

onMounted(() => {
  if (useAuthStore().isAdmin)
    void systemUpdateStore.loadVersion().catch(() => undefined)
})

onBeforeUnmount(() => {
  systemUpdateStore.disconnectUpdateEvents()
})

watch(
  () => route.fullPath,
  async () => {
    closeMobileSidebar()
    await nextTick()
    await pageScrollbarRef.value?.scrollToTop()
  },
  { flush: 'post' },
)
</script>

<template>
  <div class="relative flex h-dvh overflow-hidden bg-cp-bg-layout">
    <AppSidebar
      :collapsed="sidebarCollapsed"
      @toggle="toggleSidebar"
      @open-about="aboutOpen = true"
      @open-system-update="openSystemUpdate"
    />
    <main :inert="mobileSidebarOpen" class="relative isolate h-dvh min-w-0 flex-1 overflow-hidden">
      <BaseScrollbar ref="pageScrollbarRef">
        <div class="flex min-h-full min-w-0 flex-col p-4 min-[961px]:p-6">
          <RouterView v-slot="{ Component }">
            <component :is="Component" class="min-h-0 flex-1" />
          </RouterView>
        </div>
      </BaseScrollbar>
    </main>

    <Teleport to="body">
      <Transition name="mobile-sidebar">
        <div v-if="mobileSidebarOpen" ref="mobileSidebar" class="fixed inset-0 z-50 min-[961px]:hidden" role="dialog" aria-modal="true" aria-label="导航菜单" tabindex="-1">
          <button
            type="button"
            class="absolute inset-0 border-0 bg-black/32 backdrop-blur-[1px] cursor-default"
            aria-label="关闭侧边栏"
            @click="closeMobileSidebar"
          />
          <div class="mobile-sidebar-panel absolute inset-y-0 left-0 flex">
            <AppSidebar
              mobile
              @close="closeMobileSidebar"
              @navigate="closeMobileSidebar"
              @open-about="aboutOpen = true"
              @open-system-update="openSystemUpdate"
            />
          </div>
        </div>
      </Transition>
    </Teleport>

    <AppAboutModal v-model="aboutOpen" />
    <SystemUpdateModal v-model="systemUpdateOpen" />
  </div>
</template>

<style scoped>
.mobile-sidebar-enter-active,
.mobile-sidebar-leave-active {
  transition: opacity 180ms ease;
}

.mobile-sidebar-enter-active .mobile-sidebar-panel,
.mobile-sidebar-leave-active .mobile-sidebar-panel {
  transition: transform 220ms cubic-bezier(0.22, 1, 0.36, 1);
}

.mobile-sidebar-enter-from,
.mobile-sidebar-leave-to {
  opacity: 0;
}

.mobile-sidebar-enter-from .mobile-sidebar-panel,
.mobile-sidebar-leave-to .mobile-sidebar-panel {
  transform: translate3d(-100%, 0, 0);
}

@media (prefers-reduced-motion: reduce) {
  .mobile-sidebar-enter-active,
  .mobile-sidebar-leave-active,
  .mobile-sidebar-enter-active .mobile-sidebar-panel,
  .mobile-sidebar-leave-active .mobile-sidebar-panel {
    transition-duration: 1ms;
  }
}
</style>
