import { createRouter, createWebHistory } from 'vue-router'

import { toast } from '@/components/base/BaseToast'
import { useAuthStore } from '@/stores/modules/auth'
import { routes } from './routes'

export const router = createRouter({
  history: createWebHistory('/'),
  routes,
})

router.onError((error: Error) => {
  if (/Failed to fetch dynamically imported module|Importing a module script failed|Loading chunk .* failed|Unable to preload CSS/i.test(error.message)) {
    toast.error('页面资源加载失败，可能已更新或网络异常。请刷新页面后重试。', { duration: 10000 })
  }
})

// 路由守卫
router.beforeEach(async (to) => {
  const authStore = useAuthStore()

  // 登录页面不需要认证
  if (to.path === '/login') {
    // 如果已登录，重定向到首页
    if (authStore.isAuthenticated) {
      return authStore.isAdmin ? '/' : '/api-keys'
    }
    return
  }

  // 其他页面需要认证
  if (!authStore.isAuthenticated && !authStore.sessionChecked) {
    // 尝试检查认证状态
    const isAuth = await authStore.checkAuth()
    if (!isAuth) {
      // 未认证，跳转到登录页
      return '/login'
    }
  }

  if (!authStore.isAuthenticated) {
    return '/login'
  }
  if (!authStore.isAdmin && !['/api-keys', '/my-usage', '/profile', '/theme'].includes(to.path))
    return '/api-keys'
})
