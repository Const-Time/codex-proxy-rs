import type { User } from '@/api/modules/users'
import { defineStore } from 'pinia'
import { computed, ref } from 'vue'

import { login as apiLogin, logout as apiLogout, getAuthStatus } from '@/api'
import { resetUnauthorizedHandling } from '@/api/request'
import { errorMessage } from '@/utils/async'

export const useAuthStore = defineStore('auth', () => {
  const user = ref<User | null>(null)
  const isAdmin = computed(() => user.value?.role === 'admin')
  const isAuthenticated = ref(false)
  const sessionChecked = ref(false)
  const loading = ref(false)
  const error = ref<string | null>(null)

  async function checkAuth() {
    try {
      const status = await getAuthStatus()
      user.value = status.user
      isAuthenticated.value = status.authenticated
      if (status.authenticated)
        resetUnauthorizedHandling()
      return status.authenticated
    }
    catch {
      user.value = null
      isAuthenticated.value = false
      return false
    }
    finally {
      sessionChecked.value = true
    }
  }

  async function login(payload: Parameters<typeof apiLogin>[0]) {
    try {
      loading.value = true
      error.value = null
      await apiLogin(payload)
      if (!await checkAuth())
        throw new Error('登录状态无法验证')

      isAuthenticated.value = true
      sessionChecked.value = true
      resetUnauthorizedHandling()

      return true
    }
    catch (cause: unknown) {
      error.value = errorMessage(cause, '登录失败')
      user.value = null
      isAuthenticated.value = false
      return false
    }
    finally {
      loading.value = false
    }
  }

  async function logout() {
    try {
      await apiLogout()
    }
    catch {
      // 忽略登出错误
    }
    finally {
      user.value = null
      isAuthenticated.value = false
      sessionChecked.value = true
    }
  }

  function invalidateSession() {
    user.value = null
    isAuthenticated.value = false
    sessionChecked.value = true
    loading.value = false
    error.value = null
  }

  return {
    user,
    isAdmin,
    isAuthenticated,
    sessionChecked,
    loading,
    error,
    checkAuth,
    login,
    logout,
    invalidateSession,
  }
})
