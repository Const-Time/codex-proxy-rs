<script setup lang="ts">
import type { BaseTablePagination, PagerItem } from './pagination'
import { ChevronLeft, ChevronRight } from '@lucide/vue'

import { computed } from 'vue'
import BaseIconButton from '../BaseIconButton.vue'
import BaseSelect from '../BaseSelect.vue'
import {
  getCurrentPage,
  getPagerItems,
  getPageSizeOptions,
  getTotalPages,
} from './pagination'

const props = defineProps<{
  pagination: BaseTablePagination
  loading: boolean
}>()

const emit = defineEmits<{
  pageChange: [page: number]
  pageSizeChange: [pageSize: number]
}>()

const totalPages = computed(() => getTotalPages(props.pagination))
const currentPage = computed(() => getCurrentPage(props.pagination, totalPages.value))
const pageSizeOptions = computed(() => getPageSizeOptions(props.pagination))
const pagerItems = computed(() => getPagerItems(totalPages.value, currentPage.value))
const adjacentPagerItems = computed(() => {
  const count = Math.min(3, totalPages.value)
  const start = Math.max(1, Math.min(currentPage.value - 1, totalPages.value - count + 1))
  return Array.from({ length: count }, (_, index) => start + index)
})
const compactPagerItems = computed<PagerItem[]>(() => {
  const total = totalPages.value
  const current = currentPage.value
  if (total <= 5)
    return Array.from({ length: total }, (_, index) => index + 1)
  if (current <= 3)
    return [1, 2, 3, 'ellipsis', total]
  if (current >= total - 2)
    return [1, 'ellipsis', total - 2, total - 1, total]
  return [1, 'ellipsis', current, 'ellipsis', total]
})

const pagerVisibilityClasses = [
  'flex @min-[19.5rem]/pagination:hidden',
  'hidden @min-[19.5rem]/pagination:flex @min-[24.5rem]/pagination:hidden',
  'hidden @min-[24.5rem]/pagination:flex @min-[29.5rem]/pagination:hidden',
  'hidden @min-[29.5rem]/pagination:flex',
]

const pageSizeModel = computed({
  get: () => String(props.pagination.pageSize),
  set: (value: string) => {
    if (props.loading) {
      return
    }

    const pageSize = Number(value)
    if (Number.isFinite(pageSize) && pageSize > 0) {
      emit('pageSizeChange', pageSize)
    }
  },
})

function goToPage(page: number) {
  if (props.loading || page < 1 || page > totalPages.value || page === currentPage.value) {
    return
  }

  emit('pageChange', page)
}

function paginationPageClass(page: number) {
  return [
    'inline-flex size-8 items-center justify-center rounded-cp border-0 text-xs font-bold leading-none transition-colors duration-150 outline-none',
    page === currentPage.value
      ? 'cursor-default bg-(--cp-button-primary-bg) text-(--cp-button-primary-color)'
      : 'cursor-pointer bg-cp-fill-quaternary text-cp-text hover:bg-cp-bg-text-hover focus-visible:ring-2 focus-visible:ring-cp-control-outline focus-visible:ring-offset-2 focus-visible:ring-offset-cp-bg-container',
  ]
}
</script>

<template>
  <footer
    class="@container/pagination mt-2 min-h-10 min-w-0 shrink-0 px-0 py-1"
  >
    <div class="grid min-w-0 grid-cols-[auto_minmax(0,1fr)] items-center gap-2 @min-[36rem]/pagination:grid-cols-[minmax(0,1fr)_auto_auto] @min-[36rem]/pagination:gap-3">
      <span class="hidden min-w-0 truncate text-cp-sm font-emphasis text-cp-text-secondary @min-[36rem]/pagination:block">
        共 {{ pagination.total }} 条
      </span>
      <BaseSelect
        v-model="pageSizeModel"
        aria-label="每页条数"
        :options="pageSizeOptions"
        :disabled="loading"
        size="sm"
        class="w-28 shrink-0"
      />

      <nav
        aria-label="分页"
        class="flex shrink-0 items-center justify-self-end gap-2"
      >
        <BaseIconButton
          variant="secondary"
          size="sm"
          :disabled="loading || currentPage <= 1"
          label="上一页"
          @click="goToPage(currentPage - 1)"
        >
          <ChevronLeft class="size-4" />
        </BaseIconButton>

        <div
          v-for="(items, variant) in [[currentPage], adjacentPagerItems, compactPagerItems, pagerItems]"
          :key="variant"
          class="items-center gap-2"
          :class="pagerVisibilityClasses[variant]"
        >
          <template v-for="(item, index) in items" :key="`${item}-${index}`">
            <span
              v-if="item === 'ellipsis'"
              class="inline-flex size-8 items-center justify-center text-xs font-bold text-cp-text-quaternary"
            >
              …
            </span>
            <button
              v-else
              type="button"
              :class="paginationPageClass(item)"
              :disabled="loading || item === currentPage"
              :aria-current="item === currentPage ? 'page' : undefined"
              @click="goToPage(item)"
            >
              {{ item }}
            </button>
          </template>
        </div>

        <BaseIconButton
          variant="secondary"
          size="sm"
          :disabled="loading || currentPage >= totalPages"
          label="下一页"
          @click="goToPage(currentPage + 1)"
        >
          <ChevronRight class="size-4" />
        </BaseIconButton>
      </nav>
    </div>
  </footer>
</template>
