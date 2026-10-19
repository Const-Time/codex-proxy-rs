import type { Dayjs } from 'dayjs'
import dayjs from 'dayjs'
import { computed, shallowRef } from 'vue'
import { calendarDate, customUsageRange } from '../utils/dateRange'

export type UsageTimeRange = 'today' | '7d' | '30d' | 'custom'

export interface UsageTimeRangeParams extends Record<string, string> {
  startTime: string
  endTime: string
}

export function useUsageTimeRange(initialRange: UsageTimeRange = 'today') {
  const timeRange = shallowRef<UsageTimeRange>(initialRange)
  const rangeEnd = shallowRef(dayjs())
  const customDates = shallowRef<[string, string]>([calendarDate(new Date()), calendarDate(new Date())])
  function rangeParams(end: Dayjs): UsageTimeRangeParams {
    if (timeRange.value === 'custom') {
      // A selected historical range stays fixed across midnight; validate new selections on apply.
      const selectedEnd = customDates.value[1]
      const start = dayjs(customDates.value[0]).startOf('day')
      const nextDay = dayjs(selectedEnd).add(1, 'day').startOf('day')
      return { startTime: start.toISOString(), endTime: (nextDay.isBefore(end) ? nextDay : end).toISOString() }
    }
    return buildTimeRange(timeRange.value, end)
  }
  const timeRangeParams = computed(() => rangeParams(rangeEnd.value))
  function setCustomRange(range: [string, string]) {
    if (customUsageRange(...range))
      customDates.value = range
  }

  function refreshTimeRangeEnd() {
    rangeEnd.value = dayjs()
  }

  function latestTimeRangeParams() {
    return rangeParams(dayjs())
  }

  return {
    timeRange,
    timeRangeParams,
    customDates,
    setCustomRange,
    refreshTimeRangeEnd,
    latestTimeRangeParams,
  }
}

function buildTimeRange(range: UsageTimeRange, end: Dayjs): UsageTimeRangeParams {
  if (range === 'today') {
    return {
      startTime: end.startOf('day').toISOString(),
      endTime: end.toISOString(),
    }
  }

  const days = range === '30d' ? 29 : 6
  return {
    startTime: end.subtract(days, 'day').startOf('day').toISOString(),
    endTime: end.toISOString(),
  }
}
