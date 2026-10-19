export function calendarDate(date: Date): string {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`
}

export function usageDateBounds(now = new Date()) {
  return {
    min: calendarDate(new Date(now.getFullYear(), now.getMonth(), now.getDate() - 30)),
    max: calendarDate(now),
  }
}

export function customUsageRange(start: string, end: string, now = new Date()) {
  const bounds = usageDateBounds(now)
  if (!/^\d{4}-\d{2}-\d{2}$/.test(start) || !/^\d{4}-\d{2}-\d{2}$/.test(end)
    || start < bounds.min || end > bounds.max || start > end) {
    return null
  }
  const parse = (value: string) => {
    const [year, month, day] = value.split('-').map(Number)
    return new Date(year!, month! - 1, day!)
  }
  const first = parse(start)
  const last = parse(end)
  if (calendarDate(first) !== start || calendarDate(last) !== end)
    return null
  // Advance the calendar day rather than adding 24 hours across DST boundaries.
  const nextDay = new Date(last.getFullYear(), last.getMonth(), last.getDate() + 1)
  return { startTime: first.toISOString(), endTime: new Date(Math.min(nextDay.getTime(), now.getTime())).toISOString() }
}
