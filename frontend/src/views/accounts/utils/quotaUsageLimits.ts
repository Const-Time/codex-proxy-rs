export function parseQuotaUsageLimits(shortTerm: string, weekly: string) {
  function parse(value: string) {
    if (!value.trim())
      return 100
    const number = Number(value)
    return Number.isInteger(number) && number >= 1 && number <= 100 ? number : null
  }
  const shortTermPercent = parse(shortTerm)
  const weeklyPercent = parse(weekly)
  if (shortTermPercent === null || weeklyPercent === null)
    return null
  return { shortTermPercent, weeklyPercent }
}
