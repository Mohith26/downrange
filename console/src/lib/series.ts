// Fixed-capacity time series per channel. Charts read these directly every frame,
// so React never re-renders on data arrival.

export class Series {
  readonly t: Float64Array
  readonly v: Float64Array
  private head = 0
  count = 0

  constructor(readonly capacity: number) {
    this.t = new Float64Array(capacity)
    this.v = new Float64Array(capacity)
  }

  push(t: number, v: number) {
    this.t[this.head] = t
    this.v[this.head] = v
    this.head = (this.head + 1) % this.capacity
    if (this.count < this.capacity) this.count++
  }

  /** i-th oldest point, 0 <= i < count */
  at(i: number): [number, number] {
    const k = (this.head - this.count + i + this.capacity) % this.capacity
    return [this.t[k], this.v[k]]
  }

  last(): [number, number] | null {
    return this.count ? this.at(this.count - 1) : null
  }

  /** Index of the first point with t >= t0 (binary search over the ring). */
  lowerBound(t0: number): number {
    let lo = 0
    let hi = this.count
    while (lo < hi) {
      const mid = (lo + hi) >> 1
      const k = (this.head - this.count + mid + this.capacity) % this.capacity
      if (this.t[k] < t0) lo = mid + 1
      else hi = mid
    }
    return lo
  }
}

export interface Column {
  min: number
  max: number
  first: number
  last: number
}

/**
 * Reduce the points in [t0, t1] to one min/max column per pixel. Sample-and-hold: the
 * value in effect at t0 (the last point before the window) opens the first column, and
 * columns with no new point carry the previous value forward.
 */
export function decimate(s: Series, t0: number, t1: number, width: number, out: (Column | null)[]): void {
  out.length = width
  for (let i = 0; i < width; i++) out[i] = null
  if (s.count === 0 || width <= 0 || t1 <= t0) return
  let i = s.lowerBound(t0)
  let held: number | null = i > 0 ? s.at(i - 1)[1] : null
  const span = t1 - t0
  let col = -1
  for (; i < s.count; i++) {
    const [t, v] = s.at(i)
    if (t > t1) break
    const x = Math.min(width - 1, Math.floor(((t - t0) / span) * width))
    while (col < x - 1) {
      col++
      if (held != null && Number.isFinite(held)) out[col] = { min: held, max: held, first: held, last: held }
    }
    if (!Number.isFinite(v)) continue
    let c = out[x]
    if (!c) {
      c = held != null && Number.isFinite(held) ? { min: Math.min(held, v), max: Math.max(held, v), first: held, last: v } : { min: v, max: v, first: v, last: v }
      out[x] = c
    } else {
      if (v < c.min) c.min = v
      if (v > c.max) c.max = v
      c.last = v
    }
    held = v
    col = x
  }
  while (col < width - 1) {
    col++
    if (held != null && Number.isFinite(held)) out[col] = { min: held, max: held, first: held, last: held }
  }
}
