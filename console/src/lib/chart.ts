// Canvas strip-chart renderer. One canvas, one lane per channel, redrawn every
// animation frame straight from the Series buffers. Points are reduced to one min/max
// column per pixel, so cost scales with lanes x width, not with sample rate.
import { decimate, type Column } from './series'
import type { TelemetryStore } from './store'
import { severity } from './types'

export const COLORS = {
  bg: '#0b0f14',
  lane: '#11171e',
  grid: '#1d2630',
  text: '#d5dde5',
  muted: '#8b97a3',
  trace: '#5cc8ff',
  warn: '#e3b341',
  alarm: '#ff6b5e',
  stale: '#7d8590',
}

export interface ChartOptions {
  windowMs: number
  lanes: number[]
}

const cols: (Column | null)[] = []

function fmt(v: number): string {
  if (!Number.isFinite(v)) return '--'
  const a = Math.abs(v)
  if (a >= 100000) return v.toExponential(3)
  if (a >= 1000) return v.toFixed(0)
  if (a >= 10) return v.toFixed(1)
  if (a >= 1) return v.toFixed(2)
  return v.toPrecision(3)
}

export function drawChart(ctx: CanvasRenderingContext2D, w: number, h: number, store: TelemetryStore, opt: ChartOptions, now: number) {
  ctx.fillStyle = COLORS.bg
  ctx.fillRect(0, 0, w, h)
  const n = opt.lanes.length
  if (!n) return
  const gap = n > 24 ? 2 : 6
  const laneH = Math.max(12, (h - gap * (n - 1)) / n)
  const labelW = Math.min(220, Math.max(120, w * 0.22))
  const plotX = labelW
  const plotW = Math.max(10, Math.floor(w - labelW - 8))
  const t1 = now
  const t0 = now - opt.windowMs
  const compact = laneH < 34
  ctx.font = `${compact ? 10 : 12}px Inter, system-ui, -apple-system, sans-serif`
  ctx.textBaseline = 'middle'

  for (let li = 0; li < n; li++) {
    const ch = opt.lanes[li]
    const c = store.channels[ch]
    const s = store.series[ch]
    if (!c || !s) continue
    const y0 = li * (laneH + gap)
    ctx.fillStyle = COLORS.lane
    ctx.fillRect(0, y0, w, laneH)

    const fresh = store.isFresh(ch)
    const lvl = store.level(ch)
    const sev = severity(lvl)
    const color = !fresh ? COLORS.stale : sev === 2 ? COLORS.alarm : sev === 1 ? COLORS.warn : COLORS.trace

    // severity bar
    ctx.fillStyle = !fresh ? COLORS.stale : sev === 2 ? COLORS.alarm : sev === 1 ? COLORS.warn : '#2ea043'
    ctx.fillRect(0, y0, 3, laneH)

    decimate(s, t0, t1, plotW, cols)
    let lo = Infinity
    let hi = -Infinity
    for (const col of cols) {
      if (!col) continue
      if (col.min < lo) lo = col.min
      if (col.max > hi) hi = col.max
    }
    const lim = c.limits
    if (lo === Infinity) {
      lo = 0
      hi = 1
    }
    if (hi - lo < 1e-9) {
      const pad = Math.abs(hi) * 0.001 || 0.5
      lo -= pad
      hi += pad
    }
    const padY = (hi - lo) * 0.12
    lo -= padY
    hi += padY
    const py = (v: number) => y0 + laneH - 2 - ((v - lo) / (hi - lo)) * (laneH - 4)

    // limit lines that fall inside the visible range
    if (lim && !compact) {
      ctx.lineWidth = 1
      for (const [val, col] of [
        [lim.hi_warn, COLORS.warn],
        [lim.lo_warn, COLORS.warn],
        [lim.hi_alarm, COLORS.alarm],
        [lim.lo_alarm, COLORS.alarm],
      ] as [number | null, string][]) {
        if (val == null || val < lo || val > hi) continue
        ctx.strokeStyle = col
        ctx.globalAlpha = 0.45
        ctx.setLineDash([4, 4])
        ctx.beginPath()
        ctx.moveTo(plotX, Math.round(py(val)) + 0.5)
        ctx.lineTo(plotX + plotW, Math.round(py(val)) + 0.5)
        ctx.stroke()
      }
      ctx.setLineDash([])
      ctx.globalAlpha = 1
    }

    // trace: vertical min/max per column joined by steps
    ctx.strokeStyle = color
    ctx.lineWidth = 1.25
    ctx.beginPath()
    let started = false
    let prevY = 0
    for (let x = 0; x < plotW; x++) {
      const col = cols[x]
      if (!col) {
        started = false
        continue
      }
      const X = plotX + x + 0.5
      const yFirst = py(col.first)
      if (!started) {
        ctx.moveTo(X, yFirst)
        started = true
      } else {
        ctx.lineTo(X, prevY)
        ctx.lineTo(X, yFirst)
      }
      if (col.min !== col.max) {
        ctx.moveTo(X, py(col.min))
        ctx.lineTo(X, py(col.max))
      }
      prevY = py(col.last)
      ctx.moveTo(X, prevY)
    }
    ctx.stroke()

    // label block
    const last = s.last()
    ctx.fillStyle = COLORS.lane
    ctx.fillRect(3, y0, labelW - 6, laneH)
    const midY = y0 + laneH / 2
    ctx.fillStyle = COLORS.text
    if (compact) {
      ctx.fillText(c.label || c.id, 10, midY)
      ctx.fillStyle = color
      ctx.textAlign = 'right'
      ctx.fillText(last ? fmt(last[1]) : '--', labelW - 10, midY)
      ctx.textAlign = 'left'
    } else {
      ctx.fillText(c.label || c.id, 10, midY - 8)
      ctx.fillStyle = color
      ctx.font = '600 14px Inter, system-ui, -apple-system, sans-serif'
      ctx.fillText(`${last ? fmt(last[1]) : '--'}`, 10, midY + 9)
      const vw = ctx.measureText(last ? fmt(last[1]) : '--').width
      ctx.font = '12px Inter, system-ui, -apple-system, sans-serif'
      ctx.fillStyle = COLORS.muted
      ctx.fillText(`${c.unit}${fresh ? '' : '  STALE'}`, 16 + vw, midY + 9)
      // scale
      ctx.textAlign = 'right'
      ctx.font = '10px Inter, system-ui, -apple-system, sans-serif'
      ctx.fillText(fmt(hi - padY), w - 6, y0 + 8)
      ctx.fillText(fmt(lo + padY), w - 6, y0 + laneH - 8)
      ctx.textAlign = 'left'
      ctx.font = '12px Inter, system-ui, -apple-system, sans-serif'
    }
  }
}
