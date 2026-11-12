import { useEffect, useRef } from 'react'
import { drawChart } from '../lib/chart'
import type { TelemetryStore } from '../lib/store'

export interface FrameProbe {
  frames: number[]
}

export function ChartView({ store, lanes, windowMs, probe }: { store: TelemetryStore; lanes: number[]; windowMs: number; probe?: FrameProbe }) {
  const ref = useRef<HTMLCanvasElement>(null)
  const opts = useRef({ lanes, windowMs })
  opts.current = { lanes, windowMs }

  useEffect(() => {
    const canvas = ref.current!
    const ctx = canvas.getContext('2d', { alpha: false })!
    let raf = 0
    let last = performance.now()
    const frame = () => {
      const now = performance.now()
      probe?.frames.push(now - last)
      last = now
      const dpr = window.devicePixelRatio || 1
      const w = canvas.clientWidth
      const h = canvas.clientHeight
      if (canvas.width !== Math.round(w * dpr) || canvas.height !== Math.round(h * dpr)) {
        canvas.width = Math.round(w * dpr)
        canvas.height = Math.round(h * dpr)
      }
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
      drawChart(ctx, w, h, store, opts.current, Date.now())
      raf = requestAnimationFrame(frame)
    }
    raf = requestAnimationFrame(frame)
    return () => cancelAnimationFrame(raf)
  }, [store, probe])

  return <canvas ref={ref} className="chart" />
}
