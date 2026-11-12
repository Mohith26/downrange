// Measures how smoothly the strip charts render while telemetry streams in.
import type { FrameProbe } from './components/ChartView'
import type { TelemetryStore } from './lib/store'

export interface BenchResult {
  lanes: number
  seconds: number
  frames: number
  fps_avg: number
  frame_ms_p50: number
  frame_ms_p95: number
  frame_ms_p99: number
  frame_ms_max: number
  frames_over_33ms: number
  samples_received: number
  samples_per_s_received: number
  viewport: string
  device_pixel_ratio: number
  user_agent: string
}

function pct(sorted: number[], p: number) {
  return sorted[Math.min(sorted.length - 1, Math.round((sorted.length - 1) * p))] ?? NaN
}

export async function runRenderBench(store: TelemetryStore, probe: FrameProbe, secs: number, lanes: number): Promise<BenchResult> {
  probe.frames.length = 0
  const r0 = store.stats.received
  const t0 = performance.now()
  await new Promise((r) => setTimeout(r, secs * 1000))
  const elapsed = (performance.now() - t0) / 1000
  const frames = probe.frames.slice(1)
  const sorted = [...frames].sort((a, b) => a - b)
  const total = frames.reduce((a, b) => a + b, 0) / 1000
  const received = store.stats.received - r0
  return {
    lanes,
    seconds: Math.round(elapsed * 100) / 100,
    frames: frames.length,
    fps_avg: frames.length / total,
    frame_ms_p50: pct(sorted, 0.5),
    frame_ms_p95: pct(sorted, 0.95),
    frame_ms_p99: pct(sorted, 0.99),
    frame_ms_max: sorted[sorted.length - 1] ?? NaN,
    frames_over_33ms: frames.filter((f) => f > 33.4).length,
    samples_received: received,
    samples_per_s_received: received / elapsed,
    viewport: `${innerWidth}x${innerHeight}`,
    device_pixel_ratio: devicePixelRatio,
    user_agent: navigator.userAgent,
  }
}
