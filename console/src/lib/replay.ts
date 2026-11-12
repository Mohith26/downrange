// Replays a session recorded by the gateway (`downrange record`) in the browser.
import type { TelemetryStore } from './store'

export interface Recording {
  startUnixMs: number
  channels: string[]
  off: Float64Array
  idx: Int32Array
  value: Float64Array
  status: Uint8Array
  durationMs: number
}

export function parseRecording(text: string): Recording {
  const channels: string[] = []
  let startUnixMs = 0
  const off: number[] = []
  const idx: number[] = []
  const value: number[] = []
  const status: number[] = []
  let pos = 0
  while (pos < text.length) {
    let end = text.indexOf('\n', pos)
    if (end < 0) end = text.length
    const line = text.slice(pos, end)
    pos = end + 1
    if (!line) continue
    if (line.charCodeAt(0) === 35 /* # */) {
      if (line.startsWith('#start_unix_ms ')) startUnixMs = Number(line.slice(15))
      else if (line.startsWith('#chan ')) {
        const sp = line.indexOf(' ', 6)
        channels[Number(line.slice(6, sp))] = line.slice(sp + 1)
      }
      continue
    }
    if (line.charCodeAt(0) === 69 /* E */) continue
    const p = line.split('\t')
    if (p.length < 5) continue
    off.push(Number(p[0]))
    idx.push(Number(p[1]))
    value.push(p[3] === '' ? NaN : Number(p[3]))
    status.push(p[4] === '24' ? 0 : 1)
  }
  return {
    startUnixMs,
    channels,
    off: Float64Array.from(off),
    idx: Int32Array.from(idx),
    value: Float64Array.from(value),
    status: Uint8Array.from(status),
    durationMs: off.length ? off[off.length - 1] - off[0] : 0,
  }
}

export class ReplayPlayer {
  speed = 1
  private cursor = 0
  private virtualMs = 0
  private lastWall = 0
  private timer: ReturnType<typeof setInterval> | null = null
  private map: number[]
  loops = 0

  constructor(
    readonly rec: Recording,
    private store: TelemetryStore,
  ) {
    this.map = rec.channels.map((id) => store.channels.findIndex((c) => c.id === id))
  }

  get positionMs() {
    return this.virtualMs
  }

  start() {
    this.lastWall = performance.now()
    this.virtualMs = this.rec.off[0] ?? 0
    this.timer = setInterval(() => this.step(), 50)
  }

  stop() {
    if (this.timer) clearInterval(this.timer)
    this.timer = null
  }

  /** Jump ahead (only forward, so charts stay monotonic). */
  skip(ms: number) {
    this.virtualMs += ms
  }

  private step() {
    const now = performance.now()
    this.virtualMs += (now - this.lastWall) * this.speed
    this.lastWall = now
    const r = this.rec
    const wall = Date.now()
    while (this.cursor < r.off.length && r.off[this.cursor] <= this.virtualMs) {
      const ch = this.map[r.idx[this.cursor]]
      if (ch >= 0) this.store.ingest(ch, wall, r.value[this.cursor], r.status[this.cursor])
      this.cursor++
    }
    if (this.cursor >= r.off.length) {
      this.cursor = 0
      this.virtualMs = r.off[0] ?? 0
      this.loops++
    }
  }
}
