// Browser port of gateway/src/limits.rs, used when there is no gateway (replay and
// direct live ISS modes). Same hysteresis and freshness rules.
import { severity, type AlarmEvent, type Channel, type Level, type Limits } from './types'

function rawLevel(v: number, l: Limits, shift: number): Level {
  if (l.hi_alarm != null && v >= l.hi_alarm - shift) return 'alarm_hi'
  if (l.lo_alarm != null && v <= l.lo_alarm + shift) return 'alarm_lo'
  if (l.hi_warn != null && v >= l.hi_warn - shift) return 'warn_hi'
  if (l.lo_warn != null && v <= l.lo_warn + shift) return 'warn_lo'
  return 'nominal'
}

export function nextLevel(v: number, l: Limits, cur: Level): Level {
  const raw = rawLevel(v, l, 0)
  if (severity(raw) >= severity(cur)) return raw
  const held = rawLevel(v, l, l.hyst)
  return severity(held) >= severity(cur) ? cur : held
}

interface ChanState {
  level: Level
  lastRx: number
  value: number
  hasValue: boolean
  sourceBad: boolean
  stale: boolean
}

export class LimitEngine {
  readonly st: ChanState[]
  private hb: number
  aos: boolean
  private everSeen = false
  losEvents = 0

  constructor(
    readonly channels: Channel[],
    heartbeat: string | null,
    private heartbeatTimeoutMs = 10_000,
  ) {
    this.st = channels.map(() => ({ level: 'nominal', lastRx: 0, value: NaN, hasValue: false, sourceBad: false, stale: false }))
    this.hb = heartbeat ? channels.findIndex((c) => c.id === heartbeat) : -1
    this.aos = this.hb < 0
  }

  indexOf(id: string) {
    return this.channels.findIndex((c) => c.id === id)
  }

  private setAos(aos: boolean, reason: string, now: number, out: AlarmEvent[]) {
    if (aos === this.aos) return
    this.aos = aos
    if (!aos) this.losEvents++
    out.push({ t: now, ch: -1, kind: 'signal', aos, reason })
  }

  feed(ch: number, v: number, status: number, now: number, out: AlarmEvent[]) {
    const st = this.st[ch]
    if (!st) return
    if (ch === this.hb) {
      this.everSeen = true
      this.setAos(status === 0, status === 0 ? 'heartbeat status nominal' : 'source reports loss of signal', now, out)
    }
    st.lastRx = now
    st.sourceBad = status !== 0
    if (st.stale) {
      st.stale = false
      out.push({ t: now, ch, kind: 'stale', stale: false })
    }
    if (!Number.isFinite(v)) return
    st.value = v
    st.hasValue = true
    const l = this.channels[ch].limits
    if (l) {
      const next = nextLevel(v, l, st.level)
      if (next !== st.level) {
        out.push({ t: now, ch, kind: 'limit', from: st.level, to: next, value: v })
        st.level = next
      }
    }
  }

  tick(now: number, out: AlarmEvent[]) {
    if (this.hb >= 0 && this.aos) {
      const age = now - this.st[this.hb].lastRx
      if (age > this.heartbeatTimeoutMs || !this.everSeen) this.setAos(false, 'heartbeat timeout', now, out)
    }
    this.channels.forEach((c, i) => {
      const st = this.st[i]
      if (c.stale_ms != null && st.hasValue && !st.stale && now - st.lastRx > c.stale_ms) {
        st.stale = true
        out.push({ t: now, ch: i, kind: 'stale', stale: true })
      }
    })
  }

  isFresh(i: number, now: number): boolean {
    const st = this.st[i]
    if (!st || !st.hasValue || st.sourceBad || !this.aos) return false
    const ms = this.channels[i].stale_ms
    return ms == null || now - st.lastRx <= ms
  }
}
