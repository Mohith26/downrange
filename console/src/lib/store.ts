// Central mutable telemetry state. Sources push into it; charts read it every animation
// frame; React panels subscribe to a throttled change signal.
import { LimitEngine } from './limits'
import { Series } from './series'
import type { AlarmEvent, AuditEntry, Channel, Level } from './types'

export type LinkState = 'connecting' | 'aos' | 'los' | 'offline'

export interface FeedStats {
  received: number
  ratePerSec: number
  lastSeq: number
  gaps: number
  coalesced: number
  reconnects: number
  resumedGapless: number
}

export type LoggedAlarm = AlarmEvent & {
  id: number
  acked: boolean
}

export class TelemetryStore {
  channels: Channel[] = []
  series: Series[] = []
  engine: LimitEngine = new LimitEngine([], null)
  /** When a gateway is connected it owns alarm generation; locally we only mirror levels. */
  serverAlarms = false
  link: LinkState = 'connecting'
  linkReason = ''
  alarms: LoggedAlarm[] = []
  audit: AuditEntry[] = []
  stats: FeedStats = { received: 0, ratePerSec: 0, lastSeq: 0, gaps: 0, coalesced: 0, reconnects: 0, resumedGapless: 0 }
  private nextAlarmId = 1
  private listeners = new Set<() => void>()
  private rateWindow: { t: number; n: number }[] = []
  version = 0

  constructor(private capacity = 8192) {
    setInterval(() => this.flush(), 250)
  }

  setChannels(channels: Channel[], heartbeat: string | null) {
    this.channels = channels
    this.series = channels.map(() => new Series(this.capacity))
    this.engine = new LimitEngine(channels, heartbeat)
    this.alarms = []
    this.audit = []
    this.stats = { received: 0, ratePerSec: 0, lastSeq: 0, gaps: 0, coalesced: 0, reconnects: 0, resumedGapless: 0 }
    this.markDirty()
  }

  ingest(ch: number, t: number, v: number, status: number) {
    const s = this.series[ch]
    if (!s) return
    s.push(t, v)
    this.stats.received++
    const events: AlarmEvent[] = []
    this.engine.feed(ch, v, status, Date.now(), events)
    if (!this.serverAlarms) this.logAlarms(events)
    if (this.engine.aos && this.link !== 'aos' && !this.serverAlarms) this.link = 'aos'
  }

  tick() {
    const events: AlarmEvent[] = []
    this.engine.tick(Date.now(), events)
    if (!this.serverAlarms) {
      this.logAlarms(events)
      if (!this.engine.aos && this.link === 'aos') this.link = 'los'
    }
  }

  logAlarms(events: AlarmEvent[]) {
    for (const e of events) {
      if (e.kind === 'signal') {
        this.link = e.aos ? 'aos' : 'los'
        this.linkReason = e.reason
      }
      // Recoveries (signal back, channel fresh again) are informational and need no ack.
      const recovery = (e.kind === 'stale' && !e.stale) || (e.kind === 'signal' && e.aos)
      this.alarms.unshift({ ...e, id: this.nextAlarmId++, acked: recovery })
    }
    if (this.alarms.length > 300) this.alarms.length = 300
  }

  ack(id: number | 'all') {
    for (const a of this.alarms) if (id === 'all' || a.id === id) a.acked = true
    this.markDirty()
  }

  addAudit(e: AuditEntry) {
    this.audit.unshift(e)
    if (this.audit.length > 200) this.audit.length = 200
    this.markDirty()
  }

  setLink(link: LinkState, reason = '') {
    this.link = link
    this.linkReason = reason
    this.markDirty()
  }

  level(i: number): Level {
    return this.engine.st[i]?.level ?? 'nominal'
  }

  isFresh(i: number): boolean {
    return this.engine.isFresh(i, Date.now())
  }

  markDirty() {
    this.flush()
  }

  private flush() {
    const now = performance.now()
    this.rateWindow.push({ t: now, n: this.stats.received })
    while (this.rateWindow.length > 2 && now - this.rateWindow[0].t > 2000) this.rateWindow.shift()
    const a = this.rateWindow[0]
    this.stats.ratePerSec = now > a.t ? ((this.stats.received - a.n) * 1000) / (now - a.t) : 0
    this.tick()
    this.version++
    this.listeners.forEach((l) => l())
  }

  subscribe(fn: () => void) {
    this.listeners.add(fn)
    return () => this.listeners.delete(fn)
  }
}
