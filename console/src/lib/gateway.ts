// WebSocket client for the Rust gateway: reconnect with backoff, resume from the last
// sequence number, and request/reply for the command path.
import type { TelemetryStore } from './store'
import type { AlarmEvent, AuditEntry, Channel, CommandDef, CommandTransport, InterlockStatus, Reply } from './types'

type WsLike = {
  readyState: number
  send(data: string): void
  close(): void
  onopen: ((ev: unknown) => void) | null
  onclose: ((ev: unknown) => void) | null
  onmessage: ((ev: { data: unknown }) => void) | null
  onerror: ((ev: unknown) => void) | null
}

export interface InterlockReport {
  aos: boolean
  cmds: { name: string; ok: boolean; checks: InterlockStatus[] }[]
}

type Sample = [number, number, number | null, number, number]

export class GatewayClient implements CommandTransport {
  readonly label = 'gateway'
  ws: WsLike | null = null
  lastSeq: number | null = null
  commands: CommandDef[] = []
  interlocks: InterlockReport | null = null
  source = ''
  device = ''
  private backoff = 250
  private closed = false
  private waiters = new Map<string, (r: Reply) => void>()
  onMeta: (() => void) | null = null

  constructor(
    private url: string,
    private store: TelemetryStore,
    private makeSocket: (url: string) => WsLike = (u) => new WebSocket(u) as unknown as WsLike,
  ) {}

  start() {
    this.closed = false
    this.connect()
  }

  stop() {
    this.closed = true
    this.ws?.close()
  }

  private connect() {
    this.store.setLink('connecting', 'opening WebSocket')
    const ws = this.makeSocket(this.url)
    this.ws = ws
    ws.onopen = () => {
      this.backoff = 250
      ws.send(JSON.stringify({ type: 'hello', resume_from: this.lastSeq }))
    }
    ws.onmessage = (ev) => this.handle(String(ev.data))
    ws.onclose = () => {
      if (this.ws !== ws) return
      this.ws = null
      if (this.closed) return
      this.store.setLink('offline', 'gateway disconnected, retrying')
      this.store.stats.reconnects++
      setTimeout(() => this.connect(), this.backoff)
      this.backoff = Math.min(this.backoff * 2, 5000)
    }
    ws.onerror = () => ws.close()
  }

  private ingest(s: Sample[]) {
    for (const [ch, t, v, st] of s) this.store.ingest(ch, t, v ?? NaN, st)
  }

  handle(text: string) {
    const m = JSON.parse(text)
    const store = this.store
    switch (m.type) {
      case 'meta': {
        const sameChannels = store.channels.length === m.channels.length && store.channels.every((c, i) => c.id === m.channels[i].id)
        if (!sameChannels) {
          store.setChannels(m.channels as Channel[], m.source.startsWith('iss') ? 'TIME_000001' : null)
          this.lastSeq = null
        }
        store.serverAlarms = true
        this.commands = m.commands
        this.source = m.source
        this.device = m.device
        this.onMeta?.()
        break
      }
      case 'resume':
        if (m.gapless && this.lastSeq != null) store.stats.resumedGapless++
        if (store.link === 'connecting' || store.link === 'offline') store.setLink('aos', 'connected')
        break
      case 'snapshot':
        this.ingest(m.s)
        this.lastSeq = m.seq
        break
      case 'tm': {
        if (this.lastSeq != null && m.first > this.lastSeq + 1) store.stats.gaps++
        const skip = this.lastSeq != null ? Math.max(0, this.lastSeq + 1 - m.first) : 0
        this.ingest(skip ? m.s.slice(skip) : m.s)
        this.lastSeq = Math.max(this.lastSeq ?? 0, m.first + m.s.length - 1)
        store.stats.lastSeq = this.lastSeq
        break
      }
      case 'coalesced':
        store.stats.coalesced++
        this.ingest(m.s)
        this.lastSeq = m.seq
        break
      case 'alarm':
        store.logAlarms([m.e as AlarmEvent])
        break
      case 'link':
        if (!m.up) store.setLink('los', m.reason)
        break
      case 'interlocks':
        this.interlocks = { aos: m.aos, cmds: m.cmds }
        if (!m.aos && store.link === 'aos') store.setLink('los', 'gateway reports no signal')
        if (m.aos && store.link === 'los') store.setLink('aos', 'signal')
        break
      case 'audit':
        store.addAudit(m.e as AuditEntry)
        break
      case 'cmd_reply': {
        const r = m.reply as Reply
        const w = this.waiters.get(r.key)
        if (w) {
          this.waiters.delete(r.key)
          w(r)
        }
        break
      }
    }
  }

  private request(key: string, msg: object, timeoutMs = 3000): Promise<Reply> {
    return new Promise((resolve, reject) => {
      if (!this.ws || this.ws.readyState !== 1) return reject(new Error('not connected'))
      const timer = setTimeout(() => {
        this.waiters.delete(key)
        reject(new Error('no reply'))
      }, timeoutMs)
      this.waiters.set(key, (r) => {
        clearTimeout(timer)
        resolve(r)
      })
      this.ws.send(JSON.stringify(msg))
    })
  }

  arm(key: string, cmd: string, args: Record<string, number>) {
    return this.request(key, { type: 'arm', key, cmd, args })
  }

  confirm(key: string, token: string) {
    return this.request(key, { type: 'confirm', key, token })
  }
}
