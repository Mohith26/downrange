// Direct browser connection to NASA's public ISS Lightstreamer feed, using TLCP over
// WebSocket. Used by the hosted demo; no gateway involved.
import { MergeState, parseLine } from './tlcp'
import type { TelemetryStore } from './store'

const URL = 'wss://push.lightstreamer.com/lightstreamer'
const CID = 'mgQkwtwdysogQz2BJ4Ji%20kOj2Bg'

export class LiveIssClient {
  private ws: WebSocket | null = null
  private merge: MergeState
  private closed = false
  private backoff = 1000
  status = 'idle'

  constructor(
    private store: TelemetryStore,
    private ids: string[],
  ) {
    this.merge = new MergeState(ids.length, 3)
  }

  start() {
    this.closed = false
    this.connect()
  }

  stop() {
    this.closed = true
    this.ws?.close()
  }

  private connect() {
    this.store.setLink('connecting', 'connecting to push.lightstreamer.com')
    this.merge = new MergeState(this.ids.length, 3)
    const ws = new WebSocket(URL, 'TLCP-2.2.0.lightstreamer.com')
    this.ws = ws
    ws.onopen = () => {
      ws.send('wsok')
      ws.send(`create_session\r\nLS_adapter_set=ISSLIVE&LS_cid=${CID}&LS_send_sync=false&LS_cause=api`)
    }
    ws.onmessage = (ev) => {
      for (const line of String(ev.data).split('\r\n')) {
        if (line) this.handle(line, ws)
      }
    }
    ws.onclose = () => {
      if (this.ws !== ws) return
      this.ws = null
      if (this.closed) return
      this.store.setLink('offline', 'ISS feed disconnected, retrying')
      this.store.stats.reconnects++
      setTimeout(() => this.connect(), this.backoff)
      this.backoff = Math.min(this.backoff * 2, 30_000)
    }
    ws.onerror = () => ws.close()
  }

  private handle(line: string, ws: WebSocket) {
    const m = parseLine(line)
    if (m.kind === 'conok') {
      this.status = 'session ' + m.session
      this.backoff = 1000
      const group = encodeURIComponent(this.ids.join(' '))
      const schema = encodeURIComponent('TimeStamp Value Status.Class')
      ws.send(`control\r\nLS_reqId=1&LS_op=add&LS_subId=1&LS_mode=MERGE&LS_group=${group}&LS_schema=${schema}&LS_snapshot=true`)
    } else if (m.kind === 'update') {
      const row = this.merge.apply(m.item, m.fields)
      if (!row) return
      const ch = this.store.channels.findIndex((c) => c.id === this.ids[m.item - 1])
      if (ch < 0) return
      const v = row[1] == null || row[1] === '' ? NaN : Number(row[1])
      this.store.ingest(ch, Date.now(), v, row[2] === '24' ? 0 : 1)
    } else if (m.kind === 'loop' || m.kind === 'end' || m.kind === 'error') {
      this.status = m.kind
      ws.close()
    }
  }
}
