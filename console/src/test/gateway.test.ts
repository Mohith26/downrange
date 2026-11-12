import { describe, expect, it, vi } from 'vitest'
import { GatewayClient } from '../lib/gateway'
import { TelemetryStore } from '../lib/store'

class FakeWs {
  readyState = 1
  sent: string[] = []
  onopen: ((e: unknown) => void) | null = null
  onclose: ((e: unknown) => void) | null = null
  onmessage: ((e: { data: unknown }) => void) | null = null
  onerror: ((e: unknown) => void) | null = null
  send(d: string) {
    this.sent.push(d)
  }
  close() {
    this.readyState = 3
    this.onclose?.({})
  }
}

const meta = { type: 'meta', source: 'synthetic', device: 'loopback', commands: [], channels: [{ id: 'SYN0000', label: '', group: '', unit: '', desc: '', limits: null, stale_ms: 1000 }] }

describe('gateway client', () => {
  it('resumes from the last sequence number after a reconnect', () => {
    vi.useFakeTimers()
    const sockets: FakeWs[] = []
    const store = new TelemetryStore()
    const c = new GatewayClient('ws://x/ws', store, () => {
      const s = new FakeWs()
      sockets.push(s)
      return s
    })
    c.start()
    sockets[0].onopen?.({})
    expect(JSON.parse(sockets[0].sent[0])).toEqual({ type: 'hello', resume_from: null })
    sockets[0].onmessage?.({ data: JSON.stringify(meta) })
    sockets[0].onmessage?.({ data: JSON.stringify({ type: 'tm', first: 1, s: [[0, 1, 5, 0, 0], [0, 2, 6, 0, 0]] }) })
    expect(c.lastSeq).toBe(2)
    sockets[0].close()
    vi.advanceTimersByTime(300)
    sockets[1].onopen?.({})
    expect(JSON.parse(sockets[1].sent[0])).toEqual({ type: 'hello', resume_from: 2 })
    // Overlapping frame after resume: duplicate sample 2 is skipped.
    sockets[1].onmessage?.({ data: JSON.stringify({ type: 'tm', first: 2, s: [[0, 2, 6, 0, 0], [0, 3, 7, 0, 0]] }) })
    expect(store.series[0].count).toBe(3)
    expect(store.stats.gaps).toBe(0)
    sockets[1].onmessage?.({ data: JSON.stringify({ type: 'tm', first: 10, s: [[0, 4, 8, 0, 0]] }) })
    expect(store.stats.gaps).toBe(1)
    c.stop()
    vi.useRealTimers()
  })
})
