import { describe, expect, it } from 'vitest'
import { LimitEngine, nextLevel } from '../lib/limits'
import type { AlarmEvent, Channel } from '../lib/types'

const lim = { lo_alarm: 10, lo_warn: 20, hi_warn: 80, hi_alarm: 90, hyst: 2 }

describe('limits', () => {
  it('applies hysteresis on the way down', () => {
    let l = nextLevel(80.5, lim, 'nominal')
    expect(l).toBe('warn_hi')
    l = nextLevel(79, lim, l)
    expect(l).toBe('warn_hi')
    l = nextLevel(77.9, lim, l)
    expect(l).toBe('nominal')
    expect(nextLevel(95, lim, l)).toBe('alarm_hi')
  })

  it('treats heartbeat status and timeout as loss of signal', () => {
    const chans: Channel[] = [
      { id: 'TIME_000001', label: '', group: '', unit: '', desc: '', limits: null, stale_ms: null },
      { id: 'P', label: '', group: '', unit: '', desc: '', limits: lim, stale_ms: null },
    ]
    const e = new LimitEngine(chans, 'TIME_000001')
    const out: AlarmEvent[] = []
    e.feed(0, 1, 0, 0, out)
    e.feed(1, 50, 0, 0, out)
    expect(e.isFresh(1, 10)).toBe(true)
    e.feed(0, 2, 1, 100, out)
    expect(e.aos).toBe(false)
    expect(e.isFresh(1, 100)).toBe(false)
    e.feed(0, 3, 0, 200, out)
    e.tick(20_000, out)
    expect(e.losEvents).toBe(2)
  })
})
