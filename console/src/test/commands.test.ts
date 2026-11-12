import { describe, expect, it } from 'vitest'
import { CommandFlow, type FlowState } from '../lib/commandFlow'
import { LimitEngine } from '../lib/limits'
import { LocalCommandEngine } from '../lib/localCommands'
import type { AlarmEvent, Channel, CommandDef } from '../lib/types'

const chans: Channel[] = [{ id: 'X', label: '', group: '', unit: '', desc: '', limits: { lo_alarm: null, lo_warn: null, hi_warn: 80, hi_alarm: 90, hyst: 1 }, stale_ms: null }]
const defs: CommandDef[] = [{ name: 'SIM_GO', desc: '', args: [], interlocks: [{ type: 'in_limits', ch: 'X' }] }]

function setup(now = { t: 1000 }) {
  const eng = new LimitEngine(chans, null)
  const out: AlarmEvent[] = []
  eng.feed(0, 50, 0, 0, out)
  const local = new LocalCommandEngine(defs, () => eng, () => {}, 5000, () => now.t)
  return { eng, local, now }
}

describe('command path', () => {
  it('executes once across a lost confirm reply and retry', async () => {
    const { local } = setup()
    let st: FlowState | null = null
    const flow = new CommandFlow(local, 'SIM_GO', (s) => (st = s))
    await flow.arm({})
    expect(st!.phase).toBe('armed')
    flow.dropNextReply = true
    await flow.confirm()
    expect(st!.phase).toBe('no_reply')
    await flow.retry({})
    expect(st!.phase).toBe('executed')
    expect(st!.dedup).toBe(true)
    expect(local.total).toBe(1)
  })

  it('rejects on interlock, expired token and token reuse', async () => {
    const { eng, local, now } = setup()
    const out: AlarmEvent[] = []
    eng.feed(0, 85, 0, 0, out)
    expect((await local.arm('a', 'SIM_GO', {})).type).toBe('rejected')
    eng.feed(0, 50, 0, 0, out)
    const r = await local.arm('b', 'SIM_GO', {})
    if (r.type !== 'armed') throw new Error()
    now.t += 6000
    expect(await local.confirm('b', r.token)).toMatchObject({ type: 'rejected', reason: 'token expired, re-arm' })
    const r2 = await local.arm('c', 'SIM_GO', {})
    if (r2.type !== 'armed') throw new Error()
    await local.confirm('c', r2.token)
    expect((await local.confirm('d', r2.token)).type).toBe('rejected')
    expect(local.total).toBe(1)
  })
})
