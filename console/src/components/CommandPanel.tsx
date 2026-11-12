import { useEffect, useMemo, useState } from 'react'
import { CommandFlow, initialFlow, type FlowState } from '../lib/commandFlow'
import type { CommandDef, CommandTransport, InterlockStatus } from '../lib/types'

export interface InterlockView {
  name: string
  ok: boolean
  checks: InterlockStatus[]
}

function title(name: string) {
  return name
    .replace(/^SIM_/, '')
    .toLowerCase()
    .split('_')
    .map((w) => (['cmg', 'tcs', 'sarj'].includes(w) ? w.toUpperCase() : w))
    .join(' ')
    .replace(/^./, (c) => c.toUpperCase())
}

function CommandCard({ def, transport, il, nameOf }: { def: CommandDef; transport: CommandTransport; il: InterlockView | undefined; nameOf: (id: string) => string }) {
  const [state, setState] = useState<FlowState>(initialFlow)
  const flow = useMemo(() => new CommandFlow(transport, def.name, setState), [transport, def.name])
  const [args, setArgs] = useState<Record<string, number>>(() => Object.fromEntries(def.args.map((a) => [a.name, a.max])))
  const [dropReply, setDropReply] = useState(false)
  const [now, setNow] = useState(Date.now())

  useEffect(() => {
    if (state.phase !== 'armed') return
    const t = setInterval(() => setNow(Date.now()), 100)
    return () => clearInterval(t)
  }, [state.phase])

  const ok = il?.ok ?? false
  const busy = state.phase === 'arming' || state.phase === 'confirming'
  const remaining = Math.max(0, state.expiresMs - now)
  const expired = state.phase === 'armed' && remaining === 0
  const armDisabled = !ok || busy || state.phase === 'armed'
  const confirmDisabled = !ok || busy || expired

  const go = (fn: () => unknown) => {
    flow.dropNextReply = dropReply
    fn()
  }

  return (
    <div className={`cmd ${state.phase}`}>
      <div className="cmd-head">
        <div>
          <div className="cmd-title">{title(def.name)}</div>
          <div className="cmd-desc">{def.desc}</div>
        </div>
        <span className={`pill ${ok ? 'pill-ok' : 'pill-bad'}`}>{ok ? 'Interlocks clear' : 'Inhibited'}</span>
      </div>
      <ul className="checks">
        {(il?.checks ?? []).map((c, i) => (
          <li key={i} className={c.ok ? 'ok' : 'bad'}>
            <span className="dot" />
            {c.detail.replace(/^([A-Z0-9_]+):/, (_, id: string) => `${nameOf(id)}:`)}
          </li>
        ))}
      </ul>
      {def.args.length > 0 && (
        <div className="args">
          {def.args.map((a) => (
            <label key={a.name}>
              {a.name.replace(/_/g, ' ')}
              <input
                type="number"
                min={a.min}
                max={a.max}
                value={args[a.name]}
                disabled={state.phase === 'armed' || busy}
                onChange={(e) => setArgs({ ...args, [a.name]: Number(e.target.value) })}
              />
              <span className="range">
                {a.min} to {a.max}
              </span>
            </label>
          ))}
        </div>
      )}
      <div className="cmd-actions">
        {state.phase !== 'armed' ? (
          <button className="btn" disabled={armDisabled} onClick={() => go(() => flow.arm(args))}>
            {state.phase === 'arming' ? 'Arming…' : 'Arm'}
          </button>
        ) : (
          <>
            <button className="btn btn-commit" disabled={confirmDisabled} onClick={() => go(() => flow.confirm())}>
              Confirm and send
            </button>
            <button className="btn btn-ghost" onClick={() => flow.cancel()}>
              Cancel
            </button>
            <div className="countdown" title="Token expiry">
              <div style={{ width: `${Math.min(100, remaining / 100)}%` }} />
              <span>{expired ? 'Token expired, re-arm' : `${(remaining / 1000).toFixed(1)} s`}</span>
            </div>
          </>
        )}
        {state.phase === 'no_reply' && (
          <button className="btn btn-warn" onClick={() => go(() => flow.retry(args))}>
            Retry {state.lastStep} (same key)
          </button>
        )}
        {(state.phase === 'executed' || state.phase === 'rejected') && (
          <button className="btn btn-ghost" onClick={() => flow.cancel()}>
            Clear
          </button>
        )}
      </div>
      <label className="drop">
        <input type="checkbox" checked={dropReply} onChange={(e) => setDropReply(e.target.checked)} />
        Drop the next reply (test idempotent retry)
      </label>
      {state.key && (
        <div className="cmd-status">
          <span className="k">key {state.key.slice(0, 11)}</span>
          {state.phase === 'executed' && (
            <span className="res ok">
              {state.result}
              {state.dedup ? ' (retry deduped, not executed twice)' : ''}
            </span>
          )}
          {state.phase === 'rejected' && <span className="res bad">Rejected: {state.result}</span>}
          {state.phase === 'no_reply' && <span className="res warn">No reply: {state.result}</span>}
          {state.attempts > 1 && <span className="k">{state.attempts} requests</span>}
        </div>
      )}
    </div>
  )
}

export function CommandPanel({
  defs,
  transport,
  interlocks,
  targetLabel,
  nameOf,
}: {
  defs: CommandDef[]
  transport: CommandTransport
  interlocks: InterlockView[]
  targetLabel: string
  nameOf: (id: string) => string
}) {
  return (
    <section className="panel cmds">
      <header>
        <h2>Commands</h2>
        <span className="tag" title="There is no real actuator behind these commands">
          Target: {targetLabel}
        </span>
      </header>
      <div className="cmd-list">
        {defs.map((d) => (
          <CommandCard key={d.name + transport.label} def={d} transport={transport} il={interlocks.find((i) => i.name === d.name)} nameOf={nameOf} />
        ))}
      </div>
    </section>
  )
}
