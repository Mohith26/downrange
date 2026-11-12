import { useEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react'
import issChannels from '../../channels/iss_channels.json'
import issCommands from '../../channels/iss_commands.json'
import { ChartView, type FrameProbe } from './components/ChartView'
import { CommandPanel, type InterlockView } from './components/CommandPanel'
import { GatewayClient } from './lib/gateway'
import { LiveIssClient } from './lib/liveIss'
import { LocalCommandEngine } from './lib/localCommands'
import { parseRecording, ReplayPlayer } from './lib/replay'
import { TelemetryStore } from './lib/store'
import { severity, type Channel, type CommandDef, type CommandTransport } from './lib/types'
import { runRenderBench, type BenchResult } from './bench'

type Mode = 'replay' | 'live' | 'gateway'

const params = new URLSearchParams(location.search)
const isLocal = ['localhost', '127.0.0.1'].includes(location.hostname)
const defaultGw = `ws://${isLocal && location.port ? location.host : '127.0.0.1:8787'}/ws`
const DEFAULT_PINS = ['USLAB000058', 'USLAB000059', 'USLAB000055', 'USLAB000010', 'Z1000009', 'S0000004', 'S1000002', 'USLAB000026']

function useStore(store: TelemetryStore) {
  return useSyncExternalStore(
    (fn) => store.subscribe(fn),
    () => store.version,
  )
}

function fmtVal(v: number | undefined) {
  if (v == null || !Number.isFinite(v)) return '--'
  const a = Math.abs(v)
  if (a >= 100000) return v.toExponential(3)
  if (a >= 1000) return v.toFixed(0)
  if (a >= 10) return v.toFixed(2)
  return v.toPrecision(4)
}

function utc(t: number) {
  return new Date(t).toISOString().slice(11, 19)
}

export default function App() {
  const store = useMemo(() => new TelemetryStore(), [])
  const [mode, setMode] = useState<Mode>((params.get('source') as Mode) ?? (isLocal ? 'gateway' : 'replay'))
  const [gwUrl] = useState(params.get('gw') ?? defaultGw)
  const [speed, setSpeed] = useState(1)
  const [windowS, setWindowS] = useState(Number(params.get('window') ?? 60))
  const [pins, setPins] = useState<string[]>(DEFAULT_PINS)
  const [filter, setFilter] = useState('')
  const [, force] = useState(0)
  const player = useRef<ReplayPlayer | null>(null)
  const gw = useRef<GatewayClient | null>(null)
  const [transport, setTransport] = useState<CommandTransport | null>(null)
  const [defs, setDefs] = useState<CommandDef[]>([])
  const [interlocks, setInterlocks] = useState<InterlockView[]>([])
  const [replayInfo, setReplayInfo] = useState('')
  const [bench, setBench] = useState<BenchResult | null>(null)
  const probe = useRef<FrameProbe>({ frames: [] })
  useStore(store)

  // Source lifecycle
  useEffect(() => {
    let stopped = false
    const cleanups: (() => void)[] = []
    if (mode === 'gateway') {
      const c = new GatewayClient(gwUrl, store)
      gw.current = c
      c.onMeta = () => {
        setDefs(c.commands)
        setTransport(c)
        if (params.get('bench')) setPins(store.channels.slice(0, Number(params.get('lanes') ?? 32)).map((x) => x.id))
        force((x) => x + 1)
      }
      c.start()
      const t = setInterval(() => setInterlocks(c.interlocks?.cmds ?? []), 500)
      cleanups.push(() => {
        clearInterval(t)
        c.stop()
        gw.current = null
      })
    } else {
      store.setChannels(issChannels as Channel[], 'TIME_000001')
      store.serverAlarms = false
      const local = new LocalCommandEngine(issCommands as CommandDef[], () => store.engine, (e) => store.addAudit(e))
      setDefs(issCommands as CommandDef[])
      setTransport(local)
      const t = setInterval(() => setInterlocks(local.report()), 500)
      cleanups.push(() => clearInterval(t))
      if (mode === 'replay') {
        store.setLink('connecting', 'loading recorded session')
        fetch(`${import.meta.env.BASE_URL}iss-session.rec`)
          .then((r) => {
            if (!r.ok) throw new Error(`HTTP ${r.status}`)
            return r.text()
          })
          .then((text) => {
            if (stopped) return
            const rec = parseRecording(text)
            const p = new ReplayPlayer(rec, store)
            p.speed = speed
            p.start()
            player.current = p
            setReplayInfo(`${new Date(rec.startUnixMs).toISOString().slice(0, 16).replace('T', ' ')} UTC, ${(rec.durationMs / 60000).toFixed(0)} min, ${rec.off.length.toLocaleString()} updates`)
          })
          .catch((e) => store.setLink('offline', `could not load recording: ${e.message}`))
        cleanups.push(() => {
          player.current?.stop()
          player.current = null
        })
      } else {
        const live = new LiveIssClient(store, store.channels.map((c) => c.id))
        live.start()
        cleanups.push(() => live.stop())
      }
    }
    return () => {
      stopped = true
      cleanups.forEach((f) => f())
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mode, gwUrl, store])

  useEffect(() => {
    if (player.current) player.current.speed = speed
  }, [speed])

  // Render benchmark (local gateway runs only)
  useEffect(() => {
    if (!params.get('bench') || mode !== 'gateway') return
    const secs = Number(params.get('secs') ?? 10)
    const t = setTimeout(async () => {
      const r = await runRenderBench(store, probe.current, secs, pins.length)
      setBench(r)
      fetch('/bench-result', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(r) }).catch(() => {})
    }, 3000)
    return () => clearTimeout(t)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mode, pins.length > 8])

  const lanes = pins.map((id) => store.channels.findIndex((c) => c.id === id)).filter((i) => i >= 0)
  const groups = useMemo(() => {
    const g = new Map<string, number[]>()
    store.channels.forEach((c, i) => {
      if (filter && !`${c.label} ${c.id} ${c.desc}`.toLowerCase().includes(filter.toLowerCase())) return
      g.set(c.group, [...(g.get(c.group) ?? []), i])
    })
    return [...g.entries()]
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [store.channels, filter])

  const active = store.channels.map((_, i) => i).filter((i) => severity(store.level(i)) > 0)
  const unacked = store.alarms.filter((a) => !a.acked).length
  const linkText = { connecting: 'Connecting', aos: 'Signal', los: 'No signal (LOS)', offline: 'Offline' }[store.link]
  const targetLabel = mode === 'gateway' ? 'loopback device in the Rust gateway (simulated)' : 'in-browser loopback (simulated)'
  const togglePin = (id: string) => setPins((p) => (p.includes(id) ? p.filter((x) => x !== id) : [...p, id]))

  return (
    <div className="app">
      <header className="top">
        <div className="brand">
          <span className="logo" />
          Downrange
        </div>
        <nav className="modes">
          {(['replay', 'live', 'gateway'] as Mode[]).map((m) => (
            <button key={m} className={m === mode ? 'on' : ''} onClick={() => setMode(m)}>
              {m === 'replay' ? 'Recorded ISS pass' : m === 'live' ? 'Live ISS' : 'Local gateway'}
            </button>
          ))}
        </nav>
        <div className={`link link-${store.link}`} title={store.linkReason}>
          <span className="dot" />
          {linkText}
        </div>
        <div className="stat">
          <span>Rate</span>
          {store.stats.ratePerSec.toFixed(store.stats.ratePerSec < 100 ? 1 : 0)}/s
        </div>
        {mode === 'gateway' && (
          <div className="stat" title="Last sequence number; gaps and coalesced frames since load">
            <span>Seq</span>
            {store.stats.lastSeq.toLocaleString()}
            {store.stats.gaps + store.stats.coalesced > 0 && <em> {store.stats.coalesced} coalesced</em>}
            {store.stats.reconnects > 0 && <em> {store.stats.resumedGapless}/{store.stats.reconnects} resumed</em>}
          </div>
        )}
        {mode === 'replay' && (
          <div className="speed">
            {[1, 4, 16, 60].map((s) => (
              <button key={s} className={s === speed ? 'on' : ''} onClick={() => setSpeed(s)}>
                {s}x
              </button>
            ))}
          </div>
        )}
        <div className="window">
          {[30, 60, 300].map((s) => (
            <button key={s} className={s === windowS ? 'on' : ''} onClick={() => setWindowS(s)}>
              {s < 60 ? `${s}s` : `${s / 60}m`}
            </button>
          ))}
        </div>
        <div className="clock">{utc(Date.now())} UTC</div>
      </header>

      <div className="sub">
        {mode === 'replay' && <span>Replaying a real ISS telemetry session recorded from NASA's public feed{replayInfo && `: ${replayInfo}`}.</span>}
        {mode === 'live' && <span>Streaming directly from NASA's public ISS Lightstreamer feed in your browser. Values update on change; LOS shows when the station is out of contact.</span>}
        {mode === 'gateway' && <span>Connected to the Rust gateway at {gwUrl}. Limits, interlocks and commands are enforced server-side.</span>}
      </div>

      <main className="grid">
        <aside className="panel chans">
          <header>
            <h2>Channels</h2>
            <input className="search" placeholder="Filter" value={filter} onChange={(e) => setFilter(e.target.value)} />
          </header>
          <div className="chan-scroll">
            {groups.map(([g, idx]) => (
              <div key={g} className="group">
                <div className="group-name">{g}</div>
                {idx.map((i) => {
                  const c = store.channels[i]
                  const lvl = store.level(i)
                  const sev = severity(lvl)
                  const fresh = store.isFresh(i)
                  const last = store.series[i]?.last()
                  return (
                    <button key={c.id} className={`chan ${pins.includes(c.id) ? 'pinned' : ''} ${!fresh ? 'stale' : sev === 2 ? 'alarm' : sev === 1 ? 'warn' : ''}`} onClick={() => togglePin(c.id)} title={`${c.id}: ${c.desc}`}>
                      <span className="sev" />
                      <span className="name">{c.label || c.id}</span>
                      <span className="val">{fmtVal(last?.[1])}</span>
                      <span className="unit">{c.unit}</span>
                    </button>
                  )
                })}
              </div>
            ))}
          </div>
        </aside>

        <section className="panel charts">
          <header>
            <h2>Strip charts</h2>
            <span className="muted">{lanes.length} pinned. Click a channel to pin or unpin.</span>
          </header>
          <ChartView store={store} lanes={lanes} windowMs={windowS * 1000} probe={probe.current} />
          {bench && (
            <div className="bench">
              Render benchmark: {bench.fps_avg.toFixed(1)} fps avg, p95 frame {bench.frame_ms_p95.toFixed(1)} ms, {bench.lanes} lanes, {Math.round(bench.samples_per_s_received).toLocaleString()} samples/s
            </div>
          )}
        </section>

        <div className="right">
          <section className="panel alarms">
            <header>
              <h2>Alarms</h2>
              <span className="muted">{active.length} active, {unacked} unacked</span>
              <button className="btn btn-ghost small" onClick={() => store.ack('all')} disabled={!unacked}>
                Ack all
              </button>
            </header>
            <div className="alarm-active">
              {active.length === 0 && <div className="empty">All limit-checked channels nominal</div>}
              {active.map((i) => (
                <div key={i} className={`active ${severity(store.level(i)) === 2 ? 'alarm' : 'warn'}`}>
                  {store.channels[i].label} <b>{store.level(i).replace('_', ' ')}</b> {fmtVal(store.series[i]?.last()?.[1])} {store.channels[i].unit}
                </div>
              ))}
            </div>
            <ul className="alarm-log">
              {store.alarms.slice(0, 60).map((a) => (
                <li key={a.id} className={`${a.acked ? 'acked' : ''} k-${a.kind}`} onClick={() => store.ack(a.id)}>
                  <span className="t">{utc(a.t)}</span>
                  {a.kind === 'signal' && <span>{a.aos ? 'Signal acquired' : `Loss of signal: ${a.reason}`}</span>}
                  {a.kind === 'stale' && <span>{store.channels[a.ch]?.label} {a.stale ? 'stale' : 'fresh again'}</span>}
                  {a.kind === 'limit' && (
                    <span>
                      {store.channels[a.ch]?.label}: {a.from.replace('_', ' ')} to <b>{a.to.replace('_', ' ')}</b> at {fmtVal(a.value)}
                    </span>
                  )}
                </li>
              ))}
            </ul>
          </section>
          {transport && <CommandPanel defs={defs} transport={transport} interlocks={interlocks} targetLabel={targetLabel} nameOf={(id) => store.channels.find((c) => c.id === id)?.label || id} />}
          <section className="panel audit">
            <header>
              <h2>Audit log</h2>
            </header>
            <ul>
              {store.audit.slice(0, 40).map((e, i) => (
                <li key={i}>
                  <span className="t">{utc(e.t)}</span>
                  <span className={`ev ev-${e.event}`}>{e.event.replace(/_/g, ' ')}</span>
                  <span>{e.cmd.replace(/^SIM_/, '')}</span>
                  <span className="muted">{e.key.slice(0, 11)}</span>
                </li>
              ))}
            </ul>
          </section>
        </div>
      </main>
    </div>
  )
}
