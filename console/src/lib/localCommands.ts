// In-browser twin of gateway/src/command.rs for the hosted demo, where there is no
// gateway. Same rules: arm, then confirm with a single-use expiring token; idempotency
// keys dedupe retries; interlocks are re-checked at confirm. Target is a loopback stub.
import type { LimitEngine } from './limits'
import type { AuditEntry, CommandDef, CommandTransport, Interlock, InterlockStatus, Reply } from './types'

export function evalInterlock(il: Interlock, eng: LimitEngine, now: number): InterlockStatus {
  const i = eng.indexOf(il.ch)
  if (i < 0) return { ok: false, detail: `${il.ch}: unknown channel` }
  if (!eng.isFresh(i, now)) return { ok: false, detail: `${il.ch}: stale or no signal` }
  if (il.type === 'fresh') return { ok: true, detail: `${il.ch}: fresh` }
  if (il.type === 'in_limits') {
    const lvl = eng.st[i].level
    return { ok: lvl === 'nominal', detail: `${il.ch}: ${lvl}` }
  }
  const v = eng.st[i].value
  return { ok: v >= il.min && v <= il.max, detail: `${il.ch}: ${v} in [${il.min}, ${il.max}]` }
}

function randomToken(): string {
  const b = new Uint8Array(16)
  crypto.getRandomValues(b)
  return Array.from(b, (x) => x.toString(16).padStart(2, '0')).join('')
}

export class LocalCommandEngine implements CommandTransport {
  readonly label = 'in-browser loopback'
  private tokens = new Map<string, { key: string; cmd: string; args: string; expires: number }>()
  private keys = new Map<string, { state: 'armed'; token: string; sig: string } | { state: 'executed'; result: string }>()
  executions = new Map<string, number>()
  total = 0

  constructor(
    readonly defs: CommandDef[],
    private engine: () => LimitEngine,
    private onAudit: (e: AuditEntry) => void,
    private ttlMs = 10_000,
    private now: () => number = () => Date.now(),
  ) {}

  report() {
    const now = this.now()
    return this.defs.map((d) => {
      const checks = d.interlocks.map((il) => evalInterlock(il, this.engine(), now))
      return { name: d.name, ok: checks.every((c) => c.ok), checks }
    })
  }

  private log(key: string, cmd: string, event: string, detail: string) {
    this.onAudit({ t: this.now(), client: 0, key, cmd, event, detail })
  }

  private check(def: CommandDef): string | null {
    const failed = def.interlocks.map((il) => evalInterlock(il, this.engine(), this.now())).filter((s) => !s.ok)
    return failed.length ? 'interlock: ' + failed.map((f) => f.detail).join('; ') : null
  }

  async arm(key: string, cmd: string, args: Record<string, number>): Promise<Reply> {
    const prev = this.keys.get(key)
    if (prev?.state === 'executed') {
      this.log(key, cmd, 'dedup_arm', prev.result)
      return { type: 'executed', key, result: prev.result, dedup: true }
    }
    const reject = (reason: string): Reply => {
      this.log(key, cmd, 'arm_rejected', reason)
      return { type: 'rejected', key, reason }
    }
    const def = this.defs.find((d) => d.name === cmd)
    if (!def) return reject(`unknown command ${cmd}`)
    for (const a of def.args) {
      const v = args[a.name]
      if (!(Number.isFinite(v) && v >= a.min && v <= a.max)) return reject(`arg ${a.name} must be in [${a.min}, ${a.max}]`)
    }
    const err = this.check(def)
    if (err) return reject(err)
    const sig = JSON.stringify([cmd, args])
    if (prev?.state === 'armed') {
      if (prev.sig !== sig) return reject('idempotency key reused for a different command')
      const t = this.tokens.get(prev.token)
      if (t && t.expires > this.now()) {
        this.log(key, cmd, 'rearm_same_token', '')
        return { type: 'armed', key, token: prev.token, expires_ms: t.expires }
      }
    }
    const token = randomToken()
    const expires = this.now() + this.ttlMs
    this.tokens.set(token, { key, cmd, args: sig, expires })
    this.keys.set(key, { state: 'armed', token, sig })
    this.log(key, cmd, 'armed', `expires in ${this.ttlMs} ms`)
    return { type: 'armed', key, token, expires_ms: expires }
  }

  async confirm(key: string, token: string): Promise<Reply> {
    const prev = this.keys.get(key)
    const a = this.tokens.get(token)
    const cmd = a?.cmd ?? ''
    if (prev?.state === 'executed') {
      this.log(key, cmd, 'dedup_confirm', prev.result)
      return { type: 'executed', key, result: prev.result, dedup: true }
    }
    const reject = (reason: string): Reply => {
      this.log(key, cmd, 'confirm_rejected', reason)
      return { type: 'rejected', key, reason }
    }
    if (!a) return reject('unknown or already used token')
    if (a.key !== key) return reject('token was issued for a different key')
    if (a.expires <= this.now()) {
      this.tokens.delete(token)
      this.keys.delete(key)
      return reject('token expired, re-arm')
    }
    const err = this.check(this.defs.find((d) => d.name === a.cmd)!)
    if (err) {
      this.tokens.delete(token)
      this.keys.delete(key)
      return reject(err)
    }
    this.tokens.delete(token)
    this.total++
    this.executions.set(key, (this.executions.get(key) ?? 0) + 1)
    const result = `loopback ack #${this.total}: ${a.cmd}`
    this.keys.set(key, { state: 'executed', result })
    this.log(key, a.cmd, 'executed', result)
    return { type: 'executed', key, result, dedup: false }
  }
}
