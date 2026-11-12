// Operator-side state machine for one command: arm, then confirm, with an idempotency
// key that survives retries. Kept framework-free so it can be unit tested.
import type { CommandTransport, Reply } from './types'

export type Phase = 'idle' | 'arming' | 'armed' | 'confirming' | 'executed' | 'rejected' | 'no_reply'

export interface FlowState {
  phase: Phase
  key: string | null
  token: string | null
  expiresMs: number
  lastStep: 'arm' | 'confirm' | null
  result: string
  dedup: boolean
  attempts: number
}

export const initialFlow: FlowState = { phase: 'idle', key: null, token: null, expiresMs: 0, lastStep: null, result: '', dedup: false, attempts: 0 }

export function newKey(): string {
  const b = new Uint8Array(12)
  crypto.getRandomValues(b)
  return 'op-' + Array.from(b, (x) => x.toString(16).padStart(2, '0')).join('')
}

export class CommandFlow {
  state: FlowState = { ...initialFlow }
  /** When set, the next reply is discarded as if the network lost it. */
  dropNextReply = false

  constructor(
    private transport: CommandTransport,
    readonly cmd: string,
    private onChange: (s: FlowState) => void,
  ) {}

  private set(p: Partial<FlowState>) {
    this.state = { ...this.state, ...p }
    this.onChange(this.state)
  }

  private apply(r: Reply) {
    if (r.type === 'armed') this.set({ phase: 'armed', token: r.token, expiresMs: r.expires_ms })
    else if (r.type === 'executed') this.set({ phase: 'executed', result: r.result, dedup: r.dedup, token: null })
    else this.set({ phase: 'rejected', result: r.reason, token: null })
  }

  private async send(step: 'arm' | 'confirm', fn: () => Promise<Reply>) {
    this.set({ phase: step === 'arm' ? 'arming' : 'confirming', lastStep: step, attempts: this.state.attempts + 1 })
    try {
      const r = await fn()
      if (this.dropNextReply) {
        this.dropNextReply = false
        this.set({ phase: 'no_reply', result: 'reply lost (simulated); retrying is safe' })
        return
      }
      this.apply(r)
    } catch (e) {
      this.set({ phase: 'no_reply', result: (e as Error).message })
    }
  }

  /** Start a new operator intent with a fresh idempotency key. */
  arm(args: Record<string, number>) {
    const key = newKey()
    this.state = { ...initialFlow, key }
    return this.send('arm', () => this.transport.arm(key, this.cmd, args))
  }

  confirm() {
    const { key, token } = this.state
    if (!key || !token) return
    return this.send('confirm', () => this.transport.confirm(key, token))
  }

  /** Retry the last step with the same key (and token), which the server dedupes. */
  retry(args: Record<string, number>) {
    const { key, token, lastStep } = this.state
    if (!key) return
    if (lastStep === 'confirm' && token) return this.send('confirm', () => this.transport.confirm(key, token))
    return this.send('arm', () => this.transport.arm(key, this.cmd, args))
  }

  cancel() {
    this.state = { ...initialFlow }
    this.onChange(this.state)
  }
}
