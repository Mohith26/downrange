export interface Limits {
  lo_alarm: number | null
  lo_warn: number | null
  hi_warn: number | null
  hi_alarm: number | null
  hyst: number
}

export interface Channel {
  id: string
  label: string
  group: string
  unit: string
  desc: string
  limits: Limits | null
  /** Own staleness window. null = publishes on change only, so feed liveness decides. */
  stale_ms: number | null
}

export type Level = 'nominal' | 'warn_lo' | 'warn_hi' | 'alarm_lo' | 'alarm_hi'

export function severity(l: Level): number {
  return l === 'nominal' ? 0 : l.startsWith('warn') ? 1 : 2
}

export type AlarmEvent =
  | { t: number; ch: number; kind: 'limit'; from: Level; to: Level; value: number }
  | { t: number; ch: number; kind: 'stale'; stale: boolean }
  | { t: number; ch: number; kind: 'signal'; aos: boolean; reason: string }

export type Interlock =
  | { type: 'fresh'; ch: string }
  | { type: 'in_limits'; ch: string }
  | { type: 'range'; ch: string; min: number; max: number }

export interface ArgDef {
  name: string
  min: number
  max: number
}

export interface CommandDef {
  name: string
  desc: string
  args: ArgDef[]
  interlocks: Interlock[]
}

export interface InterlockStatus {
  ok: boolean
  detail: string
}

export type Reply =
  | { type: 'armed'; key: string; token: string; expires_ms: number }
  | { type: 'executed'; key: string; result: string; dedup: boolean }
  | { type: 'rejected'; key: string; reason: string }

export interface AuditEntry {
  t: number
  client: number
  key: string
  cmd: string
  event: string
  detail: string
}

/** Where the console gets telemetry and where commands go. */
export interface CommandTransport {
  readonly label: string
  arm(key: string, cmd: string, args: Record<string, number>): Promise<Reply>
  confirm(key: string, token: string): Promise<Reply>
}
