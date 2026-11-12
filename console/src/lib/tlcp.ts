// Subset of Lightstreamer TLCP-2.2.0 needed for one MERGE subscription.
// Mirrors gateway/src/tlcp.rs.

export type FieldDelta = { kind: 'unchanged' } | { kind: 'null' } | { kind: 'value'; v: string }

export type Msg =
  | { kind: 'conok'; session: string }
  | { kind: 'subok'; items: number; fields: number }
  | { kind: 'update'; item: number; fields: FieldDelta[] }
  | { kind: 'loop' }
  | { kind: 'end'; text: string }
  | { kind: 'error'; text: string }
  | { kind: 'other'; text: string }

export function parseFields(raw: string): FieldDelta[] {
  const out: FieldDelta[] = []
  for (const f of raw.split('|')) {
    if (f === '') out.push({ kind: 'unchanged' })
    else if (f === '#') out.push({ kind: 'null' })
    else if (f === '$') out.push({ kind: 'value', v: '' })
    else if (/^\^\d+$/.test(f)) {
      const n = Number(f.slice(1))
      for (let i = 0; i < n; i++) out.push({ kind: 'unchanged' })
    } else {
      let v = f
      try {
        v = decodeURIComponent(f)
      } catch {
        /* keep raw */
      }
      out.push({ kind: 'value', v })
    }
  }
  return out
}

export function parseLine(line: string): Msg {
  line = line.replace(/\r?\n$/, '')
  const i = line.indexOf(',')
  const head = i < 0 ? line : line.slice(0, i)
  const rest = i < 0 ? '' : line.slice(i + 1)
  switch (head) {
    case 'CONOK':
      return { kind: 'conok', session: rest.split(',')[0] }
    case 'SUBOK': {
      const p = rest.split(',')
      return { kind: 'subok', items: Number(p[1]), fields: Number(p[2]) }
    }
    case 'U': {
      const a = rest.indexOf(',')
      const b = rest.indexOf(',', a + 1)
      return { kind: 'update', item: Number(rest.slice(a + 1, b)), fields: parseFields(rest.slice(b + 1)) }
    }
    case 'LOOP':
      return { kind: 'loop' }
    case 'END':
      return { kind: 'end', text: rest }
    case 'ERROR':
    case 'CONERR':
      return { kind: 'error', text: rest }
    default:
      return { kind: 'other', text: line }
  }
}

export class MergeState {
  private rows: (string | null)[][]
  constructor(items: number, fields: number) {
    this.rows = Array.from({ length: items }, () => Array<string | null>(fields).fill(null))
  }
  /** item is 1-based as on the wire */
  apply(item: number, deltas: FieldDelta[]): (string | null)[] | null {
    const row = this.rows[item - 1]
    if (!row) return null
    deltas.forEach((d, i) => {
      if (i >= row.length) return
      if (d.kind === 'null') row[i] = null
      else if (d.kind === 'value') row[i] = d.v
    })
    return row
  }
}
