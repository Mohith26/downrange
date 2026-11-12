import { describe, expect, it } from 'vitest'
import { MergeState, parseFields, parseLine } from '../lib/tlcp'

describe('tlcp', () => {
  it('parses an update and keeps unchanged fields', () => {
    const m = parseLine('U,1,2,7587.75|753.50|24')
    expect(m.kind).toBe('update')
    const st = new MergeState(3, 3)
    if (m.kind !== 'update') throw new Error()
    st.apply(m.item, m.fields)
    const row = st.apply(2, parseFields('|754.1|'))
    expect(row).toEqual(['7587.75', '754.1', '24'])
  })
  it('expands caret runs and decodes specials', () => {
    expect(parseFields('^3|x')).toHaveLength(4)
    expect(parseFields('#|$|a%20b')).toEqual([{ kind: 'null' }, { kind: 'value', v: '' }, { kind: 'value', v: 'a b' }])
  })
  it('reads CONOK session id', () => {
    expect(parseLine('CONOK,S1234,50000,5000,*')).toEqual({ kind: 'conok', session: 'S1234' })
  })
})
