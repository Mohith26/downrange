import { describe, expect, it } from 'vitest'
import { decimate, Series, type Column } from '../lib/series'

describe('series', () => {
  it('wraps and searches by time', () => {
    const s = new Series(4)
    for (let i = 0; i < 6; i++) s.push(i * 10, i)
    expect(s.count).toBe(4)
    expect(s.at(0)).toEqual([20, 2])
    expect(s.lowerBound(35)).toBe(2)
    expect(s.last()).toEqual([50, 5])
  })

  it('decimates to min/max per pixel and holds values', () => {
    const s = new Series(1000)
    for (let i = 0; i < 1000; i++) s.push(i, i % 2 ? 1 : -1)
    const cols: (Column | null)[] = []
    decimate(s, 0, 1000, 10, cols)
    expect(cols).toHaveLength(10)
    for (const c of cols) expect(c && [c.min, c.max]).toEqual([-1, 1])
    const sparse = new Series(8)
    sparse.push(0, 5)
    decimate(sparse, 100, 200, 4, cols)
    expect(cols.every((c) => c?.min === 5)).toBe(true)
  })
})
