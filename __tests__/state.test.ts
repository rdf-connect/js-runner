import { describe, expect, test } from 'vitest'
import { State } from '../src/state'

describe('State.trackChannel', () => {
  test('keeps reader and writer stats apart for one channel URI', () => {
    const state = new State(5)
    const id = state.registerRunner('socket', 'runnerUri')
    const uri = 'http://example.org/channel'

    // A pipeline can write and read the same channel inside a single runner.
    const writer = state.trackChannel(id, uri, 'writer')
    const reader = state.trackChannel(id, uri, 'reader')

    writer.recordMessage(100, 12)
    reader.recordMessage(100)

    const channels = Object.values(state.snapshot()[0].channels)
    expect(channels).toHaveLength(2)

    const byRole = Object.fromEntries(channels.map((c) => [c.role, c]))
    expect(byRole['writer'].messageCount).toBe(1)
    expect(byRole['writer'].bytesTotal).toBe(100)
    // Latencies are a writer-only measure; reader traffic must not land here.
    expect(byRole['writer'].latenciesMs).toEqual([12])
    expect(byRole['reader'].messageCount).toBe(1)
    expect(byRole['reader'].bytesTotal).toBe(100)
    expect(byRole['reader'].latenciesMs).toEqual([])
  })

  test('reuses one record per role', () => {
    const state = new State(5)
    const id = state.registerRunner('socket', 'runnerUri')
    const uri = 'http://example.org/channel'

    state.trackChannel(id, uri, 'writer').recordMessage(10)
    state.trackChannel(id, uri, 'writer').recordMessage(10)

    const channels = Object.values(state.snapshot()[0].channels)
    expect(channels).toHaveLength(1)
    expect(channels[0].messageCount).toBe(2)
  })
})
