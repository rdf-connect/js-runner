import { describe, expect, test } from 'vitest'
import { RunnerClient } from '@rdfc/proto'
import { createLogger } from 'winston'
import Transport from 'winston-transport'
import { Runner } from '../src/runner'

class CaptureTransport extends Transport {
  readonly lines: { level: string; message: string }[] = []
  log(info: { level: string; message: unknown }, callback: () => void) {
    this.lines.push({ level: info.level, message: String(info.message) })
    callback()
  }
}

describe('close', () => {
  test('of a channel this runner does not use is only logged at debug level', async () => {
    // The orchestrator sends every close to every runner.
    const logs = new CaptureTransport()
    const runner = new Runner(
      <RunnerClient>{},
      async () => {},
      'http://example.com/ns#runner',
      createLogger({ level: 'debug', transports: [logs] }),
    )

    await runner.handleOrchMessage({
      close: { channel: 'http://example.com/ns#elsewhere' },
    })

    expect(logs.lines.filter((l) => l.level !== 'debug')).toEqual([])
    expect(
      logs.lines.some((l) =>
        l.message.includes('http://example.com/ns#elsewhere'),
      ),
    ).toBe(true)
  })
})
