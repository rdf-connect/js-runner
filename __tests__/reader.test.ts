import { describe, expect, test } from 'vitest'
import { createLogger, transports } from 'winston'
import { DataChunk, FromRunner } from '@rdfc/proto'
import { SendingStreamControl } from '@rdfc/proto/lib/generated/common.js'
import { ReaderInstance } from '../src/reader'
import { MockClientDuplexStream } from '../src/testUtils/duplex'

const logger = createLogger({
  transports: new transports.Console({
    level: process.env['DEBUG'] || 'info',
  }),
})

/**
 * Stands in for a gRPC receive stream whose connection dies: it accepts the
 * control writes and is then killed with an 'error' event, the way grpc-js
 * reports an orchestrator that disconnected mid-message.
 */
class DroppingReceiveMock {
  stream = new MockClientDuplexStream<SendingStreamControl, DataChunk>()

  constructor() {
    this.stream.register(
      (x) => x,
      () => {},
    )
  }

  receiveStreamMessage() {
    return this.stream
  }
}

/** Collects process-level unhandled rejections raised while `fn` runs. */
async function captureUnhandledRejections(
  fn: () => Promise<void>,
): Promise<unknown[]> {
  const rejections: unknown[] = []
  const onUnhandled = (reason: unknown) => rejections.push(reason)
  process.on('unhandledRejection', onUnhandled)
  try {
    await fn()
    // Node reports unhandled rejections once the microtask queue has drained.
    await new Promise((res) => setTimeout(res, 20))
  } finally {
    process.off('unhandledRejection', onUnhandled)
  }
  return rejections
}

describe('ReaderInstance', () => {
  test('surfaces a dropped stream connection to its consumers', async () => {
    const uri = 'someUri'
    const client = new DroppingReceiveMock()
    const notify = async (_msg: FromRunner) => undefined
    const reader = new ReaderInstance(uri, client as any, notify, logger)

    const consumed = (async () => {
      for await (const stream of reader.streams()) {
        for await (const _chunk of stream) {
          // drain
        }
      }
    })()

    await reader.handleStreamingMessage({
      channel: uri,
      globalSequenceNumber: 7,
    })

    client.stream.emit('error', new Error('Connection dropped'))

    await expect(consumed).rejects.toThrow(/Connection dropped/)
  })

  test('does not leak a rejection when a buffering consumer is reading', async () => {
    const uri = 'someUri'
    const client = new DroppingReceiveMock()
    const msgs: FromRunner[] = []
    const notify = async (msg: FromRunner) => {
      msgs.push(msg)
    }
    const reader = new ReaderInstance(uri, client as any, notify, logger)

    // Unlike streams(), strings() drains the chunks eagerly inside
    // pushStream(), so the dropped stream surfaces there instead of in the
    // consumer's own loop.
    const consumed = (async () => {
      for await (const _msg of reader.strings()) {
        // drain
      }
    })()
    consumed.catch(() => {})

    const rejections = await captureUnhandledRejections(async () => {
      await reader.handleStreamingMessage({
        channel: uri,
        globalSequenceNumber: 7,
      })

      client.stream.emit('error', new Error('Connection dropped'))
    })

    expect(rejections).toEqual([])
    // The failure ack is the only one: no success ack may follow it for the
    // same sequence number.
    expect(msgs.map((m) => m.processed)).toEqual([
      {
        channel: uri,
        globalSequenceNumber: 7,
        error: expect.stringMatching(/Connection dropped/),
      },
    ])
  })

  test('does not retract the ack when the stream errors after processing', async () => {
    const uri = 'someUri'
    const client = new DroppingReceiveMock()
    const msgs: FromRunner[] = []
    const notify = async (msg: FromRunner) => {
      msgs.push(msg)
    }
    const reader = new ReaderInstance(uri, client as any, notify, logger)

    const consumed = (async () => {
      for await (const stream of reader.streams()) {
        for await (const _chunk of stream) {
          // drain
        }
      }
    })()
    consumed.catch(() => {})

    await reader.handleStreamingMessage({
      channel: uri,
      globalSequenceNumber: 7,
    })

    // The stream completes normally: every consumer finishes and the success
    // ack goes out.
    client.stream.push(null)
    await new Promise((res) => setTimeout(res, 10))

    const acked = [{ channel: uri, globalSequenceNumber: 7 }]
    expect(msgs.map((m) => m.processed)).toEqual(acked)

    // grpc-js can still fail the RPC afterwards (e.g. UNAVAILABLE because the
    // status never arrived on a dropped connection). That must not send a
    // second, contradictory `processed` for a message already reported done.
    client.stream.emit('error', new Error('Connection dropped'))
    await new Promise((res) => setTimeout(res, 10))

    expect(msgs.map((m) => m.processed)).toEqual(acked)
  })

  test('tells the orchestrator the message failed when the stream drops', async () => {
    const uri = 'someUri'
    const client = new DroppingReceiveMock()
    const msgs: FromRunner[] = []
    const notify = async (msg: FromRunner) => {
      msgs.push(msg)
    }
    const reader = new ReaderInstance(uri, client as any, notify, logger)

    const consumed = (async () => {
      for await (const stream of reader.streams()) {
        for await (const _chunk of stream) {
          // drain
        }
      }
    })()
    consumed.catch(() => {})

    await reader.handleStreamingMessage({
      channel: uri,
      globalSequenceNumber: 7,
    })

    client.stream.emit('error', new Error('Connection dropped'))
    await new Promise((res) => setTimeout(res, 10))

    expect(msgs.map((m) => m.processed)).toEqual([
      {
        channel: uri,
        globalSequenceNumber: 7,
        error: expect.stringMatching(/Connection dropped/),
      },
    ])
  })
})
