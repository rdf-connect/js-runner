import { describe, expect, test, vi } from 'vitest'
import { channel, createRunner, StreamMsgMock } from '../src/testUtils'
import { MockClientDuplexStream } from '../src/testUtils/duplex'
import { WriterInstance } from '../src/writer'
import { FromRunner, StreamIdentify } from '@rdfc/proto'
import {
  ReceivingStreamControl,
  StreamChunk,
} from '@rdfc/proto/lib/generated/common.js'
import { createLogger, transports } from 'winston'

/**
 * Stands in for a gRPC stream whose connection dies: the id handshake succeeds,
 * then the first data chunk is answered with an 'error' event instead of an ack.
 */
class DroppingStreamMock {
  sendStreamMessage(): MockClientDuplexStream<
    StreamChunk,
    ReceivingStreamControl
  > {
    const stream = new MockClientDuplexStream<
      StreamChunk,
      ReceivingStreamControl
    >()
    stream.register(
      (x) => x.id,
      (_id, send) => send({ streamSequenceNumber: 1 }),
    )
    stream.register(
      (x) => x.data,
      () => {
        setTimeout(() => stream.emit('error', new Error('Connection dropped')))
      },
    )
    return stream
  }
}

/**
 * Same as {@link DroppingStreamMock}, but the connection dies while the data
 * chunk write is still in flight: its callback is never invoked, so the write
 * itself is what loses the race against the 'error' event.
 */
class HangingStreamMock {
  sendStreamMessage(): MockClientDuplexStream<
    StreamChunk,
    ReceivingStreamControl
  > {
    const stream = new MockClientDuplexStream<
      StreamChunk,
      ReceivingStreamControl
    >()
    stream.register(
      (x) => x.id,
      (_id, send) => send({ streamSequenceNumber: 1 }),
    )

    const write = stream._write.bind(stream)
    stream._write = (chunk, encoding, callback) => {
      if (chunk.data) {
        setTimeout(() => stream.emit('error', new Error('Connection dropped')))
        return // never calls back: the write stays pending forever
      }
      write(chunk, encoding, callback)
    }
    return stream
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

const encoder = new TextEncoder()
const decoder = new TextDecoder()

const logger = createLogger({
  transports: new transports.Console({
    level: process.env['DEBUG'] || 'info',
  }),
})

describe('Writer', async () => {
  test('sends strings', async () => {
    const uri = 'someUri'
    const runner = 'myRunner'
    const fn = vi.fn((id: StreamIdentify) => {
      expect(id.channel).toBe(uri)
      expect(id.runner).toBe(runner)
      return 1
    })
    const client = new StreamMsgMock(fn)
    const msgs: FromRunner[] = []
    const write = async (msg: FromRunner) => msgs.push(msg)
    const writer = new WriterInstance(uri, client as any, write, runner, logger)

    const send = writer.string('hello world')
    writer.handled()
    await send

    expect(msgs.length).toBe(1)
    expect(msgs.map((x) => decoder.decode(x.msg!.data))).toEqual([
      'hello world',
    ])

    expect(fn).toBeCalledTimes(0)
  })

  test('sends binary', async () => {
    const uri = 'someUri'
    const runner = 'myRunner'
    const fn = vi.fn((id: StreamIdentify) => {
      expect(id.channel).toBe(uri)
      expect(id.runner).toBe(runner)
      return 1
    })
    const client = new StreamMsgMock(fn)
    const msgs: FromRunner[] = []
    const write = async (msg: FromRunner) => msgs.push(msg)
    const writer = new WriterInstance(uri, client as any, write, runner, logger)

    const send = writer.buffer(encoder.encode('hello world'))
    writer.handled()
    await send

    expect(msgs.length).toBe(1)
    expect(msgs.map((x) => decoder.decode(x.msg!.data))).toEqual([
      'hello world',
    ])

    expect(fn).toBeCalledTimes(0)
  })

  test('streams data', async () => {
    const uri = 'someUri'
    const runner = 'myRunner'
    const fn = vi.fn((id: StreamIdentify) => {
      expect(id.channel).toBe(uri)
      expect(id.runner).toBe(runner)
      return 1
    })
    const client = new StreamMsgMock(fn)
    const msgs: FromRunner[] = []
    const write = async (msg: FromRunner) => msgs.push(msg)
    const writer = new WriterInstance(uri, client as any, write, runner, logger)

    async function* gen() {
      yield encoder.encode('hello')
      yield encoder.encode('world')

      setTimeout(() => writer.handled(), 20)
    }

    await writer.stream(gen())

    expect(client.data.length).toBe(2)
    expect(client.data.map((x) => decoder.decode(x.data))).toEqual([
      'hello',
      'world',
    ])
    expect(fn).toBeCalled()
  })

  test('closes', async () => {
    const uri = 'someUri'
    const runner = 'myRunner'
    const fn = vi.fn((id: StreamIdentify) => {
      expect(id.channel).toBe(uri)
      expect(id.runner).toBe(runner)
      return 1
    })
    const client = new StreamMsgMock(fn)
    const msgs: FromRunner[] = []
    const write = async (msg: FromRunner) => msgs.push(msg)
    const writer = new WriterInstance(uri, client as any, write, runner, logger)

    await writer.close()

    expect(writer.canceled).toBe(false)

    expect(msgs.length).toBe(1)
    expect(msgs.map((x) => x.close!.channel)).toEqual([uri])

    expect(fn).toBeCalledTimes(0)
  })

  test('wait to close after stream is finished', async () => {
    const uri = 'someUri'
    const runner = 'myRunner'
    const client = new StreamMsgMock(() => 1)

    const msgs: FromRunner[] = []
    const write = async (msg: FromRunner) => msgs.push(msg)
    const writer = new WriterInstance(uri, client as any, write, runner, logger)

    let closingPromise: Promise<void> | undefined = undefined
    async function* gen() {
      yield encoder.encode('hello')

      // initiate close but the channel cannot close yet, as it has an open stream message
      closingPromise = writer.close()

      await new Promise((res) => setTimeout(res, 20))

      expect(msgs.filter((x) => !!x.close)).toEqual([])
      yield encoder.encode('world')

      // we 'handled' the message
      setTimeout(() => writer.handled(), 20)
    }

    await writer.stream(gen())
    await closingPromise!
    expect(msgs.map((x) => x.close!.channel)).toEqual([uri])

    expect(client.data.length).toBe(2)
    expect(client.data.map((x) => decoder.decode(x.data))).toEqual([
      'hello',
      'world',
    ])
  })

  test('rejects a new write started while close is deferred on an open stream', async () => {
    const uri = 'someUri'
    const runner = 'myRunner'
    const client = new StreamMsgMock(() => 1)

    const msgs: FromRunner[] = []
    const write = async (msg: FromRunner) => msgs.push(msg)
    const writer = new WriterInstance(uri, client as any, write, runner, logger)

    let closingPromise: Promise<void> | undefined = undefined
    async function* gen() {
      yield encoder.encode('hello')

      // initiate close but the channel cannot close yet, as it has an open stream message
      closingPromise = writer.close()

      // an unrelated write started during the deferred close must be rejected
      await expect(writer.string('too late')).rejects.toThrow(/closed/i)

      yield encoder.encode('world')

      setTimeout(() => writer.handled(), 20)
    }

    await writer.stream(gen())
    await closingPromise!
  })

  test('keeps the ack queue aligned after a stream fails mid-flight', async () => {
    const uri = 'someUri'
    const runner = 'myRunner'
    const client = new DroppingStreamMock()

    const msgs: FromRunner[] = []
    const write = async (msg: FromRunner) => msgs.push(msg)
    const writer = new WriterInstance(uri, client as any, write, runner, logger)

    async function* gen() {
      yield encoder.encode('hello')
    }

    await expect(writer.stream(gen())).rejects.toThrow(/Connection dropped/)

    // The dead stream must not leave its ack entry behind: the next write's ack
    // would otherwise be consumed by the abandoned entry and never arrive.
    const send = writer.string('after the failure')
    writer.handled()
    await expect(send).resolves.toBeUndefined()
  })

  test('does not leak a rejection when the stream dies mid-chunk-write', async () => {
    const uri = 'someUri'
    const runner = 'myRunner'
    const client = new HangingStreamMock()

    const write = async (_msg: FromRunner) => {}
    const writer = new WriterInstance(uri, client as any, write, runner, logger)

    async function* gen() {
      yield encoder.encode('hello')
    }

    const rejections = await captureUnhandledRejections(async () => {
      // The pending ack we set up for this chunk is abandoned when the write
      // loses the race; nobody is left to handle its rejection.
      await expect(writer.stream(gen())).rejects.toThrow(/Connection dropped/)
    })

    expect(rejections).toEqual([])
  })

  test('settles deferred close callers when the close notification fails', async () => {
    const uri = 'someUri'
    const runner = 'myRunner'
    const client = new StreamMsgMock(() => 1)

    const write = async (msg: FromRunner) => {
      if (msg.close) {
        throw new Error('connection dropped')
      }
    }
    const writer = new WriterInstance(uri, client as any, write, runner, logger)

    let closingPromise: Promise<void> | undefined = undefined
    async function* gen() {
      yield encoder.encode('hello')

      // Deferred: the channel still has an open stream message.
      closingPromise = writer.close()

      setTimeout(() => writer.handled(), 20)
    }

    // The stream itself completed, so its result must not be masked by the
    // failure of the close that was deferred onto it.
    await expect(writer.stream(gen())).resolves.toBeUndefined()
    await expect(closingPromise!).rejects.toThrow(/connection dropped/)
  })

  test('is marked canceled when connected reader cancels', async () => {
    const runner = createRunner()
    const [writer, reader] = channel(runner, 'cancel-channel')

    await reader.cancel()

    expect(writer.canceled).toBe(true)
    await expect(writer.string('hello')).rejects.toThrow(/canceled/i)
  })

  test('emits a cancel event when connected reader cancels', async () => {
    const runner = createRunner()
    const [writer, reader] = channel(runner, 'cancel-listener-channel')
    const onCancel = vi.fn()

    writer.on('cancel', onCancel)
    await reader.cancel()

    expect(onCancel).toHaveBeenCalledTimes(1)
  })

  test('does not emit a cancel event on a local close', async () => {
    const uri = 'someUri'
    const runner = 'myRunner'
    const client = new StreamMsgMock(() => 1)
    const write = async (_msg: FromRunner) => undefined
    const writer = new WriterInstance(uri, client as any, write, runner, logger)
    const onCancel = vi.fn()

    writer.on('cancel', onCancel)
    await writer.close()

    expect(onCancel).not.toHaveBeenCalled()
  })

  test('throws when writing to a canceled writer', async () => {
    const uri = 'someUri'
    const runner = 'myRunner'
    const client = new StreamMsgMock(() => 1)
    const write = async (_msg: FromRunner) => undefined
    const writer = new WriterInstance(uri, client as any, write, runner, logger)

    await writer.close(true)

    expect(writer.canceled).toBe(true)
    await expect(writer.buffer(encoder.encode('x'))).rejects.toThrow(
      /canceled/i,
    )
  })

  test('rejects in-flight writes when reader cancels', async () => {
    const runner = createRunner()
    const [writer, reader] = channel(runner, 'cancel-in-flight')

    // Register a reader consumer without draining it so the writer waits for processed.
    reader.strings()

    // Set reader to canceled, without informing the writer of it, mimicking race condition where reader cancels while writer is writing, but before the writer receives the cancel message.
    // @ts-ignore
    reader['canceled'] = true
    // @ts-ignore
    reader['closed'] = true

    const pendingWrite = writer.string('hello')
    await expect(pendingWrite).rejects.toThrow(/canceled/i)
  })
})
