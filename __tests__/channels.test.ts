import { describe, expect, test, vi } from 'vitest'
import { channel, createRunner, StreamMsgMock } from '../src/testUtils'
import { WriterInstance } from '../src/writer'
import { FromRunner, StreamIdentify } from '@rdfc/proto'
import { createLogger, transports } from 'winston'

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
