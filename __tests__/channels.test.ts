import { describe, expect, test } from 'vitest'
import { DataChunk, RunnerClient } from '@rdfc/proto'
import { ClientReadableStream } from '@grpc/grpc-js'
import * as grpc from '@grpc/grpc-js'
import { ReaderInstance } from '../src/reader'
import { createLogger } from 'winston'
import winston from 'winston/lib/winston/transports'
import { ClientReadableStreamImpl } from '@grpc/grpc-js/build/src/call'

const encoder = new TextEncoder()
class TestClient extends RunnerClient {
  next: (stream: ClientReadableStream<DataChunk>) => unknown

  constructor() {
    super('localhost:5400', grpc.credentials.createInsecure())
  }

  nextStream(): Promise<ClientReadableStream<DataChunk>> {
    return new Promise((res) => (this.next = res))
  }

  receiveStreamMessage(): ClientReadableStream<DataChunk> {
    const stream = new ClientReadableStreamImpl<DataChunk>((data: Buffer) => {
      console.log('Debug', data)
      return { data }
    })
    this.next(stream)
    return stream
  }
}

async function one<T>(iter: AsyncIterable<T>): Promise<T | undefined> {
  for await (const item of iter) {
    return item;
  }
}

describe('Reader', async () => {
  const client = new TestClient()
  const uri = 'someUri'
  const logger = createLogger({
    transports: new winston.Console({ level: process.env['DEBUG'] && 'debug' }),
  })

  test('handles string', async () => {
    const reader = new ReaderInstance(uri, client, logger)

    const promise = one(reader.strings())
    reader.handleMsg({ data: encoder.encode('Hello world'), channel: uri })
    const out = await promise

    expect(out).toEqual('Hello world')
  })

  test('handles closed string', async () => {
    const reader = new ReaderInstance(uri, client, logger)

    const promise = one(reader.strings())
    reader.close()
    const out = await promise

    expect(out).toEqual(undefined)
  })

  test('handles strings', async () => {
    const reader = new ReaderInstance(uri, client, logger)

    const msgsPromise = (async () => {
      const msgs: string[] = []
      for await (const msg of reader.strings()) {
        msgs.push(msg)
      }
      return msgs
    })()
    reader.handleMsg({ data: encoder.encode('Hello'), channel: uri })
    reader.handleMsg({ data: encoder.encode('World'), channel: uri })
    reader.close()

    const msgs = await msgsPromise

    expect(msgs).toEqual(['Hello', 'World'])
  })

  test('handles buffer', async () => {
    const reader = new ReaderInstance(uri, client, logger)

    const data = encoder.encode('Hello world')
    const promise = one(reader.buffers())
    reader.handleMsg({ data, channel: uri })
    const out = await promise

    expect(out).toEqual(data)
  })

  test('handles buffers', async () => {
    const reader = new ReaderInstance(uri, client, logger)

    const data = encoder.encode('Hello world')
    const promise = one(reader.buffers())
    reader.handleMsg({ data, channel: uri })
    const out = await promise

    expect(out).toEqual(data)
  })

  test('handles stream', async () => {
    const reader = new ReaderInstance(uri, client, logger)

    const nextStream = client.nextStream()
    const promise = one(reader.streams())
    reader.handleStreamingMessage({ id: { id: 5 }, channel: uri })
    const stream = await nextStream

    stream.push('Hello')
    stream.push('World')
    console.log(stream.eventNames())

    const out = await promise
    const msgsPromise = (async () => {
      const msgs: Uint8Array[] = []
      for await (const msg of out!) {
        msgs.push(msg)
      }
      return msgs
    })()

    stream.emit('end')
    stream.emit('close')

    const msgs = await msgsPromise
    console.log(msgs)
  })
})
