import { ClientReadableStream } from '@grpc/grpc-js'
import { DataChunk, Message, RunnerClient, StreamMessage } from '@rdfc/proto'
import winston from 'winston'
import {
  Convertor,
  NoConvertor,
  StreamConvertor,
  StringConvertor,
} from './convertor'

export interface Reader {
  string(): Promise<string | undefined>
  stream(): Promise<AsyncGenerator<Uint8Array> | undefined>
  buffer(): Promise<Uint8Array | undefined>
  strings(): AsyncIterable<string>
  streams(): AsyncIterable<AsyncGenerator<Uint8Array>>
  buffers(): AsyncIterable<Uint8Array>
}

class MyIter<T> implements AsyncIterable<T> {
  private convertor: Convertor<T>
  private queue: (T | undefined)[] = []
  private resolveNext: ((value: T | undefined) => void) | null = null

  constructor(convertor: Convertor<T>) {
    this.convertor = convertor
  }

  push(buffer: Uint8Array) {
    const item = this.convertor.from(buffer)
    if (this.resolveNext) {
      this.resolveNext(item)
      this.resolveNext = null
    } else {
      this.queue.push(item)
    }
  }

  close() {
    if (this.resolveNext) {
      this.resolveNext(undefined)
      this.resolveNext = null
    } else {
      this.queue.push(undefined)
    }
  }

  async pushStream(chunks: ClientReadableStream<DataChunk>) {
    const stream = (async function* (stream) {
      for await (const c of stream) {
        const chunk: DataChunk = c
        yield chunk.data
      }
    })(chunks)
    const item = await this.convertor.fromStream(stream)
    if (this.resolveNext) {
      this.resolveNext(item)
      this.resolveNext = null
    } else {
      this.queue.push(item)
    }
  }

  async *[Symbol.asyncIterator]() {
    while (true) {
      if (this.queue.length > 0) {
        const item = this.queue.shift()!
        if (item === undefined) break
        yield item
      } else {
        const item = await new Promise<T | undefined>(
          (resolve) => (this.resolveNext = resolve),
        )
        if (item === undefined) break
        yield item
      }
    }
  }
}

class MyOnce<T> {
  private convertor: Convertor<T>
  private resolveNext: (value: T | undefined) => void
  readonly promise: Promise<T | undefined>

  constructor(convertor: Convertor<T>) {
    this.convertor = convertor
    this.promise = new Promise((res) => (this.resolveNext = res))
  }

  push(buffer: Uint8Array) {
    const item = this.convertor.from(buffer)
    this.resolveNext(item)
  }

  close() {
    this.resolveNext(undefined)
  }

  async pushStream(chunks: ClientReadableStream<DataChunk>) {
    const stream = (async function* (stream) {
      for await (const c of stream) {
        const chunk: DataChunk = c
        yield chunk.data
      }
    })(chunks)
    const item = await this.convertor.fromStream(stream)
    this.resolveNext(item)
  }
}

export class ReaderInstance implements Reader {
  private client: RunnerClient
  private uri: string
  private logger: winston.Logger

  private iterators: MyIter<unknown>[] = []
  private onces: MyOnce<unknown>[] = []

  constructor(uri: string, client: RunnerClient, logger: winston.Logger) {
    this.uri = uri
    this.client = client
    this.logger = logger
  }

  string(): Promise<string | undefined> {
    const once = new MyOnce(StringConvertor)
    this.onces.push(once)
    return once.promise
  }

  strings(): AsyncIterable<string> {
    const iter = new MyIter(StringConvertor)
    this.iterators.push(iter)
    return iter
  }

  buffer(): Promise<Uint8Array | undefined> {
    const once = new MyOnce(NoConvertor)
    this.onces.push(once)
    return once.promise
  }
  buffers(): AsyncIterable<Uint8Array> {
    const iter = new MyIter(NoConvertor)
    this.iterators.push(iter)
    return iter
  }

  stream(): Promise<AsyncGenerator<Uint8Array> | undefined> {
    const once = new MyOnce(StreamConvertor)
    this.onces.push(once)
    return once.promise
  }
  streams(): AsyncIterable<AsyncGenerator<Uint8Array>> {
    const iter = new MyIter(StreamConvertor)
    this.iterators.push(iter)
    return iter
  }

  handleMsg(msg: Message) {
    this.logger.debug(`${this.uri} handling message`)
    for (const iter of this.iterators) {
      iter.push(msg.data)
    }

    let x = this.onces.pop()
    while (x) {
      x.push(msg.data)
      x = this.onces.pop()
    }
  }

  close() {
    for (const iter of this.iterators) {
      iter.close()
    }

    let x = this.onces.pop()
    while (x) {
      x.close()
      x = this.onces.pop()
    }
  }

  handleStreamingMessage(msg: StreamMessage) {
    this.logger.debug(`${this.uri} handling streaming message`)
    const chunks = this.client.receiveStreamMessage(msg.id!)
    for (const iter of this.iterators) {
      iter.pushStream(chunks)
    }

    let x = this.onces.pop()
    while (x) {
      x.pushStream(chunks)
      x = this.onces.pop()
    }
  }
}
