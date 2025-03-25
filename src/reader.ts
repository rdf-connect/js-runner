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

export class ReaderInstance implements Reader {
  private client: RunnerClient
  private uri: string
  private logger: winston.Logger

  private iterators: MyIter<unknown>[] = []

  constructor(uri: string, client: RunnerClient, logger: winston.Logger) {
    this.uri = uri
    this.client = client
    this.logger = logger
  }

  strings(): AsyncIterable<string> {
    const iter = new MyIter(StringConvertor)
    this.iterators.push(iter)
    return iter
  }

  buffers(): AsyncIterable<Uint8Array> {
    const iter = new MyIter(NoConvertor)
    this.iterators.push(iter)
    return iter
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
  }

  close() {
    for (const iter of this.iterators) {
      iter.close()
    }
  }

  handleStreamingMessage(msg: StreamMessage) {
    this.logger.debug(`${this.uri} handling streaming message`)
    const chunks = this.client.receiveStreamMessage(msg.id!)
    for (const iter of this.iterators) {
      iter.pushStream(chunks)
    }
  }
}
