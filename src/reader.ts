import { ClientReadableStream } from '@grpc/grpc-js'
import { DataChunk, Message, RunnerClient, StreamMessage } from '@rdfc/proto'
import winston from 'winston'
import {
  AnyConvertor,
  Convertor,
  NoConvertor,
  StreamConvertor,
  StringConvertor,
} from './convertor'
import { Writable } from './runner'

export type Any =
  | {
      string: string
    }
  | {
      stream: AsyncGenerator<Uint8Array>
    }
  | {
      buffer: Uint8Array
    }

export interface Reader {
  readonly uri: string
  strings(): AsyncIterable<string>
  streams(): AsyncIterable<AsyncGenerator<Uint8Array>>
  buffers(): AsyncIterable<Uint8Array>
  anys(): AsyncIterable<Any>
}

type Todo<T> = {
  item: T
  onComplete: () => void
}

class MyIter<T> implements AsyncIterable<T> {
  private convertor: Convertor<T>
  private queue: Todo<T | undefined>[] = []
  private resolveNext: ((value: undefined) => void) | null = null

  constructor(convertor: Convertor<T>) {
    this.convertor = convertor
  }

  push(buffer: Uint8Array, onComplete: () => void) {
    const item = this.convertor.from(buffer)
    this.queue.push({ item, onComplete })
    if (this.resolveNext) {
      this.resolveNext(undefined)
      this.resolveNext = null
    }
  }

  close(onComplete: () => void) {
    this.queue.push({ item: undefined, onComplete })
    if (this.resolveNext) {
      this.resolveNext(undefined)
      this.resolveNext = null
    }
  }

  async pushStream(
    chunks: ClientReadableStream<DataChunk>,
    onComplete: () => void,
  ) {
    // This is an asyhc generator that
    const stream = (async function* (stream) {
      for await (const c of stream) {
        const chunk: DataChunk = c
        console.log('Got chunk ', chunk)
        yield chunk.data
      }
    })(chunks)

    const item = await this.convertor.fromStream(stream)
    this.queue.push({ item, onComplete })
    if (this.resolveNext) {
      this.resolveNext(undefined)
      this.resolveNext = null
    }
  }

  async *[Symbol.asyncIterator]() {
    while (true) {
      if (this.queue.length > 0) {
        const { item, onComplete } = this.queue.shift()!
        if (item === undefined) {
          onComplete()
          break
        }
        yield item
        // The generator only returns execution on the next `next` call on the generator
        onComplete()
      } else {
        await new Promise<undefined>((resolve) => (this.resolveNext = resolve))
      }
    }
  }
}

export class ReaderInstance implements Reader {
  private client: RunnerClient
  readonly uri: string
  private logger: winston.Logger
  private readonly write: Writable

  private iterators: MyIter<unknown>[] = []

  constructor(
    uri: string,
    client: RunnerClient,
    write: Writable,
    logger: winston.Logger,
  ) {
    this.uri = uri
    this.client = client
    this.logger = logger
    this.write = write
  }

  anys(): AsyncIterable<Any> {
    const iter = new MyIter(AnyConvertor)
    this.iterators.push(iter)
    return iter
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

    const promises = []
    for (const iter of this.iterators) {
      promises.push(new Promise((res) => iter.push(msg.data, () => res(null))))
    }

    Promise.all(promises).then(() =>
      this.write({ processed: { tick: msg.tick, uri: this.uri } }),
    )
  }

  close() {
    for (const iter of this.iterators) {
      iter.close(() => {})
    }
  }

  handleStreamingMessage(msg: StreamMessage) {
    this.logger.debug(`${this.uri} handling streaming message`)

    const chunks = this.client.receiveStreamMessage({ id: msg.id })
    const promises = []

    for (const iter of this.iterators) {
      promises.push(
        new Promise((res) => iter.pushStream(chunks, () => res(null))),
      )
    }

    Promise.all(promises).then(() =>
      this.write({ processed: { tick: msg.tick, uri: this.uri } }),
    )
  }
}
