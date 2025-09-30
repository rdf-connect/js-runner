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
import { promisify } from 'util'

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

  async pushStream(chunks: AsyncIterable<DataChunk>, onComplete: () => void) {
    // This is an async generator that transforms DataChunks to Buffers
    const stream = (async function*(stream) {
      for await (const c of stream) {
        const chunk: DataChunk = c
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
        // Note: execution pauses at `yield` until the consumer calls `.next()` again.
        // We call onComplete *after* resuming, so the producer knows the item was actually consumed.
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
  private readonly notifyOrchestrator: Writable

  private consumers: MyIter<unknown>[] = []

  constructor(
    uri: string,
    client: RunnerClient,
    notifyOrchestrator: Writable,
    logger: winston.Logger,
  ) {
    this.uri = uri
    this.client = client
    this.logger = logger
    this.notifyOrchestrator = notifyOrchestrator
  }

  anys(): AsyncIterable<Any> {
    const iter = new MyIter(AnyConvertor)
    this.consumers.push(iter)
    return iter
  }

  strings(): AsyncIterable<string> {
    const iter = new MyIter(StringConvertor)
    this.consumers.push(iter)
    return iter
  }

  buffers(): AsyncIterable<Uint8Array> {
    const iter = new MyIter(NoConvertor)
    this.consumers.push(iter)
    return iter
  }

  streams(): AsyncIterable<AsyncGenerator<Uint8Array>> {
    const iter = new MyIter(StreamConvertor)
    this.consumers.push(iter)
    return iter
  }

  handleMsg(msg: Message) {
    this.logger.debug(`${this.uri} handling message`)

    const promises = []
    for (const iter of this.consumers) {
      promises.push(new Promise((res) => iter.push(msg.data, () => res(null))))
    }

    Promise.all(promises).then(() =>
      this.notifyOrchestrator({ processed: { tick: msg.tick, channel: this.uri } }),
    )
  }

  close() {
    for (const iter of this.consumers) {
      iter.close(() => { })
    }
  }

  // There is a stream message available for this reader
  async handleStreamingMessage({ id, tick }: StreamMessage) {
    this.logger.debug(`${this.uri} handling streaming message`)

    const chunks = this.client.receiveStreamMessage()
    const write = promisify(chunks.write.bind(chunks))
    const consumersConsumed = []

    // After each chunk is handled by all consumer, emit a processed message
    let idx = 0
    const messageIterators = fanoutStream(
      chunks,
      this.consumers.length,
      async () => {
        await write({ processed: idx++ })
      },
    )

    for (const consumer of this.consumers) {
      consumersConsumed.push(
        new Promise((res) =>
          consumer.pushStream(messageIterators.pop()!, () => res(null)),
        ),
      )
    }

    await write({ id });

    Promise.all(consumersConsumed).then(() => {
      console.log("Writing processed for streaming message");
      this.notifyOrchestrator({ processed: { tick: tick, channel: this.uri } });
    }
    )
  }
}

/**
 * Helper function to tee a stream `numConsumers` times
 * When each tee'd stream has handled a chunk, call {@link onAllHandled}
 */
function fanoutStream<T>(
  stream: ClientReadableStream<T>,
  numConsumers: number,
  onAllHandled: () => void | Promise<void>,
): AsyncIterable<T>[] {
  type Waiter = (value: IteratorResult<T>) => void

  let ended = false
  const buffer: T[] = []
  const pending: Waiter[] = []
  let activeConsumers = numConsumers

  // consumer bookkeeping
  let awaitingAck = 0

  function pushChunk(chunk: T) {
    buffer.push(chunk)
    flush()
  }

  function flush() {
    while (buffer.length > 0 && pending.length > 0) {
      const chunk = buffer[0] // keep until all consumers ack
      const waiter = pending.shift()!
      waiter({ value: chunk, done: false })
      awaitingAck++
    }
  }

  function end() {
    ended = true
    while (pending.length > 0) {
      const waiter = pending.shift()!
      waiter({ value: undefined, done: true })
    }
  }

  stream.on('data', (chunk: T) => {
    pushChunk(chunk)
  })

  stream.on('end', () => {
    end()
  })

  stream.on('error', (err) => {
    while (pending.length > 0) {
      const waiter = pending.shift()!
      waiter({ value: undefined, done: true })
    }
    throw err
  })

  function makeIterable(): AsyncIterable<T> {
    return {
      [Symbol.asyncIterator]() {
        return {
          next(): Promise<IteratorResult<T>> {
            if (buffer.length > 0) {
              const chunk = buffer[0]
              awaitingAck++
              return Promise.resolve({ value: chunk, done: false })
            }
            if (ended) {
              return Promise.resolve({ value: undefined, done: true })
            }
            return new Promise((resolve) => {
              pending.push(resolve)
            })
          },
          async return() {
            activeConsumers--
            if (activeConsumers === 0) {
              end()
            }
            return { value: undefined, done: true }
          },
        }
      },
    }
  }

  async function ack() {
    awaitingAck--
    if (awaitingAck === 0) {
      // all consumers done with the current chunk
      buffer.shift() // drop it
      await onAllHandled()
      flush() // continue with next chunk
    }
  }

  // wrap consumer so they *must* call ack() after processing
  function wrap(iterable: AsyncIterable<T>): AsyncIterable<T> {
    return {
      async *[Symbol.asyncIterator]() {
        for await (const item of iterable) {
          yield item
          await ack()
        }
      },
    }
  }

  const rawIterables = Array.from({ length: numConsumers }, makeIterable)
  return rawIterables.map(wrap)
}
