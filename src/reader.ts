import { ClientReadableStream } from '@grpc/grpc-js'
import {
  DataChunk,
  ReceivingMessage,
  ReceivingStreamMessage,
  RunnerClient,
} from '@rdfc/proto'
import { Logger } from 'winston'
import {
  AnyConvertor,
  Convertor,
  NoConvertor,
  StreamConvertor,
  StringConvertor,
} from './convertor.js'
import { Writable } from './runner.js'
import { promisify } from 'util'
import { ChannelTracker } from './state.js'

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
  cancel(): Promise<void>
}

type Todo<T> = {
  item: T
  onComplete: () => void
}

class MyIter<T> implements AsyncIterable<T> {
  private convertor: Convertor<T>
  private queue: Todo<T | undefined>[] = []
  private resolveNext: ((value: undefined) => void) | null = null
  private closed = false

  constructor(convertor: Convertor<T>) {
    this.convertor = convertor
  }

  push(buffer: Uint8Array, onComplete: () => void) {
    if (this.closed) {
      onComplete()
      return
    }

    const item = this.convertor.from(buffer)
    this.queue.push({ item, onComplete })
    if (this.resolveNext) {
      this.resolveNext(undefined)
      this.resolveNext = null
    }
  }

  close(onComplete: () => void) {
    if (this.closed) {
      onComplete()
      return
    }

    this.closed = true
    this.queue.push({ item: undefined, onComplete })
    if (this.resolveNext) {
      this.resolveNext(undefined)
      this.resolveNext = null
    }
  }

  async pushStream(chunks: AsyncIterable<DataChunk>, onComplete: () => void) {
    if (this.closed) {
      onComplete()
      return
    }

    // This is an async generator that transforms DataChunks to Buffers
    const stream = (async function* (stream) {
      for await (const chunk of stream) {
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

  async *[Symbol.asyncIterator](): AsyncGenerator<T> {
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
  private logger: Logger
  private readonly notifyOrchestrator: Writable
  private readonly tracker: ChannelTracker | undefined

  private consumers: MyIter<unknown>[] = []
  private closed = false
  private canceled = false

  constructor(
    uri: string,
    client: RunnerClient,
    notifyOrchestrator: Writable,
    logger: Logger,
    tracker?: ChannelTracker,
  ) {
    this.uri = uri
    this.client = client
    this.logger = logger
    this.notifyOrchestrator = notifyOrchestrator
    this.tracker = tracker
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

  handleMsg(msg: ReceivingMessage) {
    this.logger.debug(`${this.uri} handling message`)
    this.tracker?.recordMessage(msg.data.length)

    if (this.closed) {
      this.notifyOrchestrator({
        processed: {
          globalSequenceNumber: msg.globalSequenceNumber,
          channel: this.uri,
          error:
            'reader is canceled; message has not been processed by the processor',
        },
      })
      return
    }

    const promises = []
    for (const iter of this.consumers) {
      promises.push(new Promise((res) => iter.push(msg.data, () => res(null))))
    }

    Promise.all(promises).then(() =>
      this.notifyOrchestrator({
        processed: {
          globalSequenceNumber: msg.globalSequenceNumber,
          channel: this.uri,
        },
      }),
    )
  }

  close() {
    if (this.closed) {
      return
    }

    this.closed = true
    for (const iter of this.consumers) {
      iter.close(() => {})
    }
  }

  async cancel(): Promise<void> {
    if (this.canceled) {
      return
    }

    this.canceled = true
    this.close()
    await this.notifyOrchestrator({
      close: { channel: this.uri },
    })
  }

  // There is a stream message available for this reader
  async handleStreamingMessage({
    channel,
    globalSequenceNumber,
  }: ReceivingStreamMessage) {
    this.logger.debug(`${this.uri} handling streaming message`)
    this.tracker?.recordMessage(0)

    if (this.closed) {
      await this.notifyOrchestrator({
        processed: { globalSequenceNumber, channel },
      })
      return
    }

    const chunks = this.client.receiveStreamMessage()

    // Exactly one `processed` reply may go out per message — the orchestrator
    // takes it as the final word on this sequence number. Both the failure path
    // (the stream's 'error' handler) and the success path (the Promise.all
    // below) can fire, in either order: a connection can die before the
    // consumers finish, but grpc-js can just as well report the RPC as failed
    // after they already did. Whichever gets here first owns the ack.
    let processedSent = false
    const sendProcessed = (error?: string) => {
      if (processedSent) return
      processedSent = true
      Promise.resolve(
        this.notifyOrchestrator({
          processed: error
            ? { globalSequenceNumber, channel, error }
            : { globalSequenceNumber, channel },
        }),
      ).catch((err) => {
        // The connection that just died is the one we'd report over.
        this.logger.debug(
          `${this.uri} could not report message ${globalSequenceNumber}: ${err}`,
        )
      })
    }

    // fanoutStream() forwards the failure to the consumers; this side reports it
    // back to the orchestrator, which would otherwise wait forever for the
    // `processed` reply that the Promise.all below can no longer send.
    chunks.on('error', (err: Error) => {
      if (processedSent) {
        // The message was already acked as processed; this is the RPC itself
        // failing afterwards (e.g. UNAVAILABLE because the status never
        // arrived). Retracting a successful ack would be a lie.
        this.logger.debug(
          `${this.uri} stream ${globalSequenceNumber} errored after the message was processed: ${err.message}`,
        )
        return
      }
      this.logger.error(
        `${this.uri} stream message ${globalSequenceNumber} dropped: ${err.message}`,
      )
      sendProcessed(err.message)
    })

    const writeControlMessage = promisify(chunks.write.bind(chunks))
    const consumersConsumed = []

    // After each chunk is handled by all consumer, emit a processed message
    let idx = 0
    const messageIterators = fanoutStream(
      chunks,
      this.consumers.length,
      async () => {
        await writeControlMessage({ streamSequenceNumber: idx++ })
      },
    )

    for (const consumer of this.consumers) {
      const messageIterator = messageIterators.pop()!
      consumersConsumed.push(
        new Promise((res) =>
          // pushStream() is async: buffering convertors (strings/buffers/...)
          // drain the iterator inside it, so a dropped stream rejects here
          // rather than in the consumer. Settle either way — leaving this
          // pending would strand the Promise.all below, and leaving it
          // unhandled would take the process down.
          consumer
            .pushStream(messageIterator, () => res(null))
            .catch((err) => {
              this.logger.debug(
                `${this.uri} consumer did not receive stream message ${globalSequenceNumber}: ${err}`,
              )
              res(null)
            }),
        ),
      )
    }

    await writeControlMessage({ globalSequenceNumber })

    Promise.all(consumersConsumed).then(() => {
      // The 'error' handler above already reported this message as failed; a
      // second `processed` for the same sequence number would double-ack it,
      // and the stream it ended on is already destroyed.
      if (processedSent) {
        return
      }
      chunks.end()
      sendProcessed()
    })
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
  type Waiter = {
    resolve: (value: IteratorResult<T>) => void
    reject: (reason: Error) => void
  }

  let ended = false
  let failure: Error | undefined
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
      waiter.resolve({ value: chunk, done: false })
      awaitingAck++
    }
  }

  function end() {
    ended = true
    while (pending.length > 0) {
      const waiter = pending.shift()!
      waiter.resolve({ value: undefined, done: true })
    }
  }

  function fail(err: Error) {
    failure = err
    ended = true
    while (pending.length > 0) {
      const waiter = pending.shift()!
      waiter.reject(err)
    }
  }

  stream.on('data', (chunk: T) => {
    pushChunk(chunk)
  })

  stream.on('end', () => {
    end()
  })

  // Rethrowing here would escape as an uncaught exception — an 'error' listener
  // runs outside any await, so nothing can catch it and the process dies. Hand
  // the failure to the consumers instead, so it surfaces as a rejection where
  // they iterate the stream.
  stream.on('error', (err: Error) => {
    fail(err)
  })

  function makeIterable(): AsyncIterable<T> {
    return {
      [Symbol.asyncIterator]() {
        return {
          next(): Promise<IteratorResult<T>> {
            if (failure) {
              return Promise.reject(failure)
            }
            if (buffer.length > 0) {
              const chunk = buffer[0]
              awaitingAck++
              return Promise.resolve({ value: chunk, done: false })
            }
            if (ended) {
              return Promise.resolve({ value: undefined, done: true })
            }
            return new Promise((resolve, reject) => {
              pending.push({ resolve, reject })
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
