import { FromRunner, RunnerClient } from '@rdfc/proto'
import { promisify } from 'util'
import { Logger } from 'winston'
import { Any } from './reader.js'
import { ChannelTracker } from './state.js'

type Writable = (msg: FromRunner) => Promise<unknown>
export type Handler<T = void> = [T] extends [void]
  ? () => void | Promise<void>
  : (value: T) => void | Promise<void>

export interface Writer {
  readonly uri: string
  readonly canceled: boolean
  on(event: 'cancel', listener: Handler): this

  /**
   * Writes a complete buffer to the channel. The Promise resolves once the message is fully processed by the remote.
   *
   * @throws Error if the channel is closed or canceled at the moment of the write operation.
   * @param buffer - The data to send as a Uint8Array
   * @returns A Promise that resolves when the message is acknowledged as processed by the remote.
   */
  buffer(buffer: Uint8Array): Promise<void>

  /**
   * Writes a stream of data to a separate stream-specific channel.
   * The Promise resolves once the entire stream is fully processed by the remote.
   *
   * @throws Error if the channel is closed or canceled at the moment of initiating a stream-specific channel.
   * @param buffer - An AsyncIterable that produces the data to send as Uint8Arrays
   * @returns A Promise that resolves when the entire stream is acknowledged as processed by the remote.
   */
  stream(buffer: AsyncIterable<Uint8Array>): Promise<void>

  /**
   * Writes a stream of data to a separate stream-specific channel.
   * The Promise resolves once the entire stream is fully processed by the remote.
   *
   * @throws Error if the channel is closed or canceled at the moment of initiating a stream-specific channel.
   * @param buffer - An AsyncIterable that produces the data to send, which will be transformed into Uint8Arrays using the provided transform function
   * @param transform - A function that transforms items from the buffer AsyncIterable into Uint8Arrays for sending. If not provided, items are assumed to already be Uint8Arrays.
   * @returns A Promise that resolves when the entire stream is acknowledged as processed by the remote.
   */
  stream<T>(
    buffer: AsyncIterable<T>,
    transform: (x: T) => Uint8Array,
  ): Promise<void>

  /**
   * Writes a string message to the channel. The Promise resolves once the message is fully processed by the remote.
   *
   * @throws Error if the channel is closed or canceled at the moment of the write operation.
   * @param buffer - The string message to send
   * @returns A Promise that resolves when the message is acknowledged as processed by the remote.
   */
  string(buffer: string): Promise<void>

  /**
   * Writes a message of any supported type (string, buffer, or stream) to the channel.
   * The Promise resolves once the message is fully processed by the remote.
   *
   * @throws Error if the channel is closed or canceled at the moment of the write operation.
   * @param any - An object containing one of the supported message types (string, buffer, or stream)
   * @returns A Promise that resolves when the message is acknowledged as processed by the remote.
   */
  any(any: Any): Promise<void>

  /**
   * Gracefully closes this channel.
   *
   * Behavior:
   * - If there are still active streams, closing is deferred until they complete.
   * - If multiple callers invoke `close()` while waiting, their Promises are queued and
   *   resolved once the channel actually closes.
   * - If this side initiated the close (`issued = false`), a close message is sent to the remote.
   *
   * @param issued - If true, indicates the close request originated remotely
   */
  close(issued?: boolean): Promise<void>
}
const encoder = new TextEncoder()

type AwaitingProcessed = {
  resolve: () => void
  reject: (reason: Error) => void
  startMs: number
  bytes: number
}

export class WriterInstance implements Writer {
  readonly uri: string
  localSequenceNumber: number = 1
  private readonly client: RunnerClient
  private readonly notifyOrchestrator: Writable
  private readonly logger: Logger

  // FIFO of message-level acknowledgements coming back from the orchestrator.
  private awaitingProcessed: Array<AwaitingProcessed> = []

  private openStreams: number = 0
  // Close callers wait here while active streams are still flushing.
  private shouldClose: Array<{
    resolve: () => void
    reject: (reason: Error) => void
  }> = []
  private closed = false
  // Set once the actual close (notify + resolve queued callers) has run, to
  // make the recursive close() call from stream()'s finally block (and any
  // redundant external close() calls) idempotent without delaying `closed`
  // itself — see close() for why these can't be the same flag.
  private closeFinalized = false
  private _canceled = false
  private remoteCloseReceived = false

  // Processors can subscribe here to stop upstream work when downstream cancels.
  private readonly cancelHandlers = new Set<Handler>()

  private readonly runnerId: string
  private readonly tracker: ChannelTracker | undefined

  constructor(
    uri: string,
    client: RunnerClient,
    notifyOrchestrator: Writable,
    runnerId: string,
    logger: Logger,
    tracker?: ChannelTracker,
  ) {
    this.client = client
    this.notifyOrchestrator = notifyOrchestrator
    this.uri = uri
    this.logger = logger
    this.runnerId = runnerId
    this.tracker = tracker
  }

  get canceled(): boolean {
    return this._canceled
  }

  on(event: 'cancel', listener: Handler): this {
    if (event === 'cancel') {
      this.cancelHandlers.add(listener)
    }

    return this
  }

  private cancellationError(): Error {
    return new Error(
      `Writer for channel ${this.uri} was canceled by the connected reader`,
    )
  }

  async any(any: Any): Promise<void> {
    if ('stream' in any) {
      await this.stream(any.stream)
    }
    if ('buffer' in any) {
      await this.buffer(any.buffer)
    }
    if ('string' in any) {
      await this.string(any.string)
    }
  }

  private assertCanWrite() {
    if (this._canceled) {
      throw this.cancellationError()
    }

    if (this.closed) {
      throw new Error(`Writer for channel ${this.uri} is closed`)
    }
  }

  async buffer(buffer: Uint8Array): Promise<void> {
    this.assertCanWrite()
    this.logger.debug(`${this.uri} sends buffer ${buffer.length} bytes`)
    const localSequenceNumber = this.localSequenceNumber++
    const handledPromise = this.awaitProcessed(buffer.length)

    await this.notifyOrchestrator({
      msg: { data: buffer, channel: this.uri, localSequenceNumber },
    })
    await handledPromise
  }

  async stream<T = Uint8Array>(
    buffer: AsyncIterable<T>,
    transform?: (x: T) => Uint8Array,
  ): Promise<void> {
    this.assertCanWrite()
    this.openStreams += 1
    const t = transform || ((x: unknown) => <Uint8Array>x)
    const stream = this.client.sendStreamMessage()

    // grpc-js emits 'error' on this stream (e.g. "Connection dropped" when the
    // orchestrator disconnects mid-stream). Without a listener, Node treats an
    // unhandled 'error' event as fatal and crashes the process. Registering a
    // listener here turns it into a normal rejection that `errorPromise` below
    // surfaces at any point we're awaiting the stream.
    let streamError: Error | undefined
    const errorPromise = new Promise<never>((_, reject) => {
      stream.on('error', (err: Error) => {
        streamError = err
        reject(err)
      })
    })
    // Avoid an "unhandled rejection" warning if the error fires after we stop
    // racing against it (e.g. once we've already moved past the await points).
    errorPromise.catch(() => {})

    const nextData = (): Promise<unknown> =>
      Promise.race([
        new Promise((res) => stream.once('data', res)),
        errorPromise,
      ])

    // Message-level ack that signals the whole stream message is fully handled.
    // Queued before the first await so that every exit path below — including
    // the error races — can find it again and discard it.
    const { promise: handledPromise, entry: handledEntry } =
      this.enqueueProcessed(0) // bytes unknown for streams
    // The finally block below may reject this entry after we've stopped
    // awaiting it; keep that from surfacing as an unhandled rejection.
    handledPromise.catch(() => {})

    try {
      const writeStreamMessageChunk = promisify(stream.write.bind(stream))
      const localSequenceNumber = this.localSequenceNumber++
      await Promise.race([
        writeStreamMessageChunk({
          id: {
            channel: this.uri,
            localSequenceNumber,
            runner: this.runnerId,
          },
        }),
        errorPromise,
      ])

      // First response confirms stream id registration on the remote side.
      const id = await nextData()

      this.logger.debug(
        `${this.uri} streams message with id ${JSON.stringify(id)}`,
      )

      // TODO: don't await to allow consuming processors to read and handle in parallel.
      for await (const msg of buffer) {
        const processedPromise = nextData()
        // The chunk write below may lose its own race against errorPromise, in
        // which case we leave the loop without ever awaiting this ack. Keep
        // that from surfacing as an unhandled rejection.
        processedPromise.catch(() => {})
        await Promise.race([
          writeStreamMessageChunk({ data: { data: t(msg) } }),
          errorPromise,
        ])
        // Await a message on the stream, indicating that the chunk has been processed
        await processedPromise
      }

      stream.end()

      await Promise.race([handledPromise, errorPromise])
    } finally {
      this.openStreams -= 1

      if (streamError) {
        this.logger.debug(
          `${this.uri} stream ended with error: ${streamError.message}`,
        )
      }

      if (!stream.writableEnded) {
        stream.end()
      }

      // No-op on the happy path (handled() already dequeued it); on any
      // abnormal exit this is what keeps the ack FIFO in step with the writes.
      this.discardProcessed(
        handledEntry,
        streamError ??
          new Error(`Stream message on channel ${this.uri} did not complete`),
      )

      // If a close call was deferred while streaming, complete it now. Its
      // failure belongs to the close() callers — drainShouldClose() has already
      // handed it to them — so don't let it replace this stream's own result.
      if (this.shouldClose.length > 0) {
        try {
          await this.close()
        } catch (error: unknown) {
          this.logger.debug(
            `${this.uri} deferred close failed: ${String(error)}`,
          )
        }
      }
    }
  }

  async string(msg: string): Promise<void> {
    this.assertCanWrite()
    this.logger.debug(`${this.uri} sends string ${msg.length} characters`)
    const localSequenceNumber = this.localSequenceNumber++
    const encoded = encoder.encode(msg)
    const handledPromise = this.awaitProcessed(encoded.length)

    await this.notifyOrchestrator({
      msg: {
        data: encoded,
        channel: this.uri,
        localSequenceNumber,
      },
    })

    await handledPromise
  }

  /**
   * Gracefully closes this channel.
   *
   * Behavior:
   * - If there are still active streams, closing is deferred until they complete.
   * - If multiple callers invoke `close()` while waiting, their Promises are queued and
   *   resolved once the channel actually closes.
   * - A close message is sent to the remote only if the close was locally initiated and
   *   the remote has not already sent a close.
   * - A remote-initiated close (`issued = true`) also cancels the writer so future writes
   *   fail and subscribed processors are notified to stop producing upstream work.
   *
   * @param issued - If true, indicates the close request originated remotely
   */
  async close(issued = false): Promise<void> {
    if (issued) {
      this.remoteCloseReceived = true

      if (!this._canceled) {
        // Remote initiated close: mark writer canceled to fail future writes and
        // notify processors so they can stop producing upstream work as well.
        this._canceled = true
        await this.emitCancel()
      }
    }

    // Reject any further writes from this point on, even while we wait below
    // for in-flight streams to finish — only writes already accepted before
    // this call (assertCanWrite() already passed) may keep flowing.
    this.closed = true

    // Case 1: Active streams still running → defer until they finish
    if (this.openStreams !== 0) {
      await new Promise<void>((resolve, reject) =>
        this.shouldClose.push({ resolve, reject }),
      )
      return
    }

    // Case 2: Actual close already ran (e.g. this is the recursive call from
    // stream()'s finally, or a redundant external close()) — nothing to do
    if (this.closeFinalized) return
    this.closeFinalized = true

    // Case 3: No active streams → perform actual close
    this.logger.debug(`${this.uri} closes stream`)
    let closeError: Error | undefined
    try {
      if (!this.remoteCloseReceived) {
        await this.notifyOrchestrator({
          close: { channel: this.uri },
        })
      }
    } catch (error: unknown) {
      closeError = error instanceof Error ? error : new Error(String(error))
      throw error
    } finally {
      // Always in a finally: a notify that fails on a dead connection would
      // otherwise leave every deferred close() caller parked forever.
      this.drainShouldClose(closeError)
    }
  }

  /**
   * A message is handled, let's notify the fifo {@link awaitProcessed}
   */
  handled(error?: string): void {
    if (this.awaitingProcessed.length > 0) {
      const { resolve, reject, startMs, bytes } =
        this.awaitingProcessed.shift()!
      if (error) {
        reject(new Error(error))
      } else {
        const latencyMs = Date.now() - startMs
        this.tracker?.recordMessage(bytes, latencyMs)
        resolve()
      }
    } else if (this.closed || this._canceled) {
      // A late ack can arrive after a close/cancel race; nothing to resolve anymore.
      return
    } else {
      this.logger.error(
        'Expected to be waiting for a message to be processed, but this is not the case ' +
          this.uri,
      )
    }
  }

  private async emitCancel() {
    await Promise.all(
      Array.from(this.cancelHandlers).map(async (handler) => {
        try {
          await handler()
        } catch (error: unknown) {
          this.logger.error(
            `Cancel listener for channel ${this.uri} failed: ${String(error)}`,
          )
        }
      }),
    )
  }

  private awaitProcessed(bytes: number): Promise<void> {
    return this.enqueueProcessed(bytes).promise
  }

  /**
   * Same as {@link awaitProcessed}, but also hands back the queued entry so the
   * caller can {@link discardProcessed} it if the write never reaches the point
   * where an ack could arrive.
   */
  private enqueueProcessed(bytes: number): {
    promise: Promise<void>
    entry: AwaitingProcessed
  } {
    const startMs = Date.now()
    let entry!: AwaitingProcessed
    const promise = new Promise<void>((resolve, reject) => {
      entry = { resolve, reject, startMs, bytes }
      this.awaitingProcessed.push(entry)
    })
    return { promise, entry }
  }

  /**
   * Removes an entry that will never be acked from the FIFO.
   *
   * `handled()` matches acks to writes positionally, so an entry left behind by
   * an aborted write silently steals the ack of the *next* write — which then
   * waits forever. No-op once the entry has already been acked.
   */
  private discardProcessed(entry: AwaitingProcessed, reason: Error) {
    const idx = this.awaitingProcessed.indexOf(entry)
    if (idx !== -1) {
      this.awaitingProcessed.splice(idx, 1)
      entry.reject(reason)
    }
  }

  /** Wakes every caller parked in {@link shouldClose} once the close resolved. */
  private drainShouldClose(error?: Error) {
    let waiter = this.shouldClose.pop()
    while (waiter) {
      if (error) {
        waiter.reject(error)
      } else {
        waiter.resolve()
      }
      waiter = this.shouldClose.pop()
    }
  }
}
