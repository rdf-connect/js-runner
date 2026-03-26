import { FromRunner, RunnerClient } from '@rdfc/proto'
import { promisify } from 'util'
import { Logger } from 'winston'
import { Any } from './reader'

type Writable = (msg: FromRunner) => Promise<unknown>
export type Handler<T = void> = [T] extends [void]
  ? () => void | Promise<void>
  : (value: T) => void | Promise<void>

export interface Writer {
  readonly uri: string
  readonly canceled: boolean
  on(event: 'cancel', listener: Handler): this
  buffer(buffer: Uint8Array): Promise<void>

  stream(buffer: AsyncIterable<Uint8Array>): Promise<void>
  stream<T>(
    buffer: AsyncIterable<T>,
    transform: (x: T) => Uint8Array,
  ): Promise<void>

  string(buffer: string): Promise<void>
  any(any: Any): Promise<void>
  close(): Promise<void>
}
const encoder = new TextEncoder()
export class WriterInstance implements Writer {
  readonly uri: string
  localSequenceNumber: number = 1
  private readonly client: RunnerClient
  private readonly notifyOrchestrator: Writable
  private readonly logger: Logger

  // FIFO of message-level acknowledgements coming back from the orchestrator.
  private awaitingProcessed: Array<{
    resolve: () => void
    reject: (reason: Error) => void
  }> = []

  private openStreams: number = 0
  // Close callers wait here while active streams are still flushing.
  private shouldClose: Array<() => void> = []
  private closed = false
  private _canceled = false

  // Shared cancellation signal to abort in-flight waits when the remote closes.
  private readonly cancelSignal = new AbortController()
  // Processors can subscribe here to stop upstream work when downstream cancels.
  private readonly cancelHandlers = new Set<Handler>()

  private readonly runnerId: string

  constructor(
    uri: string,
    client: RunnerClient,
    notifyOrchestrator: Writable,
    runnerId: string,
    logger: Logger,
  ) {
    this.client = client
    this.notifyOrchestrator = notifyOrchestrator
    this.uri = uri
    this.logger = logger
    this.runnerId = runnerId
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

  private emitCancel() {
    for (const handler of this.cancelHandlers) {
      try {
        Promise.resolve(handler()).catch((error: unknown) => {
          this.logger.error(
            `Cancel listener for channel ${this.uri} failed: ${String(error)}`,
          )
        })
      } catch (error: unknown) {
        this.logger.error(
          `Cancel listener for channel ${this.uri} failed: ${String(error)}`,
        )
      }
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

  private async raceWithCancellation<T>(promise: Promise<T>): Promise<T> {
    this.assertCanWrite()

    return await new Promise<T>((resolve, reject) => {
      const onAbort = () => reject(this.cancellationError())
      this.cancelSignal.signal.addEventListener('abort', onAbort, {
        once: true,
      })

      promise.then(
        (value) => {
          // Clean up the abort listener if the original operation finished first.
          this.cancelSignal.signal.removeEventListener('abort', onAbort)
          resolve(value)
        },
        (error) => {
          this.cancelSignal.signal.removeEventListener('abort', onAbort)
          reject(error)
        },
      )
    })
  }

  private rejectPendingProcessed(error: Error) {
    // Reject all queued message waits so callers do not hang during cancellation.
    while (this.awaitingProcessed.length > 0) {
      this.awaitingProcessed.shift()!.reject(error)
    }
  }

  private awaitProcessed(): Promise<void> {
    this.assertCanWrite()
    return new Promise((resolve, reject) =>
      this.awaitingProcessed.push({ resolve, reject }),
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

  async buffer(buffer: Uint8Array): Promise<void> {
    this.assertCanWrite()
    this.logger.debug(`${this.uri} sends buffer ${buffer.length} bytes`)
    const localSequenceNumber = this.localSequenceNumber++
    const handledPromise = this.awaitProcessed()

    await this.notifyOrchestrator({
      msg: { data: buffer, channel: this.uri, localSequenceNumber },
    })
    await handledPromise
  }

  async stream<T = Uint8Array>(
    buffer: AsyncIterable<T>,
    transform?: (x: T) => Uint8Array,
  ) {
    this.assertCanWrite()
    this.openStreams += 1
    const t = transform || ((x: unknown) => <Uint8Array>x)
    const stream = this.client.sendStreamMessage()

    try {
      // Message-level ack that signals the whole stream message is fully handled.
      const handledPromise = this.awaitProcessed()
      const writeStreamMessageChunk = promisify(stream.write.bind(stream))
      const localSequenceNumber = this.localSequenceNumber++
      await writeStreamMessageChunk({
        id: {
          channel: this.uri,
          localSequenceNumber,
          runner: this.runnerId,
        },
      })

      // First response confirms stream id registration on the remote side.
      const id = await this.raceWithCancellation(
        new Promise((res) => stream.once('data', res)),
      )

      this.logger.debug(
        `${this.uri} streams message with id ${JSON.stringify(id)}`,
      )

      for await (const msg of buffer) {
        const processedPromise = new Promise((res) => stream.once('data', res))
        await writeStreamMessageChunk({ data: { data: t(msg) } })
        // Await a message on the stream, indicating that the chunk has been processed
        await processedPromise
      }

      stream.end()

      await handledPromise
    } finally {
      this.openStreams -= 1

      if (!stream.writableEnded) {
        stream.end()
      }

      // If a close call was deferred while streaming, complete it now.
      if (this.shouldClose.length > 0) {
        await this.close()
      }
    }
  }

  async string(msg: string): Promise<void> {
    this.assertCanWrite()
    this.logger.debug(`${this.uri} sends string ${msg.length} characters`)
    const localSequenceNumber = this.localSequenceNumber++
    const handledPromise = this.awaitProcessed()

    await this.notifyOrchestrator({
      msg: {
        data: encoder.encode(msg),
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
   * - If this side initiated the close (`issued = false`), a close message is sent to the remote.
   *
   * @param issued - If true, indicates the close request originated remotely
   */
  async close(issued = false): Promise<void> {
    if (issued) {
      if (!this.closed) {
        // Remote initiated close: mark writer canceled to fail future writes.
        this._canceled = true

        // Notify processors so they can stop producing upstream work as well.
        this.emitCancel()
      }
      this.closed = true

      const cancelError = this.cancellationError()
      this.rejectPendingProcessed(cancelError)
      this.cancelSignal.abort(cancelError)

      // Unblock any local close() callers waiting for streams to settle.
      let waiting = this.shouldClose.pop()
      while (waiting) {
        waiting()
        waiting = this.shouldClose.pop()
      }
      return
    }

    if (this.closed) {
      return
    }

    // Case 1: Active streams still running → wait until they finish
    if (this.openStreams !== 0) {
      await new Promise<void>((resolve) => this.shouldClose.push(resolve))
      return
    }

    // Case 2: No active streams → perform actual close
    this.logger.debug(`${this.uri} closes stream`)
    this.closed = true
    await this.notifyOrchestrator({
      close: { channel: this.uri },
    })

    let resolve = this.shouldClose.pop()
    while (resolve) {
      resolve()
      resolve = this.shouldClose.pop()
    }
  }

  /**
   * A message is handled, let's notify the fifo {@link awaitProcessed}
   */
  handled(): void {
    if (this.awaitingProcessed.length > 0) {
      this.awaitingProcessed.shift()!.resolve()
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
}
