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

  private rejectPendingProcessed(error: Error) {
    // Reject all queued message waits so callers do not hang during cancellation.
    while (this.awaitingProcessed.length > 0) {
      this.awaitingProcessed.shift()!.reject(error)
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
  ): Promise<void> {
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
      const id = await new Promise((res) => stream.once('data', res))

      this.logger.debug(
        `${this.uri} streams message with id ${JSON.stringify(id)}`,
      )

      // TODO: don't await to allow consuming processors to read and handle in parallel.
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

  async close(issued = false): Promise<void> {
    if (issued) {
      if (!this.closed) {
        // Remote initiated close: mark writer canceled to fail future writes.
        this._canceled = true

        // Notify processors so they can stop producing upstream work as well.
        await this.emitCancel()
      }
      this.closed = true

      this.rejectPendingProcessed(this.cancellationError())

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
  handled(error?: string): void {
    if (this.awaitingProcessed.length > 0) {
      if (error) {
        this.awaitingProcessed.shift()!.reject(new Error(error))
      } else {
        this.awaitingProcessed.shift()!.resolve()
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

  private awaitProcessed(): Promise<void> {
    return new Promise((resolve, reject) =>
      this.awaitingProcessed.push({ resolve, reject }),
    )
  }
}
