import { FromRunner, RunnerClient } from '@rdfc/proto'
import { promisify } from 'util'
import { Logger } from 'winston'
import { Any } from './reader'

type Writable = (msg: FromRunner) => Promise<unknown>
export interface Writer {
  readonly uri: string
  buffer(buffer: Uint8Array): Promise<void>

  stream(buffer: AsyncIterable<Uint8Array>): Promise<void>
  stream<T>(
    buffer: AsyncIterable<T>,
    tranform: (x: T) => Uint8Array,
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

  private awaitingProcessed: Array<() => void> = []

  private openStreams: number = 0
  private shouldClose: Array<() => void> = []

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

  private awaitProcessed(): Promise<void> {
    return new Promise((res) => this.awaitingProcessed.push(res))
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
    this.openStreams += 1
    const t = transform || ((x: unknown) => <Uint8Array>x)
    const stream = this.client.sendStreamMessage()

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

    const id = await new Promise((res) => stream.once('data', res))

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

    this.openStreams -= 1

    if (this.shouldClose.length > 0) await this.close()
  }

  async string(msg: string): Promise<void> {
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
    // Case 1: Active streams still running → wait until they finish
    if (this.openStreams !== 0) {
      await new Promise<void>((resolve) => this.shouldClose.push(resolve))
      return
    }

    // Case 2: No active streams → perform actual close
    this.logger.debug(`${this.uri} closes stream`)
    if (!issued) {
      await this.notifyOrchestrator({
        close: { channel: this.uri },
      })
    }

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
      this.awaitingProcessed.shift()!()
    } else {
      this.logger.error(
        'Expected to be waiting for a message to be processed, but this is not the case ' +
        this.uri,
      )
    }
  }
}
