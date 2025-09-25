import { Id, OrchestratorMessage, RunnerClient } from '@rdfc/proto'
import { promisify } from 'util'
import { Logger } from 'winston'
import { Any } from './reader'

type Writable = (msg: OrchestratorMessage) => Promise<unknown>
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
  tick: number = 0
  private readonly client: RunnerClient
  private readonly write: Writable
  private readonly logger: Logger

  private awaitingProcessed: Array<() => void> = []

  private openStreams: number = 0
  private shouldClose: Array<() => void> = []

  private readonly runnerId: string

  constructor(
    uri: string,
    client: RunnerClient,
    write: Writable,
    runnerId: string,
    logger: Logger,
  ) {
    this.client = client
    this.write = write
    this.uri = uri
    this.logger = logger
    this.runnerId = runnerId
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
    const t = this.tick
    this.tick += 1
    const o = new Promise((res) => this.awaitingProcessed.push(() => res(null)))
    await this.write({ msg: { data: buffer, channel: this.uri, tick: t } })

    await o
  }

  async stream<T = Uint8Array>(
    buffer: AsyncIterable<T>,
    transform?: (x: T) => Uint8Array,
  ) {
    this.openStreams += 1
    const t = transform || ((x: unknown) => <Uint8Array>x)
    const stream = this.client.sendStreamMessage()

    const write = promisify(stream.write.bind(stream))
    const tick = this.tick
    this.tick += 1
    await write({ id: { channel: this.uri, tick, runner: this.runnerId } })

    const id: Id = await new Promise((res) => stream.once('data', res))

    this.logger.debug(`${this.uri} streams message with id ${id.id}`)

    for await (const msg of buffer) {
      const processedPromise = new Promise((res) => stream.once('data', res))
      await write({ data: { data: t(msg) } })
      // Await a message on the stream, indicating that the chunk has been processed
      await processedPromise
    }

    stream.end()

    await new Promise((res) => this.awaitingProcessed.push(() => res(null)))

    this.openStreams -= 1

    if (this.shouldClose.length > 0) await this.close()
  }

  async string(msg: string): Promise<void> {
    this.logger.debug(`${this.uri} sends string ${msg.length} characters`)
    const t = this.tick
    this.tick += 1
    const o = new Promise((res) => this.awaitingProcessed.push(() => res(null)))
    await this.write({
      msg: { data: encoder.encode(msg), channel: this.uri, tick: t },
    })
    await o
  }

  async close(issued = false): Promise<void> {
    if (this.openStreams != 0) {
      let res: () => void
      const out = new Promise((cb) => (res = () => cb(null)))
      this.shouldClose.push(res!)
      await out
    } else {
      this.logger.debug(`${this.uri} closes stream`)
      if (!issued) {
        await this.write({
          close: { channel: this.uri },
        })
      }
      if (this.shouldClose.length > 0) {
        for (const cb of this.shouldClose) {
          cb()
        }
      }
    }
  }

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
