import { Id, OrchestratorMessage, RunnerClient } from '@rdfc/proto'
import { promisify } from 'util'
import { Logger } from 'winston'

type Writable = (msg: OrchestratorMessage) => Promise<unknown>
export interface Writer {
  buffer(buffer: Uint8Array): Promise<void>

  stream(buffer: AsyncIterable<Uint8Array>): Promise<void>
  stream<T>(
    buffer: AsyncIterable<T>,
    tranform: (x: T) => Uint8Array,
  ): Promise<void>

  string(buffer: string): Promise<void>
  close(): Promise<void>
}
const encoder = new TextEncoder()
export class WriterInstance implements Writer {
  private readonly uri: string
  private readonly client: RunnerClient
  private readonly write: Writable
  private readonly logger: Logger

  constructor(
    uri: string,
    client: RunnerClient,
    write: Writable,
    logger: Logger,
  ) {
    this.client = client
    this.write = write
    this.uri = uri
    this.logger = logger
  }

  async buffer(buffer: Uint8Array): Promise<void> {
    this.logger.debug(`${this.uri} sends buffer ${buffer.length} bytes`)
    await this.write({ msg: { data: buffer, channel: this.uri } })
  }

  async stream<T = Uint8Array>(
    buffer: AsyncIterable<T>,
    transform?: (x: T) => Uint8Array,
  ) {
    const t = transform || ((x: unknown) => <Uint8Array>x)
    const stream = this.client.sendStreamMessage()
    const id: Id = await new Promise((res) => stream.once('data', res))
    this.logger.debug(`${this.uri} streams message with id ${id.id}`)
    await this.write({ streamMsg: { id, channel: this.uri } })

    const write = promisify(stream.write.bind(stream))
    for await (const msg of buffer) {
      await write({ data: t(msg) })
    }

    this.logger.debug(`${this.uri} is done streaming message with id ${id.id}`)
    stream.end()
  }

  async string(msg: string): Promise<void> {
    this.logger.debug(`${this.uri} sends string ${msg.length} characters`)
    await this.write({
      msg: { data: encoder.encode(msg), channel: this.uri },
    })
  }

  async close(): Promise<void> {
    this.logger.debug(`${this.uri} closes stream`)
    await this.write({
      close: { channel: this.uri },
    })
  }
}
