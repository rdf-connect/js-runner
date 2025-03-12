import { OrchestratorMessage, Processor, RunnerClient } from '@rdfc/proto'
import { createNamespace } from '@treecg/types'
import { ReaderInstance } from './reader'
import { WriterInstance } from './writer'
import { Processor as Proc } from './processor'
import { Logger } from 'winston'

import winston from 'winston'
import { RpcTransport } from './logger'

type Writable = (msg: OrchestratorMessage) => Promise<unknown>
const RDFC = createNamespace(
  'https://w3id.org/rdf-connect/ontology#',
  (x) => x,
  'Reader',
  'Writer',
)

function as_string_array(obj: unknown): string[] {
  const out = Array.isArray(obj) ? obj : [obj]
  return out.filter((x) => typeof x === 'string')
}

type ProcessorConfig = {
  location: string
  file: string
  clazz?: string
}

export class Runner {
  private readonly readers: { [uri: string]: ReaderInstance[] } = {}
  private readonly writers: { [uri: string]: WriterInstance[] } = {}
  private readonly client: RunnerClient
  private readonly write: Writable
  private readonly logger: Logger

  private readonly uri: string

  private readonly processors: Proc<unknown>[] = []

  constructor(
    client: RunnerClient,
    write: Writable,
    uri: string,
    logger: Logger,
  ) {
    this.client = client
    this.write = write
    this.uri = uri
    this.logger = logger
  }

  /* eslint-disable  @typescript-eslint/no-explicit-any */
  private revive(_key: string, value: any, logger: Logger): any {
    if (typeof value === 'object') {
      const types = as_string_array(value['@type'] || [])
      const ids = as_string_array(value['@id'] || [])[0] || ''
      if (types.includes(RDFC.Reader)) {
        if (this.readers[ids] === undefined) {
          this.readers[ids] = []
        }
        const reader = new ReaderInstance(ids, this.client, logger)
        this.readers[ids].push(reader)
        return reader
      }

      if (types.includes(RDFC.Writer)) {
        if (this.writers[ids] === undefined) {
          this.writers[ids] = []
        }
        const writer = new WriterInstance(ids, this.client, this.write, logger)
        this.writers[ids].push(writer)
        return writer
      }
    }
    return value
  }

  async addProcessor(proc: Processor) {
    const procLogger = winston.createLogger({
      transports: [
        new RpcTransport({
          entities: [proc.uri, this.uri],
          stream: this.client.logStream(() => {}),
        }),
      ],
    })

    const args = JSON.parse(proc.arguments, (k, v) =>
      this.revive(k, v, procLogger),
    )

    const config: ProcessorConfig = JSON.parse(proc.config)
    const url = new URL(config.location)
    process.chdir(url.pathname)

    const jsProgram = await import(config.file)
    const clazz = jsProgram[config.clazz || 'default']
    const instance: Proc<unknown> = new clazz(args, procLogger)
    await instance.init()

    await this.write({ init: { uri: proc.uri } })

    this.processors.push(instance)
  }

  async start() {
    await Promise.all(this.processors.map((x) => x.start()))
  }

  async handleOrchMessage(msg: OrchestratorMessage) {
    if (msg.msg) {
      this.logger.debug('Handling data msg for ' + msg.msg.channel)
      for (const reader of this.readers[msg.msg.channel] || []) {
        reader.handleMsg(msg.msg)
      }
    }

    if (msg.streamMsg) {
      for (const reader of this.readers[msg.streamMsg.channel] || []) {
        reader.handleStreamingMessage(msg.streamMsg)
      }
    }

    if (msg.close) {
      const uri = msg.close.channel

      for (const reader of this.readers[uri] || []) {
        reader.close()
      }

      for (const writer of this.writers[uri] || []) {
        await writer.close(true)
      }
    }
  }
}
