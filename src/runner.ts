import {
  Close,
  FromRunner,
  Processor,
  RunnerClient,
  ToRunner,
  LocalAck,
  ReceivingMessage,
  ReceivingStreamMessage,
} from '@rdfc/proto'
import { pathToFileURL } from 'node:url'
import { Reader, ReaderInstance } from './reader'
import { Writer, WriterInstance } from './writer'
import { Processor as Proc } from './processor'
import { Logger } from 'winston'

import { extendLogger } from './logger'
import { Cont, empty, extractShapes, Shapes } from 'rdf-lens'
import { NamedNode, Parser } from 'n3'
import { createNamespace, createUriAndTermNamespace, RDF } from '@treecg/types'
import { Quad, Term } from '@rdfjs/types'
import { State } from './state'

const RDFL = createUriAndTermNamespace(
  'https://w3id.org/rdf-lens/ontology#',
  'CBD',
  'Path',
  'PathLens',
  'Context',
  'TypedExtract',
  'EnvVariable',
  'envKey',
  'envDefault',
  'datatype',
)

const RDFC = createNamespace(
  'https://w3id.org/rdf-connect#',
  (x) => x,
  'Reader',
  'Writer',
)

export type Writable = (msg: FromRunner) => Promise<unknown>

type ProcessorConfig = {
  location: string
  file: string
  clazz?: string
}

export type FullProc<C> = C extends Proc<infer T> ? T & C : never
export class Runner {
  private readonly readers: { [uri: string]: ReaderInstance } = {}
  private readonly writers: { [uri: string]: WriterInstance } = {}
  private readonly client: RunnerClient
  private readonly notifyOrchestrator: Writable
  private readonly logger: Logger
  private shapes: Shapes
  private quads: Quad[] = []

  private readonly uri: string

  private readonly processors: Proc<unknown>[] = []
  private readonly processorTransforms: Promise<unknown>[] = []
  private readonly configPath?: string
  private readonly state?: State
  private readonly runnerId?: string

  constructor(
    client: RunnerClient,
    notifyOrchestrator: Writable,
    uri: string,
    logger: Logger,
    configPath?: string,
    state?: State,
    runnerId?: string,
  ) {
    this.client = client
    this.notifyOrchestrator = notifyOrchestrator
    this.uri = uri
    this.logger = logger
    this.configPath = configPath
    this.state = state
    this.runnerId = runnerId
  }

  makeRelative(target: string, base: string): string {
    if (!this.configPath) return target

    const targetUrl = new URL(target)
    const baseUrl = new URL(base)

    const targetParts = targetUrl.pathname.split('/')
    const baseParts = baseUrl.pathname.split('/')

    // Remove filename from base (treat as directory)
    baseParts.pop()

    // Find common path
    let i = 0
    while (
      i < targetParts.length &&
      i < baseParts.length &&
      targetParts[i] === baseParts[i]
    ) {
      i++
    }

    const up = baseParts.slice(i).map(() => '..')
    const down = targetParts.slice(i)

    const thing = './' + [...up, ...down].join('/')
    const configPath = this.configPath && pathToFileURL(this.configPath)
    console.log({ configPath, thing, base, target })
    const final = new URL(thing, configPath)
    return final.href
  }

  async createProcessor<P extends Proc<unknown>>(
    proc: Processor,
  ): Promise<FullProc<P>> {
    const procLogger = extendLogger(this.logger, proc.uri)

    const ty = this.quads
      .filter(
        (x) =>
          x.subject.value === proc.uri && x.predicate.equals(RDF.terms.type),
      )
      .map((x) => x.object.value)

    this.logger.info(
      `Parsing processor '${proc.uri}' of type(s) [${ty.join(', ')}]`,
    )
    const args = this.shapes.lenses[RDFL.TypedExtract].execute({
      id: new NamedNode(proc.uri),
      quads: this.quads,
    })

    const config: ProcessorConfig = JSON.parse(proc.config)
    const jsProgram = await import(this.makeRelative(config.file, this.uri))
    const clazz = jsProgram[config.clazz || 'default']
    const instance: Proc<unknown> = new clazz(args, procLogger)

    return <FullProc<P>>instance
  }

  async addProcessor<P extends Proc<unknown>>(
    proc: Processor,
  ): Promise<FullProc<P>> {
    this.logger.info(JSON.stringify(proc))
    const instance = await this.createProcessor<P>(proc)

    await instance.init()

    this.logger.info(`Initiated processor '${proc.uri}'`)

    this.processors.push(instance)
    this.processorTransforms.push(instance.transform())

    await this.notifyOrchestrator({ initialized: { uri: proc.uri } })

    return <FullProc<P>>instance
  }

  async start() {
    try {
      await Promise.all(this.processors.map((x) => x.produce()))
      await Promise.all(this.processorTransforms)
    } catch (ex: unknown) {
      this.logger.error('Start failed: ' + JSON.stringify(ex))
    }
  }

  createWriter(uri: Term): Writer {
    const id = uri.value

    if (this.writers[id] !== undefined) {
      return this.writers[id]
    }
    const tracker =
      this.runnerId !== undefined
        ? this.state?.trackChannel(this.runnerId, id, 'writer')
        : undefined
    const writer = new WriterInstance(
      id,
      this.client,
      this.notifyOrchestrator,
      this.uri,
      this.logger,
      tracker,
    )
    this.writers[id] = writer
    return writer
  }

  createReader(uri: Term): Reader {
    const ids = uri.value

    if (this.readers[ids] !== undefined) {
      return this.readers[ids]
    }
    const tracker =
      this.runnerId !== undefined
        ? this.state?.trackChannel(this.runnerId, ids, 'reader')
        : undefined
    const reader = new ReaderInstance(
      ids,
      this.client,
      this.notifyOrchestrator,
      this.logger,
      tracker,
    )
    this.readers[ids] = reader
    return reader
  }

  async handleOrchMessage(msg: ToRunner) {
    if (msg.msg) {
      this.handleMsg(msg.msg)
    }

    if (msg.streamMsg) {
      await this.handleStreamMsg(msg.streamMsg)
    }

    if (msg.close) {
      await this.handleClose(msg.close)
    }

    if (msg.pipeline) {
      this.handlePipeline(msg.pipeline)
    }

    if (msg.processed) {
      this.handleProcessed(msg.processed)
    }
  }

  private async handleClose(close: Close) {
    const uri = close.channel
    const r = this.readers[uri]

    let closed = false
    if (r) {
      r.close()
      closed = true
    }
    const w = this.writers[uri]
    if (w) {
      closed = true
      await w.close(true)
    }

    if (!closed) {
      this.logger.error(
        `Received a close event for channel ${uri}, but neither reader nor writer is present.`,
      )
    }
  }

  private handlePipeline(pipeline: string) {
    try {
      const quads = new Parser().parse(pipeline)
      this.shapes = extractShapes(
        quads,
        {
          [RDFC.Reader]: (x: Cont) => this.createReader(x.id),
          [RDFC.Writer]: (x: Cont) => this.createWriter(x.id),
        },
        {
          [RDFC.Reader]: empty<Cont>(),
          [RDFC.Writer]: empty<Cont>(),
        },
      )
      this.quads = quads
    } catch (ex: unknown) {
      this.logger.error('Pipeline failed: ' + JSON.stringify(ex))
    }
  }

  private handleMsg(msg: ReceivingMessage) {
    this.logger.debug('Handling data msg for ' + msg.channel)
    const r = this.readers[msg.channel]

    if (r) {
      r.handleMsg(msg)
    } else {
      this.logger.error(
        `Received message for channel ${msg.channel}, but no reader was present.`,
      )
    }
  }

  private async handleStreamMsg(streamMsg: ReceivingStreamMessage) {
    const r = this.readers[streamMsg.channel]

    if (r) {
      await r.handleStreamingMessage(streamMsg)
    } else {
      this.logger.error(
        `Received stream message for channel ${streamMsg.channel}, but no reader was present.`,
      )
    }
  }

  private handleProcessed(processed: LocalAck) {
    const writer = this.writers[processed.channel]
    if (writer) {
      writer.handled()
    } else {
      this.logger.error(
        `Received processed message for channel ${processed.channel}, but no writer was present.`,
      )
    }
  }
}
