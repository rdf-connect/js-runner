import {
  Close,
  Message,
  OrchestratorMessage,
  Processor,
  RunnerClient,
  RunnerMessage,
} from '@rdfc/proto'
import { Reader, ReaderInstance } from './reader'
import { Writer, WriterInstance } from './writer'
import { Processor as Proc } from './processor'
import { Logger } from 'winston'

import winston from 'winston'
import { RpcTransport } from './logger'
import { Cont, empty, extractShapes, Shapes } from 'rdf-lens'
import { NamedNode, Parser } from 'n3'
import { createNamespace, createUriAndTermNamespace, RDF } from '@treecg/types'
import { Quad, Term } from '@rdfjs/types'

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

export type Writable = (msg: OrchestratorMessage) => Promise<unknown>

type ProcessorConfig = {
  location: string
  file: string
  clazz?: string
}

export type FullProc<C extends Proc<unknown>> =
  C extends Proc<infer T> ? T & C : unknown
export class Runner {
  private readonly readers: { [uri: string]: ReaderInstance } = {}
  private readonly writers: { [uri: string]: WriterInstance } = {}
  private readonly client: RunnerClient
  private readonly write: Writable
  private readonly logger: Logger
  private shapes: Shapes
  private quads: Quad[] = []

  private readonly uri: string

  private readonly processors: Proc<unknown>[] = []
  private readonly processorTransforms: Promise<unknown>[] = []

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

  async addProcessor<P extends Proc<unknown>>(
    proc: Processor,
  ): Promise<FullProc<P>> {
    const procLogger = winston.createLogger({
      transports: [
        new RpcTransport({
          entities: [proc.uri, this.uri],
          stream: this.client.logStream(() => { }),
        }),
      ],
    })

    const ty = this.quads
      .filter(
        (x) =>
          x.subject.value === proc.uri && x.predicate.equals(RDF.terms.type),
      )
      .map((x) => x.object.value)

    this.logger.info('parsing ' + proc.uri + ' type ' + ty)
    const args = this.shapes.lenses[RDFL.TypedExtract].execute({
      id: new NamedNode(proc.uri),
      quads: this.quads,
    })

    const config: ProcessorConfig = JSON.parse(proc.config)
    const jsProgram = await import(config.file)
    const clazz = jsProgram[config.clazz || 'default']
    const instance: Proc<unknown> = new clazz(args, procLogger)
    await instance.init()

    this.logger.info('inited ' + proc.uri + ' type ' + ty)

    this.processors.push(instance)
    this.processorTransforms.push(instance.transform())

    await this.write({ init: { uri: proc.uri } })

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
    const writer = new WriterInstance(
      id,
      this.client,
      this.write,
      this.uri,
      this.logger,
    )
    this.writers[id] = writer
    return writer
  }

  createReader(uri: Term): Reader {
    const ids = uri.value

    if (this.readers[ids] !== undefined) {
      return this.readers[ids]
    }
    const reader = new ReaderInstance(ids, this.client, this.write, this.logger)
    this.readers[ids] = reader
    return reader
  }

  async handleOrchMessage(msg: RunnerMessage) {
    if (msg.msg) {
      this.handleMsg(msg.msg)
    }

    if (msg.streamMsg) {
      const r = this.readers[msg.streamMsg.channel]
      await r.handleStreamingMessage(msg.streamMsg)
    }

    if (msg.close) {
      this.handleClose(msg.close)
    }

    if (msg.pipeline) {
      this.handlePipeline(msg.pipeline)
    }

    if (msg.processed) {
      this.writers[msg.processed.channel].handled()
    }
  }

  private handleClose(close: Close) {
    const uri = close.channel
    const r = this.readers[uri]
    if (r) {
      r.close()
    }

    const w = this.writers[uri]
    if (w) {
      w.close(true)
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

  private handleMsg(msg: Message) {
    this.logger.debug('Handling data msg for ' + msg.channel)
    const r = this.readers[msg.channel]

    if (r) {
      r.handleMsg(msg)
    }
  }
}
