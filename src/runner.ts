import {
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
  private readonly readers: { [uri: string]: ReaderInstance[] } = {}
  private readonly writers: { [uri: string]: WriterInstance[] } = {}
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
          stream: this.client.logStream(() => {}),
        }),
      ],
    })

    const ty = JSON.stringify(
      this.quads
        .filter(
          (x) =>
            x.subject.value === proc.uri && x.predicate.equals(RDF.terms.type),
        )
        .map((x) => x.object.value),
    )
    this.logger.info('parsing ' + proc.uri + ' type ' + ty)
    const args = this.shapes.lenses[RDFL.TypedExtract].execute({
      id: new NamedNode(proc.uri),
      quads: this.quads,
    })

    const config: ProcessorConfig = JSON.parse(proc.config)
    // const url = new URL(config.location)
    // process.chdir(url.pathname)
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
    await Promise.all(this.processors.map((x) => x.produce()))
    await Promise.all(this.processorTransforms)
  }

  createWriter(uri: Term): Writer {
    const ids = uri.value

    if (this.writers[ids] === undefined) {
      this.writers[ids] = []
    }
    const writer = new WriterInstance(ids, this.client, this.write, this.logger)
    this.writers[ids].push(writer)
    return writer
  }

  createReader(uri: Term): Reader {
    const ids = uri.value

    if (this.readers[ids] === undefined) {
      this.readers[ids] = []
    }
    const reader = new ReaderInstance(ids, this.client, this.logger)
    this.readers[ids].push(reader)
    return reader
  }

  async handleOrchMessage(msg: RunnerMessage) {
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

    if (msg.pipeline) {
      // here
      const quads = new Parser().parse(msg.pipeline)
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
      this.logger.info(
        'extracted shapes ' + JSON.stringify(Object.keys(this.shapes.lenses)),
      )
    }
  }
}
