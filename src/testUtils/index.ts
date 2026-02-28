import {
  DataChunk,
  FromRunner,
  LogMessage,
  Processor as ProcConfig,
  ToRunner,
} from '@rdfc/proto'
import { extractShapes } from 'rdf-lens'
import { NamedNode, Parser, Writer as N3Writer } from 'n3'
import { readFile } from 'fs/promises'
import winston, { createLogger } from 'winston'
import { Processor } from '../processor'
import { FullProc, Runner, Writable } from '../runner'
import { Quad } from '@rdfjs/types'
import { createTermNamespace } from '@treecg/types'
import {
  ReceivingStreamControl,
  SendingStreamControl,
  StreamChunk,
  StreamIdentify,
} from '@rdfc/proto/lib/generated/common'
import { MockClientDuplexStream } from './duplex'
import { promisify } from 'util'
import { Reader } from '../reader'
import { Writer } from '../writer'

export function channel(runner: Runner, name: string): [Writer, Reader] {
  const n = new NamedNode(name)
  const reader = runner.createReader(n)
  const writer = runner.createWriter(n)

  return [writer, reader]
}

export class StreamMsgMock {
  data: DataChunk[] = []

  private readonly resolveId: (id: StreamIdentify) => number

  constructor(resolveId: (id: StreamIdentify) => number) {
    this.resolveId = resolveId
  }

  sendStreamMessage(): MockClientDuplexStream<
    StreamChunk,
    ReceivingStreamControl
  > {
    const sendingStream = new MockClientDuplexStream<
      StreamChunk,
      ReceivingStreamControl
    >()

    let at = 0
    sendingStream.register(
      (x) => x.data,
      (d, send) => {
        setTimeout(() => send({ streamSequenceNumber: ++at }), 20)
        this.data.push(d)
      },
    )
    sendingStream.register(
      (x) => x.id,
      (d, send) => send({ streamSequenceNumber: this.resolveId(d) }),
    )
    return sendingStream
  }
}

export class OrchestratorMock {
  connectStream: MockClientDuplexStream<FromRunner, ToRunner>

  streamMsgs: {
    [id: string]: {
      toProducingStream: (id: ReceivingStreamControl) => void
      receivingStream?: MockClientDuplexStream<SendingStreamControl, DataChunk>
    }
  } = {}

  streamMsgCount = 0

  connect(): MockClientDuplexStream<FromRunner, ToRunner> {
    const connectStream = new MockClientDuplexStream<FromRunner, ToRunner>()
    // Always bounce processed msgs back to the runner
    connectStream.register(
      (msg) => msg.processed,
      ({ channel, globalSequenceNumber }, send) => {
        send({
          processed: { channel, localSequenceNumber: globalSequenceNumber },
        })
      },
    )

    // Always bounce data msgs back to the runner
    connectStream.register(
      (msg) => msg.msg,
      ({ localSequenceNumber, data, channel }, send) => {
        send({
          msg: { globalSequenceNumber: localSequenceNumber, channel, data },
        })
      },
    )

    // Always bounce close msgs back to the runner
    connectStream.register(
      (msg) => msg.close,
      (close, send) => {
        send({ close })
      },
    )

    this.connectStream = connectStream
    return connectStream
  }

  sendStreamMessage(): MockClientDuplexStream<
    StreamChunk,
    ReceivingStreamControl
  > {
    const out = new MockClientDuplexStream<
      StreamChunk,
      ReceivingStreamControl
    >()
    const id = this.streamMsgCount
    this.streamMsgCount++

    out.registerOnce(
      (x) => x.id,
      ({ channel }, toProducingStream) => {
        this.streamMsgs[id] = { toProducingStream }

        // Notify stream message
        this.connectStream.send({
          streamMsg: { channel, globalSequenceNumber: id },
        })
      },
    )

    out.register(
      (x) => x.data,
      (data) => {
        // bounce data to receiving stream
        this.streamMsgs[id].receivingStream!.send(data)
      },
    )

    out.on('end', () => {
      // end receiving stream
      this.streamMsgs[id].receivingStream!.end()
    })

    return out
  }

  receiveStreamMessage(): MockClientDuplexStream<
    SendingStreamControl,
    DataChunk
  > {
    const receivingStream = new MockClientDuplexStream<
      SendingStreamControl,
      DataChunk
    >()
    let streamId = 0

    receivingStream.registerOnce(
      (x) => x.globalSequenceNumber,
      (id) => {
        streamId = id
        this.streamMsgs[id].receivingStream = receivingStream
        this.streamMsgs[id].toProducingStream({ streamSequenceNumber: 0 })
      },
    )

    receivingStream.register(
      (x) => x.streamSequenceNumber,
      (streamSequenceNumber) => {
        // Bounce processed message to producing stream
        this.streamMsgs[streamId].toProducingStream({ streamSequenceNumber })
      },
    )

    return receivingStream
  }

  logStream(): MockClientDuplexStream<LogMessage, null> {
    const logStream = new MockClientDuplexStream<LogMessage, null>()
    logStream.register((x) => x, console.log)
    return logStream
  }
}

export function createRunner(uri = 'http://example.com/ns#') {
  const logger = createLogger({
    transports: new winston.transports.Console({
      level: process.env['DEBUG'] || 'info',
    }),
  })

  // Mock the GRPC
  const client = new OrchestratorMock()

  // Connect just like client.ts:start()
  const stream = client.connect()
  const writable = promisify(stream.write.bind(stream))

  const runner = new Runner(
    /* eslint-disable @typescript-eslint/no-explicit-any */
    client as any, // Type assertion for testing
    writable as Writable,
    uri,
    logger,
  )

  stream.on('data', (msg: ToRunner) => runner.handleOrchMessage(msg))

  return runner
}

export async function one<T>(iter: AsyncIterable<T>): Promise<T | undefined> {
  for await (const item of iter) {
    return item
  }
}

const shapeQuads = `
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix sh: <http://www.w3.org/ns/shacl#>.
[ ] a sh:NodeShape;
  sh:targetClass <JsProcessorShape>;
  sh:property [
    sh:path rdfc:entrypoint;
    sh:name "location";
    sh:minCount 1;
    sh:maxCount 1;
    sh:datatype xsd:string;
  ], [
    sh:path rdfc:file;
    sh:name "file";
    sh:minCount 1;
    sh:maxCount 1;
    sh:datatype xsd:string;
  ], [
    sh:path rdfc:class;
    sh:name "clazz";
    sh:maxCount 1;
    sh:datatype xsd:string;
  ].
`
export type ConfigType = {
  location: string
  file: string
  clazz: string
}

const OWL = createTermNamespace('http://www.w3.org/2002/07/owl#', 'imports')
const processorShapes = extractShapes(new Parser().parse(shapeQuads))
const base = 'https://w3id.org/rdf-connect#'

export async function importFile(
  file: string,
  content?: string,
): Promise<Quad[]> {
  const done = new Set<string>()

  const quads: Quad[] = []
  const todo: URL[] = []

  const parse = (content: string, baseIRI: URL) => {
    done.add(baseIRI.toString())

    const extras = new Parser({ baseIRI: baseIRI.toString() }).parse(content)

    for (const o of extras
      .filter(
        (x) =>
          x.subject.value === baseIRI?.toString() &&
          x.predicate.equals(OWL.imports),
      )
      .map((x) => x.object.value)) {
      todo.push(new URL(o))
    }

    quads.push(...extras)
  }

  if (content) {
    parse(content, new URL('file://' + file))
  } else {
    todo.push(new URL('file://' + file))
  }

  let item = todo.pop()
  while (item !== undefined) {
    if (done.has(item.toString())) {
      item = todo.pop()
      continue
    }

    if (item.protocol !== 'file:') {
      throw 'No supported protocol ' + item.protocol
    }
    const txt = await readFile(item.pathname, { encoding: 'utf8' })
    parse(txt, item)

    item = todo.pop()
  }

  return quads
}

/**
 * Helper class to gradually test your processors.
 * Possible flow:
 * - import the JsRunner index file
 * - import your processor config file
 * - test if the config is as you would expect (from getConfig())
 * - import your processor definition (inline)
 * - build your processor
 * - test your processor
 */
export class ProcHelper<T extends Processor<unknown>> {
  runner: Runner
  quads: Quad[] = []
  config: ConfigType
  proc: FullProc<T>

  constructor(uri?: string) {
    this.runner = createRunner(uri)
  }

  async importInline(baseIRI: string, config: string) {
    const configQuads = await importFile(baseIRI, config)
    this.quads.push(...configQuads)
  }

  async importFile(file: string) {
    const configQuads = await importFile(file)
    this.quads.push(...configQuads)
  }

  getConfig(ty: string | NamedNode): ConfigType {
    const id = typeof ty === 'string' ? new NamedNode(base + ty) : ty
    const procConfig = <ConfigType>processorShapes.lenses[
      'JsProcessorShape'
    ].execute({
      id,
      quads: this.quads,
    })

    this.config = procConfig
    return procConfig
  }

  async getProcessor(
    uri: string = 'http://example.com/ns#processor',
  ): Promise<FullProc<T>> {
    await this.runner.handleOrchMessage({
      pipeline: new N3Writer().quadsToString(this.quads),
    })

    return await this.runner.createProcessor<T>({
      config: JSON.stringify(this.config),
      arguments: '',
      uri,
    })
  }
}

/**
 * @deprecated use {@link ProcHelper}
 */
export async function getProcInline<T extends Processor<unknown>>(
  config: string,
  ty: string,
  runner: Runner,
  baseIRI: string,
  uri = 'http://example.com/ns#processor',
): Promise<FullProc<T>> {
  const configQuads = await importFile(baseIRI, config)
  const procConfig = <ProcConfig>processorShapes.lenses[
    'JsProcessorShape'
  ].execute({
    id: new NamedNode(base + ty),
    quads: configQuads,
  })

  await runner.handleOrchMessage({
    pipeline: new N3Writer().quadsToString(configQuads),
  })

  const proc = await runner.addProcessor<T>({
    config: JSON.stringify(procConfig),
    arguments: '',
    uri,
  })

  return proc
}

/**
 * @deprecated use {@link ProcHelper}
 */
export async function getProc<T extends Processor<unknown>>(
  config: string,
  ty: string,
  runner: Runner,
  configLocation: string,
  uri = 'http://example.com/ns#processor',
): Promise<FullProc<T>> {
  const configQuads = await importFile(configLocation)
  const procConfig = processorShapes.lenses['JsProcessorShape'].execute({
    id: new NamedNode(base + ty),
    quads: configQuads,
  })

  configQuads.push(...new Parser().parse(config))
  await runner.handleOrchMessage({
    pipeline: new N3Writer().quadsToString(configQuads),
  })

  const proc = await runner.addProcessor<T>({
    config: JSON.stringify(procConfig),
    arguments: '',
    uri,
  })

  return proc
}
