import { DataChunk, RunnerClient } from '@rdfc/proto'
import * as grpc from '@grpc/grpc-js'
import { ClientReadableStream } from '@grpc/grpc-js'
import { ClientReadableStreamImpl, OrchestratorMessage } from './reexports'
import { extractShapes } from 'rdf-lens'
import { NamedNode, Parser, Writer } from 'n3'
import { readFile } from 'fs/promises'
import winston, { createLogger } from 'winston'
import { WriterInstance } from './writer'
import { ReaderInstance } from './reader'
import { Processor } from './processor'
import { FullProc, Runner } from './runner'
import { Quad, Term } from '@rdfjs/types'
import { createTermNamespace } from '@treecg/types'
import { expect } from 'vitest'

export async function getProcessorShape(
  baseIRI = process.cwd() + '/node_modules/@rdfc/js-runner/index.ttl',
) {
  const configFile = await readFile(baseIRI, { encoding: 'utf8' })
  const configQuads = new Parser().parse(configFile)
  const shapes = extractShapes(configQuads)

  return shapes
}

export class TestClient extends RunnerClient {
  next: (stream: ClientReadableStream<DataChunk>) => unknown

  constructor() {
    super('localhost:5400', grpc.credentials.createInsecure())
  }

  nextStream(): Promise<ClientReadableStream<DataChunk>> {
    return new Promise((res) => (this.next = res))
  }

  receiveStreamMessage(): ClientReadableStream<DataChunk> {
    const stream = new ClientReadableStreamImpl<DataChunk>((data: Buffer) => {
      return { data }
    })
    this.next(stream)
    return stream
  }
}

export async function one<T>(iter: AsyncIterable<T>): Promise<T | undefined> {
  for await (const item of iter) {
    return item
  }
}

export const client = new TestClient()
export const uri = 'someUri'
export const logger = createLogger({
  transports: new winston.transports.Console({
    level: process.env['DEBUG'] || 'info',
  }),
})

export function createWriter(iri = uri): [WriterInstance, ReaderInstance] {
  const reader = createReader(iri)
  const writeStream = new WriterInstance(
    iri,
    client,
    async (msg) => {
      if (msg.msg) {
        reader.handleMsg(msg.msg)
      }
      if (msg.close) {
        reader.close()
      }
    },
    '',
    logger,
  )
  return [writeStream, reader]
}

export function createReader(iri = uri): ReaderInstance {
  const reader = new ReaderInstance(
    iri,
    client,
    async () => {
      // TODO: handle msg
    },
    logger,
  )
  return reader
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
const OWL = createTermNamespace('http://www.w3.org/2002/07/owl#', 'imports')
const processorShapes = extractShapes(new Parser().parse(shapeQuads))
const base = 'https://w3id.org/rdf-connect#'

export async function importFile(file: string): Promise<Quad[]> {
  const done = new Set<string>()
  const todo = [new URL('file://' + file)]
  const quads: Quad[] = []

  let item = todo.pop()
  while (item !== undefined) {
    if (done.has(item.toString())) {
      item = todo.pop()
      continue
    }
    done.add(item.toString())
    if (item.protocol !== 'file:') {
      throw 'No supported protocol ' + item.protocol
    }

    const txt = await readFile(item.pathname, { encoding: 'utf8' })
    const extras = new Parser({ baseIRI: item.toString() }).parse(txt)

    for (const o of extras
      .filter(
        (x) =>
          x.subject.value === item?.toString() &&
          x.predicate.equals(OWL.imports),
      )
      .map((x) => x.object.value)) {
      todo.push(new URL(o))
    }
    quads.push(...extras)

    item = todo.pop()
  }

  return quads
}

export async function getProc<T extends Processor<unknown>>(
  config: string,
  ty: string,
  configLocation: string,
  uri = 'http://example.com/ns#processor',
): Promise<FullProc<T>> {
  const configQuads = await importFile(configLocation)
  const procConfig = processorShapes.lenses['JsProcessorShape'].execute({
    id: new NamedNode(base + ty),
    quads: configQuads,
  })

  const msgs: OrchestratorMessage[] = []
  const write = async (x: OrchestratorMessage) => {
    msgs.push(x)
  }
  const runner = new Runner(
    new TestClient(),
    write,
    'http://example.com/ns#',
    logger,
  )
  configQuads.push(...new Parser().parse(config))
  await runner.handleOrchMessage({
    pipeline: new Writer().quadsToString(configQuads),
  })

  const proc = await runner.addProcessor<T>({
    config: JSON.stringify(procConfig),
    arguments: '',
    uri,
  })

  return proc
}

export async function checkProcDefinition(file: string, n: string) {
  const quads = await importFile(file)
  const procConfig = <{ file: Term; location: string; clazz: string }>(
    processorShapes.lenses['JsProcessorShape'].execute({
      id: new NamedNode(base + n),
      quads: quads,
    })
  )
  expect(procConfig.file, n + ' has file').toBeDefined()
  expect(procConfig.location, n + ' has location').toBeDefined()
  expect(procConfig.clazz, n + ' has clazz').toBeDefined()
}
