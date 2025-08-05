import { DataChunk, RunnerClient } from '@rdfc/proto'
import { ClientReadableStream } from '@grpc/grpc-js'
import * as grpc from '@grpc/grpc-js'
import { ClientReadableStreamImpl } from './reexports'
import { extractShapes } from 'rdf-lens'
import { Parser } from 'n3'
import { readFile } from 'fs/promises'
import winston, { createLogger } from 'winston'
import { WriterInstance } from './writer'
import { ReaderInstance } from './reader'

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
    logger,
  )
  return [writeStream, reader]
}

export function createReader(iri = uri): ReaderInstance {
  const reader = new ReaderInstance(iri, client, logger)
  return reader
}
