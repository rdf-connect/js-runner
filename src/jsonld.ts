import { OrchestratorMessage, RunnerClient } from '@rdfc/proto'
import { createNamespace, createUriAndTermNamespace } from '@treecg/types'
import { ReaderInstance } from './reader'
import { WriterInstance } from './writer'
import { Logger } from 'winston'
import { JsonLdParser } from 'jsonld-streaming-parser'
import { pred, ShaclPath } from 'rdf-lens'
import { Quad, Term } from '@rdfjs/types'
import { NamedNode } from 'n3'

export type RunnerItems = {
  readers: { [uri: string]: ReaderInstance[] }
  writers: { [uri: string]: WriterInstance[] }
  client: RunnerClient
  write: Writable
}

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

type Writable = (msg: OrchestratorMessage) => Promise<unknown>
const RDFC = createNamespace(
  'https://w3id.org/rdf-connect#',
  (x) => x,
  'Reader',
  'Writer',
)

function as_string_array(obj: unknown): string[] {
  const out = Array.isArray(obj) ? obj : [obj]
  return out.filter((x) => typeof x === 'string')
}

function cbdToQuads(value: unknown) {
  const quads: Quad[] = []
  const parser = new JsonLdParser()
  const promise = new Promise<{ value: Quad[] }>((res, rej) =>
    parser
      .on('end', () => res({ value: quads }))
      .on('error', (e) => rej(e))
      .on('data', (q) => {
        quads.push(q)
      }),
  )

  if (value instanceof Object && '@type' in value) {
    delete value['@type']
  }
  parser.write(JSON.stringify(value))
  parser.end()
  return promise
}

async function cbdToPath(value: unknown): Promise<{ value: unknown }> {
  const qs = (
    await cbdToQuads({
      '@id': 'http://example.com/ns#me1234',
      'http://example.com/ns#innerPred': value,
    })
  ).value

  const path = pred(new NamedNode('http://example.com/ns#innerPred'))
    .one()
    .then(ShaclPath)
    .execute({
      quads: qs,
      id: new NamedNode('http://example.com/ns#me1234'),
    })
  return { value: path }
}

type Cont = { quads: Quad[]; id: Term }
async function jsonldToQuads(value: unknown): Promise<Cont> {
  const qs = (
    await cbdToQuads({
      '@id': 'http://example.com/ns#me1234',
      'http://example.com/ns#innerPred': value,
    })
  ).value

  const idx = qs.findIndex(
    (x) =>
      x.predicate.equals(new NamedNode('http://example.com/ns#innerPred')) &&
      x.subject.equals(new NamedNode('http://example.com/ns#me1234')),
  )
  if (idx < 0) throw 'This cannot happen'
  const id = qs[idx].object

  qs.splice(idx, 1)

  return {
    quads: qs,
    id,
  }
}

/* eslint-disable  @typescript-eslint/no-explicit-any */
function revive(
  _key: string,
  value: any,
  logger: Logger,
  promises: Promise<unknown>[],
  runnerItems: RunnerItems,
): any {
  if (typeof value === 'object') {
    const types = as_string_array(value['@type'] || [])
    const ids = as_string_array(value['@id'] || [])[0] || ''

    if (types.includes(RDFC.Reader)) {
      if (runnerItems.readers[ids] === undefined) {
        runnerItems.readers[ids] = []
      }
      const reader = new ReaderInstance(ids, runnerItems.client, logger)
      runnerItems.readers[ids].push(reader)
      return reader
    }

    if (types.includes(RDFC.Writer)) {
      if (runnerItems.writers[ids] === undefined) {
        runnerItems.writers[ids] = []
      }
      const writer = new WriterInstance(
        ids,
        runnerItems.client,
        runnerItems.write,
        logger,
      )
      runnerItems.writers[ids].push(writer)
      return writer
    }

    if (types.includes(RDFL.CBD)) {
      return cbdToQuads(value)
    }

    if (types.includes(RDFL.Path)) {
      return cbdToPath(value)
    }
  }

  Object.entries(value).forEach(([k, v]) => {
    if (v instanceof Promise) {
      logger.info('Found promise')
      promises.push(
        v.then((x) => {
          logger.info('Setting field ' + k)
          value[k] = x.value
        }),
      )
    }
  })

  return value
}

type Arg = {
  [id: string]: any
}
function expandArgs(args: Arg, cont: Cont, logger: Logger) {
  const context = args['@context']
  if (!context) return

  for (const key of Object.keys(args)) {
    if (key == '@context') continue
    const ctxObj = context[key]
    if (!ctxObj) {
      logger.debug("Didn't find ctx things for " + key)
      continue
    }

    logger.debug(
      'Did find ctx things for ' +
        key +
        ' ' +
        JSON.stringify(ctxObj) +
        ': ' +
        JSON.stringify(
          Object.entries(args[key]).map(([k, v]) => {
            let x = 'object'
            try {
              x = JSON.stringify(v)
              // eslint-disable-next-line @typescript-eslint/no-unused-vars
            } catch (_e: unknown) {
              // default is set
            }

            return k + ': ' + x
          }),
        ),
    )

    expandArgs(args[key], cont, logger)
  }
}

export async function parse_jsonld(
  args: string,
  logger: Logger,
  items: RunnerItems,
): Promise<unknown> {
  const promises: Promise<unknown>[] = []
  logger.info(args)
  const cont = await jsonldToQuads(JSON.parse(args))
  const out = JSON.parse(args, (k, v) => revive(k, v, logger, promises, items))

  await Promise.all(promises)

  expandArgs(out, cont, logger)
  return out
}
