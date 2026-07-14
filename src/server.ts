import { createServer } from 'node:http'
import { createServer as createTcpServer, Socket } from 'node:net'
import { readFile } from 'node:fs/promises'
import { resolve, relative } from 'node:path'
import { pathToFileURL, fileURLToPath } from 'node:url'
import { Parser, DataFactory, Writer } from 'n3'
import { extractShapes } from 'rdf-lens'
import { start } from './client.js'
import { State } from './state.js'
import { createSocketProxy } from './socketProxy.js'

import { $INLINE_FILE } from '@ajuvercr/ts-transformer-inline-file'
import { createUriAndTermNamespace, RDF, RDFS } from '@treecg/types'
// Dashboard HTML is read from the adjacent file at module load time.
const DASHBOARD_HTML = $INLINE_FILE('./dashboard.html')
const { quad, namedNode, literal } = DataFactory

const RDFC = createUriAndTermNamespace(
  'https://w3id.org/rdf-connect#',
  'jsImplementationOf',
  'JsRunnerServer',
  'CommandRunner',
  'TcpRunner',
  'Processor',
  'handlesSubjectsOf',
  'jsImplementationOf',
  'grpc',
)
const OWL = createUriAndTermNamespace(
  'http://www.w3.org/2002/07/owl#',
  'imports',
)

const { lenses } = extractShapes(
  new Parser().parse($INLINE_FILE('./server_config_shape.ttl')),
)

interface ServerConfig {
  httpPort: number
  grpcPort: number
  hostname: string
  processorPaths: string[]
}

interface ProcessorDescription {
  uri: string
  label?: string
  comment?: string
  sourceFile: string
}

function readLine(socket: Socket): Promise<string> {
  return new Promise((resolve, reject) => {
    // Accumulate as raw bytes. The runner URI is followed immediately by the
    // orchestrator's gRPC/HTTP2 server preface (binary), which TCP often
    // coalesces into the same chunk. We must never decode those trailing bytes
    // through a string: a UTF-8 round-trip mangles any byte >= 0x80 and changes
    // the buffer length, desyncing the HTTP/2 stream. So we split on the '\n'
    // byte and push the remainder back onto the socket exactly as received.
    let buffer = Buffer.alloc(0)

    const timeout = setTimeout(() => {
      cleanup()
      reject(
        new Error('Orchestrator failed to send a uri in a reasonable time'),
      )
    }, 5000)

    const cleanup = () => {
      clearTimeout(timeout)
      socket.removeListener('data', onData)
      socket.removeListener('error', onError)
    }

    const onData = (chunk: Buffer) => {
      buffer = Buffer.concat([buffer, chunk])
      const idx = buffer.indexOf(0x0a) // '\n'
      if (idx !== -1) {
        cleanup()
        // Pause before handing the socket off. Reading the line put the socket
        // in flowing mode; the next consumer (the proxy's .pipe()) attaches
        // only after an async gap (dialling the loopback proxy). Without a
        // pause, the orchestrator's HTTP/2 server preface — sent right after
        // this line — would be emitted to no listener and lost, breaking the
        // gRPC handshake. Paused, the unshifted remainder and any later bytes
        // stay buffered and are flushed in order once .pipe() resumes it.
        socket.pause()
        const remaining = buffer.subarray(idx + 1)
        if (remaining.length > 0) {
          // Original bytes, untouched — the HTTP/2 layer reads them verbatim.
          socket.unshift(remaining)
        }
        resolve(buffer.subarray(0, idx).toString('utf8').trim())
      } else if (buffer.length > 1024) {
        cleanup()
        reject(new Error('Runner Identifier exceeded 1024 bytes'))
      }
    }

    const onError = (err: Error) => {
      cleanup()
      reject(err)
    }

    socket.on('data', onData)
    socket.once('error', onError)
  })
}

export async function parseServerConfig(
  configPath: string,
): Promise<ServerConfig> {
  const absConfig = resolve(configPath)
  const content = await readFile(absConfig, { encoding: 'utf8' })
  const quads = new Parser({
    baseIRI: pathToFileURL(absConfig).toString(),
  }).parse(content)

  const serverSubject = quads.find(
    (q) =>
      q.predicate.equals(RDF.terms.type) &&
      q.object.equals(RDFC.terms.JsRunnerServer),
  )?.subject

  if (!serverSubject) {
    throw new Error(`No rdfc:JsRunnerServer found in ${absConfig}`)
  }

  const config = lenses[RDFC.JsRunnerServer].execute({
    id: serverSubject,
    quads,
  }) as {
    httpPort?: number
    grpcPort?: number
    processorConfigs?: string[]
    hostname?: string
  }

  const hostname = config.hostname ?? 'localhost'
  const httpPort = config.httpPort ?? 3000
  const grpcPort = config.grpcPort ?? 50051
  const processorPaths = (config.processorConfigs ?? []).map((val) =>
    val.startsWith('file://') ? fileURLToPath(val) : val,
  )

  return { httpPort, grpcPort, processorPaths, hostname }
}

export async function buildWhitelist(
  processorPaths: string[],
): Promise<Set<string>> {
  const whitelist = new Set<string>()
  const done = new Set<string>()
  const todo: string[] = [...processorPaths]

  while (todo.length > 0) {
    const filePath = todo.pop()!
    if (done.has(filePath)) continue
    done.add(filePath)
    whitelist.add(filePath)

    let content: string
    try {
      content = await readFile(filePath, { encoding: 'utf8' })
    } catch {
      continue
    }

    const baseIRI = pathToFileURL(filePath).toString()
    const quads = new Parser({ baseIRI }).parse(content)

    for (const quad of quads) {
      if (
        quad.subject.value === baseIRI &&
        quad.predicate.equals(OWL.terms.imports)
      ) {
        const importVal = quad.object.value
        if (importVal.startsWith('file://')) {
          todo.push(fileURLToPath(importVal))
        }
      }
    }
  }

  return whitelist
}

async function extractProcessorDescriptions(
  processorPaths: string[],
): Promise<ProcessorDescription[]> {
  const descriptions: ProcessorDescription[] = []

  for (const filePath of processorPaths) {
    let content: string
    try {
      content = await readFile(filePath, { encoding: 'utf8' })
    } catch {
      continue
    }

    const baseIRI = pathToFileURL(filePath).toString()
    const quads = new Parser({ baseIRI }).parse(content)

    const seen = new Set<string>()
    for (const quad of quads) {
      if (!quad.predicate.equals(RDFC.terms.jsImplementationOf)) continue
      const uri = quad.subject.value
      if (seen.has(uri)) continue
      seen.add(uri)

      const labelQuad = quads.find(
        (q) => q.subject.value === uri && q.predicate.equals(RDFS.terms.label),
      )
      const commentQuad = quads.find(
        (q) =>
          q.subject.value === uri && q.predicate.equals(RDFS.terms.comment),
      )

      descriptions.push({
        uri,
        label: labelQuad?.object.value,
        comment: commentQuad?.object.value,
        sourceFile: filePath,
      })
    }
  }

  return descriptions
}

function getIndexQuads() {
  const quads = new Parser().parse($INLINE_FILE('../index.ttl'))
  const otherRunners = quads
    .filter(
      (q) =>
        q.predicate.equals(RDF.terms.type) &&
        q.object.equals(RDFC.terms.CommandRunner),
    )
    .map((q) => q.subject)

  // Ignore other runner definitions
  return quads.filter(
    (q) => !otherRunners.some((runner) => q.subject.equals(runner)),
  )
}

export async function generateIndexTtl(
  processorPaths: string[],
  cwd: string,
  hostname: string,
  grpcPort: number,
): Promise<string> {
  const quads = getIndexQuads()
  const descriptions = await extractProcessorDescriptions(processorPaths)

  // add js runner
  quads.push(
    quad(namedNode('jsRunner'), RDF.terms.type, RDFC.terms.TcpRunner),
    quad(
      namedNode('jsRunner'),
      RDFC.terms.handlesSubjectsOf,
      RDFC.terms.jsImplementationOf,
    ),
    quad(
      namedNode('jsRunner'),
      RDFC.terms.grpc,
      literal(hostname + ':' + grpcPort),
    ),
  )

  for (const desc of descriptions) {
    const relPath = relative(cwd, desc.sourceFile)
    quads.push(
      quad(namedNode(desc.uri), RDF.terms.type, RDFC.terms.Processor),
      quad(namedNode(desc.uri), RDFS.terms.isDefinedBy, namedNode(relPath)),
    )
    if (desc.label) {
      quads.push(
        quad(namedNode(desc.uri), RDFS.terms.label, literal(desc.label)),
      )
    }
    if (desc.comment) {
      quads.push(
        quad(namedNode(desc.uri), RDFS.terms.comment, literal(desc.comment)),
      )
    }
  }

  const writer = new Writer({
    format: 'text/turtle',
    prefixes: {
      rdfs: 'http://www.w3.org/2000/01/rdf-schema#',
      xsd: 'http://www.w3.org/2001/XMLSchema#',
      rdfc: 'https://w3id.org/rdf-connect#',
      sh: 'http://www.w3.org/ns/shacl#',
    },
  })

  writer.addQuads(quads)

  return await new Promise((res, rej) =>
    writer.end((e, result) => {
      if (e) rej(e)
      res(result)
    }),
  )
}

export async function serve(configPath: string): Promise<void> {
  const absConfig = resolve(configPath)
  const { httpPort, grpcPort, processorPaths, hostname } =
    await parseServerConfig(absConfig)
  const whitelist = await buildWhitelist(processorPaths)
  const cwd = process.cwd()
  const indexTtl = await generateIndexTtl(
    processorPaths,
    cwd,
    hostname,
    grpcPort,
  )

  const state = new State()

  const activeConnections = new Set<AbortController>()

  const shutdown = (signal: string) => {
    console.log(
      `\nReceived ${signal}, closing ${activeConnections.size} active gRPC connection(s)...`,
    )
    for (const ctrl of activeConnections) ctrl.abort()
    tcpServer.close()
    server.close(() => process.exit(0))
  }

  process.on('SIGINT', () => shutdown('SIGINT'))
  process.on('SIGTERM', () => shutdown('SIGTERM'))

  // ── TCP server: accepts orchestrator connections on grpcPort ──────────────
  //
  // When the orchestrator wants to start a new pipeline instance it opens a
  // plain TCP connection here, writes the runner URI terminated by '\n', and
  // then treats its end of the socket as an incoming gRPC server connection
  // (via grpc.Server.createConnectionInjector).
  //
  // The runner bridges orchSocket to a stock gRPC client through a per-request
  // loopback proxy (see createSocketProxy): grpc-js dials the proxy normally
  // and its traffic is piped byte-for-byte to orchSocket. This keeps the full,
  // unmodified grpc-js channel in play — no channelOverride, no hand-rolled
  // framing.
  const tcpServer = createTcpServer(async (orchSocket) => {
    try {
      const uri = await readLine(orchSocket)
      console.log(`Received connection for runner URI: ${uri}`)

      const proxy = await createSocketProxy(orchSocket)
      const runnerId = state.registerRunner('socket', uri)
      const ctrl = new AbortController()
      activeConnections.add(ctrl)

      start(proxy.target, uri, absConfig, ctrl.signal, state, runnerId)
        .catch((err) => {
          console.error('gRPC connection error:', err)
          state.markError(runnerId)
        })
        .finally(() => {
          activeConnections.delete(ctrl)
          proxy.close()
          state.deregisterRunner(runnerId)
        })
    } catch (err) {
      console.error('TCP handler error:', err)
      orchSocket.destroy()
    }
  })

  // ── HTTP server: serves index.ttl, processor configs, and dashboard ───────
  const server = createServer(async (req, res) => {
    const method = req.method ?? 'GET'
    const url = req.url ?? '/'

    // --- Health check ---
    if (method === 'GET' && url === '/health') {
      res.writeHead(200, { 'Content-Type': 'application/json' })
      res.end(
        JSON.stringify({
          status: 'ok',
          runners: state.snapshot().length,
          activeConnections: activeConnections.size,
        }),
      )
      return
    }

    // --- State API (JSON) ---
    if (method === 'GET' && url === '/api/state') {
      res.writeHead(200, {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-store',
      })
      res.end(JSON.stringify(state.snapshot()))
      return
    }

    // --- Dashboard (HTML) ---
    if (method === 'GET' && url === '/dashboard') {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' })
      res.end(DASHBOARD_HTML)
      return
    }

    // --- Index Turtle ---
    if (method === 'GET' && url === '/') {
      res.writeHead(200, { 'Content-Type': 'text/turtle' })
      res.end(indexTtl)
      return
    }

    // --- Whitelisted processor files ---
    if (method === 'GET') {
      const reqPath = url.startsWith('/') ? url.slice(1) : url
      const absPath = resolve(cwd, reqPath)

      if (!whitelist.has(absPath)) {
        res.writeHead(403, { 'Content-Type': 'text/plain' })
        res.end('Forbidden')
        return
      }

      let content: string
      try {
        content = await readFile(absPath, { encoding: 'utf8' })
      } catch {
        res.writeHead(404, { 'Content-Type': 'text/plain' })
        res.end('Not found')
        return
      }

      res.writeHead(200, { 'Content-Type': 'text/turtle' })
      res.end(content)
      return
    }

    res.writeHead(404, { 'Content-Type': 'text/plain' })
    res.end('Not found')
  })

  await Promise.all([
    new Promise<void>((res) => {
      tcpServer.listen(grpcPort, () => {
        console.log(`js-runner gRPC TCP server listening on port ${grpcPort}`)
        res()
      })
    }),
    new Promise<void>((res) => {
      server.listen(httpPort, () => {
        console.log(`js-runner HTTP server listening on port ${httpPort}`)
        console.log(`  Dashboard: http://localhost:${httpPort}/dashboard`)
        console.log(`  Health:    http://localhost:${httpPort}/health`)
        console.log(`  State API: http://localhost:${httpPort}/api/state`)
        res()
      })
    }),
  ])
}
