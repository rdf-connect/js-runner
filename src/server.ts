import { createServer } from 'node:http'
import { createServer as createTcpServer, Socket } from 'node:net'
import { readFileSync } from 'node:fs'
import { readFile } from 'node:fs/promises'
import { resolve, relative } from 'node:path'
import { pathToFileURL, fileURLToPath } from 'node:url'
import { Parser } from 'n3'
import { extractShapes } from 'rdf-lens'
import { start } from './client.js'
import { State } from './state.js'
import { SocketChannel } from './socketChannel.js'

// Dashboard HTML is read from the adjacent file at module load time.
const DASHBOARD_HTML = readFileSync(
  fileURLToPath(new URL('./dashboard.html', import.meta.url)),
  'utf8',
)

const RDFC = 'https://w3id.org/rdf-connect#'
const OWL_IMPORTS = 'http://www.w3.org/2002/07/owl#imports'
const RDF_TYPE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
const RDFS_LABEL = 'http://www.w3.org/2000/01/rdf-schema#label'
const RDFS_COMMENT = 'http://www.w3.org/2000/01/rdf-schema#comment'

// SHACL shape for rdfc:JsRunnerServer config files
const CONFIG_SHAPE_TTL = `
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix sh: <http://www.w3.org/ns/shacl#>.

[] a sh:NodeShape;
  sh:targetClass rdfc:JsRunnerServer;
  sh:property [
    sh:path rdfc:httpPort;
    sh:name "httpPort";
    sh:maxCount 1;
    sh:datatype xsd:integer;
  ], [
    sh:path rdfc:grpcPort;
    sh:name "grpcPort";
    sh:maxCount 1;
    sh:datatype xsd:integer;
  ], [
    sh:path rdfc:processorConfig;
    sh:name "processorConfigs";
    sh:datatype xsd:string;
  ].
`

const { lenses } = extractShapes(new Parser().parse(CONFIG_SHAPE_TTL))

interface ServerConfig {
  httpPort: number
  grpcPort: number
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
    let buffer = ''
    const onData = (chunk: Buffer) => {
      buffer += chunk.toString()
      const idx = buffer.indexOf('\n')
      if (idx !== -1) {
        socket.removeListener('data', onData)
        socket.removeListener('error', onError)
        const remaining = buffer.slice(idx + 1)
        if (remaining.length > 0) {
          socket.unshift(Buffer.from(remaining))
        }
        resolve(buffer.slice(0, idx).trim())
      }
    }
    const onError = (err: Error) => {
      socket.removeListener('data', onData)
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
      q.predicate.value === RDF_TYPE &&
      q.object.value === RDFC + 'JsRunnerServer',
  )?.subject

  if (!serverSubject) {
    throw new Error(`No rdfc:JsRunnerServer found in ${absConfig}`)
  }

  const config = lenses[RDFC + 'JsRunnerServer'].execute({
    id: serverSubject,
    quads,
  }) as { httpPort?: number; grpcPort?: number; processorConfigs?: string[] }

  const httpPort = config.httpPort ?? 3000
  const grpcPort = config.grpcPort ?? 50051
  const processorPaths = (config.processorConfigs ?? []).map((val) =>
    val.startsWith('file://') ? fileURLToPath(val) : val,
  )

  return { httpPort, grpcPort, processorPaths }
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
        quad.predicate.value === OWL_IMPORTS
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
      if (quad.predicate.value !== RDFC + 'jsImplementationOf') continue
      const uri = quad.subject.value
      if (seen.has(uri)) continue
      seen.add(uri)

      const labelQuad = quads.find(
        (q) => q.subject.value === uri && q.predicate.value === RDFS_LABEL,
      )
      const commentQuad = quads.find(
        (q) => q.subject.value === uri && q.predicate.value === RDFS_COMMENT,
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

export async function generateIndexTtl(
  processorPaths: string[],
  cwd: string,
  grpcPort: number,
): Promise<string> {
  const descriptions = await extractProcessorDescriptions(processorPaths)

  const lines = `
@prefix prov: <http://www.w3.org/ns/prov#>.
@prefix sds: <https://w3id.org/sds#>.
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.
@prefix owl: <http://www.w3.org/2002/07/owl#>.
@prefix rdfl: <https://w3id.org/rdf-lens/ontology#>.
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix sh: <http://www.w3.org/ns/shacl#>.
@prefix rdfc: <https://w3id.org/rdf-connect#>.

sds:Activity rdfs:subClassOf prov:Activity.
rdfc:Processor rdfs:subClassOf sds:Activity.
sds:implementationOf rdfs:subPropertyOf rdfs:subClassOf.
rdfc:jsImplementationOf rdfs:subPropertyOf sds:implementationOf.
[ ] a sh:NodeShape;
  sh:targetSubjectsOf rdfc:jsImplementationOf;
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

<jsRunner> a rdfc:HttpRunner;
  rdfc:handlesSubjectsOf rdfc:jsImplementationOf;
  rdfc:grpcPort ${grpcPort}.
`.split('\n')

  for (const desc of descriptions) {
    const relPath = relative(cwd, desc.sourceFile)
    lines.push(`<${desc.uri}> a rdfc:Processor;`)
    if (desc.label) {
      lines.push(
        `  rdfs:label "${desc.label.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}";`,
      )
    }
    if (desc.comment) {
      lines.push(
        `  rdfs:comment "${desc.comment.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}";`,
      )
    }
    lines.push(`  rdfs:isDefinedBy <${relPath}>.`)
    lines.push('')
  }

  return lines.join('\n')
}

export async function serve(configPath: string): Promise<void> {
  const absConfig = resolve(configPath)
  const { httpPort, grpcPort, processorPaths } =
    await parseServerConfig(absConfig)
  const whitelist = await buildWhitelist(processorPaths)
  const cwd = process.cwd()
  const indexTtl = await generateIndexTtl(processorPaths, cwd, grpcPort)
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
  // The runner reverse-upgrades the socket via SocketChannel: an
  // http2.ClientHttp2Session is created directly on orchSocket, and that
  // session is used as the gRPC channel — no phantom TCP connection or local
  // proxy required.
  const tcpServer = createTcpServer(async (orchSocket) => {
    try {
      const uri = await readLine(orchSocket)
      console.log(`Received connection for runner URI: ${uri}`)

      const channel = new SocketChannel(orchSocket)
      const runnerId = state.registerRunner('socket', uri)
      const ctrl = new AbortController()
      activeConnections.add(ctrl)

      start('socket', uri, absConfig, ctrl.signal, state, runnerId, channel)
        .catch((err) => {
          console.error('gRPC connection error:', err)
          state.markError(runnerId)
        })
        .finally(() => {
          activeConnections.delete(ctrl)
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
