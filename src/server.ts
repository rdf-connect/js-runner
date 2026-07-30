import { createServer } from 'node:http'
import { createServer as createTcpServer, Socket } from 'node:net'
import { readFile } from 'node:fs/promises'
import { dirname, resolve, relative, isAbsolute, sep } from 'node:path'
import { pathToFileURL, fileURLToPath } from 'node:url'
import { Parser, DataFactory, Writer } from 'n3'
import { extractShapes } from 'rdf-lens'
import { createLogger, format, transports } from 'winston'
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

// The standalone server has no orchestrator to stream logs to — RpcTransport is
// per-runner and only exists once a pipeline connects — so it logs to its own
// console. LOG_LEVEL=debug adds a line per served HTTP request.
const logger = createLogger({
  level: process.env['LOG_LEVEL'] ?? 'info',
  transports: [
    new transports.Console({
      format: format.combine(
        format.timestamp(),
        format.printf(
          ({ timestamp, level, message }) =>
            `${timestamp} ${level.padEnd(5)} ${message}`,
        ),
      ),
    }),
  ],
})

interface ServerConfig {
  httpPort: number
  grpcPort: number
  hostname: string
  historySize: number
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
    historySize?: number
  }

  const hostname = config.hostname ?? 'localhost'
  const httpPort = config.httpPort ?? 3000
  const grpcPort = config.grpcPort ?? 50051
  const historySize = config.historySize ?? 5
  // Both the whitelist and the HTTP handler key on absolute paths, so anchor
  // relative values on the config's own directory here — leaving them relative
  // would load fine at startup but never match a request.
  const configDir = dirname(absConfig)
  const processorPaths = (config.processorConfigs ?? []).map((val) =>
    val.startsWith('file://') ? fileURLToPath(val) : resolve(configDir, val),
  )

  return { httpPort, grpcPort, processorPaths, hostname, historySize }
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
    } catch (err) {
      // Stays whitelisted: a config listed by the user is still the path we
      // want to serve if it reappears. But a typo'd path would otherwise only
      // ever surface as a 404 on the orchestrator's side.
      logger.warn(`Cannot read processor config ${filePath}: ${err}`)
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

export interface Routes {
  /** Request path (e.g. `/processors/echo.ttl`) -> absolute file path. */
  byPath: Map<string, string>
  /** Absolute file path -> the request path it is served at. */
  byFile: Map<string, string>
  /**
   * Whitelisted files that lie outside the config directory. The HTTP root maps
   * onto that directory, so no request path can address them.
   */
  unreachable: string[]
}

/**
 * Assigns every whitelisted file the request path it is served at, once, so
 * that index.ttl and the HTTP handler cannot disagree.
 *
 * Deriving the two independently — `relative(cwd, file)` when advertising and
 * `resolve(cwd, path)` when serving — is not a round trip for files outside
 * `cwd`: `relative()` emits `../…`, which the orchestrator's URL resolution
 * flattens against the server root before it ever reaches `resolve()`, so the
 * advertised URL 403s. Such files have no valid URL here at all; they are
 * reported separately rather than advertised.
 */
export function buildRoutes(files: Iterable<string>, cwd: string): Routes {
  const byPath = new Map<string, string>()
  const byFile = new Map<string, string>()
  const unreachable: string[] = []

  for (const file of files) {
    const rel = relative(cwd, file)
    if (
      rel === '' ||
      rel === '..' ||
      rel.startsWith('..' + sep) ||
      isAbsolute(rel)
    ) {
      unreachable.push(file)
      continue
    }

    // Request paths are always '/'-separated, whatever the platform uses.
    const path = '/' + rel.split(sep).join('/')
    byPath.set(path, file)
    byFile.set(file, path)
  }

  return { byPath, byFile, unreachable }
}

/**
 * Reduces a raw `req.url` to the path it addresses: query string dropped and
 * percent-escapes decoded. Both matter for the route lookup, which is an exact
 * match against the advertised paths — `/processors/echo.ttl?v=2` and
 * `/node_modules/%40rdfc/echo.ttl` must resolve to the same files as their
 * plain spellings instead of 403ing.
 *
 * @returns the decoded path, or undefined if the URL is malformed
 */
export function parseRequestPath(url: string): string | undefined {
  try {
    // The base is irrelevant — req.url is always origin-relative — but URL
    // needs one to parse.
    return decodeURIComponent(new URL(url, 'http://localhost').pathname)
  } catch {
    return undefined
  }
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
  routes: Routes,
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
    const path = routes.byFile.get(desc.sourceFile)
    if (path === undefined) {
      // buildRoutes() already reported why; advertising the processor anyway
      // would hand the orchestrator a URL this server answers with a 403.
      logger.warn(
        `Not advertising processor <${desc.uri}>: ${desc.sourceFile} is not reachable over HTTP`,
      )
      continue
    }

    quads.push(
      quad(namedNode(desc.uri), RDF.terms.type, RDFC.terms.Processor),
      quad(namedNode(desc.uri), RDFS.terms.isDefinedBy, namedNode(path)),
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
  const { httpPort, grpcPort, processorPaths, hostname, historySize } =
    await parseServerConfig(absConfig)
  const whitelist = await buildWhitelist(processorPaths)
  // Anchor HTTP-served relative paths on the config file's own directory, not
  // the shell's cwd — Runner.makeRelative re-resolves fetched file URLs
  // against dirname(configPath), so the two must agree regardless of where
  // js-runner-server was invoked from.
  const cwd = dirname(absConfig)
  const routes = buildRoutes(whitelist, cwd)

  logger.info(`Serving config ${absConfig}`)
  for (const [path, file] of routes.byPath) {
    logger.info(`  ${path} -> ${file}`)
  }
  for (const file of routes.unreachable) {
    logger.warn(
      `Cannot serve ${file}: it lies outside the config directory ${cwd}, which the HTTP root maps onto. ` +
        `Move it under that directory (or move ${absConfig} up to a common parent) to make it reachable.`,
    )
  }

  const indexTtl = await generateIndexTtl(
    processorPaths,
    routes,
    hostname,
    grpcPort,
  )

  const state = new State(historySize)

  const activeConnections = new Set<AbortController>()

  const shutdown = (signal: string) => {
    logger.info(
      `Received ${signal}, closing ${activeConnections.size} active gRPC connection(s)...`,
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
    // A bare Node socket throws (crashing the process) if 'error' fires with
    // no listener attached. readLine() and createSocketProxy() each manage
    // their own listener for the phase they own, but there are async gaps
    // between those phases (e.g. the loopback server's listen() call,
    // grpc-js lazily dialling it) where neither has one attached yet. This
    // listener spans the whole connection lifetime so one is always present;
    // it also records the error and force-destroys the socket so the check
    // below can detect a death that happened before a runner was registered.
    let socketError: Error | undefined
    orchSocket.on('error', (err) => {
      socketError = err
      orchSocket.destroy()
    })

    try {
      const uri = await readLine(orchSocket)
      const connectedAt = Date.now()
      logger.info(`Orchestrator connected for runner URI: ${uri}`)

      const proxy = await createSocketProxy(orchSocket)

      if (socketError) {
        // orchSocket died while the loopback bridge was being set up (no
        // grpc-js connection ever arrived to bridge). There's nothing to run
        // a pipeline over, so tear the proxy down instead of registering a
        // runner and calling start() against a dead socket — grpc-js would
        // still connect to the still-listening loopback server, then hang
        // piping from an already-destroyed source instead of failing fast.
        logger.error(
          `Orchestrator disconnected for runner URI ${uri} before gRPC bridge was ready: ${socketError.message}`,
        )
        proxy.close()
        return
      }

      const runnerId = state.registerRunner('socket', uri)
      const ctrl = new AbortController()
      activeConnections.add(ctrl)

      start(proxy.target, uri, absConfig, ctrl.signal, state, runnerId)
        .catch((err) => {
          const message = err instanceof Error ? err.message : String(err)
          logger.error(
            `gRPC connection error for runner URI ${uri}: ${message}`,
          )
          state.markError(runnerId)
        })
        .finally(() => {
          const connectedSecs = ((Date.now() - connectedAt) / 1000).toFixed(1)
          logger.info(
            `Orchestrator disconnected for runner URI: ${uri} (connected for ${connectedSecs}s)`,
          )
          activeConnections.delete(ctrl)
          proxy.close()
          state.deregisterRunner(runnerId)
        })
    } catch (err) {
      logger.error(`TCP handler error: ${err}`)
      orchSocket.destroy()
    }
  })

  // ── HTTP server: serves index.ttl, processor configs, and dashboard ───────
  const server = createServer(async (req, res) => {
    const method = req.method ?? 'GET'
    const path = parseRequestPath(req.url ?? '/')

    if (path === undefined) {
      logger.warn(`400 ${method} ${req.url}: malformed request URL`)
      res.writeHead(400, { 'Content-Type': 'text/plain' })
      res.end('Bad request')
      return
    }

    logger.debug(`${method} ${path}`)

    // --- Health check ---
    if (method === 'GET' && path === '/health') {
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
    if (method === 'GET' && path === '/api/state') {
      res.writeHead(200, {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-store',
      })
      res.end(JSON.stringify(state.snapshot()))
      return
    }

    // --- Dashboard (HTML) ---
    if (method === 'GET' && path === '/dashboard') {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' })
      res.end(DASHBOARD_HTML)
      return
    }

    // --- Index Turtle ---
    if (method === 'GET' && path === '/') {
      res.writeHead(200, { 'Content-Type': 'text/turtle' })
      res.end(indexTtl)
      return
    }

    // --- Whitelisted processor files ---
    if (method === 'GET') {
      // Exact lookup in the same table index.ttl was generated from, so an
      // advertised URL is by construction a servable one.
      const absPath = routes.byPath.get(path)

      if (absPath === undefined) {
        logger.warn(
          `403 GET ${path}: not one of the ${routes.byPath.size} served processor config(s)`,
        )
        res.writeHead(403, { 'Content-Type': 'text/plain' })
        res.end('Forbidden')
        return
      }

      let content: string
      try {
        content = await readFile(absPath, { encoding: 'utf8' })
      } catch (err) {
        // The path is one the config named, so this is a real problem rather
        // than a stray request: the file is missing (buildWhitelist warned
        // about that at startup) or unreadable.
        logger.error(`404 GET ${path}: cannot read ${absPath}: ${err}`)
        res.writeHead(404, { 'Content-Type': 'text/plain' })
        res.end('Not found')
        return
      }

      res.writeHead(200, { 'Content-Type': 'text/turtle' })
      res.end(content)
      return
    }

    logger.warn(`404 ${method} ${path}: no such route`)
    res.writeHead(404, { 'Content-Type': 'text/plain' })
    res.end('Not found')
  })

  // A listen() failure (EADDRINUSE is the common one — a stale instance, or the
  // default ports taken) surfaces as an 'error' event, not a rejection. Without
  // a listener attached before listen(), Node turns it into an uncaught
  // exception that serve()'s caller cannot catch, and this Promise.all would
  // never settle.
  try {
    await Promise.all([
      new Promise<void>((res, rej) => {
        tcpServer.once('error', rej)
        tcpServer.listen(grpcPort, () => {
          logger.info(`js-runner gRPC TCP server listening on port ${grpcPort}`)
          res()
        })
      }),
      new Promise<void>((res, rej) => {
        server.once('error', rej)
        server.listen(httpPort, () => {
          logger.info(`js-runner HTTP server listening on port ${httpPort}`)
          logger.info(`  Dashboard: http://localhost:${httpPort}/dashboard`)
          logger.info(`  Health:    http://localhost:${httpPort}/health`)
          logger.info(`  State API: http://localhost:${httpPort}/api/state`)
          res()
        })
      }),
    ])
  } catch (err) {
    // One of the two bound successfully in most cases; release it so the
    // process can exit instead of lingering on a half-started server. The
    // callbacks absorb the "not running" error for whichever one never bound.
    tcpServer.close(() => {})
    server.close(() => {})
    throw err
  }
}
