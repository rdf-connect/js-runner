import { createServer, IncomingMessage } from 'node:http'
import { readFile } from 'node:fs/promises'
import { resolve, relative } from 'node:path'
import { Parser } from 'n3'
import { extractShapes } from 'rdf-lens'
import { start } from './client.js'
import { State } from './state.js'

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
    sh:path rdfc:port;
    sh:name "port";
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
  port: number
  processorPaths: string[]
}

interface ProcessorDescription {
  uri: string
  label?: string
  comment?: string
  sourceFile: string
}

function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    let body = ''
    req.on('data', (chunk) => (body += chunk))
    req.on('end', () => resolve(body))
    req.on('error', reject)
  })
}

export async function parseServerConfig(
  configPath: string,
): Promise<ServerConfig> {
  const absConfig = resolve(configPath)
  const content = await readFile(absConfig, { encoding: 'utf8' })
  const quads = new Parser({ baseIRI: 'file://' + absConfig }).parse(content)

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
  }) as { port?: number; processorConfigs?: string[] }

  const port = config.port ?? 3000
  const processorPaths = (config.processorConfigs ?? []).map((val) =>
    val.startsWith('file://') ? val.slice('file://'.length) : val,
  )

  return { port, processorPaths }
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

    const baseIRI = 'file://' + filePath
    const quads = new Parser({ baseIRI }).parse(content)

    for (const quad of quads) {
      if (
        quad.subject.value === baseIRI &&
        quad.predicate.value === OWL_IMPORTS
      ) {
        const importVal = quad.object.value
        if (importVal.startsWith('file://')) {
          todo.push(importVal.slice('file://'.length))
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

    const baseIRI = 'file://' + filePath
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

<runner> a rdfc:HttpRunner;
  rdfc:handlesSubjectsOf rdfc:jsImplementationOf;
  rdfc:endpoint <./connect>.
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

function dashboardHtml(): string {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>js-runner dashboard</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: 'Courier New', monospace; background: #111; color: #ccc; padding: 1.5rem; }
    h1 { color: #7ec8e3; margin-bottom: 1rem; font-size: 1.4rem; }
    #updated { font-size: 0.75rem; color: #555; margin-bottom: 1.5rem; }
    .runner { border: 1px solid #333; border-radius: 6px; padding: 1rem; margin-bottom: 1.25rem; }
    .runner-meta { display: flex; flex-wrap: wrap; gap: 1.5rem; margin-bottom: 0.75rem; font-size: 0.85rem; }
    .runner-meta span { color: #888; }
    .runner-meta strong { color: #aaa; }
    .status { font-weight: bold; }
    .s-connecting { color: #f0a500; }
    .s-running    { color: #4caf50; }
    .s-done       { color: #666; }
    .s-error      { color: #f44336; }
    .g-IDLE              { color: #888; }
    .g-CONNECTING        { color: #f0a500; }
    .g-READY             { color: #4caf50; }
    .g-TRANSIENT_FAILURE { color: #f44336; }
    .g-SHUTDOWN          { color: #666; }
    table { width: 100%; border-collapse: collapse; font-size: 0.8rem; }
    th, td { border: 1px solid #2a2a2a; padding: 0.4rem 0.8rem; text-align: left; white-space: nowrap; }
    th { background: #1a1a1a; color: #7ec8e3; }
    td.uri { color: #aaa; max-width: 28rem; overflow: hidden; text-overflow: ellipsis; }
    td.role-reader { color: #81c784; }
    td.role-writer { color: #64b5f6; }
    .empty { color: #555; font-size: 0.8rem; padding-top: 0.5rem; }
    .no-runners { color: #555; }
  </style>
</head>
<body>
  <h1>js-runner dashboard</h1>
  <div id="updated"></div>
  <div id="content"><p class="no-runners">Loading…</p></div>
  <script>
    const throughputState = {}

    function esc(s) {
      return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    }
    function fmtBytes(b) {
      if (b < 1024) return b + ' B'
      if (b < 1048576) return (b/1024).toFixed(1) + ' KB'
      return (b/1048576).toFixed(1) + ' MB'
    }
    function fmtAge(ms) {
      if (!ms) return '-'
      const s = Math.floor((Date.now() - ms) / 1000)
      if (s < 5) return 'just now'
      if (s < 60) return s + 's ago'
      if (s < 3600) return Math.floor(s/60) + 'm ago'
      return Math.floor(s/3600) + 'h ago'
    }
    function fmtDuration(ms) {
      const s = Math.floor((Date.now() - ms) / 1000)
      if (s < 60) return s + 's'
      if (s < 3600) return Math.floor(s/60) + 'm ' + (s%60) + 's'
      return Math.floor(s/3600) + 'h ' + Math.floor((s%3600)/60) + 'm'
    }
    function pct(arr, p) {
      if (!arr || !arr.length) return '-'
      const s = [...arr].sort((a,b) => a-b)
      return s[Math.floor(p/100*s.length)] + 'ms'
    }
    function avg(arr) {
      if (!arr || !arr.length) return '-'
      return (arr.reduce((a,b)=>a+b,0)/arr.length).toFixed(1) + 'ms'
    }
    function throughput(key, count) {
      const now = Date.now()
      if (!throughputState[key]) { throughputState[key] = {count, time: now}; return '-' }
      const dt = (now - throughputState[key].time) / 1000
      const dc = count - throughputState[key].count
      throughputState[key] = {count, time: now}
      if (dt < 0.1) return '-'
      return (dc / dt).toFixed(1) + '/s'
    }

    function renderChannels(runnerId, channels) {
      const entries = Object.values(channels)
      if (!entries.length) return '<p class="empty">No channels yet.</p>'
      return \`<table>
        <thead>
          <tr>
            <th>Channel URI</th>
            <th>Role</th>
            <th>Messages</th>
            <th>Throughput</th>
            <th>Bytes</th>
            <th>Last msg</th>
            <th>Avg latency</th>
            <th>p50</th>
            <th>p99</th>
          </tr>
        </thead>
        <tbody>
          \${entries.map(ch => {
            const key = runnerId + ':' + ch.uri
            const tp = throughput(key, ch.messageCount)
            return \`<tr>
              <td class="uri" title="\${esc(ch.uri)}">\${esc(ch.uri)}</td>
              <td class="role-\${ch.role}">\${ch.role}</td>
              <td>\${ch.messageCount}</td>
              <td>\${tp}</td>
              <td>\${fmtBytes(ch.bytesTotal)}</td>
              <td>\${fmtAge(ch.lastMessageAt)}</td>
              <td>\${ch.role === 'writer' ? avg(ch.latenciesMs) : '-'}</td>
              <td>\${ch.role === 'writer' ? pct(ch.latenciesMs, 50) : '-'}</td>
              <td>\${ch.role === 'writer' ? pct(ch.latenciesMs, 99) : '-'}</td>
            </tr>\`
          }).join('')}
        </tbody>
      </table>\`
    }

    function render(runners) {
      const el = document.getElementById('content')
      if (!runners.length) {
        el.innerHTML = '<p class="no-runners">No runners registered yet.</p>'
        return
      }
      el.innerHTML = runners.map(r => \`
        <div class="runner">
          <div class="runner-meta">
            <span><strong>URI:</strong> \${esc(r.uri)}</span>
            <span><strong>Host:</strong> \${esc(r.host)}</span>
            <span><strong>Status:</strong> <span class="status s-\${r.status}">\${r.status}</span></span>
            <span><strong>gRPC:</strong> <span class="g-\${r.grpcState}">\${r.grpcState}</span></span>
            <span><strong>Uptime:</strong> \${fmtDuration(r.connectedAt)}</span>
          </div>
          \${renderChannels(r.id, r.channels)}
        </div>
      \`).join('')
    }

    async function refresh() {
      try {
        const res = await fetch('/api/state')
        const data = await res.json()
        render(data)
        document.getElementById('updated').textContent =
          'Last updated: ' + new Date().toLocaleTimeString()
      } catch (e) {
        document.getElementById('content').innerHTML =
          '<p style="color:#f44336">Failed to fetch state: ' + esc(e.message) + '</p>'
      }
    }

    refresh()
    setInterval(refresh, 2000)
  </script>
</body>
</html>`
}

export async function serve(configPath: string): Promise<void> {
  const absConfig = resolve(configPath)
  const { port, processorPaths } = await parseServerConfig(absConfig)
  const whitelist = await buildWhitelist(processorPaths)
  const cwd = process.cwd()
  const indexTtl = await generateIndexTtl(processorPaths, cwd)
  const state = new State()

  const activeConnections = new Set<AbortController>()

  const shutdown = (signal: string) => {
    console.log(
      `\nReceived ${signal}, closing ${activeConnections.size} active gRPC connection(s)...`,
    )
    for (const ctrl of activeConnections) ctrl.abort()
    server.close(() => process.exit(0))
  }

  process.on('SIGINT', () => shutdown('SIGINT'))
  process.on('SIGTERM', () => shutdown('SIGTERM'))

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
      res.end(dashboardHtml())
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

    // --- Connect: instantiate a new runner ---
    if (method === 'POST' && url === '/connect') {
      let body: string
      try {
        body = await readBody(req)
      } catch {
        res.writeHead(400, { 'Content-Type': 'text/plain' })
        res.end('Failed to read request body')
        return
      }

      let parsed: { host?: string; uri?: string }
      try {
        parsed = JSON.parse(body)
      } catch {
        res.writeHead(400, { 'Content-Type': 'text/plain' })
        res.end('Invalid JSON')
        return
      }

      const { host, uri } = parsed
      if (typeof host !== 'string' || typeof uri !== 'string') {
        res.writeHead(400, { 'Content-Type': 'text/plain' })
        res.end('Missing host or uri')
        return
      }

      const runnerId = state.registerRunner(host, uri)
      const ctrl = new AbortController()
      activeConnections.add(ctrl)

      start(host, uri, absConfig, ctrl.signal, state, runnerId)
        .catch((err) => {
          console.error('gRPC connection error:', err)
          state.markError(runnerId)
        })
        .finally(() => {
          activeConnections.delete(ctrl)
          state.deregisterRunner(runnerId)
        })

      res.writeHead(202, {
        'Content-Type': 'application/json',
        Location: '/dashboard',
      })
      res.end(JSON.stringify({ runnerId }))
      return
    }

    res.writeHead(404, { 'Content-Type': 'text/plain' })
    res.end('Not found')
  })

  await new Promise<void>((resolve) => {
    server.listen(port, () => {
      console.log(`js-runner server listening on port ${port}`)
      console.log(`  Dashboard: http://localhost:${port}/dashboard`)
      console.log(`  Health:    http://localhost:${port}/health`)
      console.log(`  State API: http://localhost:${port}/api/state`)
      resolve()
    })
  })
}
