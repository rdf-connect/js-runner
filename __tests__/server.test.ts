import { afterEach, describe, expect, test } from 'vitest'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createServer, Server } from 'node:net'
import { Parser } from 'n3'

// server.ts uses $INLINE_FILE, which only the ts-patch build applies — vitest's
// esbuild transform would leave the macro unresolved. `npm test` builds first,
// so import the built module here.
import {
  buildRoutes,
  generateIndexTtl,
  parseRequestPath,
  parseServerConfig,
  serve,
} from '../lib/server.js'

const tempDirs: string[] = []
const openServers: Server[] = []

async function tempConfig(body: string): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), 'js-runner-server-'))
  tempDirs.push(dir)
  const configPath = join(dir, 'server.ttl')
  await writeFile(
    configPath,
    `@prefix rdfc: <https://w3id.org/rdf-connect#>.\n<> a rdfc:JsRunnerServer;\n${body}`,
  )
  return configPath
}

function occupyPort(): Promise<number> {
  return new Promise((res) => {
    const server = createServer(() => {})
    openServers.push(server)
    server.listen(0, () => {
      res((server.address() as { port: number }).port)
    })
  })
}

afterEach(async () => {
  await Promise.all(
    openServers.splice(0).map(
      (server) =>
        new Promise<void>((res) => {
          server.close(() => res())
        }),
    ),
  )
  await Promise.all(
    tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })),
  )
})

describe('parseServerConfig', () => {
  test('resolves a relative processorConfig against the config directory', async () => {
    const configPath = await tempConfig(
      '  rdfc:processorConfig "sub/procs.ttl".',
    )

    const { processorPaths } = await parseServerConfig(configPath)

    expect(processorPaths).toEqual([join(configPath, '..', 'sub', 'procs.ttl')])
  })
})

describe('parseRequestPath', () => {
  test('drops the query string', () => {
    expect(parseRequestPath('/processors/echo.ttl?v=2')).toBe(
      '/processors/echo.ttl',
    )
  })

  test('percent-decodes escaped characters', () => {
    expect(parseRequestPath('/node_modules/%40rdfc/echo.ttl')).toBe(
      '/node_modules/@rdfc/echo.ttl',
    )
  })

  test('returns undefined for a malformed escape', () => {
    expect(parseRequestPath('/%zz')).toBeUndefined()
  })
})

describe('buildRoutes', () => {
  test('serves files under the config directory at their relative path', () => {
    const routes = buildRoutes(
      [join('/etc/rdfc', 'processors', 'echo.ttl')],
      '/etc/rdfc',
    )

    expect([...routes.byPath]).toEqual([
      ['/processors/echo.ttl', '/etc/rdfc/processors/echo.ttl'],
    ])
    expect(routes.byFile.get('/etc/rdfc/processors/echo.ttl')).toBe(
      '/processors/echo.ttl',
    )
    expect(routes.unreachable).toEqual([])
  })

  test('reports files outside the config directory as unreachable', () => {
    // relative() would give '../../opt/procs/echo.ttl' here; the orchestrator
    // flattens that against the server root, so the file has no valid URL.
    const routes = buildRoutes(['/opt/procs/echo.ttl'], '/etc/rdfc')

    expect([...routes.byPath]).toEqual([])
    expect(routes.unreachable).toEqual(['/opt/procs/echo.ttl'])
  })

  test('does not mistake a leading-dots filename for an escape', () => {
    const routes = buildRoutes(['/etc/rdfc/..echo.ttl'], '/etc/rdfc')

    expect(routes.byPath.get('/..echo.ttl')).toBe('/etc/rdfc/..echo.ttl')
    expect(routes.unreachable).toEqual([])
  })
})

describe('generateIndexTtl', () => {
  test('advertises only paths the server can answer', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'js-runner-index-'))
    tempDirs.push(dir)
    const outside = await mkdtemp(join(tmpdir(), 'js-runner-outside-'))
    tempDirs.push(outside)

    const inside = join(dir, 'echo.ttl')
    const external = join(outside, 'log.ttl')
    await writeFile(
      inside,
      '@prefix rdfc: <https://w3id.org/rdf-connect#>.\nrdfc:EchoProcessor rdfc:jsImplementationOf rdfc:Processor.',
    )
    await writeFile(
      external,
      '@prefix rdfc: <https://w3id.org/rdf-connect#>.\nrdfc:LogProcessor rdfc:jsImplementationOf rdfc:Processor.',
    )

    const routes = buildRoutes([inside, external], dir)
    const ttl = await generateIndexTtl(
      [inside, external],
      routes,
      'localhost',
      50051,
    )

    // Parsed the way an orchestrator would: relative to the index URL.
    const quads = new Parser({ baseIRI: 'http://example.org/' }).parse(ttl)
    const definedBy = quads
      .filter(
        (q) =>
          q.predicate.value ===
          'http://www.w3.org/2000/01/rdf-schema#isDefinedBy',
      )
      .map((q) => q.object.value)

    expect(definedBy).toEqual(['http://example.org/echo.ttl'])
    // Every advertised URL must resolve back to a served route.
    for (const url of definedBy) {
      expect(routes.byPath.has(new URL(url).pathname)).toBe(true)
    }
  })
})

describe('serve', () => {
  test('rejects instead of crashing when the http port is already in use', async () => {
    const httpPort = await occupyPort()
    const grpcPort = await occupyPort()
    const configPath = await tempConfig(
      `  rdfc:httpPort ${httpPort};\n  rdfc:grpcPort ${grpcPort}.`,
    )

    await expect(serve(configPath)).rejects.toThrow(/EADDRINUSE/)
  })
})
