import { afterEach, describe, expect, test } from 'vitest'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createServer, Server } from 'node:net'

// server.ts uses $INLINE_FILE, which only the ts-patch build applies — vitest's
// esbuild transform would leave the macro unresolved. `npm test` builds first,
// so import the built module here.
import { parseRequestPath, parseServerConfig, serve } from '../lib/server.js'

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
