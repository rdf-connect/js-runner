import { afterEach, describe, expect, test, vi } from 'vitest'
import * as grpc from '@grpc/grpc-js'
import {
  FromRunner,
  LogMessage,
  RunnerServer,
  RunnerService,
  ToRunner,
} from '@rdfc/proto'
import path from 'node:path'
import { pathToFileURL } from 'node:url'
import { start } from '../src/client'

const RUNNER = 'http://example.com/ns#runner'
const PROCESSOR = 'http://example.com/ns#processor'

/**
 * An orchestrator that records what a runner sends it, and that the test
 * drives.
 */
async function fakeOrchestrator() {
  const received: FromRunner[] = []
  const logs: string[] = []
  let call: grpc.ServerDuplexStream<FromRunner, ToRunner> | undefined

  const server = new grpc.Server()
  const service: RunnerServer = {
    connect: (c) => {
      call = c
      c.on('data', (msg: FromRunner) => received.push(msg))
      // Like the orchestrator, end the call when the runner does
      c.on('end', () => c.end())
    },
    logStream: (c, callback) => {
      c.on('data', (msg: LogMessage) => logs.push(`${msg.level}: ${msg.msg}`))
      c.on('end', () => callback(null, {}))
    },
    sendStreamMessage: () => {},
    receiveStreamMessage: () => {},
  }
  server.addService(RunnerService, service)

  const port = await new Promise<number>((res, rej) =>
    server.bindAsync(
      '127.0.0.1:0',
      grpc.ServerCredentials.createInsecure(),
      (err, port) => (err ? rej(err) : res(port)),
    ),
  )

  const connected = async () => {
    await vi.waitFor(() => expect(call).toBeDefined())
    return call!
  }

  return { server, addr: `127.0.0.1:${port}`, received, logs, connected }
}

let orchestrator: Awaited<ReturnType<typeof fakeOrchestrator>> | undefined

afterEach(() => {
  orchestrator?.server.forceShutdown()
  orchestrator = undefined
})

describe('A processor that fails to start', () => {
  test('is reported to the orchestrator, which decides to stop', async () => {
    orchestrator = await fakeOrchestrator()
    const running = start(orchestrator.addr, RUNNER)
    running.catch(() => {})

    const call = await orchestrator.connected()
    const missing = pathToFileURL(path.resolve('__tests__/missing.js')).href
    call.write({
      pipeline: `
@prefix sh: <http://www.w3.org/ns/shacl#>.
@prefix ex: <http://example.org/>.
[ ] a sh:NodeShape; sh:targetClass ex:Thing.
<${PROCESSOR}> a ex:Thing.
`,
    })
    call.write({
      proc: {
        uri: PROCESSOR,
        config: JSON.stringify({ file: missing, location: missing }),
        arguments: '',
      },
    })

    // The runner reports why, and logs it, both reaching the orchestrator
    await vi.waitFor(() =>
      expect(orchestrator!.received).toContainEqual({
        initialized: {
          uri: PROCESSOR,
          error: { cause: expect.stringContaining('Cannot find module') },
        },
      }),
    )
    await vi.waitFor(() =>
      expect(orchestrator!.logs).toContainEqual(
        expect.stringMatching(/^error: Processor failed to start: Cannot find/),
      ),
    )

    // Then waits for the orchestrator, instead of leaving on its own
    const settled = await Promise.race([
      running.then(
        () => 'resolved',
        () => 'rejected',
      ),
      new Promise((res) => setTimeout(() => res('pending'), 100)),
    ])
    expect(settled).toBe('pending')

    // Once the orchestrator stops, the runner ends with that error
    call.end()
    await expect(running).rejects.toThrow('Cannot find module')
  })
})
