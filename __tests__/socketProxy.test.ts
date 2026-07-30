import { afterEach, describe, expect, test } from 'vitest'
import { connect, createServer, Server, Socket } from 'node:net'
import { once } from 'node:events'
import { createSocketProxy } from '../src/socketProxy'

const openServers: Server[] = []
const openSockets: Socket[] = []

/**
 * Tracks a socket this test owns. Both ends get torn down abruptly during the
 * test, so swallow the resulting RSTs — a bare socket with no 'error' listener
 * crashes the process.
 */
function track(socket: Socket): Socket {
  socket.on('error', () => {})
  openSockets.push(socket)
  return socket
}

afterEach(async () => {
  for (const socket of openSockets.splice(0)) socket.destroy()
  await Promise.all(
    openServers.splice(0).map(
      (server) =>
        new Promise<void>((res) => {
          server.close(() => res())
        }),
    ),
  )
})

/** A stand-in for the orchestrator's end of the TCP connection. */
async function orchestratorSocket(): Promise<Socket> {
  const sink = createServer(() => {})
  openServers.push(sink)
  await new Promise<void>((res) => sink.listen(0, '127.0.0.1', () => res()))
  const { port } = sink.address() as { port: number }
  const socket = track(connect(port, '127.0.0.1'))
  await once(socket, 'connect')
  return socket
}

describe('createSocketProxy', () => {
  test('survives repeated close() calls after grpc-js has connected', async () => {
    const proxy = await createSocketProxy(await orchestratorSocket())

    const [, portStr] = proxy.target.split(':')
    const grpcSocket = track(connect(Number(portStr), '127.0.0.1'))
    await once(grpcSocket, 'connect')

    // The connection handler already closed the listener; these must not raise
    // an 'error' event on a server that no longer has a listener attached.
    proxy.close()
    proxy.close()
    proxy.close()

    await new Promise((res) => setTimeout(res, 50))
  })
})
