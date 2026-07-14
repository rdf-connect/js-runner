import { createServer, Socket, AddressInfo } from 'node:net'

/**
 * Bridges a pre-existing orchestrator TCP socket to a stock gRPC client.
 *
 * grpc-js can only dial a target by address; it cannot adopt an already-open
 * socket. Rather than reimplement grpc-js's `Channel`/`Call` interface (as a
 * `channelOverride` would require), we stand up a throwaway loopback TCP server
 * and let grpc-js connect to it normally. Its first — and only — connection is
 * piped byte-for-byte to `orchSocket`, so grpc-js runs its full, unmodified
 * channel (framing, compression, keepalive, retries, …) while the traffic
 * actually flows over the orchestrator's socket.
 *
 * The listener is closed the instant grpc-js connects, so no second (phantom)
 * connection can ever be established: `orchSocket` backs exactly one gRPC
 * connection, by construction.
 */
export interface SocketProxy {
  /** `127.0.0.1:<port>` address for a stock `RunnerClient` to dial. */
  readonly target: string
  /** Close the loopback listener and tear down both sockets. */
  close(): void
}

export function createSocketProxy(orchSocket: Socket): Promise<SocketProxy> {
  return new Promise((resolve, reject) => {
    const server = createServer((grpcSocket) => {
      // First and only connection: stop listening so grpc-js can never open a
      // second backing connection to this ephemeral port.
      server.close()

      const destroyBoth = () => {
        orchSocket.destroy()
        grpcSocket.destroy()
      }
      orchSocket.once('error', destroyBoth)
      grpcSocket.once('error', destroyBoth)

      // Bidirectional byte pump. `pipe` also forwards any bytes previously
      // `unshift`ed back onto orchSocket (the orchestrator's HTTP/2 preface
      // that arrived in the same TCP segment as the runner URI line), and
      // propagates end-of-stream in both directions.
      orchSocket.pipe(grpcSocket)
      grpcSocket.pipe(orchSocket)
    })

    server.once('error', reject)

    // Loopback only — the bridge must never be reachable beyond this host.
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address() as AddressInfo
      resolve({
        target: `127.0.0.1:${port}`,
        close: () => {
          // Idempotent: closing an already-closed server is a no-op. Destroying
          // orchSocket cascades through the pipe to close the gRPC side too.
          server.close()
          orchSocket.destroy()
        },
      })
    })
  })
}
