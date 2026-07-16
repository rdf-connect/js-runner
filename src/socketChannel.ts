/**
 * SocketChannel: wraps a pre-existing TCP socket as a gRPC client channel.
 *
 * grpc-js exposes a `channelOverride` option on `ClientOptions` that accepts
 * any object implementing the `Channel` interface (exported as
 * `grpc.ChannelInterface`).  By implementing that interface over an
 * `http2.ClientHttp2Session` created from `orchSocket`, we can route gRPC
 * traffic through a pre-existing TCP connection without any proxy or sink
 * server.
 *
 * The framing contract mirrors grpc-js internals:
 *   - `sendMessageWithContext` receives already-framed bytes
 *     (5-byte gRPC header + protobuf payload).  The compression filter in
 *     grpc-js adds framing before the message reaches our `Call`.
 *   - `listener.onReceiveMessage` must be called with full framed bytes; the
 *     compression filter strips the 5-byte header before deserialization.
 */
import * as grpc from '@grpc/grpc-js'
import * as http2 from 'node:http2'
import { Socket } from 'node:net'

/** Reassembles gRPC length-prefixed frames from a stream of HTTP/2 data chunks. */
class FrameDecoder {
  private buf = Buffer.alloc(0)

  write(data: Buffer): Buffer[] {
    this.buf = Buffer.concat([this.buf, data])
    const frames: Buffer[] = []
    while (this.buf.length >= 5) {
      const msgLen = this.buf.readUInt32BE(1)
      if (this.buf.length < 5 + msgLen) break
      frames.push(this.buf.slice(0, 5 + msgLen))
      this.buf = this.buf.slice(5 + msgLen)
    }
    return frames
  }
}

let callCounter = 0
let channelCounter = 0

/** Implements the internal grpc-js `Call` interface over an HTTP/2 stream. */
class SocketCall {
  private stream?: http2.ClientHttp2Stream
  private statusDelivered = false
  private readonly number = ++callCounter

  constructor(
    private readonly session: http2.ClientHttp2Session,
    private readonly method: string,
  ) {}

  start(metadata: grpc.Metadata, listener: grpc.InterceptingListener): void {
    const reqHeaders: http2.OutgoingHttpHeaders = {
      ':method': 'POST',
      ':path': this.method,
      'content-type': 'application/grpc',
      te: 'trailers',
      ...metadata.toHttp2Headers(),
    }

    this.stream = this.session.request(reqHeaders, { endStream: false })
    const decoder = new FrameDecoder()

    this.stream.once('response', (headers, flags) => {
      if (flags & http2.constants.NGHTTP2_FLAG_END_STREAM) {
        this.deliverStatus(headers as Record<string, string>, listener)
      } else {
        listener.onReceiveMetadata(
          grpc.Metadata.fromHttp2Headers(headers as http2.IncomingHttpHeaders),
        )
      }
    })

    this.stream.on('data', (chunk: Buffer) => {
      for (const frame of decoder.write(chunk)) {
        listener.onReceiveMessage(frame)
      }
    })

    this.stream.once('trailers', (headers) => {
      this.deliverStatus(headers as Record<string, string>, listener)
    })

    this.stream.once('error', (err) => {
      if (!this.statusDelivered) {
        this.statusDelivered = true
        listener.onReceiveStatus({
          code: grpc.status.UNAVAILABLE,
          details: err.message,
          metadata: new grpc.Metadata(),
        })
      }
    })
  }

  private deliverStatus(
    headers: Record<string, string>,
    listener: grpc.InterceptingListener,
  ): void {
    if (this.statusDelivered) return
    this.statusDelivered = true

    let meta: grpc.Metadata
    try {
      meta = grpc.Metadata.fromHttp2Headers(
        headers as http2.IncomingHttpHeaders,
      )
    } catch {
      meta = new grpc.Metadata()
    }

    const map = meta.getMap()
    const code =
      typeof map['grpc-status'] === 'string'
        ? parseInt(map['grpc-status'], 10)
        : grpc.status.UNKNOWN

    let details = ''
    if (typeof map['grpc-message'] === 'string') {
      try {
        details = decodeURI(map['grpc-message'])
      } catch {
        details = map['grpc-message']
      }
    }

    meta.remove('grpc-status')
    meta.remove('grpc-message')
    listener.onReceiveStatus({ code, details, metadata: meta })
  }

  sendMessageWithContext(
    context: { callback?: (err?: Error) => void },
    message: Buffer,
  ): void {
    if (!this.stream) return
    // Use nextTick to avoid potential stack overflows in write callbacks,
    // mirroring grpc-js SubchannelCall behaviour.
    process.nextTick(() => {
      this.stream!.write(message, (err) => {
        process.nextTick(() => context.callback?.(err ?? undefined))
      })
    })
  }

  startRead(): void {
    // HTTP/2 manages flow control automatically; no-op.
  }

  halfClose(): void {
    this.stream?.end()
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  cancelWithStatus(_status: grpc.status, _details: string): void {
    this.stream?.close(grpc.status.CANCELLED)
  }

  getPeer(): string {
    return 'socket'
  }

  getCallNumber(): number {
    return this.number
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  setCredentials(_credentials: grpc.CallCredentials): void {}

  getAuthContext(): null {
    return null
  }
}

/**
 * A gRPC `Channel` that routes all calls through a pre-existing TCP socket
 * by creating an `http2.ClientHttp2Session` directly on `orchSocket`.
 *
 * Pass an instance as `channelOverride` in `ClientOptions`:
 * ```ts
 * new RunnerClient('socket', grpc.credentials.createInsecure(), {
 *   channelOverride: new SocketChannel(orchSocket) as unknown as grpc.Channel,
 * })
 * ```
 */
export class SocketChannel {
  private readonly session: http2.ClientHttp2Session
  // ChannelRef shape — not exported from @grpc/grpc-js main index, so typed as any.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private readonly ref: any = {
    kind: 'channel',
    id: ++channelCounter,
    name: 'socket-channel',
  }
  private closed = false

  constructor(orchSocket: Socket) {
    // `http2.connect()` requires a URL to determine whether to use TLS
    // (`https://`) or plain-text HTTP/2 (`http://`).  Since gRPC runs over
    // HTTP/2, and the socket is already established (no TLS), we use `http://`.
    // The hostname is a placeholder — `createConnection` overrides the actual
    // TCP connection, so nothing is ever resolved or dialled.
    this.session = http2.connect('http://localhost/', {
      createConnection: () => orchSocket,
    })
    // Surface session errors as close events so watchConnectivityState fires.
    this.session.on('error', () => this.session.close())
  }

  close(): void {
    this.closed = true
    this.session.close()
  }

  getTarget(): string {
    return 'socket'
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  getConnectivityState(_tryToConnect: boolean): grpc.connectivityState {
    if (this.closed || this.session.destroyed) {
      return grpc.connectivityState.SHUTDOWN
    }
    return grpc.connectivityState.READY
  }

  watchConnectivityState(
    _currentState: grpc.connectivityState,
    _deadline: Date | number,
    callback: (error?: Error) => void,
  ): void {
    if (this.closed || this.session.destroyed) {
      callback()
      return
    }
    this.session.once('close', () => callback())
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  getChannelzRef(): any {
    return this.ref
  }

  // Return type is `any` because the internal `Call` interface from
  // `@grpc/grpc-js/build/src/call-interface` is not re-exported from the
  // main package index.
  createCall(
    method: string,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _deadline: unknown,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _host: unknown,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _parent: unknown,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _flags: unknown,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  ): any {
    return new SocketCall(this.session, method)
  }
}
