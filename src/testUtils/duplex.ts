import { Duplex } from 'stream'
import type {
  ClientDuplexStream,
  InterceptingCall,
} from '@grpc/grpc-js'
import { AuthContext } from '@grpc/grpc-js/build/src/auth-context'

type Matcher<Req, T> = (req: Req) => T | undefined
type Handler<Req, Res> = (req: Req, send: (res: Res) => void) => void

type MatchObject<Req, Res, T> = {
  matcher: Matcher<Req, T>
  handler: Handler<T, Res>
}

let count = 0
export class MockClientDuplexStream<Req, Res>
  extends Duplex
  implements ClientDuplexStream<Req, Res>
{
  private capabilities: Array<MatchObject<Req, Res, unknown>> = []
  private onceCapabilities: Array<MatchObject<Req, Res, unknown>> = []

  public readonly id: number

  call?: InterceptingCall | undefined
  constructor() {
    super({ objectMode: true })
    this.id = count++
  }

  getAuthContext(): AuthContext | null {
    return null
  }

  // ---- SurfaceCall stubs ----
  cancel(): void {}
  getPeer(): string {
    return 'mock-peer'
  }

  // ---- ObjectWritable<Req> ----
  _write(
    chunk: Req,
    _encoding: BufferEncoding,
    callback: (error?: Error | null) => void,
  ): void {
    let handled = false
    // check registered capabilities
    for (const { matcher, handler } of this.capabilities) {
      const o = matcher(chunk)
      if (o !== undefined) {
        handled = true
        handler(o, (res) => this.send(res))
      }
    }

    const newOnce: typeof this.onceCapabilities = []
    for (const { matcher, handler } of this.onceCapabilities) {
      const o = matcher(chunk)
      if (o !== undefined) {
        handled = true
        handler(o, (res) => this.send(res))
      } else {
        newOnce.push({ matcher, handler })
      }
    }

    this.onceCapabilities = newOnce
    if (!handled) {
      console.error('Unhandled!', Object.keys(chunk as object))
    }

    callback()
  }

  // ---- ObjectReadable<Res> ----
  _read(): void {
    // no-op: we push manually with send()
  }

  // ---- gRPC-style helpers ----
  serialize(value: Req): Buffer {
    return Buffer.from(JSON.stringify(value))
  }

  deserialize(chunk: Buffer): Res {
    return JSON.parse(chunk.toString())
  }

  send(response: Res): void {
    this.push(response)
  }

  end(): this {
    this.push(null)
    return this
  }

  // ---- Capability registration ----
  register<T>(matcher: Matcher<Req, T>, handler: Handler<T, Res>): void {
    this.capabilities.push({ matcher, handler })
  }

  registerOnce<T>(matcher: Matcher<Req, T>, handler: Handler<T, Res>): void {
    this.onceCapabilities.push({ matcher, handler })
  }

  awaitMsg<T>(matcher: Matcher<Req, T>): Promise<T> {
    return new Promise((res) => this.registerOnce(matcher, res))
  }
}
