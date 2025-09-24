import { Duplex } from "stream";
import type { ClientDuplexStream, InterceptingCallInterface } from "@grpc/grpc-js";
import { AuthContext } from "@grpc/grpc-js/build/src/auth-context";
import { grpc } from "@rdfc/proto";

type Matcher<Req, T> = (req: Req) => T | undefined;
type Handler<Req, Res> = (req: Req, send: (res: Res) => void) => void;

type MatchObject<Req, Res, T> = {
  matcher: Matcher<Req, T>; handler: Handler<T, Res>
}

export class MockClientDuplexStream<Req, Res>
  extends Duplex
  implements ClientDuplexStream<Req, Res> {
  private capabilities: Array<MatchObject<Req, Res, unknown>> = [];
  private onceCapabilities: Array<MatchObject<Req, Res, unknown>> = [];

  call?: InterceptingCallInterface | undefined;
  constructor() {
    super({ objectMode: true });
  }

  getAuthContext(): AuthContext | null {
    return null;
  }

  // ---- SurfaceCall stubs ----
  cancel(): void {
  }
  getPeer(): string {
    return "mock-peer";
  }

  // ---- ObjectWritable<Req> ----
  _write(
    chunk: Req,
    _encoding: BufferEncoding,
    callback: (error?: Error | null) => void
  ): void {
    // check registered capabilities
    for (const { matcher, handler } of this.capabilities) {
      const o = matcher(chunk);
      if (o !== undefined) {
        handler(o, (res) => this.send(res));
      }
    }

    const newOnce: typeof this.onceCapabilities = []
    for (const { matcher, handler } of this.onceCapabilities) {
      const o = matcher(chunk);
      if (o !== undefined) {
        handler(o, (res) => this.send(res));
      } else {
        newOnce.push({ matcher, handler })
      }
    }

    this.onceCapabilities = newOnce

    callback();
  }

  // ---- ObjectReadable<Res> ----
  _read(_size: number): void {
    // no-op: we push manually with send()
  }

  // ---- gRPC-style helpers ----
  serialize(value: Req): Buffer {
    return Buffer.from(JSON.stringify(value));
  }

  deserialize(chunk: Buffer): Res {
    return JSON.parse(chunk.toString());
  }

  send(response: Res): void {
    this.push(response);
  }
  end(chunk?: unknown, encoding?: unknown, cb?: unknown): this {
    this.push(null);
    return this;
  }

  endStream(): void {
    this.push(null);
  }

  // ---- Capability registration ----
  register<T>(matcher: Matcher<Req, T>, handler: Handler<T, Res>): void {
    this.capabilities.push({ matcher, handler });
  }

  registerOnce<T>(matcher: Matcher<Req, T>, handler: Handler<T, Res>): void {
    this.onceCapabilities.push({ matcher, handler });
  }

  awaitMsg<T>(matcher: Matcher<Req, T>): Promise<T> {
    return new Promise(
      res => this.registerOnce(matcher, res)
    )
  }
}
