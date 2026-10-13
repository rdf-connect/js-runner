import { createLogger, LogEntry, Logger } from 'winston'
import Transport from 'winston-transport'
import { setWarningHandler } from 'rdf-lens'

import * as grpc from '@grpc/grpc-js'
import { LogMessage } from '@rdfc/proto'

export class RpcTransport extends Transport {
  private readonly stream: grpc.ClientWritableStream<LogMessage>
  private readonly entities: string[]
  private readonly aliases: string[]
  private draining: boolean

  constructor(opts: {
    stream: grpc.ClientWritableStream<LogMessage>
    entities: string[]
    aliases?: string[]
  }) {
    super({ level: 'debug' })

    this.stream = opts.stream
    this.entities = opts.entities
    this.aliases = opts.aliases || []
    this.draining = true

    this.stream.on('drain', () => {
      this.draining = true
    })
  }

  log(info: LogEntry, callback: () => void) {
    if (!this.stream.closed && !this.stream.destroyed && this.stream.writable) {
      if (this.draining) {
        const ok = this.stream.write({
          msg: info.message,
          level: info.level,
          entities: this.entities,
          aliases: this.aliases,
        })
        if (!ok) {
          this.draining = false
        }
      }
    } else {
      console.log('Output stream closed')
    }
    callback()
  }

  withEntity(entity: string): RpcTransport {
    return new RpcTransport({
      stream: this.stream,
      entities: [...this.entities, entity],
      aliases: this.aliases,
    })
  }
}

/**
 * withLensWarnings runs fn with the warnings rdf-lens gives about shapes, such
 * as a deprecated sh:datatype, logged to logger instead of console.warn. The
 * handler is global and one process can host many runners, so it is only set
 * for this synchronous call.
 */
export function withLensWarnings<T>(logger: Logger, fn: () => T): T {
  setWarningHandler((message) => logger.warn(message))
  try {
    return fn()
  } finally {
    setWarningHandler()
  }
}

export function extendLogger(baseLogger: Logger, newEntity: string): Logger {
  const newTransports = baseLogger.transports.map((t) => {
    if (t instanceof RpcTransport) {
      return t.withEntity(newEntity)
    }
    return t
  })

  return createLogger({
    level: baseLogger.level,
    format: baseLogger.format,
    defaultMeta: baseLogger.defaultMeta,
    transports: newTransports,
  })
}
