import { createLogger, LogEntry, Logger } from 'winston'
import Transport from 'winston-transport'

import * as grpc from '@grpc/grpc-js'
import { LogMessage } from '@rdfc/proto'

export class RpcTransport extends Transport {
  private readonly stream: grpc.ClientWritableStream<LogMessage>
  private readonly entities: string[]
  private readonly aliases: string[]

  constructor(opts: {
    stream: grpc.ClientWritableStream<LogMessage>
    entities: string[]
    aliases?: string[]
  }) {
    super({ level: 'debug' })

    this.stream = opts.stream
    this.entities = opts.entities
    this.aliases = opts.aliases || []
  }

  log(info: LogEntry, callback: () => void) {
    if (!this.stream.closed) {
      this.stream.write(
        {
          msg: info.message,
          level: info.level,
          entities: this.entities,
          aliases: this.aliases,
        },
        callback,
      )
    } else {
      console.log('Output stream closed')
      callback()
    }
  }

  withEntity(entity: string): RpcTransport {
    return new RpcTransport({
      stream: this.stream,
      entities: [...this.entities, entity],
      aliases: this.aliases,
    })
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
