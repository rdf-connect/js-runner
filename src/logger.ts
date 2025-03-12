import winston from 'winston'
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

  log(info: winston.LogEntry, callback: () => void) {
    this.stream.write(
      {
        msg: info.message,
        level: info.level,
        entities: this.entities,
        aliases: this.aliases,
      },
      callback,
    )
  }
}
