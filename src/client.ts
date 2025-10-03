import * as grpc from '@grpc/grpc-js'
import { promisify } from 'util'
import { RunnerClient, ToRunner } from '@rdfc/proto'
import winston from 'winston'
import { RpcTransport } from './logger'
import { Runner } from './runner'

export async function start(addr: string, uri: string) {
  const client = new RunnerClient(addr, grpc.credentials.createInsecure())

  const logger = winston.createLogger({
    transports: [
      new RpcTransport({
        entities: [uri, 'cli'],
        stream: client.logStream(() => {}),
      }),
    ],
  })

  const stream = client.connect()

  logger.info('Connected with server ' + addr)
  const writable = promisify(stream.write.bind(stream))
  const runner = new Runner(client, writable, uri, logger)

  await writable({ identify: { uri } })

  /* eslint-disable no-async-promise-executor */
  await new Promise(async (res) => {
    for await (const chunk of stream) {
      const msg: ToRunner = chunk
      if (msg.proc) {
        await runner.addProcessor(msg.proc)
      }
      if (msg.start) {
        runner.start().then(res)
      }

      await runner.handleOrchMessage(msg)
    }

    logger.error('Stream ended')
  })

  logger.info('All processors are finished')
  stream.end()
  client.close()
  setTimeout(() => process.exit(0), 500)
}
