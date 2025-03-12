import * as grpc from '@grpc/grpc-js'
import { promisify } from 'util'
import { RunnerClient, RunnerMessage } from '@rdfc/proto'
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

  let processorsEnd!: (v: unknown) => unknown
  const processorsEnded = new Promise((res) => (processorsEnd = res))
  ;(async () => {
    for await (const chunk of stream) {
      const msg: RunnerMessage = chunk
      if (msg.proc) {
        await runner.addProcessor(msg.proc)
      }
      if (msg.start) {
        runner.start().then(processorsEnd)
      }

      await runner.handleOrchMessage(msg)
    }

    logger.error('Stream ended')
  })()

  await processorsEnded

  logger.info('All processors are finished')
  stream.end()
  client.close()
  process.exit(0)
}
