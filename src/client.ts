import * as grpc from '@grpc/grpc-js'
import { promisify } from 'util'
import { RunnerClient, ToRunner } from '@rdfc/proto'
import { createLogger } from 'winston'
import { RpcTransport } from './logger'
import { Runner } from './runner'
import { State } from './state'

const GRPC_STATE_NAMES = [
  'IDLE',
  'CONNECTING',
  'READY',
  'TRANSIENT_FAILURE',
  'SHUTDOWN',
] as const

function watchGrpcState(
  client: RunnerClient,
  state: State,
  runnerId: string,
  signal: AbortSignal,
): void {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const ch = (client as any).getChannel() as grpc.Channel

  const update = () => {
    if (signal.aborted) return
    const s = ch.getConnectivityState(false)
    state.setGrpcState(runnerId, GRPC_STATE_NAMES[s] ?? String(s))
    if (s === grpc.connectivityState.SHUTDOWN) return
    // Watch for the next state change (1-hour deadline)
    ch.watchConnectivityState(s, new Date(Date.now() + 3_600_000), update)
  }

  update()
}

export async function start(
  addr: string,
  uri: string,
  configPath?: string,
  signal?: AbortSignal,
  state?: State,
  runnerId?: string,
) {
  const client = new RunnerClient(addr, grpc.credentials.createInsecure())

  const logStream = client.logStream(() => {})

  const logger = createLogger({
    transports: [
      new RpcTransport({
        entities: [uri, 'cli'],
        stream: logStream,
      }),
    ],
  })

  const stream = client.connect()

  const closeConnection = () => {
    stream.end()
    logStream.end()
    client.close()
  }

  const abortCtrl = signal ?? new AbortController().signal
  abortCtrl.addEventListener('abort', closeConnection, { once: true })

  if (state && runnerId) {
    watchGrpcState(client, state, runnerId, abortCtrl)
  }

  try {
    logger.info('Connected with server ' + addr)
    const writable = promisify(stream.write.bind(stream))
    const runner = new Runner(
      client,
      writable,
      uri,
      logger,
      configPath,
      state,
      runnerId,
    )

    await writable({ identify: { uri } })

    let runnerDone = false
    /* eslint-disable no-async-promise-executor */
    await new Promise(async (res, rej) => {
      try {
        for await (const chunk of stream) {
          const msg: ToRunner = chunk
          if (msg.proc) {
            await runner.addProcessor(msg.proc)
          }
          if (msg.start) {
            if (state && runnerId) state.setStatus(runnerId, 'running')
            runner.start().then(() => {
              runnerDone = true
              res(undefined)
            })
          }

          await runner.handleOrchMessage(msg)
        }

        logger.error('Stream ended')
      } catch (err) {
        if (runnerDone) {
          // Stream error after runner finished (e.g. connection dropped during cleanup) — safe to ignore
          return
        }
        rej(err)
      }
    })

    logger.info('All processors are finished')
  } finally {
    abortCtrl.removeEventListener('abort', closeConnection)
    closeConnection()
  }
}
