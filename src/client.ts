import * as grpc from '@grpc/grpc-js'
import { promisify } from 'util'
import { RunnerClient, ToRunner } from '@rdfc/proto'
import { createLogger } from 'winston'
import { RpcTransport } from './logger.js'
import { Runner } from './runner.js'
import { State } from './state.js'

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
  // Same rationale as the writer's stream: an abrupt disconnect (e.g. the
  // orchestrator shutting down) can emit 'error' on this stream. It's just a
  // log sink, so swallow it instead of letting an unhandled 'error' event
  // crash the process.
  logStream.on('error', () => {})

  const logger = createLogger({
    transports: [
      new RpcTransport({
        entities: [uri, 'cli'],
        stream: logStream,
      }),
    ],
  })

  const stream = client.connect()
  // The orchestrator ends the call once it read all this runner wrote
  const callEnded = new Promise<void>((res) => {
    stream.once('status', () => res())
    stream.once('error', () => res())
    stream.once('close', () => res())
  })

  const closeConnection = async () => {
    stream.end()
    logStream.end()
    // Closing the client, or exiting right after, drops what was written but
    // not yet sent, such as why a processor failed to start, so give the
    // orchestrator a moment to read it
    await settledWithin(callEnded, 2_000)
    client.close()
  }

  const abortCtrl = signal ?? new AbortController().signal
  const onAbort = () => void closeConnection()
  abortCtrl.addEventListener('abort', onAbort, { once: true })

  if (state && runnerId) {
    watchGrpcState(client, state, runnerId, abortCtrl)
  }

  try {
    logger.info('Connected with server ' + addr)

    if (state && runnerId) state.setStatus(runnerId, 'connecting')
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
    // Why a processor did not start, which this runner reported to the
    // orchestrator, and ends with once the orchestrator stops the pipeline
    let startupError: unknown
    /* eslint-disable no-async-promise-executor */
    await new Promise(async (res, rej) => {
      try {
        for await (const chunk of stream) {
          const msg: ToRunner = chunk
          if (msg.proc) {
            try {
              await runner.addProcessor(msg.proc)
            } catch (err) {
              // Keep reading: leaving the loop cancels the call, dropping the
              // report that was not sent yet
              startupError ??= err
              if (state && runnerId) {
                state.setStatus(runnerId, 'error')
                state.markError(runnerId)
              }
            }
          }
          if (msg.start && startupError) {
            // An orchestrator that does not stop the pipeline when a processor
            // fails to start: do not run it without that processor
            rej(startupError)
          } else if (msg.start) {
            if (state && runnerId) state.setStatus(runnerId, 'running')
            runner.start().then(
              () => {
                runnerDone = true
                if (state && runnerId) state.setStatus(runnerId, 'done')
                res(undefined)
              },
              (err) => {
                runnerDone = true
                if (state && runnerId) {
                  state.setStatus(runnerId, 'error')
                  state.markError(runnerId)
                }
                rej(err)
              },
            )
          }

          await runner.handleOrchMessage(msg)
        }

        // Stream ended without the runner completing normally
        if (!runnerDone) {
          rej(
            startupError ??
              new Error('gRPC stream ended before runner completed'),
          )
        }
      } catch (err) {
        if (runnerDone) {
          // Stream error after runner finished (e.g. connection dropped during cleanup) — safe to ignore
          return
        }
        rej(startupError ?? err)
      }
    })

    logger.info('All processors are finished')
  } finally {
    abortCtrl.removeEventListener('abort', onAbort)
    await closeConnection()
  }
}

/**
 * settledWithin waits for a promise to settle, or for ms to pass.
 */
function settledWithin(promise: Promise<unknown>, ms: number): Promise<void> {
  return new Promise((res) => {
    const timer = setTimeout(res, ms)
    promise.finally(() => {
      clearTimeout(timer)
      res()
    })
  })
}
