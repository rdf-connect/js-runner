export type RunnerStatus = 'connecting' | 'running' | 'done' | 'error'

export interface ChannelStats {
  uri: string
  role: 'reader' | 'writer'
  messageCount: number
  bytesTotal: number
  lastMessageAt: number | null
  /** Last N round-trip latencies in ms (writers only) */
  latenciesMs: number[]
}

export interface RunnerStats {
  id: string
  host: string
  uri: string
  connectedAt: number
  status: RunnerStatus
  grpcState: string
  channels: Record<string, ChannelStats>
}

export interface ChannelTracker {
  recordMessage(bytes: number, latencyMs?: number): void
}

const MAX_LATENCY_SAMPLES = 100

export class State {
  private readonly runners: Map<string, RunnerStats> = new Map()
  private nextId = 1

  registerRunner(host: string, uri: string): string {
    const id = String(this.nextId++)
    this.runners.set(id, {
      id,
      host,
      uri,
      connectedAt: Date.now(),
      status: 'connecting',
      grpcState: 'IDLE',
      channels: {},
    })
    return id
  }

  setStatus(id: string, status: RunnerStatus): void {
    const r = this.runners.get(id)
    if (r) r.status = status
  }

  setGrpcState(id: string, state: string): void {
    const r = this.runners.get(id)
    if (r) r.grpcState = state
  }

  deregisterRunner(id: string): void {
    const r = this.runners.get(id)
    if (r && r.status !== 'error') r.status = 'done'
  }

  markError(id: string): void {
    const r = this.runners.get(id)
    if (r) r.status = 'error'
  }

  trackChannel(
    runnerId: string,
    uri: string,
    role: 'reader' | 'writer',
  ): ChannelTracker {
    const runner = this.runners.get(runnerId)
    if (!runner) return { recordMessage() {} }

    if (!runner.channels[uri]) {
      runner.channels[uri] = {
        uri,
        role,
        messageCount: 0,
        bytesTotal: 0,
        lastMessageAt: null,
        latenciesMs: [],
      }
    }

    const channel = runner.channels[uri]
    return {
      recordMessage(bytes: number, latencyMs?: number): void {
        channel.messageCount++
        channel.bytesTotal += bytes
        channel.lastMessageAt = Date.now()
        if (latencyMs !== undefined) {
          channel.latenciesMs.push(latencyMs)
          if (channel.latenciesMs.length > MAX_LATENCY_SAMPLES) {
            channel.latenciesMs.shift()
          }
        }
      },
    }
  }

  snapshot(): RunnerStats[] {
    return [...this.runners.values()]
  }
}
