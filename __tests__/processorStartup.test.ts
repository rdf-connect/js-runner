import { describe, expect, test } from 'vitest'
import { FromRunner, RunnerClient } from '@rdfc/proto'
import { createLogger } from 'winston'
import Transport from 'winston-transport'
import path from 'node:path'
import { pathToFileURL } from 'node:url'
import { Processor } from '../src/processor'
import { Runner } from '../src/runner'

export class FailingInitProcessor extends Processor<{ message: string }> {
  async init(): Promise<void> {
    throw new Error('the database is down')
  }
  async transform(): Promise<void> {}
  async produce(): Promise<void> {}
}

export class StartingProcessor extends Processor<{ message: string }> {
  async init(): Promise<void> {}
  async transform(): Promise<void> {}
  async produce(): Promise<void> {}
}

class CaptureTransport extends Transport {
  readonly lines: string[] = []
  log(info: { level: string; message: unknown }, callback: () => void) {
    this.lines.push(`${info.level}: ${info.message}`)
    callback()
  }
}

const PROCESSOR = 'http://example.com/ns#processor'
const THIS_FILE = pathToFileURL(
  path.resolve('__tests__/processorStartup.test.ts'),
).href

/**
 * A runner with a pipeline holding one processor, whose message is the given
 * term, and what it tells the orchestrator and logs.
 */
async function runnerWith(message: string) {
  const sent: FromRunner[] = []
  const logs = new CaptureTransport()
  const runner = new Runner(
    <RunnerClient>{},
    async (msg) => sent.push(msg),
    'http://example.com/ns#runner',
    createLogger({ transports: [logs] }),
  )

  await runner.handleOrchMessage({
    pipeline: `
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix sh: <http://www.w3.org/ns/shacl#>.

[ ] a sh:NodeShape;
  sh:targetClass rdfc:Test;
  sh:property [
    sh:path rdfc:message;
    sh:name "message";
    sh:nodeKind sh:Literal;
    sh:minCount 1;
    sh:maxCount 1;
  ].

<${PROCESSOR}> a rdfc:Test;
  rdfc:message ${message}.
`,
  })

  const addProcessor = (file: string, clazz: string) =>
    runner.addProcessor({
      uri: PROCESSOR,
      config: JSON.stringify({ file, clazz, location: THIS_FILE }),
      arguments: '',
    })

  return { addProcessor, sent, logs }
}

describe('Processor startup', () => {
  test('a processor file that cannot be imported is reported', async () => {
    const { addProcessor, sent, logs } = await runnerWith('"hello"')
    const missing = pathToFileURL(path.resolve('__tests__/missing.js')).href

    await expect(addProcessor(missing, 'Missing')).rejects.toThrow(
      'Cannot find module',
    )
    expect(sent).toEqual([
      {
        initialized: {
          uri: PROCESSOR,
          error: { cause: expect.stringContaining('__tests__/missing.js') },
        },
      },
    ])
    expect(logs.lines).toContainEqual(
      expect.stringMatching(/^error: Processor failed to start: Cannot find/),
    )
  })

  test('arguments that do not fit the shape are reported', async () => {
    const { addProcessor, sent } = await runnerWith('<http://example.org/iri>')

    await expect(
      addProcessor(THIS_FILE, 'StartingProcessor'),
    ).rejects.toBeDefined()
    expect(sent).toEqual([
      {
        initialized: {
          uri: PROCESSOR,
          error: { cause: expect.stringContaining('Node kind violation') },
        },
      },
    ])
  })

  test('a processor whose init fails is reported', async () => {
    const { addProcessor, sent } = await runnerWith('"hello"')

    await expect(
      addProcessor(THIS_FILE, 'FailingInitProcessor'),
    ).rejects.toThrow('the database is down')
    expect(sent).toEqual([
      {
        initialized: {
          uri: PROCESSOR,
          error: { cause: 'the database is down' },
        },
      },
    ])
  })

  test('a processor that starts is reported without an error', async () => {
    const { addProcessor, sent } = await runnerWith('"hello"')

    await addProcessor(THIS_FILE, 'StartingProcessor')
    expect(sent).toEqual([{ initialized: { uri: PROCESSOR } }])
  })
})
