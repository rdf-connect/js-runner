import { describe, expect, test } from 'vitest'
import { Reader } from '../src/reader'
import { createRunner, channel } from '../src/testUtils'
import { Processor } from '../src/processor'
import { Writer } from '../src/writer'
import { FullProc } from '../src/runner'
import winston, { createLogger } from 'winston'

const encoder = new TextEncoder()
const decoder = new TextDecoder()

const logger = createLogger({
  transports: new winston.transports.Console({
    level: process.env['DEBUG'] || 'info',
  }),
})

class EchoProcessor extends Processor<{
  reader: Reader
  writer: Writer
  streams: boolean
}> {
  async init(this: { reader: Reader; writer: Writer } & this): Promise<void> {
    this.logger.info('EchoProcessor initialized')
  }

  async transform(
    this: { reader: Reader; writer: Writer; streams: boolean } & this,
  ): Promise<void> {
    this.logger.info('EchoProcessor transforming')

    if (this.streams) {
      for await (const msg of this.reader.streams()) {
        this.logger.info(`EchoProcessor received stream`)
        await this.writer.stream(msg)
      }
    } else {
      for await (const msg of this.reader.strings()) {
        this.logger.info(`EchoProcessor received: ${msg}`)
        await this.writer.string(msg)
      }
    }

    this.logger.info('EchoProcessor transformed')
    await this.writer.close()
  }

  async produce(
    this: { reader: Reader; writer: Writer } & this,
  ): Promise<void> {
    // Nothing to produce, transform handles everything
  }
}

describe('EchoProcessor', () => {
  test('echoes string messages correctly', async () => {
    const runner = createRunner()

    const [inputWriter, inputReader] = channel(runner, 'input')
    const [outputWriter, outputReader] = channel(runner, 'output')

    const proc = <FullProc<EchoProcessor>>(
      new EchoProcessor(
        { reader: inputReader, writer: outputWriter, streams: false },
        logger,
      )
    )

    await proc.init()
    const transformPromise = proc.transform()

    const msgs: string[] = []

    ;(async () => {
      for await (const m of outputReader.strings()) {
        msgs.push(m)
      }
    })()

    await inputWriter.string('Hello')
    expect(msgs).toEqual(['Hello'])
    await inputWriter.string('world')

    await inputWriter.close()

    // Wait for processing to complete
    await transformPromise

    expect(msgs).toEqual(['Hello', 'world'])
  })

  test('echoes stream messages correctly', async () => {
    const runner = createRunner()

    const [inputWriter, inputReader] = channel(runner, 'input')
    const [outputWriter, outputReader] = channel(runner, 'output')

    const proc = <FullProc<EchoProcessor>>(
      new EchoProcessor(
        { reader: inputReader, writer: outputWriter, streams: true },
        logger,
      )
    )

    await proc.init()
    const transformPromise = proc.transform()

    const msgs: string[] = []

    ;(async () => {
      for await (const m of outputReader.strings()) {
        msgs.push(m)
      }
    })()

    const gen = async function* () {
      yield encoder.encode('Hello')
      yield encoder.encode('World')
    }

    await inputWriter.stream(gen())
    expect(msgs).toEqual(['HelloWorld'])
    await inputWriter.stream(gen())

    await inputWriter.close()

    // Wait for processing to complete
    await transformPromise

    expect(msgs).toEqual(['HelloWorld', 'HelloWorld'])
  })
})
