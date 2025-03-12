import { Processor, Reader, Writer } from '@rdfc/js-runner'

type EchoArgs = {
  reader: Reader
  writer: Writer
}

export class EchoProcessor extends Processor<EchoArgs> {
  async init(this: this & EchoArgs): Promise<void> {
    this.logger.info('Init echo processor')
    this.setup()
  }

  async setup(this: this & EchoArgs) {
    for await (const msg of this.reader.streams()) {
      this.logger.info('Echoing message')
      await this.writer.stream(msg)
    }
    await this.writer.close()
    this.logger.info('closed')
  }

  async start(this: this & EchoArgs): Promise<void> {
    this.logger.info('starting')
  }
}

type LogArgs = {
  reader: Reader
}

export class LogProcessor extends Processor<LogArgs> {
  async init(): Promise<void> {
    this.logger.info('Init log processor')
    this.setup()
  }

  async setup() {
    for await (const msg of this.get('reader').strings()) {
      this.logger.info('Got msg' + msg)
    }

    this.logger.info('Closed')
  }

  async start(): Promise<void> {
    this.logger.info('Start log processor')
  }
}

type SendArgs = {
  msgs: string[]
  writer: Writer
}

export class SendProcessor extends Processor<SendArgs> {
  async init(): Promise<void> {
    this.logger.info('Init send processor')
  }

  async start(): Promise<void> {
    for (const msg of this.get('msgs')) {
      await this.get('writer').string(msg)
      this.logger.info('Sending ' + msg)
      await new Promise((res) => setTimeout(res, 2000))
    }
    await this.get('writer').close()
    this.logger.info('Closed')
  }
}
