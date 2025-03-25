import { Processor, Reader, Writer } from '@rdfc/js-runner'

type EchoArgs = {
  reader: Reader
  writer: Writer
}

export class EchoProcessor extends Processor<EchoArgs> {
  async init(this: this & EchoArgs): Promise<void> {
    this.logger.info('Init echo processor')
  }

  async transform(this: EchoArgs & this): Promise<void> {
    for await (const msg of this.reader.streams()) {
      this.logger.info('Echoing message')
      await this.writer.stream(msg)
    }
    await this.writer.close()
    this.logger.info('closed')
  }

  async produce(this: this & EchoArgs): Promise<void> {
    // nothing
  }
}

type LogArgs = {
  reader: Reader
  writer: Writer
}

export class LogProcessor extends Processor<LogArgs> {
  async init(): Promise<void> {
    this.logger.info('Init log processor')
  }

  async transform(this: LogArgs & this): Promise<void> {
    for await (const msg of this.args.reader.strings()) {
      this.logger.info('Got msg' + msg)
    }
  }

  async produce() {}
}

type SendArgs = {
  msgs: string[]
  writer: Writer
}

export class SendProcessor extends Processor<SendArgs> {
  async init(): Promise<void> {
    this.logger.info('Init send processor')
  }

  async transform(): Promise<void> {}
  async produce(): Promise<void> {
    for (const msg of this.get('msgs')) {
      await this.get('writer').string(msg)

      this.logger.info('Sending ' + msg)
      await new Promise((res) => setTimeout(res, 1000))
    }
    await this.get('writer').close()
    this.logger.info('Closed')
  }
}
