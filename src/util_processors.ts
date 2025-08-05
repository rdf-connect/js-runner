import { Processor } from './processor'
import { Reader } from './reader'

type LogArgs = {
  reader: Reader
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
