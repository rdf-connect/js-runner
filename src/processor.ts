import { Logger } from 'winston'
import { Reader } from './reader'
import { Writer } from './writer'

export type Primitive = string | number | Writer | Reader | ProcessorArgs

export type ProcessorArgs = {
  [id: string]: Primitive | Primitive[]
}
export type BGetter<T> = {
  [K in keyof T]: T[K]
}

export abstract class Processor<T> {
  protected readonly args: T // Store args safely
  protected readonly logger: Logger

  constructor(args: T, logger: Logger) {
    Object.assign(this, args)
    this.args = args
    this.logger = logger
  }

  get<K extends keyof T>(key: K): T[K] {
    return this.args[key]
  }

  abstract init(this: T & this): Promise<void>
  abstract start(this: T & this): Promise<void>
}
