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

  protected get<K extends keyof T>(key: K): T[K] {
    return this.args[key]
  }

  // This is the first function that is called (and awaited), when creating a processor
  // This is the perfect location to start things like database connections
  abstract init(this: T & this): Promise<void>

  // Function to start reading channels
  // This function is called for each processor before _produce_ is called
  abstract transform(this: T & this): Promise<void>

  // Function to start the production of data, starting the pipeline
  // This function is called after all processors are completely setup
  abstract produce(this: T & this): Promise<void>
}
