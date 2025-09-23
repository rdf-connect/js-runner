import { Any } from './reader'

const decoder = new TextDecoder()
export interface Convertor<T> {
  from(buffer: Uint8Array): T
  fromStream(stream: AsyncIterable<Uint8Array>): Promise<T>
}

export const AnyConvertor: Convertor<Any> = {
  from: function (buffer: Uint8Array): Any {
    return {
      buffer,
    }
  },
  fromStream: async function (inp: AsyncIterable<Uint8Array>): Promise<Any> {
    const stream = (async function* () {
      yield* inp
    })()
    return { stream }
  },
}

export const StringConvertor: Convertor<string> = {
  from(buffer) {
    return decoder.decode(buffer)
  },
  async fromStream(stream) {
    const chunks: Uint8Array[] = []
    for await (const chunk of stream) {
      chunks.push(chunk)
    }
    console.log(
      'Chunks',
      chunks.map((x) => x.length),
    )
    return decoder.decode(Buffer.concat(chunks))
  },
}
export const StreamConvertor: Convertor<AsyncGenerator<Uint8Array>> = {
  from(buffer) {
    return (async function* () {
      yield buffer
    })()
  },

  async fromStream(stream) {
    return (async function* () {
      yield* stream
    })()
  },
}
export const NoConvertor: Convertor<Uint8Array> = {
  from(buffer) {
    return buffer
  },
  async fromStream(stream) {
    const chunks: Uint8Array[] = []
    for await (const chunk of stream) {
      chunks.push(chunk)
    }
    return Buffer.concat(chunks)
  },
}
