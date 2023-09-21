import { createTermNamespace, createUriNamespace } from "@treecg/types";

import * as ws from "./ws";
import * as http from "./http";
import * as kafka from "./kafka";
import * as file from "./file";
import { NamedNode, Term } from "@rdfjs/types";
export * as ws from "./ws";
export * as http from "./http";
export * as kafka from "./kafka";
export * as file from "./file";

export const Conn = createTermNamespace(
  "https://w3id.org/conn#",
  "FileReaderChannel",
  "FileWriterChannel",
  "HttpReaderChannel",
  "HttpWriterChannel",
  "KafkaReaderChannel",
  "KafkaWriterChannel",
  "WsReaderChannel",
  "WsWriterChannel",
);

export interface Config {
  ty: NamedNode;
}

export type ReaderConstructor<C extends Config> = (config: C) => {
  reader: Stream<string>;
  init: () => Promise<void>;
};

export type WriterConstructor<C extends Config> = (config: C) => {
  writer: Writer<string>;
  init: () => Promise<void>;
};

const JsOntology = createTermNamespace(
  "https://w3id.org/conn/js#",
  "JsChannel",
  "JsReaderChannel",
  "JsWriterChannel",
);
type JsChannel = {
  channel?: {
    id: Term;
  };
};

export class ChannelFactory {
  private inits: (() => Promise<void>)[] = [];
  private jsChannelsNamedNodes: { [label: string]: SimpleStream<string> } = {};
  private jsChannelsBlankNodes: { [label: string]: SimpleStream<string> } = {};

  createReader(config: Config): Stream<string> {
    if (config.ty.equals(Conn.FileReaderChannel)) {
      const { reader, init } = file.startFileStreamReader(
        <file.FileReaderConfig>config,
      );
      this.inits.push(init);

      return reader;
    }

    if (config.ty.equals(Conn.WsReaderChannel)) {
      const { reader, init } = ws.startWsStreamReader(
        <ws.WsReaderConfig>config,
      );
      this.inits.push(init);

      return reader;
    }

    if (config.ty.equals(Conn.KafkaReaderChannel)) {
      const { reader, init } = kafka.startKafkaStreamReader(
        <kafka.KafkaReaderConfig>config,
      );
      this.inits.push(init);
      return reader;
    }

    if (config.ty.equals(Conn.HttpReaderChannel)) {
      const { reader, init } = http.startHttpStreamReader(
        <http.HttpReaderConfig>config,
      );
      this.inits.push(init);
      return reader;
    }

    if (config.ty.equals(JsOntology.JsReaderChannel)) {
      const c = <JsChannel>config;
      if (c.channel) {
        const id = c.channel.id.value;
        if (c.channel.id.termType === "NamedNode") {
          if (!this.jsChannelsNamedNodes[id]) {
            this.jsChannelsNamedNodes[id] = new SimpleStream<string>();
          }

          return this.jsChannelsNamedNodes[id];
        }
        if (c.channel.id.termType === "BlankNode") {
          if (!this.jsChannelsBlankNodes[id]) {
            this.jsChannelsBlankNodes[id] = new SimpleStream<string>();
          }

          return this.jsChannelsBlankNodes[id];
        }
      }
    }
    throw "Unknown reader channel " + config.ty.value;
  }

  createWriter(config: Config): Writer<string> {
    if (config.ty.equals(Conn.FileWriterChannel)) {
      const { writer, init } = file.startFileStreamWriter(
        <file.FileWriterConfig>config,
      );
      this.inits.push(init);

      return writer;
    }

    if (config.ty.equals(Conn.WsWriterChannel)) {
      const { writer, init } = ws.startWsStreamWriter(
        <ws.WsWriterConfig>config,
      );
      this.inits.push(init);

      return writer;
    }

    if (config.ty.equals(Conn.KafkaWriterChannel)) {
      const { writer, init } = kafka.startKafkaStreamWriter(
        <kafka.KafkaWriterConfig>config,
      );
      this.inits.push(init);
      return writer;
    }

    if (config.ty.equals(Conn.HttpWriterChannel)) {
      const { writer, init } = http.startHttpStreamWriter(
        <http.HttpWriterConfig>config,
      );
      this.inits.push(init);
      return writer;
    }

    if (config.ty.equals(JsOntology.JsWriterChannel)) {
      const c = <JsChannel>config;
      if (c.channel) {
        const id = c.channel.id.value;
        if (c.channel.id.termType === "NamedNode") {
          if (!this.jsChannelsNamedNodes[id]) {
            this.jsChannelsNamedNodes[id] = new SimpleStream<string>();
          }

          return this.jsChannelsNamedNodes[id];
        }
        if (c.channel.id.termType === "BlankNode") {
          if (!this.jsChannelsBlankNodes[id]) {
            this.jsChannelsBlankNodes[id] = new SimpleStream<string>();
          }

          return this.jsChannelsBlankNodes[id];
        }
      }
    }

    throw "Unknown writer channel " + config.ty.value;
  }

  async init(): Promise<void> {
    await Promise.all(this.inits.map((x) => x()));
  }
}

export interface Writer<T> {
  push(item: T): Promise<void>;
  end(): Promise<void>;
}

export interface Stream<T> {
  lastElement?: T;
  end(): Promise<void>;
  data(listener: (t: T) => PromiseLike<void> | void): this;
  on(event: "data", listener: (t: T) => PromiseLike<void> | void): this;
  on(event: "end", listener: () => PromiseLike<void> | void): this;
}

export type Handler<T> = (item: T) => Promise<void> | void;

export class SimpleStream<T> implements Stream<T> {
  private readonly dataHandlers: Handler<T>[] = [];
  private readonly endHandlers: Handler<void>[] = [];

  public readonly disconnect: () => Promise<void>;
  public lastElement?: T | undefined;

  public constructor(onDisconnect?: () => Promise<void>) {
    this.disconnect = onDisconnect || (async () => {});
  }

  public data(listener: Handler<T>): this {
    this.dataHandlers.push(listener);
    return this;
  }

  public async push(data: T): Promise<void> {
    this.lastElement = data;
    await Promise.all(this.dataHandlers.map((handler) => handler(data)));
  }

  public async end(): Promise<void> {
    await this.disconnect();
    await Promise.all(this.endHandlers.map((handler) => handler()));
  }

  public on(event: "data", listener: Handler<T>): this;
  public on(event: "end", listener: Handler<void>): this;
  public on(event: "data" | "end", listener: Handler<any>): this {
    if (event === "data") {
      this.dataHandlers.push(listener);
    }
    if (event === "end") {
      this.endHandlers.push(listener);
    }
    return this;
  }
}
