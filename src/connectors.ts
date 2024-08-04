import { NamedNode, Term } from "@rdfjs/types";

import {
    FileReaderConfig,
    FileWriterConfig,
    startFileStreamReader,
    startFileStreamWriter,
} from "./connectors/file";
export * from "./connectors/file";

import {
    startWsStreamReader,
    startWsStreamWriter,
    WsReaderConfig,
    WsWriterConfig,
} from "./connectors/ws";
export * from "./connectors/ws";

import {
    KafkaReaderConfig,
    KafkaWriterConfig,
    startKafkaStreamReader,
    startKafkaStreamWriter,
} from "./connectors/kafka";
export * from "./connectors/kafka";

import {
    HttpReaderConfig,
    HttpWriterConfig,
    startHttpStreamReader,
    startHttpStreamWriter,
} from "./connectors/http";
import { LOG, RDFC, RDFC_JS } from "./util";
export * from "./connectors/http";

export interface Config<T> {
    id: Term;
    ty: NamedNode;
    config: T;
}

export type ReaderConstructor<C> = (config: C) => {
    reader: Stream<string | Buffer>;
    init: () => Promise<void>;
};

export type WriterConstructor<C> = (config: C) => {
    writer: Writer<string | Buffer>;
    init: () => Promise<void>;
};


type JsChannel = {
    channel: Term;
};

export class ChannelFactory {
    private inits: (() => Promise<void>)[] = [];
    private jsChannelsNamedNodes: { [label: string]: SimpleStream<string> } = {};
    private jsChannelsBlankNodes: { [label: string]: SimpleStream<string> } = {};

    createReader(config: Config<unknown>): Stream<string | Buffer> {
        LOG.channel("Creating reader %s: a %s", config.id.value, config.ty.value);
        if (config.ty.equals(RDFC.FileReaderChannel)) {
            const { reader, init } = startFileStreamReader(<FileReaderConfig>config.config);
            this.inits.push(init);

            return reader;
        }

        if (config.ty.equals(RDFC.WebSocketReaderChannel)) {
            const { reader, init } = startWsStreamReader(<WsReaderConfig>config.config);
            this.inits.push(init);

            return reader;
        }

        if (config.ty.equals(RDFC.KafkaReaderChannel)) {
            const { reader, init } = startKafkaStreamReader(
                <KafkaReaderConfig>config.config,
            );
            this.inits.push(init);
            return reader;
        }

        if (config.ty.equals(RDFC.HttpReaderChannel)) {
            const { reader, init } = startHttpStreamReader(
                <HttpReaderConfig>config.config,
            );
            this.inits.push(init);
            return reader;
        }

        if (config.ty.equals(RDFC_JS.JSReaderChannel)) {
            const c = <JsChannel>config.config;
            if (c.channel) {
                const id = c.channel.value;
                if (c.channel.termType === "NamedNode") {
                    if (!this.jsChannelsNamedNodes[id]) {
                        this.jsChannelsNamedNodes[id] = new SimpleStream<string>();
                    }

                    return this.jsChannelsNamedNodes[id];
                }

                if (c.channel.termType === "BlankNode") {
                    if (!this.jsChannelsBlankNodes[id]) {
                        this.jsChannelsBlankNodes[id] = new SimpleStream<string>();
                    }

                    return this.jsChannelsBlankNodes[id];
                }
                throw "Should have found a thing";
            }
        }
        throw "Unknown reader channel " + config.ty.value;
    }

    createWriter(config: Config<unknown>): Writer<string | Buffer> {
        LOG.channel("Creating writer %s: a %s", config.id.value, config.ty.value);
        if (config.ty.equals(RDFC.FileWriterChannel)) {
            const { writer, init } = startFileStreamWriter(
                <FileWriterConfig>config.config,
            );
            this.inits.push(init);

            return writer;
        }

        if (config.ty.equals(RDFC.WebSocketWriterChannel)) {
            const { writer, init } = startWsStreamWriter(
                <WsWriterConfig>config.config,
            );
            this.inits.push(init);

            return writer;
        }

        if (config.ty.equals(RDFC.KafkaWriterChannel)) {
            const { writer, init } = startKafkaStreamWriter(
                <KafkaWriterConfig>config.config,
            );
            this.inits.push(init);
            return writer;
        }

        if (config.ty.equals(RDFC.HttpWriterChannel)) {
            const { writer, init } = startHttpStreamWriter(
                <HttpWriterConfig>config.config,
            );
            this.inits.push(init);
            return writer;
        }

        if (config.ty.equals(RDFC_JS.JSWriterChannel)) {
            const c = <JsChannel>config.config;
            if (c.channel) {
                const id = c.channel.value;
                if (c.channel.termType === "NamedNode") {
                    if (!this.jsChannelsNamedNodes[id]) {
                        this.jsChannelsNamedNodes[id] = new SimpleStream<string>();
                    }

                    return this.jsChannelsNamedNodes[id];
                }

                if (c.channel.termType === "BlankNode") {
                    if (!this.jsChannelsBlankNodes[id]) {
                        this.jsChannelsBlankNodes[id] = new SimpleStream<string>();
                    }

                    return this.jsChannelsBlankNodes[id];
                }
                throw "Should have found a thing";
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
        this.disconnect = onDisconnect || (async () => { });
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
    public on(event: "data" | "end", listener: Handler<T> | Handler<void>): this {
        if (event === "data") {
            this.dataHandlers.push(<Handler<T>>listener);
        }
        if (event === "end") {
            this.endHandlers.push(<Handler<void>>listener);
        }
        return this;
    }
}
