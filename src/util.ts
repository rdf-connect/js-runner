import { rdfDereferencer } from "rdf-dereference";
import stream from "stream";
import { DataFactory, Parser, Store } from "n3";
import { createTermNamespace, createUriAndTermNamespace } from "@treecg/types";
import { Source } from ".";
import { Quad, Term } from "@rdfjs/types";
import path from "path";

import debug from "debug";

export const LOG = (function () {
    const main = debug("js-runner");
    const channel = main.extend("channel");
    const util = main.extend("util");

    return { main, channel, util };
})();

export function toArray<T>(stream: stream.Readable): Promise<T[]> {
    const output: T[] = [];
    return new Promise((res, rej) => {
        stream.on("data", (x) => output.push(x));
        stream.on("end", () => res(output));
        stream.on("close", () => res(output));
        stream.on("error", rej);
    });
}

export const OWL = createUriAndTermNamespace(
    "http://www.w3.org/2002/07/owl#",
    "imports",
);

export const RDFC = createTermNamespace(
    "https://w3id.org/rdf-connect#",
    "WriterChannel",
    "ReaderChannel",
    "FileReaderChannel",
    "FileWriterChannel",
    "HttpReaderChannel",
    "HttpWriterChannel",
    "KafkaReaderChannel",
    "KafkaWriterChannel",
    "WebSocketReaderChannel",
    "WebSocketWriterChannel"
);

export const RDFC_JS = createTermNamespace(
    "https://w3id.org/rdf-connect/js#",
    "Processor",
    "JSChannel",
    "JSReaderChannel",
    "JSWriterChannel",
);

export const { namedNode, literal } = DataFactory;

export type Keyed<T> = { [Key in keyof T]: Term | undefined };

export type Map<V, K, T, O> = (value: V, key: K, item: T) => O;

export function safeJoin(a: string, b: string) {
    if (b.startsWith("/")) {
        return b;
    }
    return path.join(a, b);
}

export async function load_quads(location: string, baseIRI?: string) {
    try {
        LOG.util("Loading quads %s", location);
        const { data } = await rdfDereferencer.dereference(location, { localFiles: true });
        const quads: Quad[] = [];

        for await (const quad of data) {
            quads.push(quad);
        }
        return quads;
    } catch (ex) {
        console.error("Failed to load_quads", location, baseIRI);
        console.error(ex);
        return [];
    }
}

function load_memory_quads(value: string, baseIRI: string) {
    const parser = new Parser({ baseIRI });
    return parser.parse(value);
}

const loaded = new Set();
export async function load_store(
    location: Source,
    store: Store,
    recursive = true,
) {
    if (loaded.has(location)) return;
    loaded.add(location);

    const quads =
        location.type === "remote"
            ? await load_quads(location.location)
            : load_memory_quads(location.value, location.baseIRI);

    store.addQuads(quads);

    if (recursive) {
        let other_imports = [];
        let loc = "";

        if (location.type === "remote") {
            loc = location.location;
            other_imports = store.getObjects(
                namedNode(loc.startsWith("/") ? `file://${loc}` : loc),
                OWL.terms.imports,
                null,
            );
        } else {
            loc = location.baseIRI;
            other_imports = store.getObjects(
                namedNode(loc),
                OWL.terms.imports,
                null,
            );
        }
        for (const other of other_imports) {
            await load_store(
                {
                    location: other.value.startsWith("file://") ? other.value.slice(7) : other.value, 
                    type: "remote"
                },
                store, true
            );
        }
    }
}
