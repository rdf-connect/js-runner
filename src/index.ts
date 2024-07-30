import { Store } from "n3";
import { getArgs } from "./args";
import { load_store, LOG } from "./util";

export * from "./connectors";

import path from "path";
import { RDF } from "@treecg/types";
import { ChannelFactory, Conn, JsOntology } from "./connectors";
import { Quad, Term } from "@rdfjs/types";

import { extractShapes, Shapes } from "rdf-lens";

function safeJoin(a: string, b: string) {
    if (b.startsWith("/")) {
        return b;
    }
    return path.join(a, b);
}

type Processor = {
    ty: Term;
    file: string;
    location: string;
    func: string;
    mapping: { parameters: { parameter: string; position: number }[] };
};

export type Source =
    | { type: "remote"; location: string }
    | {
        type: "memory";
        value: string;
        baseIRI: string;
    };

export type Extracted = {
    processors: Processor[];
    quads: Quad[];
    shapes: Shapes;
};

export async function extractProcessors(
    source: Source,
    apply?: { [label: string]: (item: unknown) => unknown },
): Promise<Extracted> {
    const store = new Store();
    await load_store(source, store);
    const quads = store.getQuads(null, null, null, null);

    const config = extractShapes(quads, apply);
    const subjects = quads
        .filter(
            (x) =>
                x.predicate.equals(RDF.terms.type) &&
                x.object.equals(JsOntology.JsProcess),
        )
        .map((x) => x.subject);
    const processorLens = config.lenses[JsOntology.JsProcess.value];
    const processors = subjects.map((id) => processorLens.execute({ id, quads }));
    return { processors, quads, shapes: config };
}

export function extractSteps(
    proc: Processor,
    quads: Quad[],
    config: Shapes,
): unknown[][] {
    const out: unknown[][] = [];

    const subjects = quads
        .filter(
            (x) => x.predicate.equals(RDF.terms.type) && x.object.equals(proc.ty),
        )
        .map((x) => x.subject);
    const processorLens = config.lenses[proc.ty.value];

    const fields = proc.mapping.parameters;

    for (const id of subjects) {
        const obj = processorLens.execute({ id, quads });
        const functionArgs = new Array(fields.length);

        for (const field of fields) {
            functionArgs[field.position] = obj[field.parameter];
        }

        out.push(functionArgs);
    }

    return out;
}

export async function jsRunner() {
    const args = getArgs();
    const cwd = process.cwd();

    const source: Source = {
        location: safeJoin(cwd, args!.input).replaceAll("\\", "/"),
        type: "remote",
    };

    const factory = new ChannelFactory();
    /// Small hack, if something is extracted from these types, that should be converted to a reader/writer
    const apply: { [label: string]: (item: unknown) => unknown } = {};
    apply[Conn.ReaderChannel.value] = factory.createReader.bind(factory);
    apply[Conn.WriterChannel.value] = factory.createWriter.bind(factory);

    const {
        processors,
        quads,
        shapes: config,
    } = await extractProcessors(source, apply);

    LOG.main("Found %d processors", processors.length);

    const starts = [];
    for (const proc of processors) {
        const argss = extractSteps(proc, quads, config);
        const jsProgram = await import("file://" + proc.file);
        process.chdir(proc.location);
        for (const args of argss) {
            starts.push(await jsProgram[proc.func](...args));
        }
    }

    await factory.init();

    for (const s of starts) {
        if (s && typeof s === "function") {
            s();
        }
    }
}

