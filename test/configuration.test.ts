import { describe, expect, test } from "vitest";
import { NamedNode, Quad } from "@rdfjs/types";
import { RDF } from "@treecg/types";
import { readFileSync } from "fs";
import { DataFactory, Parser, Store } from "n3";
import { extractShapes } from "rdf-lens";
import { load_store, RDFC_JS, safeJoin } from "../src/util";

function parseQuads(inp: string): Quad[] {
    return new Parser().parse(inp);
}

async function parseConfig() {
    const store = new Store();
    await load_store({
        type: "remote",
        location: safeJoin(process.cwd(), "ontology.ttl")
    }, store);
    return extractShapes(store.getQuads(null, null, null, null));
}

const JSProcessor = DataFactory.namedNode("https://w3id.org/rdf-connect/js#Processor");
describe("Input test", () => {
    test("Parse configuration", async () => {
        const output = await parseConfig();
        expect(output.shapes.length).toBe(25);
        expect(output.lenses[JSProcessor.value]).toBeDefined();
    });

    test("Parse processor config", async () => {
        const config = await parseConfig();
        const processorFile = readFileSync("./processor/send.ttl", {
            encoding: "utf8",
        });
        const quads = parseQuads(processorFile);

        const quad = quads.find(
            (x) =>
                x.predicate.equals(RDF.terms.type) &&
                x.object.equals(JSProcessor),
        )!;
        const object = config.lenses[quad.object.value].execute({
            id: quad.subject,
            quads,
        });

        expect(object).toBeDefined();
    });

    test("parse js-runner pipeline", async () => {
        const store = new Store();

        await load_store({
            type: "remote",
            location: safeJoin(process.cwd(), "input.ttl")
        }, store);

        const quads = store.getQuads(null, null, null, null);
        const config = extractShapes(quads);

        const subjects = quads
            .filter(
                (x) =>
                    x.predicate.equals(RDF.terms.type) &&
                    x.object.equals(JSProcessor),
            )
            .map((x) => x.subject);
        const processorLens = config.lenses[JSProcessor.value];
        const processors = subjects.map((id) =>
            processorLens.execute({ id, quads: quads }),
        );

        const found: unknown[] = [];
        for (const proc of processors) {
            const processorLens = config.lenses[proc.ty.value];
            const subjects = quads
                .filter(
                    (x) =>
                        x.predicate.equals(RDF.terms.type) &&
                        x.object.equals(proc.ty),
                )
                .map((x) => x.subject);
                
            found.push(
                ...subjects.map((id) =>
                    processorLens.execute({ id, quads: quads }),
                ),
            );
        }
        expect(found.length).toBe(2);
        expect((<{ msg: string, output: { id: NamedNode, ty: NamedNode } }>found[0]).msg).toBe("Hello");
        expect((<{ msg: string, output: { id: NamedNode, ty: NamedNode } }>found[0]).output.id).toBeDefined();
        expect((<{ msg: string, output: { id: NamedNode, ty: NamedNode } }>found[0]).output.ty.value)
            .toBe(RDFC_JS.JSWriterChannel.value);
    });
});
