import { describe, expect, test } from "@jest/globals";
import { Quad } from "@rdfjs/types";
import { RDF } from "@treecg/types";
import { readFileSync } from "fs";
import { DataFactory, Parser } from "n3";
import { extractShapes } from "../src/shacl";

function parseQuads(inp: string): Quad[] {
  return new Parser().parse(inp);
}

function parseConfig() {
  const file = readFileSync("./ontology.ttl", { encoding: "utf8" });
  const quads = parseQuads(file);
  return extractShapes(quads);
}

const JsProcessor = DataFactory.namedNode("https://w3id.org/conn/js#JsProcess");
describe("Input test", () => {
  test("Parse configuration", () => {
    const output = parseConfig();
    expect(output.shapes.length).toBe(6);
    expect(output.lenses[JsProcessor.value]).toBeDefined();
  });

  test("Parse processor config", () => {
    const config = parseConfig();
    const processorFile = readFileSync("./processor/send.ttl", {
      encoding: "utf8",
    });
    const quads = parseQuads(processorFile);

    const quad = quads.find(
      (x) => x.predicate.equals(RDF.terms.type) && x.object.equals(JsProcessor),
    )!;
    const object = config.lenses[quad.object.value].execute({
      id: quad.subject,
      quads,
    });

    expect(object).toBeDefined();
  });

  test("parse js-runner pipeline", () => {
    const parse = (location: string) =>
      parseQuads(readFileSync(location, { encoding: "utf8" }));
    const files = [
      "./ontology.ttl",
      "./processor/send.ttl",
      "./processor/resc.ttl",
      "./input.ttl",
    ];
    const quads = files.flatMap(parse);
    const config = extractShapes(quads);

    const subjects = quads
      .filter(
        (x) =>
          x.predicate.equals(RDF.terms.type) && x.object.equals(JsProcessor),
      )
      .map((x) => x.subject);
    const processorLens = config.lenses[JsProcessor.value];
    const processors = subjects.map((id) =>
      processorLens.execute({ id, quads }),
    );

    const found: any[] = [];
    for (let proc of processors) {
      const subjects = quads
        .filter(
          (x) => x.predicate.equals(RDF.terms.type) && x.object.equals(proc.ty),
        )
        .map((x) => x.subject);
      console.log(
        "checking proc",
        proc.ty.value,
        subjects.map((x) => x.value),
      );
      const processorLens = config.lenses[proc.ty.value];

      found.push(...subjects.map((id) => processorLens.execute({ id, quads })));
    }

    console.log(
      config.shapes.map((x) => x.ty.value),
      processors,
    );
    console.log(found);
  });
});
