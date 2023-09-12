import { describe, expect, test } from "@jest/globals";
import { Parser } from "n3";
import { BasicLens, subjects, unique } from "rdf-lens";
import { ProcessorLens } from "../src/models";

const prefixes = `
@prefix js: <https://w3id.org/conn/js#> .
@prefix fno: <https://w3id.org/function/ontology#> .
@prefix fnom: <https://w3id.org/function/vocabulary/mapping#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix : <https://w3id.org/conn#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
`;

describe("Processor Lens", () => {
  test("Parse full config", () => {
    const turtle = `
${prefixes}

js:Echo a js:JsProcess;
  js:file <./test.js>;
  js:function "echo";
  js:location <./>;
  js:mapping [
    a fno:Mapping;
    fno:parameterMapping [
      a fnom:PositionParameterMapping ;
      fnom:functionParameter js:input ;
      fnom:implementationParameterPosition "0"^^xsd:int
    ], [
      a fnom:PositionParameterMapping ;
      fnom:functionParameter js:output ;
      fnom:implementationParameterPosition "1"^^xsd:int
    ]
  ].

[] a sh:NodeShape;
  sh:targetClass js:Echo;
  sh:property [
    sh:class :ReaderChannel;
    sh:path js:input;
    sh:name "Input Channel"
  ], [
    sh:class :WriterChannel;
    sh:path js:output;
    sh:name "Output Channel"
  ].
`;
    const quads = new Parser().parse(turtle);

    const lens = subjects().then(unique()).asMulti().thenSome(ProcessorLens)
    const out = lens.execute(quads);

    expect(out.length).toBe(1);
    expect(out[0].id).toEqual("https://w3id.org/conn/js#Echo");
    expect(out[0].mappings.length).toBe(2);
    expect(out[0].shape).toBeInstanceOf(BasicLens);
  });

  test("2 + 2 = 4", () => {
    const four = 2 + 2;
    expect(four).toBe(4);
  })
})
