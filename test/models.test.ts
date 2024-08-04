import { describe, expect, test } from "vitest";

const prefixes = `
@prefix rdfc-js: <https://w3id.org/rdf-connect/js#> .
@prefix fno: <https://w3id.org/function/ontology#> .
@prefix fnom: <https://w3id.org/function/vocabulary/mapping#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix rdfc: <https://w3id.org/rdf-connect#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
`;

describe("Processor Lens", () => {
    test("Parse full config", () => {
        const turtle = `
${prefixes}

rdfc-js:Echo a rdfc-js:Processor;
    rdfc-js:file <./test.js>;
    rdfc-js:function "echo";
    rdfc-js:location <./>;
    rdfc-js:mapping [
        a fno:Mapping;
            fno:parameterMapping [
            a fnom:PositionParameterMapping ;
            fnom:functionParameter js:input ;
            fnom:implementationParameterPosition "0"^^xsd:integer
        ], [
            a fnom:PositionParameterMapping ;
            fnom:functionParameter js:output ;
            fnom:implementationParameterPosition "1"^^xsd:integer
        ]
  ].

[] a sh:NodeShape;
    sh:targetClass rdfc-js:Echo;
    sh:property [
        sh:class rdfc:ReaderChannel;
        sh:path rdfc-js:input;
        sh:name "Input Channel"
    ], [
        sh:class rdfc:WriterChannel;
        sh:path rdfc-js:output;
        sh:name "Output Channel"
    ].
`;
        // const quads = new Parser().parse(turtle);
        //
        // const lens = subjects().then(unique()).asMulti().thenSome(ProcessorLens)
        // const out = lens.execute(quads);
        //
        // expect(out.length).toBe(1);
        // expect(out[0].id).toEqual("https://w3id.org/conn/js#Echo");
        // expect(out[0].mappings.length).toBe(2);
        // expect(out[0].shape).toBeInstanceOf(BasicLens);
    });

    test("2 + 2 = 4", () => {
        const four = 2 + 2;
        expect(four).toBe(4);
    });
});
