import { Store } from "n3";
import * as RDF from "rdf-js";
import { QueryEngine } from "@comunica/query-sparql";
import { Keyed } from "./util";

const engine = new QueryEngine();

export async function executeQuery<T extends Keyed<T>>(store: Store, query: string, fields: (keyof T)[]): Promise<T[]> {
  const out = [];

  const bindingsStream = await engine.queryBindings(query, {
    sources: [store],
  });

  for (let match of await bindingsStream.toArray()) {
    const ne = {} as T;

    for (let field of fields)
      ne[field] = <T[typeof field]>match.get(<string>field);


    out.push(ne);
  }

  return out;
}

export const procQuery = `PREFIX js: <https://w3id.org/conn/js#>
PREFIX fno: <https://w3id.org/function/ontology#>
PREFIX fnom: <https://w3id.org/function/vocabulary/mapping#>
PREFIX sh: <http://www.w3.org/ns/shacl#>
PREFIX : <https://w3id.org/conn#> 
                
  SELECT * WHERE {
    ?tys a js:JsProcess;
      js:file ?file;
      js:function ?func;
      js:mapping [
        fno:parameterMapping [
          fnom:functionParameter ?path;
          fnom:implementationParameterPosition ?loc;
        ]
      ].

    ?shape sh:targetClass ?tys;
      sh:property [
        sh:path ?path;
      ].

    ?subject a ?tys;
      ?path ?value.

    OPTIONAL { ?shape sh:property [ sh:path ?path; sh:datatype ?datatype ] }
    OPTIONAL { ?shape sh:property [ sh:path ?path; sh:class ?class ] }
  }`;

export const procOutputFields: (keyof ProcOutput)[] = ["file", "func", "loc", "value", "class", "datatype", "subject"];

export type ProcOutput = {
  file: RDF.Literal,
  func: RDF.Literal,
  value: RDF.Term,
  loc: RDF.Literal,
  class?: RDF.Term,
  datatype?: RDF.Term,
  subject: RDF.Term,

  id?: RDF.Literal,
};

function channelQuery(type: "ReaderChannel" | "WriterChannel", self: "reader" | "writer"): string {
  return `
PREFIX js: <https://w3id.org/conn/js#>
PREFIX fno: <https://w3id.org/function/ontology#>
PREFIX fnom: <https://w3id.org/function/vocabulary/mapping#>
PREFIX sh: <http://www.w3.org/ns/shacl#>
PREFIX : <https://w3id.org/conn#> 
                
  SELECT * WHERE {
    ?tys a js:JsProcess;
      js:mapping [
        fno:parameterMapping [
          fnom:functionParameter ?path;
          fnom:implementationParameterPosition ?loc;
        ]
      ].

    ?shape sh:targetClass ?tys;
      sh:property [
        sh:class :${type};
        sh:path ?path;
      ].

    ?subject a ?tys;
      ?path ?${self}.

    ?${self} ?prop ?value. 

    OPTIONAL { [] :reader ?reader; :writer ?writer }
  }
`;
}

export const readerQuery = channelQuery("ReaderChannel", "reader");
export const writerQuery = channelQuery("WriterChannel", "writer");

export const channelOutputFields: (keyof ChannelOutput<true & false>)[] = ["reader", "writer", "loc", "subject", "prop", "value"];
type ChannelOutput<T extends true | false> = {
  reader: T extends true ? RDF.Term : RDF.Term | undefined,
  writer: T extends false ? RDF.Term : RDF.Term | undefined,
  subject: RDF.Term,
  prop: RDF.NamedNode,
  value: RDF.Term,
  loc: RDF.Term,
};

export type ReaderOutput = ChannelOutput<true>;
export type WriterOutput = ChannelOutput<false>;

