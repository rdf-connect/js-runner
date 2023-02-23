import * as RDF from "rdf-js";
import { createReadStream } from "fs";
import http from "http";
import https from "https";
import stream from "stream";
import { Store, StreamParser, DataFactory } from "n3";
import { createUriAndTermNamespace } from "@treecg/types";

export function toArray<T>(stream: stream.Readable): Promise<T[]> {
  const output: T[] = []
  return new Promise((res, rej) => {
    stream.on("data", x => output.push(x));
    stream.on("end", () => res(output));
    stream.on("close", () => res(output));
    stream.on("error", rej);
  });
} 

export const OWL = createUriAndTermNamespace("http://www.w3.org/2002/07/owl#", "imports");
export const CONN2 = createUriAndTermNamespace("https://w3id.org/conn#", "install", "build", "GitInstall", "url", "procFile");
const { namedNode } = DataFactory;

export type Keyed<T> = { [Key in keyof T]: (RDF.Term | undefined) };

export type Map<V, K, T, O> = (value: V, key: K, item: T) => O;

export function merge<
  VT extends keyof T,
  KT extends keyof T,
  T extends Keyed<T>,
  CT extends keyof T,
  V = string,
  >(
    items: T[], subject: keyof T, key: KT, value: VT, consts: CT[] = [], mapper?: Map<T[VT], T[KT], T, V>,
): { [label: string]: { [label: string]: typeof mapper extends undefined ? string : V } & { [K in CT]: string } } {

  const cs = {} as { [label: string]: { [K in CT]: string } };
  const out = {} as { [label: string]: { [label: string]: typeof mapper extends undefined ? string : V } };
  const m = mapper ? mapper : <Map<T[VT], T[KT], T, V>><any>((x: T[VT], _key: T[KT], _item: T) => x!.value);

  for (let item of items) {
    const subjectV = item[subject]!;
    const keyV = item[key]!;
    const valueV = item[value];

    if (!out[subjectV.value]) {
      const v: { [K in CT]?: string } = {};
      for (let c of consts) {
        v[c] = item[c]?.value;
      }
      cs[subjectV.value] = <{ [K in CT]: string }>v;
      out[subjectV.value] = {} as { [label: string]: typeof mapper extends undefined ? string : V };
    }

    out[subjectV.value][keyV.value] = m(valueV, keyV, item);
  }


  const outs: { [label: string]: { [label: string]: typeof mapper extends undefined ? string : V } & { [K in CT]: string } } = {};
  for (let k in out) {
    outs[k] = Object.assign(out[k], cs[k]);
  }

  return outs;
}


async function get_readstream(location: string): Promise<stream.Readable> {
  if (location.startsWith("https")) {
    return new Promise((res) => {
      https.get(location, res);
    });
  } else if (location.startsWith("http")) {
    return new Promise((res) => {
      http.get(location, res);
    });
  } else {
    return createReadStream(location);
  }
}

export async function load_quads(location: string, baseIRI?: string) {
  const parser = new StreamParser({ baseIRI: baseIRI || location});
  const rdfStream = await get_readstream(location);
  rdfStream.pipe(parser);

  const quads: RDF.Quad[] = await toArray(parser);
  return quads;
}

const loaded = new Set();
export async function load_store(location: string, store: Store, recursive = true, process?: (quads: RDF.Quad[], baseIRI: string) =>PromiseLike<RDF.Quad[]>) {
  console.log("STARTING LOAD STORE");

  const _process = process || ((q: RDF.Quad[]) => q);

  if (loaded.has(location)) { return; }
  loaded.add(location);

  console.log("Loading", location);

  const quads = await load_quads(location);
  store.addQuads(await _process(quads, location));

  if (recursive) {
    const other_imports = store.getObjects(namedNode(location), OWL.terms.imports, null)
    for (let other of other_imports) {
      await load_store(other.value, store, true, process);
    }
  }
}
