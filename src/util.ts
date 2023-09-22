import * as RDF from "rdf-js";
import { createReadStream } from "fs";
import http from "http";
import https from "https";
import stream from "stream";
import { DataFactory, Parser, Store, StreamParser } from "n3";
import { createUriAndTermNamespace } from "@treecg/types";

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
export const CONN2 = createUriAndTermNamespace(
  "https://w3id.org/conn#",
  "install",
  "build",
  "GitInstall",
  "LocalInstall",
  "url",
  "procFile",
  "path",
);
const { namedNode } = DataFactory;

export type Keyed<T> = { [Key in keyof T]: RDF.Term | undefined };

export type Map<V, K, T, O> = (value: V, key: K, item: T) => O;

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
  try {
    console.log("load_quads", location, baseIRI)
    const parser = new StreamParser({ baseIRI: baseIRI || location });
    const rdfStream = await get_readstream(location);
    rdfStream.pipe(parser);

    const quads: RDF.Quad[] = await toArray(parser);
    return quads;
  } catch (ex) {
    console.error("Failed to load_quads", location, baseIRI);
    console.error(ex);
    return [];
  }
}

const loaded = new Set();
export async function load_store(
  location: string,
  store: Store,
  recursive = true,
  process?: (quads: RDF.Quad[], baseIRI: string) => PromiseLike<RDF.Quad[]>,
) {
  console.log("STARTING LOAD STORE");

  const _process = process || ((q: RDF.Quad[]) => q);

  if (loaded.has(location)) return;
  loaded.add(location);

  console.log("Loading", location);

  const quads = await load_quads(location);
  store.addQuads(await _process(quads, location));

  if (recursive) {
    const other_imports = store.getObjects(
      namedNode(location),
      OWL.terms.imports,
      null,
    );
    for (let other of other_imports) {
      await load_store(other.value, store, true, process);
    }
  }
}
