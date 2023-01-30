import { Term } from "@rdfjs/types";
import { AllReaderFactory, AllWriterFactory, loadReaderConfig, loadWriterConfig } from "@treecg/connector-all";
import { Writer, Stream, SimpleStream } from "@treecg/connector-types";
import { RDF } from "@treecg/types";

import { Stores } from ".";

const readerFactory = new AllReaderFactory();
const writerFactory = new AllWriterFactory();

const streams: { [label: string]: SimpleStream<any> } = {};

function getOrCreateStream(id: string): SimpleStream<any> {
  const optionalStream = streams[id];
  if (optionalStream) return optionalStream;

  const out = new SimpleStream<string>();
  streams[id] = out;
  return out;
}

async function createJsReader(id: string): Promise<Stream<string>> {
  return getOrCreateStream(id);
}

async function createJsWriter(id: string): Promise<Writer<string>> {
  return getOrCreateStream(id);
}

export async function createReader(subj: Term, stores: Stores, reader?: string): Promise<Stream<string>> {
  const ty = stores.flatMap(s => s.getObjects(subj, RDF.terms.type, null))[0];
  if (ty.value === "https://w3id.org/conn#JsReaderChannel") {
    return createJsReader(reader!);
  }
  const config = await loadReaderConfig(subj, async (s, p, o) => stores.flatMap(store => store.getQuads(<any>s, <any>p, <any>o, null)));
  return readerFactory.build(config);
}

export async function createWriter(subj: Term, stores: Stores, reader?: string): Promise<Writer<string>> {
  const ty = stores.flatMap(s => s.getObjects(subj, RDF.terms.type, null))[0];
  if (ty.value === "https://w3id.org/conn#JsWriterChannel") {
    return createJsWriter(reader!);
  }
  const config = await loadWriterConfig(subj, async (s, p, o) => stores.flatMap(store => store.getQuads(<any>s, <any>p, <any>o, null)));
  return writerFactory.build(config);
}

