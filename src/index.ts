import { Store } from "n3";
import { getArgs } from "./args";
import { load_store } from "./util";

export * from "./connectors";
export * from "./shacl";

import path from "path";
import { extractShapes, Shapes } from "./shacl";
import { RDF } from "@treecg/types";
import { ChannelFactory, Conn, JsOntology } from "./connectors";
import { Quad, Term } from "@rdfjs/types";

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
  apply?: { [label: string]: (item: any) => any },
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
): any[][] {
  const out: any[][] = [];

  const subjects = quads
    .filter(
      (x) => x.predicate.equals(RDF.terms.type) && x.object.equals(proc.ty),
    )
    .map((x) => x.subject);
  const processorLens = config.lenses[proc.ty.value];

  const fields = proc.mapping.parameters;

  for (let id of subjects) {
    const obj = processorLens.execute({ id, quads });
    const functionArgs = new Array(fields.length);

    for (let field of fields) {
      functionArgs[field.position] = obj[field.parameter];
    }

    out.push(functionArgs);
  }

  return out;
}

export async function jsRunner() {
  console.log("JS runner is running!");
  const args = getArgs();
  const cwd = process.cwd();

  const source: Source = {
    location: safeJoin(cwd, args.input).replaceAll("\\", "/"),
    type: "remote",
  };

  const factory = new ChannelFactory();
  /// Small hack, if something is extracted from these types, that should be converted to a reader/writer
  const apply: { [label: string]: (item: any) => any } = {};
  for (let ty of [
    Conn.FileReaderChannel,
    Conn.WsReaderChannel,
    Conn.FileReaderChannel,
    Conn.KafkaReaderChannel,
    JsOntology.JsReaderChannel,
  ]) {
    apply[ty.value] = (x) => factory.createReader(x);
  }

  for (let ty of [
    Conn.FileWriterChannel,
    Conn.WsWriterChannel,
    Conn.FileWriterChannel,
    Conn.KafkaWriterChannel,
    JsOntology.JsWriterChannel,
  ]) {
    apply[ty.value] = (x) => factory.createWriter(x);
  }

  const {
    processors,
    quads,
    shapes: config,
  } = await extractProcessors(source, apply);

  const starts = [];
  for (let proc of processors) {
    const argss = extractSteps(proc, quads, config);
    const jsProgram = await import("file://" + proc.file);
    process.chdir(proc.location);
    for (let args of argss) {
      starts.push(await jsProgram[proc.func](...args));
    }
  }

  await factory.init();

  for (let s of starts) {
    if (s && typeof s === "function") {
      s();
    }
  }
}
