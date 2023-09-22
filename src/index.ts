import { Store } from "n3";
import { getArgs } from "./args";
import { load_store } from "./util";

export * from "./connectors";
export * from "./shacl";

import path from "path";
import { extractShapes } from "./shacl";
import { RDF } from "@treecg/types";
import { ChannelFactory, Conn, JsOntology } from "./connectors";
import { Term } from "@rdfjs/types";

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
export async function jsRunner() {
  console.log("JS runner is running!");
  const args = getArgs();
  const cwd = process.cwd();

  const store = new Store();
  await load_store(safeJoin(cwd, args.input).replaceAll("\\", "/"), store);
  const quads = store.getQuads(null, null, null, null);

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

  const config = extractShapes(quads, apply);
  const subjects = quads
    .filter(
      (x) =>
        x.predicate.equals(RDF.terms.type) &&
        x.object.equals(JsOntology.JsProcess),
    )
    .map((x) => x.subject);
  const processorLens = config.lenses[JsOntology.JsProcess.value];
  const processors: Processor[] = subjects.map((id) =>
    processorLens.execute({ id, quads }),
  );

  const starts = [];
  for (let proc of processors) {
    const subjects = quads
      .filter(
        (x) => x.predicate.equals(RDF.terms.type) && x.object.equals(proc.ty),
      )
      .map((x) => x.subject);
    const processorLens = config.lenses[proc.ty.value];

    const fields = proc.mapping.parameters;
    const jsProgram = await import("file://" + proc.file);
    process.chdir(proc.location);

    for (let id of subjects) {
      const obj = processorLens.execute({ id, quads });
      const functionArgs = new Array(fields.length);

      for (let field of fields) {
        functionArgs[field.position] = obj[field.parameter];
      }

      starts.push(await jsProgram[proc.func](...functionArgs));
    }
  }

  await factory.init();

  for (let s of starts) {
    if (s && typeof s === "function") {
      s();
    }
  }
}
