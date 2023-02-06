import { Store, StreamParser, DataFactory } from "n3";
import { createReadStream } from "fs";
import { getArgs } from "./args";
import { executeQuery, procQuery, ProcOutput, procOutputFields, channelOutputFields, readerQuery, ReaderOutput, writerQuery, WriterOutput } from "./query";
import { merge } from "./util";
import { createReader, createWriter } from "./channels";
import { Writer, Stream } from "@treecg/connector-types";

import http from "http";
import https from "https";
import stream from "stream";
import path from "path";
import { createUriAndTermNamespace } from "@treecg/types";

const OWL = createUriAndTermNamespace("http://www.w3.org/2002/07/owl#", "imports");
const { namedNode } = DataFactory;

export type Stores = [Store, ...Store[]];

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


const loaded = new Set();
async function load_store(location: string, store: Store, recursive = true) {
  if (loaded.has(location)) { return; }
  loaded.add(location);

  console.log("Loading", location);

  const parser = new StreamParser({ baseIRI: location });
  const rdfStream = await get_readstream(location);
  rdfStream.pipe(parser);

  await new Promise(res => store.import(parser).on('end', res));

  if (recursive) {
    const other_imports = store.getObjects(namedNode(location), OWL.terms.imports, null)
    for (let other of other_imports) {
      await load_store(other.value, store, true);
    }
  }
}

type ChannelParts = { [label: string]: any };

type ProcField = { loc: number, value: string };
type ProcOut = { file: string, func: string, location: string };
async function handleProcs(store: Stores, readers: ChannelParts, writers: ChannelParts): Promise<{ [label: string]: { [label: string]: ProcField } & ProcOut }> {
  const fields: (keyof ProcOutput)[] = procOutputFields;
  const res = await executeQuery<ProcOutput>(store, procQuery, fields);

  const grouped = merge(res, "subject", "loc", "value", ["func", "file", "location"], (v, k, item) => {
    const loc = parseInt(k.value);
    if (item.class?.value === "https://w3id.org/conn#ReaderChannel") {
      return {
        loc,
        value: readers[v.value],
      }
    } else if (item.class?.value === "https://w3id.org/conn#WriterChannel") {
      return {
        loc,
        value: writers[v.value],
      }
    } else {
      return {
        loc,
        value: v.value,
      }
    }
  });


  return grouped;
}


async function handleChannels(store: Stores): Promise<[ChannelParts, ChannelParts]> {
  const readerPromises: Promise<[Stream<string>, string]>[] = [];
  const writerPromises: Promise<[Writer<string>, string]>[] = [];

  const fields = channelOutputFields;
  const readers = await executeQuery<ReaderOutput>(store, readerQuery, fields);

  for (let reader of readers) {
    readerPromises.push(createReader(reader.reader, store, reader.reader?.value).then(x => [x, reader.reader.value]));
  }

  const writers = await executeQuery<WriterOutput>(store, writerQuery, fields);

  for (let writer of writers) {
    writerPromises.push(createWriter(writer.writer, store, writer.reader?.value).then(x => [x, writer.writer.value]));
  }

  const [rs, ws] = await Promise.all([Promise.all(readerPromises), Promise.all(writerPromises)]);
  const readerParts = {} as ChannelParts;
  const writerParts = {} as ChannelParts;

  for (let [r, id] of rs) {
    readerParts[id] = r;
  }
  for (let [w, id] of ws) {
    writerParts[id] = w;
  }

  return [readerParts, writerParts];
}


interface Args {
  fields: ProcField[],
  procOut: ProcOut,
}
function executeProc(args: Args): PromiseLike<any> {
  const fields = args.fields;
  const proc = args.procOut;

  process.chdir(proc.location);
  const root = path.join(process.cwd(), proc.file);
  const jsProgram = require(root);

  const functionArgs = new Array(fields.length);

  for (let field of fields) {
    if (typeof field === "object" && field["loc"] != undefined) {
      functionArgs[field.loc] = field.value;
    }
  }

  return jsProgram[proc.func](...functionArgs);
}

async function main() {
  const args = getArgs();
  const cwd = process.cwd();

  const store = new Store();
  await load_store(path.join(cwd, args.input), store);
  const stores = <[Store, ...Store[]]>[store];

  const [readers, writers] = await handleChannels(stores);

  const procs = await handleProcs(stores, readers, writers);

  const execs = [];
  for (let proc of Object.values(procs)) {
    const procConfig: ProcOut = {
      file: proc.file,
      func: proc.func,
      location: proc.location,
    };

    const fields = Object.values(proc);

    const args: Args = { fields: <ProcField[]>fields, procOut: procConfig };
    execs.push(executeProc(args).then(() => {
      console.log(`Finished ${procConfig.file}:${procConfig.func}`);
    }));
  }

  await Promise.all(execs);
}

main();

