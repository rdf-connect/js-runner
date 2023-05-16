import { Store } from "n3";
import { getArgs } from "./args.js";
import { executeQuery, procQuery, ProcOutput, procOutputFields, channelOutputFields, readerQuery, ReaderOutput, writerQuery, WriterOutput } from "./query.js";
import { load_store, merge } from "./util.js";
import { createReader, createWriter } from "./channels.js";
import { Writer, Stream } from "@treecg/connector-types";
export * from "./docker.js";

import path from "path";


export type Stores = [Store, ...Store[]];


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
async function executeProc(args: Args): Promise<PromiseLike<any>> {
  const fields = args.fields;
  const proc = args.procOut;
  console.log("chdir", proc.location);

  process.chdir(proc.location);
  console.log("at", process.cwd());
  const jsProgram = await import("file://" + proc.file);

  const functionArgs = new Array(fields.length);

  for (let field of fields) {
    if (typeof field === "object" && field["loc"] != undefined) {
      functionArgs[field.loc] = field.value;
    }
  }

  await jsProgram[proc.func](...functionArgs);
}

function safeJoin(a: string, b: string) {
  if (b.startsWith("/")) {
    return b;
  }
  return path.join(a, b);
}

export async function jsRunner() {
  const args = getArgs();
  const cwd = process.cwd();

  const store = new Store();
  await load_store(safeJoin(cwd, args.input).replaceAll("\\", "/"), store);
  const stores = <[Store, ...Store[]]>[store];

  const [readers, writers] = await handleChannels(stores);

  const procs = await handleProcs(stores, readers, writers);
  console.log(`Found ${Object.values(procs).length} processors ${Object.values(procs).map(x => x.file + ":" + x.func).join(", ")} `);

  const execs = [];
  for (let proc of Object.values(procs)) {
    const procConfig: ProcOut = {
      file: proc.file,
      func: proc.func,
      location: proc.location,
    };

    const fields = Object.values(proc);

    const args: Args = { fields: <ProcField[]>fields, procOut: procConfig };
    await executeProc(args);
    // execs.push(executeProc(args).then(() => {
    console.log(`Finished ${procConfig.file}:${procConfig.func}`);
    // }));
  }

  // await Promise.all(execs);
}


