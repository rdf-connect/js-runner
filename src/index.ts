import { Store, StreamParser } from "n3";
import { createReadStream } from "fs";
import { getArgs } from "./args";
import { executeQuery, procQuery, ProcOutput, procOutputFields, channelOutputFields, readerQuery, ReaderOutput, writerQuery, WriterOutput } from "./query";
import { merge } from "./util";
import { createReader, createWriter } from "./channels";
import { Writer, Stream } from "@treecg/connector-types";

import * as path from 'path';

function importFile(store: Store, file: string): Promise<void> {
  const parser = new StreamParser();
  const rdfStream = createReadStream(file);
  rdfStream.pipe(parser);

  return new Promise(res => store.import(parser).on('end', res));
}

type ChannelParts = { [label: string]: any };

type ProcField = { loc: number, value: string };
type ProcOut = { file: string, func: string };
async function handleProcs(store: Store, readers: ChannelParts, writers: ChannelParts): Promise<{ [label: string]: { [label: string]: ProcField } & ProcOut }> {
  const fields: (keyof ProcOutput)[] = procOutputFields;
  const res = await executeQuery<ProcOutput>(store, procQuery, fields);

  const grouped = merge(res, "subject", "loc", "value", ["func", "file"], (v, k, item) => {
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

async function handleChannels(store: Store): Promise<[ChannelParts, ChannelParts]> {
  const readerPromises: Promise<[Stream<string>, string]>[] = [];
  const writerPromises: Promise<[Writer<string>, string]>[] = [];

  const fields = channelOutputFields;
  const readers = await executeQuery<ReaderOutput>(store, readerQuery, fields);

  const rGrouped = merge(readers, "reader", "prop", "value", ["reader"])

  for (let id in rGrouped) {
    readerPromises.push(createReader(rGrouped[id]).then(x => [x, id]));
  }

  const writers = await executeQuery<WriterOutput>(store, writerQuery, fields);
  const wGrouped = merge(writers, "writer", "prop", "value", ["reader"])

  for (let id in wGrouped) {
    writerPromises.push(createWriter(wGrouped[id]).then(x => [x, id]));
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

function executeProc(fields: ProcField[], proc: ProcOut) {
  // const root = path.join(pWd, processorConfig.location || process.cwd(), processorConfig.config.jsFile);
  const root = path.join(process.cwd(), proc.file);
  const jsProgram = require(root);

  const args = new Array(fields.length);

  for (let field of fields) {
    if (typeof field === "object" && field["loc"] != undefined) {
      args[field.loc] = field.value;
    }
  }

  jsProgram[proc.func](...args);
}

async function main() {
  const args = getArgs();
  const store = new Store();

  // Loading ontologies
  await Promise.all(args.ontology.map(f => importFile(store, f)));

  await importFile(store, args.input);

  const [readers, writers] = await handleChannels(store);

  const procs = await handleProcs(store, readers, writers);

  const execs = [];
  for (let proc of Object.values(procs)) {
    const procConfig: ProcOut = {
      file: proc.file,
      func: proc.func,
    };

    const fields = Object.values(proc);
    execs.push(executeProc(<ProcField[]>fields, procConfig))
  }

  await Promise.all(execs);
}

main();

