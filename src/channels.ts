import { AllReaderFactory, AllWriterFactory } from "@treecg/connector-all";
import { Writer, Stream, SimpleStream } from "@treecg/connector-types";
import { typeNode } from "./util";
import { WsReaderConfig, WsWriterConfig } from "@treecg/connector-ws";

type Map = { [label: string]: string };

function objToWsConfig(obj: Map): WsReaderConfig & WsWriterConfig {
  let port, url, host;

  if (obj["https://w3id.org/conn#wsPort"]) {
    port = parseInt(obj["https://w3id.org/conn#wsPort"]);
    url = "ws://0.0.0.0:" + port;
    host = "0.0.0.0";
  } else if (obj["https://w3id.org/conn#wsUri"]) {
    url = obj["https://w3id.org/conn#wsUri"];
    const parsed = new URL(url);
    port = parseInt(parsed.port);
    host = parsed.host;
  } else {
    throw "Nope";
  }

  return {
    url, port, host
  }
}

function getType(obj: Map): string {
  const ty = obj[typeNode];

  if (!ty)
    throw "No type field found";
  return ty;
}



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


export function createReader(obj: Map): Promise<Stream<string>> {
  const ty = getType(obj);
  switch (ty) {
    case "https://w3id.org/conn#WsReaderChannel":
      return readerFactory.build({
        type: "ws",
        config: objToWsConfig(obj),
      });

    case "https://w3id.org/conn#JsReaderChannel":
      return createJsReader(obj.reader);

    default:
      throw `Reader type not supported ${ty}`;
  }
}

export function createWriter(obj: Map): Promise<Writer<string>> {
  const ty = getType(obj);
  switch (ty) {
    case "https://w3id.org/conn#WsWriterChannel":
      return writerFactory.build({
        type: "ws",
        config: objToWsConfig(obj),
      });

    case "https://w3id.org/conn#JsWriterChannel":
      return createJsWriter(obj.reader);

    default:
      throw `Writer type not supported ${ty}`;
  }
}

