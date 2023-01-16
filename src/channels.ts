import { AllReaderFactory, AllWriterFactory } from "@treecg/connector-all";
import { Writer, Stream, SimpleStream } from "@treecg/connector-types";
import { typeNode } from "./util";
import { WsReaderConfig, WsWriterConfig } from "@treecg/connector-ws";
import { HttpReaderConfig, HttpWriterConfig } from "@treecg/connector-http";
import { FileReaderConfig, FileWriterConfig } from "@treecg/connector-file";

type Map = { [label: string]: string };

const CONN = "https://w3id.org/conn#";
function objToWsConfig(obj: Map): WsReaderConfig & WsWriterConfig {
  let port, url, host;

  if (obj[CONN + "wsPort"]) {
    port = parseInt(obj[CONN + "wsPort"]);
    url = "ws://0.0.0.0:" + port;
    host = "0.0.0.0";
  } else if (obj[CONN + "wsUri"]) {
    url = obj[CONN + "wsUri"];
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



function parseBool(stringValue: string): boolean {
  switch (stringValue?.toLowerCase()?.trim()) {
    case "true":
    case "yes":
    case "1":
      return true;

    case "false":
    case "no":
    case "0":
    case null:
    case undefined:
      return false;

    default:
      return JSON.parse(stringValue);
  }
}

function objToFileConfig(obj: Map): FileReaderConfig & FileWriterConfig {
  const maybePath = obj[CONN + "filePath"];
  const maybeOnReplace = obj[CONN + "fileOnReplace"];
  const encoding = obj[CONN + "fileEncoding"];
  const maybeReadFirstContent = obj[CONN + "fileReadFirstContent"];

  if (!maybePath) { throw `${CONN + "filePath"} is not specified` };
  const path = maybePath!;
  const onReplace = parseBool(maybeOnReplace);
  const readFirstContent = parseBool(maybeReadFirstContent);


  return {

    path,
    onReplace,
    encoding,
    readFirstContent

  }
}

function objToHttpConfig(obj: Map): HttpReaderConfig & HttpWriterConfig {
  let port, url, host, method;

  if (obj[CONN + "httpPort"]) {
    port = parseInt(obj[CONN + "httpPort"]);
    url = "http://0.0.0.0:" + port;
    host = "0.0.0.0";
    method = obj[CONN + "httpMethod"]
  } else if (obj[CONN + "httpEndpoint"]) {
    url = obj[CONN + "httpEndpoint"];
    const parsed = new URL(url);
    port = parseInt(parsed.port);
    host = parsed.host;
    method = obj[CONN + "httpMethod"]
  } else {
    throw "Nope";
  }

  method = method || "POST";

  return {
    url,
    port,
    host,
    method
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
    case CONN + "HttpReaderChannel":
      return readerFactory.build({
        type: "http",
        config: objToHttpConfig(obj),
      });
    case CONN + "FileReaderChannel":
      return readerFactory.build({
        type: "file",
        config: objToFileConfig(obj),
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

    case CONN + "HttpWriterChannel":
      return writerFactory.build({
        type: "http",
        config: objToHttpConfig(obj),
      });

    case CONN + "FileWriterChannel":
      return writerFactory.build({
        type: "file",
        config: objToFileConfig(obj),
      });

    case "https://w3id.org/conn#JsWriterChannel":
      return createJsWriter(obj.reader);

    default:
      throw `Writer type not supported ${ty}`;
  }
}

