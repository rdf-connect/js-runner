import * as http from "http";
import type * as https from "https";
import type {
  IncomingMessage,
  RequestListener,
  Server,
  ServerResponse,
} from "http";
import { createServer } from "http";
import type { Readable } from "stream";
import {
  Config,
  ReaderConstructor,
  SimpleStream,
  WriterConstructor,
} from "../connectors";

function streamToString(stream: Readable): Promise<string> {
  const datas = <Buffer[]>[];
  return new Promise((res) => {
    stream.on("data", (data) => {
      datas.push(data);
    });
    stream.on("end", () => res(Buffer.concat(datas).toString()));
  });
}

export interface HttpReaderConfig extends Config {
  endpoint: string;
  port: number;
}

export const startHttpStreamReader: ReaderConstructor<HttpReaderConfig> = (
  config,
) => {
  let server: Server;

  const stream = new SimpleStream<string>(
    () =>
      new Promise((res) => {
        const cb = (): void => res();
        if (server !== undefined) {
          server.close(cb);
        } else {
          cb();
        }
      }),
  );

  const requestListener: RequestListener = async function (
    req: IncomingMessage,
    res: ServerResponse,
  ) {
    try {
      const content = await streamToString(req);
      stream.push(content).catch((error) => {
        throw error;
      });
    } catch (error: unknown) {
      console.error("Failed", error);
    }

    res.writeHead(200);
    res.end("OK");
  };

  server = createServer(requestListener);
  const init = () => {
    console.log("HTTP init!");
    return new Promise<void>((res) => {
      const cb = (): void => res(undefined);
      if (server) {
        server.listen(config.port, config.endpoint, cb);
      } else {
        cb();
      }
    });
  };
  return { reader: stream, init };
};

export interface HttpWriterConfig extends Config {
  endpoint: string;
  method: string;
}

export const startHttpStreamWriter: WriterConstructor<HttpWriterConfig> = (
  config,
) => {
  const requestConfig = <https.RequestOptions>new URL(config.endpoint);

  const push = async (item: string): Promise<void> => {
    await new Promise((res) => {
      const options = {
        hostname: requestConfig.hostname,
        path: requestConfig.path,
        method: config.method,
        port: requestConfig.port,
      };
      const cb = (response: IncomingMessage): void => {
        response.on("data", () => {});
        response.on("end", () => {
          res(null);
        });
      };

      const req = http.request(options, cb);
      req.write(item, () => res(null));
      req.end();
    });
  };

  const end = async (): Promise<void> => {};

  return { writer: { push, end }, init: async () => {} };
};
