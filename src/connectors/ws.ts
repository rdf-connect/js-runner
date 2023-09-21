import {
  Config,
  ReaderConstructor,
  SimpleStream,
  Stream,
  Writer,
  WriterConstructor,
} from ".";
import { RawData, WebSocket } from "ws";
import { WebSocketServer } from "ws";

export interface WsWriterConfig extends Config {
  url: string;
}

function _connectWs(url: string, res: (value: WebSocket) => void): void {
  const ws = new WebSocket(url, {});
  ws.on("error", () => {
    setTimeout(() => _connectWs(url, res), 300);
  });

  ws.on("ping", () => ws.pong());
  ws.on("open", () => {
    res(ws);
  });
}

function connectWs(url: string): Promise<WebSocket> {
  return new Promise((res) => _connectWs(url, res));
}

export interface WsReaderConfig extends Config {
  host: string;
  port: number;
}

export const startWsStreamReader: ReaderConstructor<WsReaderConfig> = (
  config,
) => {
  const server = new WebSocketServer(config);
  server.on("error", (error) => {
    console.error("Ws server error:");
    console.error(error);
  });

  const connections: { socket: WebSocket; alive: boolean }[] = [];

  const interval = setInterval(() => {
    connections.forEach((instance, i) => {
      if (!instance) {
        return;
      }
      if (!instance.alive) {
        instance.socket.terminate();
        delete connections[i];

        return;
      }

      instance.socket.ping();
      instance.alive = false;
    });
  }, 30_000);

  const reader = new SimpleStream<string>(
    () =>
      new Promise((res) => {
        clearInterval(interval);
        server.close(() => res());
      }),
  );

  server.on("connection", (ws) => {
    const instance = { socket: ws, alive: true };
    connections.push(instance);

    ws.on("message", async (msg: RawData) => {
      reader.push(msg.toString()).catch((error) => {
        throw error;
      });
    });

    ws.on("pong", () => {
      instance.alive = true;
    });
  });

  return { reader, init: async () => {} };
};

export const startWsStreamWriter: WriterConstructor<WsWriterConfig> = (
  config,
) => {
  let ws: WebSocket;
  const init = async () => {
    ws = await connectWs(config.url);
    ws.on("open", () => console.log("open"));
  };

  const push = async (item: any): Promise<void> => {
    ws.send(item);
  };

  const end = async (): Promise<void> => {
    ws.close();
  };

  return { writer: { push, end }, init };
};
