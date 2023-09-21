import { describe, expect, test } from "@jest/globals";
import { writeFile } from "fs/promises";

import * as conn from "../../src/connectors/index";

describe("connector-ws", () => {
  test("Should write -> WebSocket -> read", async () => {
    const readerConfig: conn.ws.WsReaderConfig = {
      ty: conn.Conn.WsReaderChannel,
      host: "0.0.0.0",
      port: 8123,
    };

    const writerConfig: conn.ws.WsWriterConfig = {
      ty: conn.Conn.WsWriterChannel,
      url: "ws://127.0.0.1:8123",
    };

    const factory = new conn.ChannelFactory();
    const reader = factory.createReader(readerConfig);
    const writer = factory.createWriter(writerConfig);
    const items: unknown[] = [];
    reader.data((x) => {
      items.push(x);
    });

    await factory.init();

    await writer.push("test1");
    await writer.push("test2");
    await sleep(200);

    expect(items).toEqual(["test1", "test2"]);

    await Promise.all([writer.end(), reader.end()]);
  });
});

function sleep(x: number): Promise<unknown> {
  return new Promise((resolve) => setTimeout(resolve, x));
}
