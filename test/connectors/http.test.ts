import { describe, expect, test } from "@jest/globals";
import * as conn from "../../src/connectors";
import { HttpReaderConfig, HttpWriterConfig } from "../../src/connectors/http";

describe("connector-http", () => {
  test("Should write -> HTTP -> read", async () => {
    const readerConfig: HttpReaderConfig = {
      endpoint: "localhost",
      port: 8080,
      ty: conn.Conn.HttpReaderChannel,
    };
    const writerConfig: HttpWriterConfig = {
      endpoint: "http://localhost:8080",
      method: "POST",
      ty: conn.Conn.HttpWriterChannel,
    };

    const factory = new conn.ChannelFactory();
    const reader = factory.createReader(readerConfig);
    const writer = factory.createWriter(writerConfig);
    reader.data((data) => {
      items.push(data);
    });

    await factory.init();

    const items: unknown[] = [];

    await writer.push("test1");
    await sleep(200);
    await writer.push("test2");
    await sleep(200);

    expect(items).toEqual(["test1", "test2"]);

    await Promise.all([reader.end(), writer.end()]);
  });
});

function sleep(x: number): Promise<unknown> {
  return new Promise((resolve) => setTimeout(resolve, x));
}
