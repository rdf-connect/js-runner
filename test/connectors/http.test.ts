import { describe, expect, test } from "@jest/globals";
import * as conn from "../../src/connectors";
import { HttpReaderConfig, HttpWriterConfig } from "../../src/connectors/http";

describe("connector-http", () => {
  test("Should write -> HTTP -> read (string)", async () => {
    const readerConfig: HttpReaderConfig = {
      endpoint: "localhost",
      port: 8080,
      binary: false,
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

  test("Should write -> HTTP -> read (Buffer)", async () => {
    const readerConfig: HttpReaderConfig = {
      endpoint: "localhost",
      port: 8081,
      binary: true,
      ty: conn.Conn.HttpReaderChannel,
    };
    const writerConfig: HttpWriterConfig = {
      endpoint: "http://localhost:8081",
      method: "POST",
      ty: conn.Conn.HttpWriterChannel,
    };

    const factory = new conn.ChannelFactory();
    const reader = factory.createReader(readerConfig);
    const writer = factory.createWriter(writerConfig);
    
    reader.data((data) => {
      expect(Buffer.isBuffer(data)).toBeTruthy();
      items.push(data.toString());
    });

    await factory.init();

    const items: unknown[] = [];

    await writer.push(Buffer.from("test1", "utf8"));
    await sleep(200);
    await writer.push(Buffer.from("test2", "utf8"));
    await sleep(200);

    expect(items).toEqual(["test1", "test2"]);

    await Promise.all([reader.end(), writer.end()]);
  });
});

function sleep(x: number): Promise<unknown> {
  return new Promise((resolve) => setTimeout(resolve, x));
}
