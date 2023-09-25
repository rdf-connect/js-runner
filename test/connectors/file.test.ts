import { describe, expect, test } from "@jest/globals";
import { writeFile } from "fs/promises";

import * as conn from "../../src/connectors";
import { FileReaderConfig, FileWriterConfig } from "../../src/connectors/file";

describe("File Channel", () => {
  test("Reader - Writer", async () => {
    const config: FileReaderConfig = {
      ty: conn.Conn.FileReaderChannel,
      path: "/tmp/test.txt",
      onReplace: true,
      encoding: "utf-8",
    };
    const writerConfig: FileWriterConfig = {
      ty: conn.Conn.FileWriterChannel,
      path: "/tmp/test.txt",
      onReplace: true,
      encoding: "utf-8",
    };

    await writeFile("/tmp/test.txt", "");

    const factory = new conn.ChannelFactory();
    const items: string[] = [];

    const reader = factory.createReader(config);
    expect(reader).toBeInstanceOf(conn.SimpleStream);

    reader.data((x) => {
      items.push(x);
    });

    const writer = factory.createWriter(writerConfig);
    await factory.init();
    await writer.push("Number 1 " + Math.random());

    await sleep(300);
    expect(items.length).toBe(1);
    console.log(items);
    expect(items[0].startsWith("Number 1")).toBeTruthy();

    await writer.push("Number 2");

    await sleep(300);
    expect(items.length).toBe(2);
    expect(items[1]).toBe("Number 2");
  });
});

function sleep(x: number): Promise<unknown> {
  return new Promise((resolve) => setTimeout(resolve, x));
}
