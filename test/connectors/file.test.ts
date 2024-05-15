import { describe, expect, test } from "vitest";
import { writeFile } from "fs/promises";
import { FileReaderConfig, FileWriterConfig } from "../../src/connectors/file";
import * as conn from "../../src/connectors";
import { namedNode } from "../../src/util";

describe("File Channel", () => {
    test("Reader - Writer", async () => {
        const config: FileReaderConfig = {
            path: "/tmp/test.txt",
            onReplace: true,
            encoding: "utf-8",
        };
        const writerConfig: FileWriterConfig = {
            path: "/tmp/test.txt",
            onReplace: true,
            encoding: "utf-8",
        };

        await writeFile("/tmp/test.txt", "");

        const factory = new conn.ChannelFactory();
        const items: string[] = [];

        const reader = factory.createReader({
            config,
            id: namedNode("reader"),
            ty: conn.Conn.FileReaderChannel,
        });
        expect(reader).toBeInstanceOf(conn.SimpleStream);

        reader.data((x) => {
            items.push(<string>x);
        });

        const writer = factory.createWriter({
            config: writerConfig,
            id: namedNode("writer"),
            ty: conn.Conn.FileWriterChannel,
        });
        await factory.init();
        await writer.push("Number 1 " + Math.random());

        await sleep(300);
        expect(items.length).toBe(1);
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
