import { describe, expect, test } from "vitest";
import * as conn from "../../src/connectors";
import { HttpReaderConfig, HttpWriterConfig } from "../../src/connectors/http";
import { namedNode } from "../../src/util";

describe("connector-http", () => {
    test("Should write -> HTTP -> read (string)", async () => {
        const readerConfig: HttpReaderConfig = {
            endpoint: "localhost",
            port: 8080,
            binary: false,
        };
        const writerConfig: HttpWriterConfig = {
            endpoint: "http://localhost:8080",
            method: "POST",
        };

        const factory = new conn.ChannelFactory();
        const reader = factory.createReader({
            config: readerConfig,
            id: namedNode("reader"),
            ty: conn.Conn.HttpReaderChannel,
        });
        const writer = factory.createWriter({
            config: writerConfig,
            id: namedNode("writer"),
            ty: conn.Conn.HttpWriterChannel,
        });

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
            waitHandled: false,
        };
        const writerConfig: HttpWriterConfig = {
            endpoint: "http://localhost:8081",
            method: "POST",
        };

        const factory = new conn.ChannelFactory();
        const reader = factory.createReader({
            config: readerConfig,
            id: namedNode("reader"),
            ty: conn.Conn.HttpReaderChannel,
        });
        const writer = factory.createWriter({
            config: writerConfig,
            id: namedNode("writer"),
            ty: conn.Conn.HttpWriterChannel,
        });

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

    test("Should write -> HTTP -> read (Buffer) and await response", async () => {
        const readerConfig: HttpReaderConfig = {
            endpoint: "localhost",
            port: 8082,
            binary: true,
            waitHandled: true,
        };
        const writerConfig: HttpWriterConfig = {
            endpoint: "http://localhost:8082",
            method: "POST",
        };

        const factory = new conn.ChannelFactory();
        const reader = factory.createReader({
            config: readerConfig,
            id: namedNode("reader"),
            ty: conn.Conn.HttpReaderChannel,
        });
        const writer = factory.createWriter({
            config: writerConfig,
            id: namedNode("writer"),
            ty: conn.Conn.HttpWriterChannel,
        });

        reader.data(async (data) => {
            expect(Buffer.isBuffer(data)).toBeTruthy();
            items.push(data.toString());
            await sleep(1500);
        });

        await factory.init();

        const items: unknown[] = [];

        const start = new Date().getTime();
        await writer.push(Buffer.from("test1", "utf8"));
        const end = new Date().getTime();
        await sleep(200);

        expect(end - start > 1000).toBeTruthy();
        expect(items).toEqual(["test1"]);

        await Promise.all([reader.end(), writer.end()]);
    });

    test("http channel uses correct response code", async () => {
        const readerConfig: HttpReaderConfig = {
            endpoint: "localhost",
            port: 8083,
            binary: false,
            responseCode: 202,
        };

        const factory = new conn.ChannelFactory();
        const reader = factory.createReader({
            config: readerConfig,
            id: namedNode("reader"),
            ty: conn.Conn.HttpReaderChannel,
        });

        reader.data((data) => {
            items.push(data);
        });

        await factory.init();

        const items: unknown[] = [];

        const resp = await fetch("http://localhost:8083", {
            body: "test1",
            method: "PUT",
        });

        expect(items).toEqual(["test1"]);
        expect(resp.status).toEqual(202);

        await Promise.all([reader.end()]);
    });
});

function sleep(x: number): Promise<unknown> {
    return new Promise((resolve) => setTimeout(resolve, x));
}
