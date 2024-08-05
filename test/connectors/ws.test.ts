import { describe, expect, test } from "vitest";
import * as conn from "../../src/connectors";
import { WsReaderConfig, WsWriterConfig } from "../../src/connectors/ws";
import { namedNode, RDFC } from "../../src/util";

describe("connector-ws", () => {
    test("Should write -> WebSocket -> read", async () => {
        const readerConfig: WsReaderConfig = {
            host: "0.0.0.0",
            port: 8123,
        };

        const writerConfig: WsWriterConfig = {
            url: "ws://127.0.0.1:8123",
        };

        const factory = new conn.ChannelFactory();
        const reader = factory.createReader({
            config: readerConfig,
            id: namedNode("reader"),
            ty: RDFC.WebSocketReaderChannel,
        });
        const writer = factory.createWriter({
            config: writerConfig,
            id: namedNode("writer"),
            ty: RDFC.WebSocketWriterChannel,
        });
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
