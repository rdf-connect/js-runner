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
    ReaderConstructor,
    SimpleStream,
    WriterConstructor,
} from "../connectors";

function streamToString(
    stream: Readable,
    binary: boolean,
): Promise<string | Buffer> {
    const datas = <Buffer[]>[];
    return new Promise((res) => {
        stream.on("data", (data) => {
            datas.push(data);
        });
        stream.on("end", () => {
            const streamData = Buffer.concat(datas);
            res(binary ? streamData : streamData.toString());
        });
    });
}

export interface HttpReaderConfig {
    endpoint: string;
    port: number;
    binary: boolean;
    waitHandled?: boolean;
    responseCode?: number;
}

export const startHttpStreamReader: ReaderConstructor<HttpReaderConfig> = (
    config,
) => {
    let server: Server | undefined = undefined;

    const stream = new SimpleStream<string | Buffer>(
        () =>
            new Promise((res) => {
                if (server !== undefined) {
                    server.close(() => {
                        res();
                    });
                } else {
                    res();
                }
            }),
    );

    const requestListener: RequestListener = async function (
        req: IncomingMessage,
        res: ServerResponse,
    ) {
        try {
            const content = await streamToString(req, config.binary);

            const promise = stream.push(content).catch((error) => {
                throw error;
            });
            if (config.waitHandled) {
                await promise;
            }
        } catch (error: unknown) {
            console.error("Failed", error);
        }

        res.writeHead(config.responseCode || 200);
        res.end("OK");
    };

    server = createServer(requestListener);
    const init = () => {
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

export interface HttpWriterConfig {
    endpoint: string;
    method: string;
}

export const startHttpStreamWriter: WriterConstructor<HttpWriterConfig> = (
    config,
) => {
    const requestConfig = <https.RequestOptions>new URL(config.endpoint);

    const writer = new SimpleStream<string | Buffer>();

    writer.push = async (item: string | Buffer): Promise<void> => {
        await new Promise((resolve) => {
            const options = {
                hostname: requestConfig.hostname,
                path: requestConfig.path,
                method: config.method,
                port: requestConfig.port,
            };

            const cb = (response: IncomingMessage): void => {
                response.on("data", () => {});
                response.on("end", () => {
                    resolve(null);
                });
            };

            const req = http.request(options, cb);
            req.write(item, () => {
                req.end();
            });
            // res(null);
        });
    };

    return { writer, init: async () => {} };
};
