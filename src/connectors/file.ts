import { createReadStream, openSync } from "fs";
import { appendFile, readFile, stat, writeFile } from "fs/promises";
import { isAbsolute } from "path";
import { watch } from "node:fs";
import {
    ReaderConstructor,
    SimpleStream,
    WriterConstructor,
} from "../connectors";

interface FileError extends Error {
    code: string;
}

export interface FileReaderConfig {
    path: string;
    onReplace: boolean;
    readFirstContent?: boolean;
    encoding?: string;
}

export interface FileWriterConfig {
    path: string;
    onReplace: boolean;
    readFirstContent?: boolean;
    encoding?: string;
}

async function getFileSize(path: string): Promise<number> {
    return (await stat(path)).size;
}

function readPart(
    path: string,
    start: number,
    end: number,
    encoding: BufferEncoding,
): Promise<string> {
    return new Promise((res) => {
        const stream = createReadStream(path, { encoding, start, end });
        let buffer = "";
        stream.on("data", (chunk) => {
            buffer += chunk;
        });
        stream.on("close", () => res(buffer));
    });
}

function debounce<A>(func: (t: A) => void, timeout = 100): (t: A) => void {
    let timer: ReturnType<typeof setTimeout>;
    return (...args) => {
        clearTimeout(timer);
        timer = setTimeout(() => {
            func(...args);
        }, timeout);
    };
}
export const startFileStreamReader: ReaderConstructor<FileReaderConfig> = (
    config,
) => {
    const path = isAbsolute(config.path)
        ? config.path
        : `${process.cwd()}/${config.path}`;
    openSync(path, "a+");
    const encoding: BufferEncoding = <BufferEncoding>config.encoding || "utf-8";
    const reader = new SimpleStream<string>();

    const init = async () => {
        let currentPos = await getFileSize(path);
        const watcher = watch(path, { encoding: "utf-8" });
        watcher.on(
            "change",
            debounce(async () => {
                try {
                    let content: string;
                    if (config.onReplace) {
                        content = await readFile(path, { encoding });
                    } else {
                        const newSize = await getFileSize(path);

                        if (newSize <= currentPos) {
                            currentPos = newSize;
                            return;
                        }

                        content = await readPart(
                            path,
                            currentPos,
                            newSize,
                            encoding,
                        );
                        currentPos = newSize;
                    }

                    await reader.push(content);
                } catch (error: unknown) {
                    if ((<FileError>error).code === "ENOENT") {
                        return;
                    }
                    throw error;
                }
            }),
        );

        if (config.onReplace && config.readFirstContent) {
            const content = await readFile(path, { encoding });
            await reader.push(content);
        }
    };

    return { reader, init };
};

// export interface FileWriterConfig extends FileReaderConfig {}

export const startFileStreamWriter: WriterConstructor<FileWriterConfig> = (
    config,
) => {
    const path = isAbsolute(config.path)
        ? config.path
        : `${process.cwd()}/${config.path}`;
    const encoding: BufferEncoding = <BufferEncoding>config.encoding || "utf-8";

    const writer = new SimpleStream<string>();

    const init = async () => {
        if (!config.onReplace) {
            await writeFile(path, "", { encoding });
        }
    };

    writer.push = async (item: string): Promise<void> => {
        if (config.onReplace) {
            await writeFile(path, item, { encoding });
        } else {
            await appendFile(path, item, { encoding });
        }
    };

    return { writer, init };
};
