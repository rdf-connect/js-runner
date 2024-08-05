import http from "http";
import { Buffer } from "buffer";

function streamToString(ev) {
    const datas = [];
    return new Promise((res) => {
        ev.on("data", (d) => datas.push(d));
        ev.on("end", () => res(Buffer.concat(datas)));
    });
}

export async function send(msg, writer) {
    const host = "0.0.0.0";
    const port = 8000;
    const requestListener = async function (req, res) {
        const data = await streamToString(req);
        const ret = `${msg} ${data}`;
        await writer.push(ret);
        res.writeHead(200);
        res.end(ret);
    };
    const server = http.createServer(requestListener);

    await new Promise((res) => {
        server.listen(port, host, () => {
            console.log(`Server is running on http://${host}:${port} prefix ${msg}`);
            res();
        });
    });

    return () => writer.push("Hallo!");
}

export function resc(reader) {
    reader.data((x) => console.log("data", x));
}

export async function echo(inputChannel, outputChannel) {
    inputChannel.data(async (x) => {
        await outputChannel.push(x);
    });
    inputChannel.on("end", async () => { await outputChannel.end(); });
    if (inputChannel.lastElement) {
        await outputChannel.push(inputChannel.lastElement);
    }
}
