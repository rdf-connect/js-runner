import http from "http";

function streamToString(ev) {
  const datas = [];
  return new Promise((res) => {
    ev.on("data", d => datas.push(d));
    ev.on("end", () => res(Buffer.concat(datas)))
  });
}

export async function send(msg, writer) {
  const host = '0.0.0.0';
  const port = 8000;
  const requestListener = function(req, res) {
    streamToString(req).then(st => writer.push(st.toString()));
    res.writeHead(200);
    res.end(msg);
  };
  const server = http.createServer(requestListener);
  server.listen(port, host, () => {
    console.log(`Server is running on http://${host}:${port} prefix ${msg}`);
  });

  writer.push("Hallo!");
}

export async function resc(reader) {
  if (reader.lastElement) {
    console.log("data", reader.lastElement);
  }
  reader.data(x => console.log("data", x));
}

