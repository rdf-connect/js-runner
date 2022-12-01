
async function send(msg, writer) {
  await writer.push(msg);

  await new Promise(res => setTimeout(res, 200));
  await writer.push(msg);
}

async function resc(reader) {
  if (reader.lastElement) {
    console.log(reader.lastElement);
  }
  reader.data(x => console.log("data", x));
}

exports.send = send;
exports.resc = resc;
