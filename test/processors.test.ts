import { describe, expect, test } from "@jest/globals";
import { extractProcessors, extractSteps, Source } from "../src/index";
const prefixes = `
@prefix js: <https://w3id.org/conn/js#>.
@prefix ws: <https://w3id.org/conn/ws#>.
@prefix : <https://w3id.org/conn#>.
@prefix owl: <http://www.w3.org/2002/07/owl#>.
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix sh: <http://www.w3.org/ns/shacl#>.
`;

const JS = "https://w3id.org/conn/js#";
describe("test existing processors", () => {
  test("resc.ttl", async () => {
    const value = `${prefixes}
<> owl:imports <./ontology.ttl>, <./processor/resc.ttl>.

[ ] a :Channel;
  :reader <jr>;
  :writer <jw>.
<jr> a js:JsReaderChannel.
[ ] a js:Resc;
  js:rescReader <jr>.
`;
    const baseIRI = process.cwd() + "/config.ttl";
    console.log(baseIRI);

    const source: Source = {
      value,
      baseIRI,
      type: "memory",
    };

    const {
      processors,
      quads,
      shapes: config,
    } = await extractProcessors(source);

    const proc = processors.find((x) => x.ty.value === JS + "Resc");
    expect(proc).toBeDefined();

    const argss = extractSteps(proc!, quads, config);
    expect(argss.length).toBe(1);
    expect(argss[0].length).toBe(1);

    const [[arg]] = argss;
    expect(arg).toBeInstanceOf(Object);
    expect(arg.channel).toBeDefined();
    expect(arg.channel.id).toBeDefined();
    expect(arg.ty).toBeDefined();
  });

  test("send.ttl", async () => {
    const value = `${prefixes}
<> owl:imports <./ontology.ttl>, <./processor/send.ttl> .

[ ] a :Channel;
  :reader <jr>;
  :writer <jw>.
<jr> a js:JsReaderChannel.
<jw> a js:JsWriterChannel.
[ ] a js:Send;
  js:msg "Hello world!";
  js:sendWriter <jw>.
`;
    const baseIRI = process.cwd() + "/config.ttl";
    console.log(baseIRI);

    const source: Source = {
      value,
      baseIRI,
      type: "memory",
    };

    const {
      processors,
      quads,
      shapes: config,
    } = await extractProcessors(source);

    const proc = processors.find((x) => x.ty.value === JS + "Send");
    expect(proc).toBeDefined();

    const argss = extractSteps(proc!, quads, config);
    expect(argss.length).toBe(1);
    expect(argss[0].length).toBe(2);

    const [[msg, writer]] = argss;
    expect(msg).toBe("Hello world!");
    expect(writer).toBeInstanceOf(Object);
    expect(writer.channel).toBeDefined();
    expect(writer.channel.id).toBeDefined();
    expect(writer.ty).toBeDefined();
  });

  describe("send.ttl from env", async () => {
    const value = `${prefixes}
<> owl:imports <./ontology.ttl>, <./processor/send.ttl> .

[ ] a :Channel;
  :reader <jr>;
  :writer <jw>.
<jr> a js:JsReaderChannel.
<jw> a js:JsWriterChannel.
[ ] a js:Send;
  js:msg [
    a :EnvVariable;
    :envDefault "FromEnv";
    :envKey "msg"
  ];
  js:sendWriter <jw>.
`;
    const baseIRI = process.cwd() + "/config.ttl";
    console.log(baseIRI);

    const source: Source = {
      value,
      baseIRI,
      type: "memory",
    };

    const {
      processors,
      quads,
      shapes: config,
    } = await extractProcessors(source);

    test("Env default value", () => {
      const proc = processors.find((x) => x.ty.value === JS + "Send");
      expect(proc).toBeDefined();

      const argss = extractSteps(proc!, quads, config);
      expect(argss.length).toBe(1);
      expect(argss[0].length).toBe(2);

      const [[msg, writer]] = argss;
      expect(msg).toBe("FromEnv");
      expect(writer).toBeInstanceOf(Object);
      expect(writer.channel).toBeDefined();
      expect(writer.channel.id).toBeDefined();
      expect(writer.ty).toBeDefined();
    });

    test("Env value", () => {
      process.env["msg"] = "FROM ENV";
      const proc = processors.find((x) => x.ty.value === JS + "Send");
      expect(proc).toBeDefined();

      const argss = extractSteps(proc!, quads, config);
      expect(argss.length).toBe(1);
      expect(argss[0].length).toBe(2);

      const [[msg, writer]] = argss;
      expect(msg).toBe("FROM ENV");
      expect(writer).toBeInstanceOf(Object);
      expect(writer.channel).toBeDefined();
      expect(writer.channel.id).toBeDefined();
      expect(writer.ty).toBeDefined();
    });
  });

  test("echo.ttl", async () => {
    const value = `${prefixes}
<> owl:imports <./ontology.ttl>, <./processor/echo.ttl> .

[ ] a :Channel;
  :reader <jr>;
  :writer <jw>.
<jr> a js:JsReaderChannel.
<jw> a js:JsWriterChannel.
[ ] a js:Echo;
  js:input <jr>;
  js:output <jw>.
`;
    const baseIRI = process.cwd() + "/config.ttl";
    console.log(baseIRI);

    const source: Source = {
      value,
      baseIRI,
      type: "memory",
    };

    const {
      processors,
      quads,
      shapes: config,
    } = await extractProcessors(source);

    const proc = processors.find((x) => x.ty.value === JS + "Echo");
    expect(proc).toBeDefined();
    const argss = extractSteps(proc!, quads, config);
    expect(argss.length).toBe(1);
    expect(argss[0].length).toBe(2);
    const [[reader, writer]] = argss;

    expect(reader).toBeInstanceOf(Object);
    expect(reader.channel).toBeDefined();
    expect(reader.channel.id).toBeDefined();
    expect(reader.ty).toBeDefined();

    expect(writer).toBeInstanceOf(Object);
    expect(writer.channel).toBeDefined();
    expect(writer.channel.id).toBeDefined();
    expect(writer.ty).toBeDefined();
  });
});
