import { describe, expect, test } from "vitest";
import { extractProcessors, extractSteps, Source } from "../src/index";

type ChannelArg = {
    config: { channel: { id: string } };
    ty: string;
};

const prefixes = `
@prefix rdfc-js: <https://w3id.org/rdf-connect/js#>.
@prefix ws: <https://w3id.org/conn/ws#>.
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix owl: <http://www.w3.org/2002/07/owl#>.
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix sh: <http://www.w3.org/ns/shacl#>.
@prefix rdfl: <https://w3id.org/rdf-lens/ontology#>.
`;

const JS = "https://w3id.org/rdf-connect/js#";
describe("test existing processors", () => {
    test("resc.ttl", async () => {
        const value = `${prefixes}
<> owl:imports <./ontology.ttl>, <./processor/resc.ttl>.

[ ] a rdfc-js:JSChannel;
    rdfc:reader <jr>;
    rdfc:writer <jw>.
<jr> a rdfc-js:JSReaderChannel.
[ ] a rdfc-js:Resc;
    rdfc-js:rescReader <jr>.
`;
        const baseIRI = process.cwd() + "/config.ttl";

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
        expect((<ChannelArg>arg).config.channel).toBeDefined();
        expect((<ChannelArg>arg).config.channel.id).toBeDefined();
        expect((<ChannelArg>arg).ty).toBeDefined();
    });

    test("send.ttl", async () => {
        const value = `${prefixes}
<> owl:imports <./ontology.ttl>, <./processor/send.ttl> .

[ ] a rdfc-js:JSChannel;
    rdfc:reader <jr>;
    rdfc:writer <jw>.
<jr> a rdfc-js:JSReaderChannel.
<jw> a rdfc-js:JSWriterChannel.
[ ] a rdfc-js:Send;
    rdfc-js:msg "Hello world!";
    rdfc-js:sendWriter <jw>.
`;
        const baseIRI = process.cwd() + "/config.ttl";

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
        expect((<ChannelArg>writer).config.channel).toBeDefined();
        expect((<ChannelArg>writer).config.channel.id).toBeDefined();
        expect((<ChannelArg>writer).ty).toBeDefined();
    });

    describe("send.ttl from env", async () => {
        const value = `${prefixes}
<> owl:imports <./ontology.ttl>, <./processor/send.ttl> .

[ ] a rdfc-js:JSChannel;
    rdfc:reader <jr>;
    rdfc:writer <jw>.
<jr> a rdfc-js:JSReaderChannel.
<jw> a rdfc-js:JSWriterChannel.
[ ] a rdfc-js:Send;
    rdfc-js:msg [
        a rdfl:EnvVariable;
        rdfl:envDefault "FromEnv";
        rdfl:envKey "msg"
    ];
    rdfc-js:sendWriter <jw>.
`;
        const baseIRI = process.cwd() + "/config.ttl";

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
            expect((<ChannelArg>writer).config.channel).toBeDefined();
            expect((<ChannelArg>writer).config.channel.id).toBeDefined();
            expect((<ChannelArg>writer).ty).toBeDefined();
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
            expect((<{ config: { channel: { id: string } }, ty: string }>writer).config.channel).toBeDefined();
            expect((<{ config: { channel: { id: string } }, ty: string }>writer).config.channel.id).toBeDefined();
            expect((<{ config: { channel: { id: string } }, ty: string }>writer).ty).toBeDefined();
        });
    });

    test("echo.ttl", async () => {
        const value = `${prefixes}
<> owl:imports <./ontology.ttl>, <./processor/echo.ttl> .

[ ] a rdfc-js:JSChannel;
    rdfc:reader <jr>;
    rdfc:writer <jw>.

<jr> a rdfc-js:JSReaderChannel.
<jw> a rdfc-js:JSWriterChannel.

[ ] a rdfc-js:Echo;
    rdfc-js:input <jr>;
    rdfc-js:output <jw>.
`;
        const baseIRI = process.cwd() + "/config.ttl";

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
        expect((<ChannelArg>reader).config.channel).toBeDefined();
        expect((<ChannelArg>reader).config.channel.id).toBeDefined();
        expect((<ChannelArg>reader).ty).toBeDefined();

        expect(writer).toBeInstanceOf(Object);
        expect((<ChannelArg>reader).config.channel).toBeDefined();
        expect((<ChannelArg>reader).config.channel.id).toBeDefined();
        expect((<ChannelArg>reader).ty).toBeDefined();
    });
});
