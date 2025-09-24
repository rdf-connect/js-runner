
import { describe, expect, test, vi } from 'vitest'
import { StreamMsgMock } from '../src/testUtils'
import { WriterInstance } from '../src/writer'
import { OrchestratorMessage } from '@rdfc/proto'
import winston, { createLogger } from 'winston'
import { StreamIdentify } from '@rdfc/proto/lib/generated/common'

const encoder = new TextEncoder()
const decoder = new TextDecoder()

const logger = createLogger({
  transports: new winston.transports.Console({
    level: process.env['DEBUG'] || 'info',
  }),
})

describe("Writer", async () => {
  test("sends strings", async () => {
    const uri = "someUri";
    const fn = vi.fn((id: StreamIdentify) => {
      expect(id.channel).toBe("someUri");
      expect(id.runner).toBe("myRunner");
      return { id: 1 };
    })
    const client = new StreamMsgMock(
      fn
    );
    const msgs: OrchestratorMessage[] = [];
    const write = async (msg: OrchestratorMessage) => msgs.push(msg);
    const writer = new WriterInstance(uri, client as any, write, "myRunner", logger);

    const send = writer.string("hello world");
    writer.handled();
    await send;

    expect(msgs.length).toBe(1);
    expect(msgs.map(x => decoder.decode(x.msg!.data))).toEqual(["hello world"]);

    expect(fn).toBeCalledTimes(0);
  });


  test("sends binary", async () => {
    const uri = "someUri";
    const fn = vi.fn((id: StreamIdentify) => {
      expect(id.channel).toBe("someUri");
      expect(id.runner).toBe("myRunner");
      return { id: 1 };
    })
    const client = new StreamMsgMock(
      fn
    );
    const msgs: OrchestratorMessage[] = [];
    const write = async (msg: OrchestratorMessage) => msgs.push(msg);
    const writer = new WriterInstance(uri, client as any, write, "myRunner", logger);

    const send = writer.buffer(encoder.encode("hello world"));
    writer.handled();
    await send;

    expect(msgs.length).toBe(1);
    expect(msgs.map(x => decoder.decode(x.msg!.data))).toEqual(["hello world"]);

    expect(fn).toBeCalledTimes(0);
  });

  test("streams data", async () => {
    const uri = "someUri";
    const fn = vi.fn((id: StreamIdentify) => {
      expect(id.channel).toBe("someUri");
      expect(id.runner).toBe("myRunner");
      return { id: 1 };
    })
    const client = new StreamMsgMock(
      fn
    );
    const msgs: OrchestratorMessage[] = [];
    const write = async (msg: OrchestratorMessage) => msgs.push(msg);
    const writer = new WriterInstance(uri, client as any, write, "myRunner", logger);

    async function* gen() {
      yield encoder.encode("hello")
      yield encoder.encode("world")

      setTimeout(() => writer.handled(), 20)
    }

    await writer.stream(gen());

    expect(client.data.length).toBe(2);
    expect(client.data.map(x => decoder.decode(x.data))).toEqual(["hello", "world"]);
    expect(fn).toBeCalled();
  });

  test("closes", async () => {
    const uri = "someUri";
    const fn = vi.fn((id: StreamIdentify) => {
      expect(id.channel).toBe("someUri");
      expect(id.runner).toBe("myRunner");
      return { id: 1 };
    })
    const client = new StreamMsgMock(
      fn
    );
    const msgs: OrchestratorMessage[] = [];
    const write = async (msg: OrchestratorMessage) => msgs.push(msg);
    const writer = new WriterInstance(uri, client as any, write, "myRunner", logger);

    await writer.close()

    expect(msgs.length).toBe(1);
    expect(msgs.map(x => x.close!.channel)).toEqual([uri]);

    expect(fn).toBeCalledTimes(0);
  });

  test("wait to close after stream is finished", async () => {
    const uri = "someUri";
    const client = new StreamMsgMock(
      () => ({ id: 1 })
    );

    const msgs: OrchestratorMessage[] = [];
    const write = async (msg: OrchestratorMessage) => msgs.push(msg);
    const writer = new WriterInstance(uri, client as any, write, "myRunner", logger);

    async function* gen() {
      yield encoder.encode("hello")

      writer.close(); // initiate close

      await new Promise(res => setTimeout(res, 20))

      expect(msgs.filter(x => !!x.close)).toEqual([]);
      yield encoder.encode("world")

      // we 'handled' the message
      setTimeout(() => writer.handled(), 20)
    }

    await writer.stream(gen());
    expect(msgs.map(x => x.close!.channel)).toEqual([uri]);

    expect(client.data.length).toBe(2);
    expect(client.data.map(x => decoder.decode(x.data))).toEqual(["hello", "world"]);
  });
});


