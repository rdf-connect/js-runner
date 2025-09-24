
import { describe, expect, test, vi } from 'vitest'
import { ProcHelper } from '../src/testUtils'
import { Processor } from '../src/processor'
import path from 'path/posix'


// Example test processor
export class TestProcessor extends Processor<{ message: string }> {
  async init(): Promise<void> {
    this.logger.info('Test processor initialized')
  }

  async transform(): Promise<void> {
    // Mock transform - just wait
    await new Promise(resolve => setTimeout(resolve, 10))
  }

  async produce(): Promise<void> {
    // Mock produce - do nothing
  }
}

describe("Test processor", async () => {
  test("Is well defined", async () => {
    const helper = new ProcHelper<TestProcessor>();

    await helper.importFile(path.resolve('./index.ttl'));
    await helper.importInline(
      path.resolve('config.ttl'),
      `@prefix ex: <http://example.org/>.
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix sh: <http://www.w3.org/ns/shacl#>.
@prefix owl: <http://www.w3.org/2002/07/owl#>.
@prefix rdfc: <https://w3id.org/rdf-connect#>.

rdfc:Test a owl:Class;
  rdfc:jsImplementationOf rdfc:Processor;
  rdfc:file <./__tests__/testProcessor.test.ts>;
  rdfc:class "TestProcessor";
  rdfc:entrypoint <./>.

[ ] a sh:NodeShape;
  sh:targetClass rdfc:Test;
  sh:property [
    sh:datatype xsd:string;
    sh:path rdfc:message;
    sh:name "message";
    sh:maxCount 1;
    sh:minCount 1;
  ].`);

    const config = helper.getConfig('Test')
    expect(config.clazz).toEqual("TestProcessor");
    expect(config.location).toBeDefined();
    expect(config.file).toBeDefined();

    await helper.importInline(
      path.resolve('pipeline.ttl'),
      `@prefix rdfc: <https://w3id.org/rdf-connect#>.

<http://example.org/proc> a rdfc:Test;
  rdfc:message "Hello world".`);

    const proc = await helper.getProcessor('http://example.org/proc');

    expect(proc.message).toEqual("Hello world");
  });
});
