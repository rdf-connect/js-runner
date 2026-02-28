# JavaScript runner for RDF-Connect
[![Build and tests with Node.js](https://github.com/rdf-connect/js-runner/actions/workflows/build-test.yml/badge.svg)](https://github.com/rdf-connect/js-runner/actions/workflows/build-test.yml) [![npm](https://img.shields.io/npm/v/@rdfc/js-runner.svg?style=popout)](https://npmjs.com/package/@rdfc/js-runner)

This package provides a JavaScript runner implementation for RDF-Connect, which allows you to run JavaScript/TypeScript processors in your [RDF-Connect](https://github.com/rdf-connect/rdf-connect) pipeline.

## Usage

To use the JavaScript (Node or Bun) runner for RDF-Connect, you need to have a pipeline configuration that includes JavaScript/TypeScript processors.
The runner can be added to your RDF-Connect pipeline as follows:

```turtle
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix owl: <http://www.w3.org/2002/07/owl#>.

### Import the runner
<> owl:imports <./node_modules/@rdfc/js-runner/index.ttl>.

### Define the pipeline and add the Node runner
<> a rdfc:Pipeline;
   rdfc:consistsOf [
       rdfc:instantiates rdfc:NodeRunner;  # Use rdfc:BunRunner for Bun
       rdfc:processor <log>, <send>;  # List of JavaScript processors to be used in the pipeline. You should define and configure these processors separately.
   ].
```

This example assumes that the js-runner is installed in your project via npm or yarn in the current working directory.

You can install the js-runner package using the following command:

```bash
npm install @rdfc/js-runner
```

## Logging

The JavaScript runner and processors use the `winston` logging library for logging.
The JavasScript runner initiates a logger that is passed to each processor, allowing them to log messages at various levels (info, warn, error, debug).
You can access this logger in your processor class code on the `this.logger` property.
Here's an example of how to use the logger in a processor:

```typescript
import { Processor } from '@rdfc/js-runner'

class MyProcessor extends Processor<MyProcessorArgs> {
  async init(this: MyProcessorArgs & this): Promise<void> {
    this.logger.info('I am initializing my processor!')
  }
  // ...
}
```

This logger is configured to forward log messages to the RDF-Connect logging system.
This means you can view and manage these logs in the RDF-Connect logging interface, allowing for consistent log management across different components of your RDF-Connect pipeline.

If you want to create a child logger for a subclass or submethod, you can do so using the `extendLogger` method.
Here's an example:

```typescript
import { Processor, extendLogger } from '@rdfc/js-runner'

class MyProcessor extends Processor<MyProcessorArgs> {
  async init(this: MyProcessorArgs & this): Promise<void> {
    const childLogger = extendLogger(this.logger, 'init')
    childLogger.debug('This is a debug message from init.')
  }
  // ...
}
```

## Develop a processor for this runner

The simplest way to start developing a processor for the JavaScript runner, is to start from the [template-processor-ts](https://github.com/rdf-connect/template-processor-ts) template repository.
It has everything set up to get you started quickly and let you focus on the actual processor logic.

At the very least, a JavaScript/TypeScript processor should consist of a class that inherits from the `Processor` abstract base class provided by the `@rdfc/js-runner` package.
This class should implement the `init` method, which is called when the processor is initialized.
This method is where you can set up any necessary configuration or state for your processor like opening a database connection or reading a file.
Additionally, you should implement the `transform` method, which is called before the `produce` method.
In this `transform` method, you should put any logic that handles incoming data by consuming readers, possibly transforming it, and passing it to the next processor in the pipeline.
This method should only write to writers as reply to the data it receives from readers, not produce new data, as it is important that it does not write data to channels before all readers have been initialized and are ready to consume data.
Finally, you should implement the `produce` method, which is called after all readers have been initialized and are ready to consume data.
It is called after the `transform` method.
This method is where you can produce (new) output data by writing to writers to send the data to the next step in the pipeline.

Next to the class, you should define a configuration for the processor in the `processor.ttl` file of your package.
JavaScript/TypeScript processors must include the JavaScript specific configuration parameters `rdfc:file`, `rdfc:class`, and `rdfc:entrypoint`, which specify the file, class, and entrypoint path of the processor, respectively.

## Development of the js-runner

The JavaScript runner is implemented in TypeScript.
The source code is contained in the `src` folder.
The main cli entry point is the `bin/runner.js` file, which you can also run after installation of the package using `npx js-runner`.

If you want to get started with the development of the js-runner, you can clone the repository and install the dependencies using the following commands.

Clone the repository:

```bash
git clone git@github.com:rdf-connect/js-runner.git
cd js-runner
```

Install the dependencies:

```bash
npm install
```

You can run the formatter and linter using:

```bash
npm run format
npm run lint
```

You can run the tests using:

```bash
npm test
```

You can then build the project using:

```bash
npm run build
```

Lastly, you can publish the package to npm using:

```bash
npm publish --access=public
```
