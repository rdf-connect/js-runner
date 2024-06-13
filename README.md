# Js-runner

[![Bun CI](https://github.com/rdf-connect/js-runner/actions/workflows/build-test.yml/badge.svg)](https://github.com/rdf-connect/js-runner/actions/workflows/build-test.yml) [![npm](https://img.shields.io/npm/v/@rdfc/js-runner.svg?style=popout)](https://npmjs.com/package/@rdfc/js-runner)

Typescript/Javascript executor for an [RDF-Connect](https://rdf-connect.github.io/rdfc.github.io/) pipeline. Starting from a declarative RDF file describing the pipeline.

## Process definition

Each js process must have `js:file`, `js:function` and `js:mapping` objects.

- `js:file` points to the location of the main javascript file, containing the function.
- `js:location` points to the starting location for `js:file` relative from the current file.
- `js:function` points to the function name in the file.
- `js:mapping` is a `fno:Mapping` object that links properties to function arguments.

When you declare a new js process, it is required to add a SHACL shape.
Each `sh:property` is accounted for, noting the type `sh:class` or `sh:datatype`.

Example definitions are available in `processor/configs/*.ttl`.

## Pipeline configuration

In a js pipeline you can use all declared js processes, as defined in their SHACL shapes.

An example can be found in `input.ttl`, here a `js:Send` process and a `js:Resc` process are defined.
`js:Send` takes in a message to send, and a channel to send it to.
`js:Resc` only takes a channel to read from.

(implementation can be found in `procossor/test.js`)

Note: currently Websockets are configured, but changing the configuration to use the JsReaderChannel and JsWriterChannel will work too, even without a serialization step.

You can execute this pipeline with

```bash
tsc
bun bin/js-runner.js input.ttl 
```

This example input configuration file uses `owl:imports` to specify additional configuration files.