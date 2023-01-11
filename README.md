# Js-runner

Executor for a javascript workbench. Starting from a turtle file describing the workbench.

## Process definition

Each js process must have `js:file`, `js:function` and `js:mapping` objects. 

- `js:file` points to the location of the main javascript file, containing the function.
- `js:location` points to the starting location for `js:file` relative from the current file.
- `js:function` points to the function name in the file.
- `js:mapping` is a `fno:Mapping` object that links properties to function arguments.


When you declare a new js process, it is required to add a shacl shape.
Each `sh:property` is accounted for, noting the type `sh:class` or `sh:datatype`.

Example definitions are available in `processor/configs/*.ttl`.


## Pipeline configuration

In a js pipeline you can use all declared js processes, as defined in their shacl shapes.

An example can be found in `input.ttl`, here a `js:Send` process and a `js:Resc` process are defined.
`js:Send` takes in a message to send, and a channel to send it to.
`js:Resc` only takes a channel to read from.

(implementation can be found in `procossor/test.js`)

Note: currently websockets are configured, but changing the configuration to use the JsReaderChannel and JsWriterChannel will work too, even without a serialization step.

You can execute this pipeline with
```bash
$ tsc
$ node lib/index.js input.ttl -o processor/configs/*.ttl
```





