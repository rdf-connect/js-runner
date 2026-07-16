# js-runner end-to-end fixtures

> [!NOTE]
> This is a minimal internal fixture for testing the js-runner itself, **not** an example of how
> to build an RDF-Connect pipeline or processor. If you're looking for a real-world example
> pipeline, see [rdf-connect/example-pipelines](https://github.com/rdf-connect/example-pipelines).
> If you want to write your own processor, start from
> [rdf-connect/template-processor-ts](https://github.com/rdf-connect/template-processor-ts).

This folder contains a small, self-contained RDF-Connect pipeline that is used both:

- **manually**, by a developer working on the js-runner who wants to quickly see whether their
  changes still work end-to-end, and
- **automatically**, via `e2e.test.ts`, which is picked up by `npm test` like any other Vitest
  test file.

The pipeline consists of three JS processors (defined in `src/processors.ts`, compiled to
`lib/processors.js`):

- `<sender>` (`SendProcessor`) writes a couple of messages to a channel.
- `<echo>` (`EchoProcessor`) reads from that channel and echoes each message to a second channel.
- `<log>` (`LogProcessor`) reads from the second channel and logs every message it receives.

## Manual testing

Install dependencies and build the processors once:

```bash
cd __tests__/e2e
npm install
npm run build
```

Then run the local pipeline (uses the `rdfc:NodeRunner`, spawning `npx js-runner` as a
subprocess) with the real RDF-Connect orchestrator:

```bash
npx rdfc pipeline.ttl
```

You should see log lines for every processor being initialized, the messages being sent,
echoed, and logged, and the run exiting with code `0`.

### Remote runner

`server.ttl` and `remote_pipeline.ttl` set up the same processors behind a js-runner server
(`rdfc:JsRunnerServer`) instead of spawning a local process. Start the server first:

```bash
npx js-runner-server ./server.ttl
```

Then, in another terminal, run the pipeline against it:

```bash
npx rdfc remote_pipeline.ttl
```

Note: this requires an orchestrator with support for `rdfc:HttpRunner`; it is not covered by
the automated test in this folder.

## Automated testing

`e2e.test.ts` installs/builds this fixture if needed and then spawns
`npx rdfc pipeline.ttl` exactly like the manual steps above, asserting that every processor
logs the expected messages and that the orchestrator exits successfully. Run it together with
the rest of the test suite from the repository root:

```bash
npm test
```
