#!/usr/bin/env node

import { start } from '../lib/client.js'

const host = process.argv[2]
const uri = process.argv[3]

if (!host || !uri) {
  console.error('Usage: js-runner <host> <uri>')
  process.exit(1)
}

// One-shot CLI mode: connect to an existing orchestrator, run processors, then exit.
// process.exit is needed because grpc-js keeps the event loop alive after client.close().
start(host, uri)
  .then(() => process.exit(0))
  .catch((err) => {
    console.error(err)
    process.exit(1)
  })
