#!/usr/bin/env node

import { start } from '../lib/client.js'
import { serve } from '../lib/server.js'

if (process.argv[2] === 'serve') {
  const configIndex = process.argv.indexOf('--config')
  if (configIndex === -1 || !process.argv[configIndex + 1]) {
    console.error('Usage: js-runner serve --config <path>')
    process.exit(1)
  }
  const configPath = process.argv[configIndex + 1]
  serve(configPath).catch((err) => {
    console.error(err)
    process.exit(1)
  })
} else {
  // One-shot CLI mode: connect to an existing orchestrator, run processors, then exit.
  // process.exit is needed because grpc-js keeps the event loop alive after client.close().
  const host = process.argv[2]
  const uri = process.argv[3]
  start(host, uri)
    .then(() => process.exit(0))
    .catch((err) => {
      console.error(err)
      process.exit(1)
    })
}
