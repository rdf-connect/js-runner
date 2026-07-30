#!/usr/bin/env node

import { serve } from '../lib/server.js'

const configPath = process.argv[2]
if (!configPath) {
  console.error('Usage: js-runner-server <config>')
  process.exit(1)
}
serve(configPath).catch((err) => {
  console.error(err)
  process.exit(1)
})
