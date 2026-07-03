#!/usr/bin/env node

import { shacl2owl } from '../lib/shacl2owl.js'

const pattern = process.argv[2]
const outDir = process.argv[3]

if (!pattern || !outDir) {
  console.error('Usage: rdfc-shacl2owl <input-glob> <output-dir>')
  process.exit(1)
}

shacl2owl(pattern, outDir)
