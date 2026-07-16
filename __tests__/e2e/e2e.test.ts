import { existsSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { execFileSync, spawnSync } from 'node:child_process'
import { beforeAll, describe, expect, test } from 'vitest'

// This suite drives the js-runner exactly like a developer would: it starts
// the real RDF-Connect orchestrator (`npx rdfc`) against the pipeline files
// in this directory and asserts on the resulting pipeline behaviour.
// You can run the same pipeline manually to poke at it:
//
//   cd __tests__/e2e
//   npm install && npm run build
//   npx rdfc pipeline.ttl

const e2eDir = dirname(fileURLToPath(import.meta.url))

beforeAll(() => {
  if (!existsSync(join(e2eDir, 'node_modules'))) {
    execFileSync('npm', ['install'], { cwd: e2eDir, stdio: 'inherit' })
  }
  execFileSync('npm', ['run', 'build'], { cwd: e2eDir, stdio: 'inherit' })
}, 300_000)

describe('echo pipeline (e2e)', () => {
  test('runs the local pipeline with the NodeRunner via the rdfc orchestrator', () => {
    const result = spawnSync('npx', ['rdfc', 'pipeline.ttl'], {
      cwd: e2eDir,
      encoding: 'utf-8',
      timeout: 60_000,
    })

    const output = `${result.stdout ?? ''}${result.stderr ?? ''}`

    expect(output).toContain('Init send processor')
    expect(output).toContain('Init echo processor')
    expect(output).toContain('Init log processor')
    expect(output).toContain('Sending hallo')
    expect(output).toContain('Sending world')
    expect(output).toContain('Echoing message')
    expect(output).toContain('Got msghallo')
    expect(output).toContain('Got msgworld')
    expect(output).toContain('Closed')

    expect(result.status).toBe(0)
  }, 60_000)
})
