import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'
import {
  mkdtemp,
  mkdir,
  readFile,
  readdir,
  rm,
  writeFile,
} from 'node:fs/promises'
import { existsSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
import { Parser } from 'n3'
import type { Quad } from '@rdfjs/types'
import { shacl2owl } from '../src/shacl2owl'

// Serialize a Turtle document to a stable, comparable set of N-Triples so that
// assertions are independent of prefix/formatting choices made by the writer.
function toTripleSet(turtle: string, baseIRI: string): Set<string> {
  const quads = new Parser({ baseIRI: pathToFileURL(baseIRI).href }).parse(
    turtle,
  ) as Quad[]
  return new Set(
    quads.map(
      (q) =>
        `${q.subject.value} ${q.predicate.value} ${q.object.value} ${q.object.termType}`,
    ),
  )
}

// Install a fake reasoning endpoint. By default it echoes the posted body back
// as the "reasoned" output, which keeps assertions focused on file routing.
function stubFetch(
  impl?: (
    url: string,
    opts: { body: string },
  ) => {
    ok: boolean
    status: number
    text: () => Promise<string>
  },
) {
  const fetchMock = vi.fn(async (url: string, opts: { body: string }) =>
    impl
      ? impl(url, opts)
      : {
          ok: true,
          status: 200,
          text: async () => opts.body,
        },
  )
  vi.stubGlobal('fetch', fetchMock)
  return fetchMock
}

describe('shacl2owl', () => {
  let dir: string
  let inDir: string
  let outDir: string

  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), 'shacl2owl-'))
    inDir = join(dir, 'in')
    outDir = join(dir, 'out')
    await mkdir(inDir, { recursive: true })
    await mkdir(outDir, { recursive: true })
    // Keep the console quiet during tests.
    vi.spyOn(console, 'log').mockImplementation(() => {})
    vi.spyOn(console, 'warn').mockImplementation(() => {})
  })

  afterEach(async () => {
    vi.unstubAllGlobals()
    vi.restoreAllMocks()
    await rm(dir, { recursive: true, force: true })
  })

  test('writes to the local file: owl:imports target inside outDir', async () => {
    stubFetch()
    const input = join(inDir, 'shapes.ttl')
    const target = join(outDir, 'ontology.ttl')
    await writeFile(
      input,
      `@prefix owl: <http://www.w3.org/2002/07/owl#>.
@prefix ex: <http://example.org/>.
<> owl:imports <${pathToFileURL(target).href}>.
ex:A a ex:Thing.
`,
    )

    await shacl2owl(join(inDir, '*.ttl'), outDir)

    expect(existsSync(target)).toBe(true)
    const content = await readFile(target, 'utf8')
    expect(toTripleSet(content, target)).toContain(
      'http://example.org/A http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://example.org/Thing NamedNode',
    )
  })

  test('falls back to outDir/<inputBasename> when there is no owl:imports', async () => {
    stubFetch()
    const input = join(inDir, 'plain.ttl')
    await writeFile(
      input,
      `@prefix ex: <http://example.org/>.\nex:A a ex:Thing.\n`,
    )

    await shacl2owl(join(inDir, '*.ttl'), outDir)

    const fallback = join(outDir, 'plain.ttl')
    expect(existsSync(fallback)).toBe(true)
  })

  test('ignores HTTP/HTTPS owl:imports and uses the fallback path', async () => {
    stubFetch()
    const input = join(inDir, 'remote.ttl')
    await writeFile(
      input,
      `@prefix owl: <http://www.w3.org/2002/07/owl#>.
@prefix ex: <http://example.org/>.
<> owl:imports <https://example.org/remote-ontology.ttl>.
ex:A a ex:Thing.
`,
    )

    await shacl2owl(join(inDir, '*.ttl'), outDir)

    // The HTTP import must not create a file; the fallback is used instead.
    expect(existsSync(join(outDir, 'remote.ttl'))).toBe(true)
    const outputs = await readdir(outDir)
    expect(outputs).toEqual(['remote.ttl'])
  })

  test('picks the local in-outDir import when mixed with an unrelated HTTP import', async () => {
    stubFetch()
    const input = join(inDir, 'mixed.ttl')
    const target = join(outDir, 'mixed-out.ttl')
    await writeFile(
      input,
      `@prefix owl: <http://www.w3.org/2002/07/owl#>.
@prefix ex: <http://example.org/>.
<> owl:imports <https://example.org/vocab>.
<> owl:imports <${pathToFileURL(target).href}>.
ex:A a ex:Thing.
`,
    )

    await shacl2owl(join(inDir, '*.ttl'), outDir)

    expect(existsSync(target)).toBe(true)
    expect(existsSync(join(outDir, 'mixed.ttl'))).toBe(false)
  })

  test('ignores owl:imports pointing to a file outside outDir', async () => {
    stubFetch()
    const outside = join(dir, 'elsewhere', 'ont.ttl')
    const input = join(inDir, 'outside.ttl')
    await writeFile(
      input,
      `@prefix owl: <http://www.w3.org/2002/07/owl#>.
@prefix ex: <http://example.org/>.
<> owl:imports <${pathToFileURL(outside).href}>.
ex:A a ex:Thing.
`,
    )

    await shacl2owl(join(inDir, '*.ttl'), outDir)

    expect(existsSync(outside)).toBe(false)
    expect(existsSync(join(outDir, 'outside.ttl'))).toBe(true)
  })

  test('merges results when several inputs resolve to the same output', async () => {
    stubFetch()
    const target = join(outDir, 'shared.ttl')
    const targetIri = pathToFileURL(target).href
    await writeFile(
      join(inDir, 'a.ttl'),
      `@prefix owl: <http://www.w3.org/2002/07/owl#>.
@prefix ex: <http://example.org/>.
<> owl:imports <${targetIri}>.
ex:A a ex:Thing.
`,
    )
    await writeFile(
      join(inDir, 'b.ttl'),
      `@prefix owl: <http://www.w3.org/2002/07/owl#>.
@prefix ex: <http://example.org/>.
<> owl:imports <${targetIri}>.
ex:B a ex:Thing.
`,
    )

    await shacl2owl(join(inDir, '*.ttl'), outDir)

    const merged = toTripleSet(await readFile(target, 'utf8'), target)
    expect(merged).toContain(
      'http://example.org/A http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://example.org/Thing NamedNode',
    )
    expect(merged).toContain(
      'http://example.org/B http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://example.org/Thing NamedNode',
    )
    // Only one output file should exist.
    expect(await readdir(outDir)).toEqual(['shared.ttl'])
  })

  test('throws when the reasoning endpoint responds with a non-OK status', async () => {
    stubFetch(() => ({
      ok: false,
      status: 500,
      text: async () => 'boom',
    }))
    await writeFile(
      join(inDir, 'fail.ttl'),
      `@prefix ex: <http://example.org/>.\nex:A a ex:Thing.\n`,
    )

    await expect(shacl2owl(join(inDir, '*.ttl'), outDir)).rejects.toThrow(
      /Reasoning failed/,
    )
  })

  test('warns and writes nothing when the pattern matches no files', async () => {
    const fetchMock = stubFetch()
    const warn = vi.spyOn(console, 'warn')

    await shacl2owl(join(inDir, '*.nomatch'), outDir)

    expect(fetchMock).not.toHaveBeenCalled()
    expect(warn).toHaveBeenCalled()
    expect(await readdir(outDir)).toEqual([])
  })
})
