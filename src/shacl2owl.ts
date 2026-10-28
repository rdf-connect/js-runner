import { existsSync } from 'node:fs'
import { glob, mkdir, readFile, writeFile } from 'node:fs/promises'
import { basename, dirname, isAbsolute, relative, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { Parser, Store, Writer } from 'n3'
import type { Quad } from '@rdfjs/types'
import { createNamespace } from '@treecg/types'

const OWL = createNamespace(
  'http://www.w3.org/2002/07/owl#',
  (x) => x,
  'imports',
)

// True when `child` lives inside `parent`.
function isInside(child: string, parent: string): boolean {
  const rel = relative(parent, child)
  return rel !== '' && !rel.startsWith('..') && !isAbsolute(rel)
}

// Parse Turtle into quads while collecting its prefix declarations.
function parseTurtle(
  turtle: string,
  baseIRI: string,
): {
  quads: Quad[]
  prefixes: Record<string, string>
} {
  if (!baseIRI.startsWith('file:')) {
    baseIRI = pathToFileURL(baseIRI).href
  }
  const prefixes: Record<string, string> = {}
  const quads = new Parser({ baseIRI }).parse(turtle, null, (prefix, iri) => {
    prefixes[prefix] = typeof iri === 'string' ? iri : iri.value
  }) as Quad[]
  return { quads, prefixes }
}

// Merge several Turtle documents into a single serialized document,
// preserving prefixes and de-duplicating triples.
function mergeTurtle(baseIRI: string, ...turtles: string[]): Promise<string> {
  const store = new Store()
  const prefixes: Record<string, string> = {}
  for (const turtle of turtles) {
    const parsed = parseTurtle(turtle, baseIRI)
    store.addQuads(parsed.quads)
    Object.assign(prefixes, parsed.prefixes)
  }
  const writer = new Writer({ prefixes })
  writer.addQuads(store.getQuads(null, null, null, null))
  return new Promise((res, rej) =>
    writer.end((err, result) => (err ? rej(err) : res(result))),
  )
}

// Written when reasoning is unavailable, so the `owl:imports` target still
// resolves to a valid (empty) Turtle document.
const UNREASONED_PLACEHOLDER =
  '# Reasoning was skipped because the shacl2owl endpoint was unavailable.\n' +
  '# Rebuild once it is reachable again to include the inferred OWL axioms.\n'

// Default time to wait for the reasoning endpoint, overridable through
// `SHACL2OWL_TIMEOUT_MS`.
const DEFAULT_TIMEOUT_MS = 60_000

// POST `turtle` to the reasoning endpoint and return the reasoned graph.
// Returns `undefined` when the endpoint is unreachable, times out or fails
// server-side (5xx), so the caller can fall back instead of failing the build.
// Client errors (4xx) still throw, as they point at a problem with the input.
async function reason(
  endpoint: string,
  input: string,
  turtle: string,
  timeoutMs: number,
): Promise<string | undefined> {
  let res: Response
  let body: string
  try {
    res = await fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'text/turtle' },
      body: turtle,
      signal: AbortSignal.timeout(timeoutMs),
    })
    // Read the body here too, so a timeout while streaming it is handled.
    body = await res.text()
  } catch (ex) {
    const message = ex instanceof Error ? ex.message : String(ex)
    console.warn(
      `Reasoning endpoint ${endpoint} did not respond for ${input}: ${message}`,
    )
    return undefined
  }

  if (res.status >= 500) {
    console.warn(
      `Reasoning endpoint ${endpoint} failed for ${input} (${res.status})`,
    )
    return undefined
  }

  if (!res.ok) {
    throw new Error(`Reasoning failed for ${input} (${res.status}): ${body}`)
  }

  return body
}

/**
 * Reason over every Turtle file matching `pattern` and write the resulting graph
 * to the file it declares via `owl:imports` inside `outDir`.
 *
 * Each matched file is POSTed to the reasoning endpoint
 * (`SERVER_URL`, default `https://shacl2owl.knows.idlab.ugent.be/reason`). The output path is
 * the file's `owl:imports` target that, once expanded, is contained in `outDir`.
 * Only local `file:` imports are considered: `owl:imports` values that are
 * HTTP/HTTPS (or any other non-`file:`) URLs, or that resolve to a path outside
 * `outDir`, are ignored. When no such `owl:imports` target exists, the output
 * falls back to `${outDir}/${filename}`, reusing the input file's name.
 *
 * When several inputs resolve to the same output path within a single run, the
 * later results are pretty-appended: the existing file is parsed, the new quads
 * are merged in, and the combined graph is re-serialized to disk.
 *
 * When the endpoint is unreachable, does not respond within
 * `SHACL2OWL_TIMEOUT_MS` (default 60 seconds) or responds with a server error
 * (5xx), a warning is logged and reasoning is skipped for that input: an
 * existing output file is kept as is, otherwise an empty placeholder is written
 * so the `owl:imports` target still resolves. Client errors (4xx) still throw.
 */
export async function shacl2owl(
  pattern: string,
  outDir: string,
): Promise<void> {
  const endpoint =
    process.env.SERVER_URL ?? 'https://shacl2owl.knows.idlab.ugent.be/reason'
  const timeoutMs =
    Number(process.env.SHACL2OWL_TIMEOUT_MS) || DEFAULT_TIMEOUT_MS
  const outDirPath = resolve(outDir)

  // Absolute output paths already written during this run.
  const written = new Set<string>()

  let count = 0
  for await (const input of glob(pattern)) {
    const turtle = await readFile(input, 'utf8')

    // Determine the output file: the owl:imports target that expands to a
    // path contained in the output directory. Only local `file:` imports that
    // resolve inside `outDir` qualify; non-file (HTTP/HTTPS) or outside targets
    // are ignored. When absent, fall back to the input file name inside outDir.
    const { quads } = parseTurtle(turtle, input)
    const importIri = quads.find(
      (q) =>
        q.predicate.value === OWL.imports &&
        q.object.termType === 'NamedNode' &&
        q.object.value.startsWith('file:') &&
        isInside(fileURLToPath(q.object.value), outDirPath),
    )?.object.value
    // `importIri` is a `file:` URL; convert it to a filesystem path before use.
    const outputPath = importIri
      ? fileURLToPath(importIri)
      : resolve(outDirPath, basename(input))

    const reasoned = await reason(endpoint, input, turtle, timeoutMs)
    await mkdir(dirname(outputPath), { recursive: true })

    if (reasoned === undefined) {
      // Keep the owl:imports target resolvable: reuse whatever an earlier run
      // or input left there, or write an empty placeholder graph.
      if (written.has(outputPath) || existsSync(outputPath)) {
        console.warn(`Skipped reasoning for ${input}, kept ${outputPath}`)
      } else {
        await writeFile(outputPath, UNREASONED_PLACEHOLDER, 'utf8')
        written.add(outputPath)
        console.warn(
          `Skipped reasoning for ${input}, wrote empty placeholder ${outputPath}`,
        )
      }
    } else if (written.has(outputPath)) {
      // Already written this run: pretty-append by merging the existing graph
      // with the new quads and re-serializing.
      const existing = await readFile(outputPath, 'utf8')
      const merged = await mergeTurtle(outputPath, existing, reasoned)
      await writeFile(outputPath, merged, 'utf8')
      console.log(`Appended reasoned ${outputPath} (${merged.length} bytes)`)
    } else {
      await writeFile(outputPath, reasoned, 'utf8')
      written.add(outputPath)
      console.log(`Wrote reasoned ${outputPath} (${reasoned.length} bytes)`)
    }
    count++
  }

  if (count === 0) {
    console.warn(`No files matched pattern ${pattern}`)
  }
}
