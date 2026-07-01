import { glob, mkdir, readFile, writeFile } from "node:fs/promises";
import { basename, dirname, isAbsolute, relative, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { Parser, Store, Writer } from "n3";
import type { Quad } from "@rdfjs/types";
import { createNamespace } from "@treecg/types";

const OWL = createNamespace(
  'http://www.w3.org/2002/07/owl#',
  (x) => x,
  'imports',
)

// Resolve an owl:imports IRI to an absolute filesystem path.
function iriToPath(iri: string, baseDir: string): string {
  if (iri.startsWith('file:')) return fileURLToPath(iri)
  if (isAbsolute(iri)) return iri
  return resolve(baseDir, iri)
}

// True when `child` lives inside `parent`.
function isInside(child: string, parent: string): boolean {
  const rel = relative(parent, child)
  return rel !== '' && !rel.startsWith('..') && !isAbsolute(rel)
}

// Parse Turtle into quads while collecting its prefix declarations.
function parseTurtle(turtle: string, baseIRI: string): {
  quads: Quad[]
  prefixes: Record<string, string>
} {
  if (!baseIRI.startsWith("file:")) {
    baseIRI = pathToFileURL(baseIRI).href;
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
    const parsed = parseTurtle(turtle, baseIRI);
    store.addQuads(parsed.quads)
    Object.assign(prefixes, parsed.prefixes)
  }
  const writer = new Writer({ prefixes })
  writer.addQuads(store.getQuads(null, null, null, null))
  return new Promise((res, rej) =>
    writer.end((err, result) => (err ? rej(err) : res(result))),
  )
}

/**
 * Reason over every Turtle file matching `pattern` and write the resulting graph
 * to the file it declares via `owl:imports` inside `outDir`.
 *
 * Each matched file is POSTed to the reasoning endpoint
 * (`SERVER_URL`, default `https://shacl2owl.knows.idlab.ugent.be/reason`). The output path is
 * the file's `owl:imports` target that, once expanded, is contained in `outDir`.
 * When no such `owl:imports` target exists, the output falls back to
 * `${outDir}/${filename}`, reusing the input file's name.
 *
 * When several inputs resolve to the same output path within a single run, the
 * later results are pretty-appended: the existing file is parsed, the new quads
 * are merged in, and the combined graph is re-serialized to disk.
 */
export async function shacl2owl(
  pattern: string,
  outDir: string,
): Promise<void> {
  const endpoint =
    process.env.SERVER_URL ?? 'https://shacl2owl.knows.idlab.ugent.be/reason'
  const outDirPath = resolve(outDir)

  // Absolute output paths already written during this run.
  const written = new Set<string>()

  let count = 0
  for await (const input of glob(pattern)) {
    const turtle = await readFile(input, 'utf8')

    // Determine the output file: the owl:imports target that expands to a
    // path contained in the output directory. When absent, fall back to the
    // input file name inside outDir.
    const { quads } = parseTurtle(turtle, input);
    const baseDir = dirname(input)
    const importQuad = quads.find(
      (q) =>
        q.predicate.value === OWL.imports &&
        isInside(iriToPath(q.object.value, baseDir), outDirPath),
    )
    const output = importQuad
      ? iriToPath(importQuad.object.value, baseDir)
      : resolve(outDirPath, basename(input))

    const res = await fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'text/turtle' },
      body: turtle,
    })

    if (!res.ok) {
      throw new Error(
        `Reasoning failed for ${input} (${res.status}): ${await res.text()}`,
      )
    }

    const reasoned = await res.text()
    const outputPath = resolve(output)
    await mkdir(dirname(outputPath), { recursive: true })

    if (written.has(outputPath)) {
      // Already written this run: pretty-append by merging the existing graph
      // with the new quads and re-serializing.
      const existing = await readFile(outputPath, 'utf8')
      const merged = await mergeTurtle(outputPath, existing, reasoned);
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
