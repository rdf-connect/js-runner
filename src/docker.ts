import { Store, Writer } from "n3";
import path from "path";
import { getArgs } from "./args.js";
import { load_store, CONN2, load_quads } from "./util.js";
import * as RDF from "rdf-js";
import { RDF as RDFT } from "@treecg/types";
import { randomUUID } from "crypto";
import { exec } from "child_process";
import { writeFile } from "fs/promises";

function hashCode(st: string) {
  var hash = 0,
    i, chr;
  if (st.length === 0) return hash;
  for (i = 0; i < st.length; i++) {
    chr = st.charCodeAt(i);
    hash = ((hash << 5) - hash) + chr;
    hash |= 0; // Convert to 32bit integer
  }
  return hash;
}
interface Installer {
  check(): void;
  cacheId(): string;
  build(): Promise<string>;
}

class GitInstaller implements Installer {
  url?: RDF.Term;
  buildCmd?: RDF.Term;
  procFile?: RDF.Term;
  source_path?: string;

  constructor(obj: { [label: string]: RDF.Term }) {
    for (let key of Object.keys(obj)) {
      if (key === CONN2.build) {
        this.buildCmd = obj[key];
      }
      if (key === CONN2.url) {
        this.url = obj[key];
      }
      if (key === CONN2.procFile) {
        this.procFile = obj[key];
      }
      if (key === CONN2.path) {
        this.source_path = obj[key].value;
      }
    }
  }
  check(): void {
    if (!this.buildCmd || !this.url) {
      throw `To install a component, please provide ${CONN2.build}, ${CONN2.url} and ${RDFT.type} == ${CONN2.GitInstall}`;
    }
  }

  cacheId(): string {
    return this.source_path ? path.join(this.url!.value, this.source_path) : this.url!.value
  }

  async build(): Promise<string> {
    const location = path.join(process.cwd(), "tmp/" + randomUUID());
    await execute(`git clone ${this.url!.value} ${location}`);
    const fullLocation = this.source_path ? path.join(location, this.source_path) : location;
    await execute(`cd ${fullLocation}; ${this.buildCmd!.value}`);
    return fullLocation;
  }
}

class LocalInstaller implements Installer {
  source_path?: string;
  constructor(obj: { [label: string]: RDF.Term }) {
    for (let key of Object.keys(obj)) {
      if (key === CONN2.path) {
        this.source_path = obj[key].value;
      }
    }
  }

  check(): void {
    if (!this.source_path) {
      throw `To locally install a component, please provide ${CONN2.path}`;
    }
  }

  cacheId(): string {
    return "" + hashCode(this.source_path!);
  }

  async build(): Promise<string> {
    const location = path.join(process.cwd(), "tmp/" + randomUUID());
    await execute(`cp -r ${this.source_path!} ${location}`);
    return location;
  }
}

async function execute(command: string) {
  console.log("Executing $", command)
  const proc = exec(command);
  await new Promise(res => proc.once("exit", res));
}

const locationCache: { [url: string]: string } = {};
async function process_quads(quads: RDF.Quad[], baseIRI: string): Promise<RDF.Quad[]> {

  const baseIRIs = quads.filter(q => q.subject.value === baseIRI);
  console.log("found ", baseIRIs.length, " base quads", baseIRIs.map(q => q.predicate.value).join(", "));

  const installId = quads.find(q => q.subject.value === baseIRI && q.predicate.equals(CONN2.terms.install))?.object;
  if (installId) {
    const obj: { [label: string]: RDF.Term } = {};

    quads.filter(q => q.subject.equals(installId)).forEach(q => {
      obj[q.predicate.value] = q.object;
    });

    let installer;
    if (obj[RDFT.type].value === CONN2.GitInstall) {
      installer = new GitInstaller(obj);
    }
    if (obj[RDFT.type].value === CONN2.LocalInstall) {
      installer = new LocalInstaller(obj);
    }

    if (!installer) {
      throw "No installer found!";
    }
    installer.check();

    const cacheId = installer.cacheId();
    if (!locationCache[cacheId]) {
      locationCache[cacheId] = await installer.build();
    }

    const location = locationCache[cacheId];
    const file = path.join(location, "tmp.ttl");
    const o_quads = await load_quads(baseIRI, file);


    const quads_string = new Writer().quadsToString(o_quads);
    await writeFile(path.join(location, "tmp.ttl"), quads_string);

    return o_quads;
  }

  return quads;
}

export async function docker() {
  const args = getArgs();
  const cwd = process.cwd();

  const store = new Store();
  await load_store(path.join(cwd, args.input), store, true, process_quads);

  const quads = store.getQuads(null, null, null, null);
  const quads_string = new Writer().quadsToString(quads);
  await writeFile("/tmp/run.ttl", quads_string);
}

