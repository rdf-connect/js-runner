import { Store, Writer } from "n3";
import path from "path";
import { getArgs } from "./args.js";
import { load_store, CONN2, load_quads } from "./util.js";
import * as RDF from "rdf-js";
import { RDF as RDFT } from "@treecg/types";
import { randomUUID } from "crypto";
import { exec } from "child_process";
import { writeFile } from "fs/promises";

async function execute(command: string) {
      console.log("Executing $", command)
      const proc = exec(command);
      await new Promise(res => proc.once("exit", res));
}

const locationCache: {[url: string]: string} = {};
async function process_quads(quads: RDF.Quad[], baseIRI: string): Promise<RDF.Quad[]> {
// - if file with `<> :install`, install package
//   - assume :GitInstall
//   - git clone :url to some random location
//   - goto location and run :install
//   - move imported file to this location 
//   - Reimport new file to knowledge 

  const baseIRIs = quads.filter(q => q.subject.value === baseIRI);
  console.log("found ", baseIRIs.length, " base quads", baseIRIs.map(q => q.predicate.value).join(", "));

  const installId = quads.find(q => q.subject.value === baseIRI && q.predicate.equals(CONN2.terms.install))?.object;
  if (installId) {
    let ty: RDF.Term | undefined, url: RDF.Term | undefined, build: RDF.Term | undefined, procFile: RDF.Term | undefined;

    quads.filter(q => q.subject.equals(installId)).forEach(q => {
      if(q.predicate.equals(CONN2.terms.build)) {
        build = q.object;
      }
      if(q.predicate.equals(CONN2.terms.url)) {
        url = q.object;
      }
      if(q.predicate.equals(RDFT.terms.type)) {
        ty = q.object;
      }
      if(q.predicate.equals(CONN2.terms.procFile)) {
        procFile = q.object;
      }
    });

    if(!ty || !build || !url || !ty.equals(CONN2.terms.GitInstall)) {
      throw `To install a component, please provide ${CONN2.build}, ${CONN2.url} and ${RDFT.type} == ${CONN2.GitInstall}`;
    }

    if(!locationCache[url.value]) {
      // Random location
      const location = path.join(process.cwd(), "tmp/" + randomUUID());
      await execute(`git clone ${url.value} ${location}`);
      await execute(`cd ${location}; ${build.value}`);
      locationCache[url.value] = location;
    }

    const location = locationCache[url.value];
    const file = path.join(location, procFile?.value || "tmp.ttl");
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

