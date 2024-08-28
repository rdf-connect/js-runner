import { docker } from "../lib/docker.js";

docker().catch((e) => { console.error("Error:", e); console.error(e.stack); });
