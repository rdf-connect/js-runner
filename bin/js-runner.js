import { jsRunner } from "../lib/index.js";

jsRunner().catch((e) => { console.error("Error:", e); console.error(e.stack) });
