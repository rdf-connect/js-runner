#!/usr/bin/env node
import { jsRunner } from "../dist/index.js";

jsRunner().catch((e) => { console.error("Error:", e); console.error(e.stack); });
