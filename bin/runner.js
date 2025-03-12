import { start } from '../lib/client.js'

const host = process.argv[2]
const uri = process.argv[3]

start(host, uri)
