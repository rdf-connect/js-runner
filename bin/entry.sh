#!/usr/bin/env bash
echo "Running" "/usr/bin/env node --experimental-specifier-resolution=node ./bin/js-runner.js $@"
/usr/bin/env node --experimental-specifier-resolution=node ./bin/js-runner.js "$@"
