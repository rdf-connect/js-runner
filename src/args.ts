import commandLineArgs from "command-line-args";
import commandLineUsage from "command-line-usage";

const optionDefinitions = [
  { name: 'input', type: String, defaultOption: true, summary: "Specify what input file to start up" },
  { name: 'ontologies', alias: 'o', type: String, multiple: true, description: "Specify what ontology to use", typeLabel: "{underline file}" },
  { name: 'help', alias: 'h', type: Boolean, description: "Display this help message" },
];

const sections = [
  {
    header: "Js-runner",
    content: "JS-runner is part of the {italic connector architecture}. Starting from an input file start up all JsProcessors that are defined."
  },
  {
    header: "Synopsis",
    content: "$ js-runner <options> <input>"
  },
  {
    header: "Command List",
    content: [{ name: "input", summary: "Specify what input file to start up" }],
  },
  {
    optionList: [optionDefinitions[1], optionDefinitions[2]]
  }
];

export type Args = {
  ontologies: string[],
  input: string,
};

function validArgs(args: any): boolean {
  if (!args.input) return false;
  if (!args.ontologies
    || !Array.isArray(args.ontologies)
    || !args.ontologies.every((x: any) => typeof x === 'string')
  ) return false;

  return true;
}

function printUsage() {
  const usage = commandLineUsage(sections);
  console.log(usage);
  process.exit(0);
}

export function getArgs(): Args {
  let args: any;
  try {
    args = commandLineArgs(optionDefinitions);
  } catch (e) {
    console.error(e);
    printUsage();
  }

  if (args.help || !validArgs(args)) {
    printUsage();
  }

  return <Args>args;
}



