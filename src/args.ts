import commandLineArgs from "command-line-args";
import commandLineUsage from "command-line-usage";

const optionDefinitions = [
    { name: "input", type: String, defaultOption: true, summary: "Specify what input file to start up" },
    { name: "help", alias: "h", type: Boolean, description: "Display this help message" },
];

const sections = [
    {
        header: "Js-runner",
        content: "JS-runner is part of the {italic connector architecture}. Starting from an input file start up all JsProcessors that are defined. Please do not use blank nodes, skolemize your data somewhere else!"
    },
    {
        header: "Synopsis",
        content: "$ js-runner <input>"
    },
    {
        header: "Command List",
        content: [{ name: "input", summary: "Specify what input file to start up" }],
    },
    {
        optionList: [optionDefinitions[1]]
    }
];

export type Args = {
    input: string,
    help?: boolean,
};

function validArgs(args: Args): boolean {
    if (!args.input) return false;
    return true;
}

function printUsage() {
    const usage = commandLineUsage(sections);
    console.log(usage);
    process.exit(0);
}

export function getArgs(): Args | undefined {
    let args: Args;
    try {
        args = <Args>commandLineArgs(optionDefinitions);
        if (args.help || !validArgs(<Args>args)) {
            printUsage();
        }
        return args;
    } catch (e) {
        console.error(e);
        printUsage();
    }
}



