import { Quad, Term } from "@rdfjs/types";
import { createTermNamespace, SHACL } from "@treecg/types";
import {
  BasicLens,
  BasicLensM,
  Cont,
  empty,
  invPred,
  pred,
  shacl,
} from "rdf-lens";

const JS = createTermNamespace(
  "https://w3id.org/conn/js#",
  "file",
  "function",
  "location",
  "mapping",
);

const FNO = createTermNamespace(
  "https://w3id.org/function/ontology#",
  "parameterMapping",
);
const FNOM = createTermNamespace(
  "https://w3id.org/function/vocabulary/mapping#",
  "functionParameter",
  "implementationParameterPosition",
);

export type Mapping = {
  predicate: string;
  location: number;
};

export type Processor = {
  id: string;
  file: string;
  function: string;
  location: string;

  mappings: Mapping[];
  shape: BasicLens<Quad[], shacl.Obj>;
};

function fieldLens<T extends string>(
  predicate: Term,
  field: T,
): BasicLens<Cont, Record<T, string>> {
  return pred(predicate).one().map((x) => {
    const out = <{ [label: string]: string }>{};
    out[field] = x.id.value;
    return <Record<T, string>>out;
  });
}

const MappingLens: BasicLens<Cont, Mapping> = fieldLens(
  FNOM.functionParameter,
  "predicate",
).and(
  fieldLens(FNOM.implementationParameterPosition, "location"),
).map(([{ predicate }, { location }]) => ({ predicate, location: +location }));

const MappingsLens: BasicLensM<Cont, Mapping> = pred(JS.mapping).thenFlat(
  pred(FNO.parameterMapping),
)
  .thenSome(MappingLens);

const ShapeLens = invPred(SHACL.terms.targetClass).one().then(shacl.ShaclShape)
  .map((shape) => ({
    shape,
  }));

export const ProcessorLens: BasicLens<Cont, Processor> = empty<Cont>().map((
  x,
) => ({
  id: x.id.value,
})).and(
  fieldLens(JS.file, "file"),
  fieldLens(JS.function, "function"),
  fieldLens(JS.location, "location"),
  MappingsLens.map((mappings) => ({ mappings })),
  ShapeLens,
).map((xs) => Object.assign({}, ...xs));
