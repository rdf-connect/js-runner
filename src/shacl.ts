import { Quad, Term } from "@rdfjs/types";
import { createTermNamespace, RDF, XSD } from "@treecg/types";
import {
  BasicLens,
  BasicLensM,
  Cont,
  empty,
  invPred,
  pred,
  subjects,
  unique,
} from "rdf-lens";

export const RDFS = createTermNamespace(
  "http://www.w3.org/2000/01/rdf-schema#",
  "subClassOf",
);

export const SHACL = createTermNamespace(
  "http://www.w3.org/ns/shacl#",
  // Basics
  "Shape",
  "NodeShape",
  "PropertyShape",
  // SHACL target constraints
  "targetNode",
  "targetClass",
  "targetSubjectsOf",
  "targetObjectsOf",
  // Property things
  "property",
  "path",
  "class",
  "name",
  "description",
  "defaultValue",
  // Path things
  "alternativePath",
  "zeroOrMorePath",
  "inversePath",
  "minCount",
  "maxCount",
  "datatype",
);

export interface ShapeField {
  name: string;
  path: BasicLensM<Cont, Cont>;
  minCount?: number;
  maxCount?: number;
  extract: (term: Term, quads: Quad[]) => any;
}

export interface Shape {
  id: string;
  ty: Term;
  subTypes: Term[];
  description?: string;
  fields: ShapeField[];
}

export function toLens(
  shape: Shape,
): BasicLens<Cont, { [label: string]: any }> {
  if (shape.fields.length === 0) return empty<Cont>().map(() => ({}));

  const fields = shape.fields.map((field) => {
    const minCount = field.minCount || 0;
    const maxCount = field.maxCount || Number.MAX_SAFE_INTEGER;
    const base =
      maxCount < 2 // There will be at most one
        ? field.path.one(undefined).then(
            new BasicLens((inp) => {
              if (inp) {
                return field.extract(inp.id, inp.quads);
              } else {
                if (minCount > 0) {
                  throw `${shape.ty} required field ${field.name} `;
                }
              }
            }),
          )
        : field.path
            .thenFlat(
              // We extract an array
              new BasicLensM(({ id, quads }) => field.extract(id, quads)),
            )
            .then(
              new BasicLens((xs) => {
                if (xs.length < minCount) {
                  throw `${shape.ty}:${field.name} required at least ${minCount} elements, found ${xs.length}`;
                }
                if (xs.length > maxCount) {
                  throw `${shape.ty}:${field.name} required at most ${maxCount} elements, found ${xs.length}`;
                }
                return xs;
              }),
            );

    const asField = base.map((x) => {
      const out = <{ [label: string]: any }>{};
      out[field.name] = x;
      return out;
    });

    return (field.minCount || 0) > 0
      ? asField
      : asField.or(empty().map(() => ({})));
  });

  return fields[0]
    .and(...fields.slice(1))
    .map((xs) => Object.assign({}, ...xs));
}

const RDFListElement = pred(RDF.terms.first)
  .one()
  .and(pred(RDF.terms.rest).one());
export const RdfList: BasicLens<Cont, Term[]> = new BasicLens((c) => {
  if (c.id.equals(RDF.terms.nil)) {
    return [];
  }

  const [first, rest] = RDFListElement.execute(c);
  const els = RdfList.execute(rest);
  els.unshift(first.id);
  return els;
});

export const ShaclSequencePath: BasicLens<
  Cont,
  BasicLensM<Cont, Cont>
> = new BasicLens((c) => {
  const pathList = RdfList.execute(c);

  if (pathList.length === 0) {
    return new BasicLensM((c) => [c]);
  }

  let start = pred(pathList[0]);

  for (let i = 1; i < pathList.length; i++) {
    start = start.thenFlat(pred(pathList[i]));
  }

  return start;
});

export const ShaclAlternativepath: BasicLens<
  Cont,
  BasicLensM<Cont, Cont>
> = new BasicLens((c) => {
  const options = pred(SHACL.alternativePath).one().then(RdfList).execute(c);
  const optionLenses = options.map((id) =>
    ShaclPath.execute({ id, quads: c.quads }),
  );
  return optionLenses[0].orAll(...optionLenses.slice(1));
});

export const ShaclPredicatePath: BasicLens<
  Cont,
  BasicLensM<Cont, Cont>
> = new BasicLens((c) => {
  return pred(c.id);
});

export const ShaclInversePath: BasicLens<Cont, BasicLensM<Cont, Cont>> = pred(
  SHACL.inversePath,
)
  .one()
  .then(
    new BasicLens<Cont, BasicLensM<Cont, Cont>>((c) => {
      const pathList = RdfList.execute(c);

      if (pathList.length === 0) {
        return new BasicLensM((c) => [c]);
      }

      pathList.reverse();

      let start = invPred(pathList[0]);

      for (let i = 1; i < pathList.length; i++) {
        start = start.thenFlat(invPred(pathList[i]));
      }

      return start;
    }).or(
      new BasicLens<Cont, BasicLensM<Cont, Cont>>((c) => {
        return invPred(c.id);
      }),
    ),
  );

export const ShaclPath = ShaclSequencePath.or(
  ShaclAlternativepath,
  ShaclInversePath,
  ShaclPredicatePath,
);

function extractSubtypes(): BasicLens<Cont, Term[]> {
  return empty<Cont>()
    .map(({ id }) => [id])
    .and(pred(RDFS.subClassOf).one(undefined))
    .map(([subs, next]) => {
      if (next) {
        const rest = extractSubtypes().execute(next);
        subs.push(...rest);
      }
      return subs;
    });
}

function field<T extends string, O = string>(
  predicate: Term,
  name: T,
  convert?: (inp: string) => O,
): BasicLens<Cont, { [F in T]: O }> {
  const conv = convert || ((x: string) => <O>x);

  return pred(predicate)
    .one()
    .map(({ id }) => {
      const out = <{ [F in T]: O }>{};
      out[name] = conv(id.value);
      return out;
    });
}

function optionalField<T extends string, O = string>(
  predicate: Term,
  name: T,
  convert?: (inp: string) => O | undefined,
): BasicLens<Cont, { [F in T]: O | undefined }> {
  const conv = convert || ((x: string) => <O | undefined>x);

  return pred(predicate)
    .one(undefined)
    .map((inp) => {
      const out = <{ [F in T]: O | undefined }>{};
      if (inp) {
        out[name] = conv(inp.id.value);
      }
      return out;
    });
}
function dataTypeToExtract(dataType: Term): (t: Term) => any {
  if (dataType.equals(XSD.terms.integer)) return (t) => +t.value;
  if (dataType.equals(XSD.terms.string)) return (t) => t.value;
  if (dataType.equals(XSD.terms.dateTime)) return (t) => new Date(t.value);

  return (t) => t;
}

type Cache = {
  [clazz: string]: { lens: BasicLens<Cont, any>; subClasses: Term[] };
};
function extractProperty(cache: Cache): BasicLens<Cont, ShapeField> {
  const pathLens = pred(SHACL.path)
    .one()
    .then(ShaclPath)
    .map((path) => ({
      path,
    }));
  const nameLens = field(SHACL.name, "name");
  const minCount = optionalField(SHACL.minCount, "minCount", (x) => +x);
  const maxCount = optionalField(SHACL.maxCount, "maxCount", (x) => +x);
  const clazzLens: BasicLens<Cont, { extract: ShapeField["extract"] }> = field(
    SHACL.class,
    "clazz",
  ).map(({ clazz }) => {
    return {
      extract: (id, quads) => {
        // How do I extract this value: use a pre
        let use = clazz;

        const ty = quads.find(
          (q) => q.subject.equals(id) && q.predicate.equals(RDF.terms.type),
        )?.object;

        if (ty && ty.value !== clazz) {
          const self = cache[ty.value];
          if (!self) throw "Could not extract class " + clazz;
          if (!self.subClasses.some((x) => x.value === clazz)) {
            throw `Class ${ty.value} is not a supertype of the expected type ${clazz}`;
          }
          use = ty.value;
        }

        const lens = cache[use];
        const lenses = lens.subClasses.map((x) => cache[x.value]?.lens);
        const validLenses = lenses
          .filter((x) => !!x)
          .map((x) => <BasicLens<Cont, any>>x);
        if (validLenses.length < 1) throw "Could not extract class " + clazz;
        return validLenses[0]
          .and(...validLenses.slice(1))
          .map((xs) => Object.assign({}, ...xs))
          .execute({ id, quads });
      },
    };
  });
  const dataTypeLens: BasicLens<Cont, { extract: ShapeField["extract"] }> =
    pred(SHACL.datatype)
      .one()
      .map(({ id }) => ({
        extract: dataTypeToExtract(id),
      }));

  return pathLens
    .and(nameLens, minCount, maxCount, clazzLens.or(dataTypeLens))
    .map((xs) => Object.assign({}, ...xs));
}

export function extractShape(cache: Cache): BasicLens<Cont, Shape[]> {
  const checkTy = pred(RDF.terms.type)
    .one()
    .map(({ id }) => {
      if (id.equals(SHACL.NodeShape)) return {};
      throw "Shape is not sh:NodeShape";
    });

  const idLens = empty<Cont>().map(({ id }) => ({ id: id.value }));
  const clazzs = pred(SHACL.targetClass);

  const multiple = clazzs.thenAll(
    extractSubtypes()
      .map((subTypes) => ({
        subTypes,
      }))
      .and(empty<Cont>().map(({ id }) => ({ ty: id }))),
  );

  // TODO: Add implictTargetClass
  const descriptionClassLens = optionalField(SHACL.description, "description");
  const fields = pred(SHACL.property)
    .thenSome(extractProperty(cache))
    .map((fields) => ({ fields }));

  return multiple
    .and(checkTy, idLens, descriptionClassLens, fields)
    .map(([multiple, ...others]) =>
      multiple.map((xs) => <Shape>Object.assign({}, ...xs, ...others)),
    );
}

export function extractShapes(quads: Quad[]): {
  shapes: Shape[];
  lenses: Cache;
} {
  const cache: Cache = {};
  const shapes = subjects()
    .then(unique())
    .asMulti()
    .thenSome(extractShape(cache))
    .execute(quads)
    .flat();
  const lenses = [];

  // Populate cache
  for (let shape of shapes) {
    const lens = toLens(shape);
    const target = cache[shape.ty.value];
    if (target && target.lens) {
      cache[shape.ty.value] = {
        lens: target.lens.or(lens),
        subClasses: shape.subTypes,
      };
    } else {
      cache[shape.ty.value] = { lens, subClasses: shape.subTypes };
    }
    lenses.push(lens);
  }

  return { lenses: cache, shapes };
}
