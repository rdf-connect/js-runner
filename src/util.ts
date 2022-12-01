import * as RDF from "rdf-js";

export const typeNode = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

export type Keyed<T> = { [Key in keyof T]: (RDF.Term | undefined) };

export type Map<V, K, T, O> = (value: V, key: K, item: T) => O;

export function merge<
  VT extends keyof T,
  KT extends keyof T,
  T extends Keyed<T>,
  CT extends keyof T,
  V = string,
  >(
    items: T[], subject: keyof T, key: KT, value: VT, consts: CT[] = [], mapper?: Map<T[VT], T[KT], T, V>,
): { [label: string]: { [label: string]: typeof mapper extends undefined ? string : V } & { [K in CT]: string } } {

  const cs = {} as { [label: string]: { [K in CT]: string } };
  const out = {} as { [label: string]: { [label: string]: typeof mapper extends undefined ? string : V } };
  const m = mapper ? mapper : <Map<T[VT], T[KT], T, V>><any>((x: T[VT], _key: T[KT], _item: T) => x!.value);

  for (let item of items) {
    const subjectV = item[subject]!;
    const keyV = item[key]!;
    const valueV = item[value];

    if (!out[subjectV.value]) {
      const v: { [K in CT]?: string } = {};
      for (let c of consts) {
        v[c] = item[c]?.value;
      }
      cs[subjectV.value] = <{ [K in CT]: string }>v;
      out[subjectV.value] = {} as { [label: string]: typeof mapper extends undefined ? string : V };
    }

    out[subjectV.value][keyV.value] = m(valueV, keyV, item);
  }


  const outs: { [label: string]: { [label: string]: typeof mapper extends undefined ? string : V } & { [K in CT]: string } } = {};
  for (let k in out) {
    outs[k] = Object.assign(out[k], cs[k]);
  }

  return outs;
} 
