/**
 * A partial Spanner entity instance, where nested objects can also be partial.
 */
export type RecursivePartialEntity<T> = T extends Date
  ? T
  : T extends object
  ? { [P in keyof T]?: RecursivePartialEntity<T[P]> }
  : T;
