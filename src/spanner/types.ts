import type {
  SpannerReadOnlyTransaction,
  SpannerReadWriteTransaction,
} from './entity-manager.js';

/**
 * A partial Spanner entity instance, where nested objects can also be partial.
 */
export type RecursivePartialEntity<T> = T extends Date
  ? T
  : T extends object
    ? Partial<T> | { [P in keyof T]?: RecursivePartialEntity<T[P]> }
    : T;

/**
 * Option for a function that accepts a Spanner read-only transaction.
 */
export type SpannerReadOnlyTransactionOption = {
  /**
   * The transaction to use.
   */
  readonly transaction?: SpannerReadOnlyTransaction;
};

/**
 * Option for a function that accepts a Spanner read-write transaction.
 */
export type SpanerReadWriteTransactionOption = {
  /**
   * The transaction to use.
   */
  readonly transaction?: SpannerReadWriteTransaction;
};
