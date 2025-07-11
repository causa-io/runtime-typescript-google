import type {
  SpannerReadOnlyTransaction,
  SpannerReadWriteTransaction,
} from './entity-manager.js';

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
export type SpannerReadWriteTransactionOption = {
  /**
   * The transaction to use.
   */
  readonly transaction?: SpannerReadWriteTransaction;
};
