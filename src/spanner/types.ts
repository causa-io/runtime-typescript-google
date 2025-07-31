import { protos } from '@google-cloud/spanner';
import type {
  SpannerReadOnlyTransaction,
  SpannerReadWriteTransaction,
} from './entity-manager.js';

export const SpannerRequestPriority =
  protos.google.spanner.v1.RequestOptions.Priority;

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
export type SpannerReadWriteTransactionOption =
  | {
      /**
       * The transaction to use.
       */
      readonly transaction: SpannerReadWriteTransaction;
    }
  | {
      /**
       * The transaction to use.
       */
      readonly transaction?: undefined;

      /**
       * A tag to assign to the transaction.
       * This can only be provided when creating a new transaction.
       */
      readonly tag?: string;
    };
