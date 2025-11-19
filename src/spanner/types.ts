import { protos, Snapshot, Transaction } from '@google-cloud/spanner';
import type { Type } from '@google-cloud/spanner/build/src/codec.js';
export type SqlParamType = Type;

export const SpannerRequestPriority =
  protos.google.spanner.v1.RequestOptions.Priority;

/**
 * A key for a Spanner row.
 */
export type SpannerKey = (string | null)[];

/**
 * Any Spanner transaction that can be used for reading.
 */
export type SpannerReadOnlyTransaction = Snapshot | Transaction;

/**
 * A Spanner transaction that can be used for reading and writing.
 */
export type SpannerReadWriteTransaction = Transaction;

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

/**
 * Parameters for a SQL statement run using `SpannerEntityManager.query`.
 */
export type SqlStatementParams = Record<string, any>;

/**
 * Types for parameters in a SQL statement run using `SpannerEntityManager.query`.
 */
export type SqlStatementTypes = Record<string, SqlParamType>;

/**
 * A SQL statement run using `SpannerEntityManager.query`.
 */
export type SqlStatement = {
  /**
   * The SQL statement to run.
   */
  sql: string;

  /**
   * The values for the parameters referenced in the statement.
   */
  params?: SqlStatementParams;

  /**
   * The types of the parameters in the statement.
   */
  types?: SqlStatementTypes;
};
