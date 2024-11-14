import { RetryableError } from '@causa/runtime';
import { status } from '@grpc/grpc-js';
import type { Type } from '@nestjs/common';

/**
 * Error raised when the class for a Spanner table is not correctly defined (e.g. a decorator is missing).
 */
export class InvalidEntityDefinitionError extends Error {
  constructor(entityType: Type, message?: string) {
    super(
      message ??
        `The definition of the Spanner entity class '${entityType.name}' is not valid.`,
    );
  }
}

/**
 * Error raised when something went wrong due to an incorrect use of Spanner.
 */
export abstract class UnexpectedSpannerError extends Error {}

/**
 * An error thrown when a partial or full entity does not contain all the values for the columns that are part of the
 * primary key.
 */
export class EntityMissingPrimaryKeyError extends UnexpectedSpannerError {
  constructor(message?: string) {
    super(
      message ??
        'The entity is missing at least one of its primary key columns.',
    );
  }
}

/**
 * An error thrown when the query passed to Spanner is not valid.
 */
export class InvalidQueryError extends UnexpectedSpannerError {
  constructor(message?: string) {
    super(message ?? 'The read query is invalid.');
  }
}

/**
 * Error raised when the transaction has been committed or rolled back but it was not expected.
 */
export class TransactionFinishedError extends UnexpectedSpannerError {
  constructor(message?: string) {
    super(
      message ??
        'Failed to commit or rollback a transaction that was already ended.',
    );
  }
}

/**
 * An error thrown when an invalid argument is passed to a Spanner query.
 * For example, this is raised when using a snapshot for a SQL query that attempts to write rows.
 */
export class InvalidArgumentError extends UnexpectedSpannerError {
  constructor(message: string) {
    super(message);
  }
}

/**
 * Error raised when Spanner returns an error that makes the current operation fail, but can usually be retried.
 * For example, the call has been cancelled or timed out.
 * An optional gRPC status code can be provided, which should match the source error.
 */
export class TemporarySpannerError extends RetryableError {
  constructor(
    message: string,
    readonly code?: status,
  ) {
    super(message);
  }

  /**
   * Creates a new {@link TemporarySpannerError} that can be thrown to retry a transaction using the Spanner client
   * retry mechanism.
   *
   * @param message The error message.
   * @returns The error to throw to retry the transaction.
   */
  static retryableInTransaction(message: string): TemporarySpannerError {
    // In order for the transaction to be retried, the error must be detected as an "aborted" response by the Spanner
    // NodeJS client.
    // https://github.com/googleapis/nodejs-spanner/blob/45b985436ff968e8f7d05272c41103859692a509/src/transaction-runner.ts#L34
    return new TemporarySpannerError(message, status.ABORTED);
  }
}
