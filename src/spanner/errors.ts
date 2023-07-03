import { RetryableError } from '@causa/runtime';

/**
 * Error raised when something went wrong due to an incorrect use of Spanner.
 */
export abstract class UnexpectedSpannerError extends Error {}

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
 * Error raised when Spanner returns an error that makes the current operation fail, but can usually be retried.
 * For example, the call has been cancelled or timed out.
 */
export class TemporarySpannerError extends RetryableError {
  constructor(message: string) {
    super(message);
  }
}
