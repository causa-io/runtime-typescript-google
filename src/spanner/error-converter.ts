import { EntityAlreadyExistsError, RetryableError } from '@causa/runtime';
import { SessionPoolExhaustedError } from '@google-cloud/spanner/build/src/session-pool.js';
import { status } from '@grpc/grpc-js';
import {
  InvalidArgumentError,
  InvalidQueryError,
  TemporarySpannerError,
  UnexpectedSpannerError,
} from './errors.js';

/**
 * Converts an error thrown by Spanner to an entity error or a Spanner error subclass.
 *
 * @param error The error thrown by Spanner.
 * @returns The specific error, or undefined if it could not be converted.
 */
export function convertSpannerToEntityError(error: any): Error | undefined {
  // Those are errors that have already been converted (or that don't come from Spanner).
  if (
    error instanceof RetryableError ||
    error instanceof UnexpectedSpannerError
  ) {
    return;
  }

  // Those are not gRPC errors and are thrown by the session pool.
  if (
    error instanceof SessionPoolExhaustedError ||
    error.message == 'Timeout occurred while acquiring session.'
  ) {
    return new TemporarySpannerError(error.message);
  }

  switch (error.code) {
    case status.INVALID_ARGUMENT:
      return new InvalidArgumentError(error.message);
    case status.NOT_FOUND:
      // `NOT_FOUND` errors are often thrown when resources other than rows are missing (e.g. an index, a column, etc).
      // This means that they usually describe a developer error rather than a missing entity.
      // A "real" `NOT_FOUND` can only happen for a row during an update. However in this case the repository uses a
      //  transaction to retrieve the entire entity first. During this phase, a proper not found error can be thrown.
      return new InvalidQueryError(error.message);
    case status.ALREADY_EXISTS:
      return new EntityAlreadyExistsError(null, null);
    case status.CANCELLED:
    case status.DEADLINE_EXCEEDED:
    case status.INTERNAL:
    case status.UNAVAILABLE:
    case status.ABORTED:
    case status.RESOURCE_EXHAUSTED:
      return new TemporarySpannerError(error.message, error.code, {
        cause: error,
      });
    default:
      return;
  }
}
