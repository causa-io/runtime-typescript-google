import { RetryableError } from '@causa/runtime';
import { status } from '@grpc/grpc-js';

/**
 * An error thrown when a Firestore operation fails due to a temporary error.
 * Those errors are usually transient and can be retried.
 * An optional gRPC status code can be provided, which should match the source error.
 */
export class TemporaryFirestoreError extends RetryableError {
  constructor(
    message: string,
    readonly code?: status,
  ) {
    super(message);
  }

  /**
   * Creates a new {@link TemporaryFirestoreError} that can be thrown to retry a transaction using the Firestore client
   * retry mechanism.
   *
   * @param message The error message.
   * @returns The error to throw to retry the transaction.
   */
  static retryableInTransaction(message: string): TemporaryFirestoreError {
    // Based on the logic in the Firestore client:
    // https://github.com/googleapis/nodejs-firestore/blob/e598b9daf628cbc54dc10dab80bb0f46e2a3e2a2/dev/src/transaction.ts#L750
    return new TemporaryFirestoreError(message, status.ABORTED);
  }
}
