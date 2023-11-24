import { RetryableError } from '@causa/runtime';

/**
 * An error thrown when a Firestore operation fails due to a temporary error.
 * Those errors are usually transient and can be retried.
 */
export class TemporaryFirestoreError extends RetryableError {
  constructor(message: string) {
    super(message);
  }
}
