import { RetryableError } from '@causa/runtime';

/**
 * A temporary error thrown during an operation on Cloud Tasks.
 * Those errors are usually transient and can be retried.
 */
export class TemporaryCloudTasksError extends RetryableError {
  constructor(message: string) {
    super(message);
  }
}
