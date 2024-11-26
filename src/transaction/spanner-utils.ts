import { TransactionOldTimestampError } from '@causa/runtime';
import { setTimeout } from 'timers/promises';
import { TemporarySpannerError } from '../spanner/index.js';

/**
 * The delay, in milliseconds, over which a timestamp issue is deemed irrecoverable.
 */
const ACCEPTABLE_PAST_DATE_DELAY = 25000;

/**
 * Checks if the given error is a `TransactionOldTimestampError` and throws a `TemporarySpannerError` such that the
 * transaction can be retried by the runner.
 * If the {@link TransactionOldTimestampError.delay} is too large, the error is deemed irrecoverable and nothing is
 * thrown (it is up to the caller to handle the error).
 *
 * @param error The error to test.
 */
export async function throwRetryableInTransactionIfNeeded(
  error: unknown,
): Promise<void> {
  // `TransactionOldTimestampError`s indicate that the transaction is using a timestamp older than what is
  // observed in the state (Spanner).
  if (!(error instanceof TransactionOldTimestampError)) {
    return;
  }

  const delay = error.delay ?? Infinity;
  if (delay >= ACCEPTABLE_PAST_DATE_DELAY) {
    return;
  }
  if (delay > 0) {
    await setTimeout(delay);
  }

  // Throwing a `TemporarySpannerError` will cause the transaction to be retried with a newer timestamp.
  throw TemporarySpannerError.retryableInTransaction(error.message);
}
