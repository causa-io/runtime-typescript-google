import { TransactionOldTimestampError } from '@causa/runtime';

/**
 * The delay, in milliseconds, over which a timestamp issue is deemed irrecoverable.
 */
const ACCEPTABLE_PAST_DATE_DELAY = 25000;

/**
 * The maximum number of attempts for a transaction failing with `TransactionOldTimestampError`s.
 */
const MAX_OLD_TIMESTAMP_ATTEMPTS = 10;

/**
 * The maximum random delay, in milliseconds, added before the first retry.
 * It doubles with each attempt, up to {@link MAX_RETRY_JITTER}.
 */
const BASE_RETRY_JITTER = 10;

/**
 * The maximum random delay, in milliseconds, added before a retry.
 */
const MAX_RETRY_JITTER = 1000;

/**
 * Returns the delay after which a transaction that failed with the given error should be retried.
 * Only `TransactionOldTimestampError`s are retried, as long as the timestamp issue is deemed recoverable and the
 * maximum number of attempts has not been reached.
 *
 * @param error The error that caused the transaction to fail.
 * @param attempt The number of the attempt that failed, starting at `1`.
 * @returns The delay in milliseconds before retrying the transaction, or `undefined` if it should not be retried.
 */
export function getOldTimestampRetryDelay(
  error: unknown,
  attempt: number,
): number | undefined {
  // `TransactionOldTimestampError`s indicate that the transaction is using a timestamp older than what is
  // observed in the state (Spanner).
  if (
    !(error instanceof TransactionOldTimestampError) ||
    attempt >= MAX_OLD_TIMESTAMP_ATTEMPTS
  ) {
    return undefined;
  }

  const delay = error.delay ?? Infinity;
  if (delay >= ACCEPTABLE_PAST_DATE_DELAY) {
    return undefined;
  }

  // The timestamp of the new transaction should be strictly after the observed date, `transactionTimestamp + delay`.
  const remainingDelay =
    error.transactionTimestamp.getTime() + delay + 1 - Date.now();
  // Avoids retrying in lockstep when several transactions conflict on the same entity.
  const jitter =
    Math.random() *
    Math.min(MAX_RETRY_JITTER, BASE_RETRY_JITTER * 2 ** (attempt - 1));
  return Math.max(remainingDelay, 0) + jitter;
}
