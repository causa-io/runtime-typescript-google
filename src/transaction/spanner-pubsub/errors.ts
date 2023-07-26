import { TransactionOldTimestampError } from '@causa/runtime';
import { status } from '@grpc/grpc-js';

/**
 * An error that can be thrown while inside the transaction to abort and retry it.
 * To be used internally when a {@link TransactionOldTimestampError} error is thrown and the transaction should be
 * retried.
 */
export class SpannerTransactionOldTimestampError extends Error {
  // In order for the transaction to be retried, the error must be detected as an "aborted" response by the Spanner
  // NodeJS client.
  // https://github.com/googleapis/nodejs-spanner/blob/45b985436ff968e8f7d05272c41103859692a509/src/transaction-runner.ts#L34
  readonly code = status.ABORTED;

  constructor(readonly parent: TransactionOldTimestampError) {
    super(parent.message);
  }
}
