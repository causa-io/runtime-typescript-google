import { BufferEventTransaction, Transaction } from '@causa/runtime';
import { Transaction as SpannerTransaction } from '@google-cloud/spanner';
import { SpannerEntityManager } from '../../spanner/index.js';
import { SpannerStateTransaction } from './state-transaction.js';

/**
 * A {@link Transaction} that uses Spanner for state storage and Pub/Sub for event publishing.
 */
export class SpannerPubSubTransaction extends Transaction<
  SpannerStateTransaction,
  BufferEventTransaction
> {
  /**
   * The underlying {@link SpannerTransaction} used by the state transaction.
   */
  get spannerTransaction(): SpannerTransaction {
    return this.stateTransaction.transaction;
  }

  /**
   * The underlying {@link SpannerEntityManager} used by the state transaction.
   */
  get entityManager(): SpannerEntityManager {
    return this.stateTransaction.entityManager;
  }
}
