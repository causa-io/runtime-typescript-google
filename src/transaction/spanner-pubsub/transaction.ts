import { type EventTransaction, Transaction } from '@causa/runtime';
import {
  SpannerEntityManager,
  type SpannerReadWriteTransaction,
} from '../../spanner/index.js';
import { SpannerStateTransaction } from '../spanner-state-transaction.js';

/**
 * A {@link Transaction} that uses Spanner for state storage and Pub/Sub for event publishing.
 */
export class SpannerPubSubTransaction<
  ET extends EventTransaction = EventTransaction,
> extends Transaction<SpannerStateTransaction, ET> {
  /**
   * The underlying {@link SpannerTransaction} used by the state transaction.
   */
  get spannerTransaction(): SpannerReadWriteTransaction {
    return this.stateTransaction.transaction;
  }

  /**
   * The underlying {@link SpannerEntityManager} used by the state transaction.
   */
  get entityManager(): SpannerEntityManager {
    return this.stateTransaction.entityManager;
  }
}
