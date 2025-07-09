import {
  OutboxEventTransaction,
  Transaction,
  type OutboxTransaction,
  type PublishOptions,
  type TransactionOption,
} from '@causa/runtime';
import type { Type } from '@nestjs/common';
import {
  SpannerEntityManager,
  type SpannerReadWriteTransaction,
} from '../../spanner/index.js';
import type { SpannerStateTransaction } from './state-transaction.js';

/**
 * Option for a function that accepts a {@link SpannerOutboxTransaction}.
 */
export type SpannerOutboxTransactionOption =
  TransactionOption<SpannerOutboxTransaction>;

/**
 * A {@link Transaction} that uses Spanner for state (and outbox) storage, and Pub/Sub for event publishing.
 */
export class SpannerOutboxTransaction
  extends Transaction
  implements OutboxTransaction
{
  constructor(
    readonly stateTransaction: SpannerStateTransaction,
    readonly eventTransaction: OutboxEventTransaction,
    publishOptions: PublishOptions = {},
  ) {
    super(publishOptions);
  }

  /**
   * The underlying {@link SpannerTransaction} used by the state transaction.
   */
  get spannerTransaction(): SpannerReadWriteTransaction {
    return this.stateTransaction.spannerTransaction;
  }

  /**
   * The underlying {@link SpannerEntityManager} used by the state transaction.
   */
  get entityManager(): SpannerEntityManager {
    return this.stateTransaction.entityManager;
  }

  set<T extends object>(entity: T): Promise<void> {
    return this.stateTransaction.set(entity);
  }

  delete<T extends object>(type: Type<T> | T, key?: Partial<T>): Promise<void> {
    return this.stateTransaction.delete(type, key);
  }

  get<T extends object>(
    type: Type<T>,
    entity: Partial<T>,
  ): Promise<T | undefined> {
    return this.stateTransaction.get(type, entity);
  }

  publish(
    topic: string,
    event: object,
    options?: PublishOptions,
  ): Promise<void> {
    return this.eventTransaction.publish(topic, event, options);
  }
}
