import {
  OutboxEventTransaction,
  Transaction,
  type PublishOptions,
  type TransactionOption,
} from '@causa/runtime';
import type { Type } from '@nestjs/common';
import { Transaction as FirestoreTransaction } from 'firebase-admin/firestore';
import { FirestoreReadOnlyStateTransaction } from './readonly-state-transaction.js';
import type { FirestoreStateTransaction } from './state-transaction.js';
import type { FirestoreCollectionResolver } from './types.js';

/**
 * Option for a function that accepts a {@link FirestorePubSubTransaction}.
 */
export type FirestoreOutboxTransactionOption =
  TransactionOption<FirestorePubSubTransaction>;

/**
 * A {@link Transaction} that uses Firestore for state storage and Pub/Sub for event publishing.
 */
export class FirestorePubSubTransaction
  extends Transaction
  implements FirestoreReadOnlyStateTransaction
{
  constructor(
    readonly stateTransaction: FirestoreStateTransaction,
    private readonly eventTransaction: OutboxEventTransaction,
    publishOptions: PublishOptions = {},
  ) {
    super(publishOptions);
  }

  /**
   * The underlying {@link FirestoreTransaction} used by the state transaction.
   */
  get firestoreTransaction(): FirestoreTransaction {
    return this.stateTransaction.firestoreTransaction;
  }

  get collectionResolver(): FirestoreCollectionResolver {
    return this.stateTransaction.collectionResolver;
  }

  set<T extends object>(entity: T): Promise<void> {
    return this.stateTransaction.set(entity);
  }

  delete<T extends object>(type: Type<T> | T, key?: Partial<T>): Promise<void> {
    return this.stateTransaction.delete(type, key);
  }

  get<T extends object>(type: Type<T>, entity: Partial<T>): Promise<T | null> {
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
