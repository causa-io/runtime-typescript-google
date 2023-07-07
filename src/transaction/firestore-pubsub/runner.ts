import { BufferEventTransaction, TransactionRunner } from '@causa/runtime';
import { Firestore } from '@google-cloud/firestore';
import { PubSubPublisher } from '../../pubsub/index.js';
import {
  FirestoreCollectionResolver,
  FirestoreStateTransaction,
} from './state-transaction.js';
import { FirestorePubSubTransaction } from './transaction.js';

/**
 * A {@link TransactionRunner} that uses Firestore for state and Pub/Sub for events.
 * A Firestore transaction is used as the main transaction. If it succeeds, events are published to Pub/Sub outside of
 * it.
 * This runner and the transaction use the {@link FirestoreStateTransaction}, which handles soft-deleted documents. All
 * entities that are written to the state should be decorated with the `SoftDeletedFirestoreCollection` decorator.
 */
export class FirestorePubSubTransactionRunner extends TransactionRunner<FirestorePubSubTransaction> {
  constructor(
    readonly firestore: Firestore,
    readonly pubSubPublisher: PubSubPublisher,
    readonly collectionResolver: FirestoreCollectionResolver,
  ) {
    super();
  }

  async run<T>(
    runFn: (transaction: FirestorePubSubTransaction) => Promise<T>,
  ): Promise<[T]> {
    const { result, eventTransaction } = await this.firestore.runTransaction(
      async (firestoreTransaction) => {
        const stateTransaction = new FirestoreStateTransaction(
          firestoreTransaction,
          this.collectionResolver,
        );
        const eventTransaction = new BufferEventTransaction(
          this.pubSubPublisher,
        );
        const transaction = new FirestorePubSubTransaction(
          stateTransaction,
          eventTransaction,
        );

        const result = await runFn(transaction);

        return { result, eventTransaction };
      },
    );

    await eventTransaction.commit();

    return [result];
  }
}
