import { BufferEventTransaction, TransactionRunner } from '@causa/runtime';
import { Logger } from '@causa/runtime/nestjs';
import { Firestore } from '@google-cloud/firestore';
import { Injectable } from '@nestjs/common';
import { wrapFirestoreOperation } from '../../firestore/index.js';
import { PubSubPublisher } from '../../pubsub/index.js';
import {
  type FirestoreCollectionResolver,
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
@Injectable()
export class FirestorePubSubTransactionRunner extends TransactionRunner<FirestorePubSubTransaction> {
  constructor(
    readonly firestore: Firestore,
    readonly pubSubPublisher: PubSubPublisher,
    readonly collectionResolver: FirestoreCollectionResolver,
    private readonly logger: Logger,
  ) {
    super();
    this.logger.setContext(FirestorePubSubTransactionRunner.name);
  }

  async run<T>(
    runFn: (transaction: FirestorePubSubTransaction) => Promise<T>,
  ): Promise<[T]> {
    this.logger.info('Creating a Firestore Pub/Sub transaction.');

    const { result, eventTransaction } = await wrapFirestoreOperation(() =>
      this.firestore.runTransaction(async (firestoreTransaction) => {
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

        this.logger.info('Committing the Firestore transaction.');
        return { result, eventTransaction };
      }),
    );

    this.logger.info('Publishing Pub/Sub events.');
    await eventTransaction.commit();

    return [result];
  }
}
