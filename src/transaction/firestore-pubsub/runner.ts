import {
  OutboxEventTransaction,
  TransactionRunner,
  type ReadWriteTransactionOptions,
  type TransactionFn,
} from '@causa/runtime';
import { Logger } from '@causa/runtime/nestjs';
import { Firestore } from '@google-cloud/firestore';
import { Injectable } from '@nestjs/common';
import { wrapFirestoreOperation } from '../../firestore/index.js';
import { PubSubPublisher } from '../../pubsub/index.js';
import { FirestoreReadOnlyStateTransaction } from './readonly-state-transaction.js';
import { FirestoreStateTransaction } from './state-transaction.js';
import { FirestorePubSubTransaction } from './transaction.js';
import type { FirestoreCollectionResolver } from './types.js';

/**
 * A {@link TransactionRunner} that uses Firestore for state and Pub/Sub for events.
 * A Firestore transaction is used as the main transaction. If it succeeds, events are published to Pub/Sub outside of
 * it.
 * This runner and the transaction use the {@link FirestoreStateTransaction}, which handles soft-deleted documents. All
 * entities that are written to the state should be decorated with the `SoftDeletedFirestoreCollection` decorator.
 */
@Injectable()
export class FirestorePubSubTransactionRunner extends TransactionRunner<
  FirestorePubSubTransaction,
  FirestoreReadOnlyStateTransaction
> {
  constructor(
    readonly firestore: Firestore,
    readonly pubSubPublisher: PubSubPublisher,
    readonly collectionResolver: FirestoreCollectionResolver,
    private readonly logger: Logger,
  ) {
    super();
    this.logger.setContext(FirestorePubSubTransactionRunner.name);
  }

  protected async runReadWrite<RT>(
    options: ReadWriteTransactionOptions,
    runFn: TransactionFn<FirestorePubSubTransaction, RT>,
  ): Promise<RT> {
    this.logger.info('Creating a Firestore Pub/Sub transaction.');

    const { result, eventTransaction } = await wrapFirestoreOperation(() =>
      this.firestore.runTransaction(async (firestoreTransaction) => {
        const stateTransaction = new FirestoreStateTransaction(
          firestoreTransaction,
          this.collectionResolver,
        );
        const eventTransaction = new OutboxEventTransaction(
          this.pubSubPublisher,
          options.publishOptions,
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

    if (eventTransaction.events.length > 0) {
      this.logger.info('Publishing Pub/Sub events.');
      await Promise.all(
        eventTransaction.events.map((e) => this.pubSubPublisher.publish(e)),
      );
    }

    return result;
  }

  protected async runReadOnly<RT>(
    runFn: TransactionFn<FirestoreReadOnlyStateTransaction, RT>,
  ): Promise<RT> {
    return await wrapFirestoreOperation(() =>
      this.firestore.runTransaction(
        async (firestoreTransaction) => {
          const transaction = new FirestoreReadOnlyStateTransaction(
            firestoreTransaction,
            this.collectionResolver,
          );

          return await runFn(transaction);
        },
        { readOnly: true },
      ),
    );
  }
}
