import {
  BufferEventTransaction,
  TransactionOldTimestampError,
  TransactionRunner,
} from '@causa/runtime';
import { Logger } from '@causa/runtime/nestjs';
import { Injectable } from '@nestjs/common';
import { setTimeout } from 'timers/promises';
import { PubSubPublisher } from '../../pubsub/index.js';
import {
  SpannerEntityManager,
  TemporarySpannerError,
} from '../../spanner/index.js';
import { SpannerStateTransaction } from '../spanner-state-transaction.js';
import { SpannerPubSubTransaction } from './transaction.js';

/**
 * The delay, in milliseconds, over which a timestamp issue is deemed irrecoverable.
 */
const ACCEPTABLE_PAST_DATE_DELAY = 25000;

/**
 * A {@link TransactionRunner} that uses Spanner for state and Pub/Sub for events.
 * A Spanner transaction is used as the main transaction. If it succeeds, events are published to Pub/Sub outside of it.
 */
@Injectable()
export class SpannerPubSubTransactionRunner extends TransactionRunner<SpannerPubSubTransaction> {
  /**
   * Creates a new {@link SpannerPubSubTransactionRunner}.
   *
   * @param entityManager The {@link SpannerEntityManager} to use for the transaction.
   * @param publisher The {@link PubSubPublisher} to use for the transaction.
   * @param logger The {@link Logger} to use.
   */
  constructor(
    readonly entityManager: SpannerEntityManager,
    readonly publisher: PubSubPublisher,
    private readonly logger: Logger,
  ) {
    super();
  }

  async run<T>(
    runFn: (transaction: SpannerPubSubTransaction) => Promise<T>,
  ): Promise<[T]> {
    this.logger.info('Creating a Spanner Pub/Sub transaction.');

    const { result, eventTransaction } = await this.entityManager.transaction(
      async (dbTransaction) => {
        const stateTransaction = new SpannerStateTransaction(
          this.entityManager,
          dbTransaction,
        );
        // This must be inside the Spanner transaction because staged messages should be cleared when the transaction is retried.
        const eventTransaction = new BufferEventTransaction(this.publisher);
        const transaction = new SpannerPubSubTransaction(
          stateTransaction,
          eventTransaction,
        );

        try {
          const result = await runFn(transaction);

          this.logger.info('Committing the Spanner transaction.');
          return { result, eventTransaction };
        } catch (error) {
          // `TransactionOldTimestampError`s indicate that the transaction is using a timestamp older than what is
          // observed in the state (Spanner).
          // Throwing a `SpannerTransactionOldTimestampError` will cause the transaction to be retried with a newer
          // timestamp.
          if (!(error instanceof TransactionOldTimestampError)) {
            throw error;
          }

          const delay = error.delay ?? Infinity;
          if (delay >= ACCEPTABLE_PAST_DATE_DELAY) {
            throw error;
          }
          if (delay > 0) {
            await setTimeout(delay);
          }

          throw TemporarySpannerError.retryableInTransaction(error.message);
        }
      },
    );

    this.logger.info('Publishing Pub/Sub events.');
    await eventTransaction.commit();

    return [result];
  }
}
