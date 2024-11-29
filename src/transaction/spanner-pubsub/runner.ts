import { BufferEventTransaction, TransactionRunner } from '@causa/runtime';
import { Logger } from '@causa/runtime/nestjs';
import { Injectable } from '@nestjs/common';
import { PubSubPublisher } from '../../pubsub/index.js';
import { SpannerEntityManager } from '../../spanner/index.js';
import { SpannerStateTransaction } from '../spanner-state-transaction.js';
import { SpannerTransaction } from '../spanner-transaction.js';
import { throwRetryableInTransactionIfNeeded } from '../spanner-utils.js';

/**
 * A {@link TransactionRunner} that uses Spanner for state and Pub/Sub for events.
 * A Spanner transaction is used as the main transaction. If it succeeds, events are published to Pub/Sub outside of it.
 */
@Injectable()
export class SpannerPubSubTransactionRunner extends TransactionRunner<SpannerTransaction> {
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
    this.logger.setContext(SpannerPubSubTransactionRunner.name);
  }

  async run<T>(
    runFn: (transaction: SpannerTransaction) => Promise<T>,
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
        const transaction = new SpannerTransaction(
          stateTransaction,
          eventTransaction,
        );

        try {
          const result = await runFn(transaction);

          this.logger.info('Committing the Spanner transaction.');
          return { result, eventTransaction };
        } catch (error) {
          await throwRetryableInTransactionIfNeeded(error);
          throw error;
        }
      },
    );

    this.logger.info('Publishing Pub/Sub events.');
    await eventTransaction.commit();

    return [result];
  }
}
