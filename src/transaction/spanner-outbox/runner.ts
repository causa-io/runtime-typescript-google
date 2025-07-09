import {
  OutboxTransactionRunner,
  type OutboxEvent,
  type OutboxEventTransaction,
  type ReadWriteTransactionOptions,
  type TransactionFn,
} from '@causa/runtime';
import { Logger } from '@causa/runtime/nestjs';
import type { Type } from '@nestjs/common';
import { SpannerEntityManager } from '../../spanner/index.js';
import { SpannerReadOnlyStateTransaction } from './readonly-transaction.js';
import { SpannerOutboxSender } from './sender.js';
import { throwRetryableInTransactionIfNeeded } from './spanner-utils.js';
import { SpannerStateTransaction } from './state-transaction.js';
import { SpannerOutboxTransaction } from './transaction.js';

/**
 * An {@link OutboxTransactionRunner} that uses a {@link SpannerOutboxTransaction} to run transactions.
 * Events are stored in a Spanner table before being published.
 */
export class SpannerOutboxTransactionRunner extends OutboxTransactionRunner<
  SpannerOutboxTransaction,
  SpannerReadOnlyStateTransaction
> {
  constructor(
    readonly entityManager: SpannerEntityManager,
    outboxEventType: Type<OutboxEvent>,
    sender: SpannerOutboxSender,
    logger: Logger,
  ) {
    super(outboxEventType, sender, logger);
  }

  protected async runReadOnly<RT>(
    runFn: TransactionFn<SpannerReadOnlyStateTransaction, RT>,
  ): Promise<RT> {
    return await this.entityManager.snapshot(async (dbTransaction) => {
      const transaction = new SpannerReadOnlyStateTransaction(
        this.entityManager,
        dbTransaction,
      );

      return await runFn(transaction);
    });
  }

  protected async runStateTransaction<RT>(
    eventTransactionFactory: () => OutboxEventTransaction,
    options: ReadWriteTransactionOptions,
    runFn: TransactionFn<SpannerOutboxTransaction, RT>,
  ): Promise<RT> {
    return await this.entityManager.transaction(async (dbTransaction) => {
      const stateTransaction = new SpannerStateTransaction(
        this.entityManager,
        dbTransaction,
      );
      const eventTransaction = eventTransactionFactory();

      const transaction = new SpannerOutboxTransaction(
        stateTransaction,
        eventTransaction,
        options.publishOptions,
      );

      try {
        return await runFn(transaction);
      } catch (error) {
        await throwRetryableInTransactionIfNeeded(error);
        throw error;
      }
    });
  }
}
