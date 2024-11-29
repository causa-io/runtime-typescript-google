import {
  OutboxTransactionRunner,
  type OutboxEvent,
  type OutboxEventTransaction,
} from '@causa/runtime';
import { Logger } from '@causa/runtime/nestjs';
import type { Type } from '@nestjs/common';
import { SpannerEntityManager } from '../../spanner/index.js';
import { SpannerStateTransaction } from '../spanner-state-transaction.js';
import { SpannerTransaction } from '../spanner-transaction.js';
import { throwRetryableInTransactionIfNeeded } from '../spanner-utils.js';
import { SpannerOutboxSender } from './sender.js';

/**
 * A {@link SpannerTransaction} that uses an {@link OutboxEventTransaction}.
 */
export type SpannerOutboxTransaction =
  SpannerTransaction<OutboxEventTransaction>;

/**
 * An {@link OutboxTransactionRunner} that uses a {@link SpannerTransaction} to run transactions.
 * Events are stored in a Spanner table before being published.
 */
export class SpannerOutboxTransactionRunner extends OutboxTransactionRunner<SpannerOutboxTransaction> {
  constructor(
    readonly entityManager: SpannerEntityManager,
    outboxEventType: Type<OutboxEvent>,
    sender: SpannerOutboxSender,
    logger: Logger,
  ) {
    super(outboxEventType, sender, logger);
  }

  protected async runStateTransaction<RT>(
    eventTransaction: OutboxEventTransaction,
    runFn: (transaction: SpannerOutboxTransaction) => Promise<RT>,
  ): Promise<RT> {
    return await this.entityManager.transaction(async (dbTransaction) => {
      const stateTransaction = new SpannerStateTransaction(
        this.entityManager,
        dbTransaction,
      );
      const transaction = new SpannerTransaction(
        stateTransaction,
        eventTransaction,
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