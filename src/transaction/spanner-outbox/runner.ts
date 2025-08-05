import {
  OutboxTransactionRunner,
  type OutboxEvent,
  type OutboxEventTransaction,
  type ReadOnlyTransactionOption,
  type ReadWriteTransactionOptions,
  type TransactionFn,
  type TransactionOption,
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
 * Options for a Spanner outbox transaction.
 */
export type SpannerOutboxReadWriteTransactionOptions =
  ReadWriteTransactionOptions & {
    /**
     * The Spanner tag to assign to the transaction.
     */
    tag?: string;
  };

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
    options: SpannerOutboxReadWriteTransactionOptions,
    runFn: TransactionFn<SpannerOutboxTransaction, RT>,
  ): Promise<RT> {
    return await this.entityManager.transaction(
      { tag: options.tag },
      async (dbTransaction) => {
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
      },
    );
  }

  run<RT>(runFn: TransactionFn<SpannerOutboxTransaction, RT>): Promise<RT>;
  run<RT>(
    options:
      | TransactionOption<SpannerOutboxTransaction>
      | SpannerOutboxReadWriteTransactionOptions,
    runFn: TransactionFn<SpannerOutboxTransaction, RT>,
  ): Promise<RT>;
  run<RT>(
    options: ReadOnlyTransactionOption<SpannerReadOnlyStateTransaction> & {
      readOnly: true;
    },
    runFn: TransactionFn<SpannerReadOnlyStateTransaction, RT>,
  ): Promise<RT>;
  async run<RT>(
    optionsOrRunFn:
      | TransactionOption<SpannerOutboxTransaction>
      | SpannerOutboxReadWriteTransactionOptions
      | (ReadOnlyTransactionOption<SpannerReadOnlyStateTransaction> & {
          readOnly: true;
        })
      | TransactionFn<SpannerOutboxTransaction, RT>,
    runFn?:
      | TransactionFn<SpannerOutboxTransaction, RT>
      | TransactionFn<SpannerReadOnlyStateTransaction, RT>,
  ): Promise<RT> {
    return super.run(optionsOrRunFn as any, runFn as any);
  }
}
