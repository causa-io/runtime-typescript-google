import type { EventPublisher, OutboxEvent } from '@causa/runtime';
import { EVENT_PUBLISHER_INJECTION_NAME, Logger } from '@causa/runtime/nestjs';
import type { DynamicModule, Type } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { SpannerEntityManager } from '../../spanner/index.js';
import { SpannerOutboxEvent } from './event.js';
import { SpannerOutboxTransactionRunner } from './runner.js';
import {
  SpannerOutboxSender,
  type SpannerOutboxSenderOptions,
  type SpannerOutboxSenderShardingOptions,
} from './sender.js';

/**
 * Options for the {@link SpannerOutboxTransactionModule}.
 */
export type SpannerOutboxTransactionModuleOptions =
  SpannerOutboxSenderOptions & {
    /**
     * The type of {@link OutboxEvent} used by the {@link SpannerOutboxTransactionRunner}.
     * This should be a valid class decorated with `@SpannerTable`.
     * Defaults to {@link SpannerOutboxEvent}.
     */
    outboxEventType?: Type<OutboxEvent>;
  };

/**
 * Combines options passed to the module with the configuration (from the environment).
 *
 * @param options Options passed to the module.
 * @param config The {@link ConfigService} to use.
 * @returns The parsed {@link SpannerOutboxSenderOptions}.
 */
function parseSenderOptions(
  options: SpannerOutboxTransactionModuleOptions,
  config: ConfigService,
): SpannerOutboxSenderOptions {
  function validateIntOrUndefined(name: string): number | undefined {
    const strValue = config.get<string>(name);
    if (strValue === undefined) {
      return undefined;
    }

    const value = parseInt(strValue);
    if (isNaN(value)) {
      throw new Error(`Environment variable ${name} must be a number.`);
    }

    return value;
  }

  const batchSize = validateIntOrUndefined('SPANNER_OUTBOX_BATCH_SIZE');
  const pollingInterval = validateIntOrUndefined(
    'SPANNER_OUTBOX_POLLING_INTERVAL',
  );
  const idColumn = config.get<string>('SPANNER_OUTBOX_ID_COLUMN');
  const leaseExpirationColumn = config.get<string>(
    'SPANNER_OUTBOX_LEASE_EXPIRATION_COLUMN',
  );
  const index = config.get<string>('SPANNER_OUTBOX_INDEX');
  const shardingColumn = config.get<string>('SPANNER_OUTBOX_SHARDING_COLUMN');
  const shardingCount = validateIntOrUndefined('SPANNER_OUTBOX_SHARDING_COUNT');
  const leaseDuration = validateIntOrUndefined('SPANNER_OUTBOX_LEASE_DURATION');

  const sharding: SpannerOutboxSenderShardingOptions | undefined =
    shardingColumn && shardingCount
      ? { column: shardingColumn, count: shardingCount }
      : undefined;

  const envOptions: SpannerOutboxSenderOptions = {
    batchSize,
    pollingInterval,
    idColumn,
    leaseExpirationColumn,
    index,
    sharding,
    leaseDuration,
  };

  return { ...envOptions, ...options };
}

/**
 * The module providing the {@link SpannerOutboxTransactionRunner}.
 * This assumes the `SpannerModule` and an {@link EventPublisher} are available (as well as the `LoggerModule`).
 */
export class SpannerOutboxTransactionModule {
  /**
   * Initializes the {@link SpannerOutboxTransactionModule} with the given options.
   * The returned module is always global.
   *
   * @param options Options for the {@link SpannerOutboxTransactionModule}.
   * @returns The module.
   */
  static forRoot(
    options: SpannerOutboxTransactionModuleOptions = {},
  ): DynamicModule {
    const { outboxEventType, ...senderOptions } = {
      outboxEventType: SpannerOutboxEvent,
      ...options,
    };

    return {
      module: SpannerOutboxTransactionModule,
      global: true,
      providers: [
        {
          provide: SpannerOutboxSender,
          useFactory: (
            entityManager: SpannerEntityManager,
            publisher: EventPublisher,
            logger: Logger,
            config: ConfigService,
          ) => {
            const options = parseSenderOptions(senderOptions, config);
            return new SpannerOutboxSender(
              entityManager,
              outboxEventType,
              publisher,
              logger,
              options,
            );
          },
          inject: [
            SpannerEntityManager,
            EVENT_PUBLISHER_INJECTION_NAME,
            Logger,
            ConfigService,
          ],
        },
        {
          provide: SpannerOutboxTransactionRunner,
          useFactory: (
            entityManager: SpannerEntityManager,
            sender: SpannerOutboxSender,
            logger: Logger,
          ) =>
            new SpannerOutboxTransactionRunner(
              entityManager,
              outboxEventType,
              sender,
              logger,
            ),
          inject: [SpannerEntityManager, SpannerOutboxSender, Logger],
        },
      ],
      exports: [SpannerOutboxTransactionRunner],
    };
  }
}
