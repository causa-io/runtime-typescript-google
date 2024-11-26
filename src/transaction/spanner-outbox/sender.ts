import {
  OutboxEventSender,
  type EventPublisher,
  type OutboxEvent,
  type OutboxEventPublishResult,
  type OutboxEventSenderOptions,
} from '@causa/runtime';
import { Logger } from '@causa/runtime/nestjs';
import type { Statement } from '@google-cloud/spanner/build/src/transaction.js';
import type { Type } from '@nestjs/common';
import { SpannerEntityManager } from '../../spanner/index.js';

/**
 * Sharding options for the {@link SpannerOutboxSender}.
 */
export type SpannerOutboxSenderShardingOptions = {
  /**
   * The name of the column used for sharding.
   */
  readonly column: string;

  /**
   * The number of shards.
   */
  readonly count: number;
};

/**
 * Options for the {@link SpannerOutboxSender}.
 */
export type SpannerOutboxSenderOptions = OutboxEventSenderOptions & {
  /**
   * Sharding options.
   * If not set, queries to fetch events will not use sharding.
   */
  readonly sharding?: SpannerOutboxSenderShardingOptions;

  /**
   * The name of the column used to store the event ID.
   * Defaults to `id`.
   */
  readonly idColumn?: string;

  /**
   * The name of the column used to store the lease expiration.
   * Defaults to `leaseExpiration`.
   */
  readonly leaseExpirationColumn?: string;

  /**
   * The index used to fetch events.
   */
  readonly index?: string;
};

/**
 * The default name for the {@link OutboxEvent.id} column.
 */
const DEFAULT_ID_COLUMN = 'id';

/**
 * The default name for the {@link OutboxEvent.leaseExpiration} column.
 */
const DEFAULT_LEASE_EXPIRATION_COLUMN = 'leaseExpiration';

/**
 * An {@link OutboxEventSender} that uses a Spanner table to store events.
 */
export class SpannerOutboxSender extends OutboxEventSender {
  /**
   * Sharding options.
   * If `null`, queries to fetch events will not use sharding.
   */
  readonly sharding: SpannerOutboxSenderShardingOptions | undefined;

  /**
   * The name of the column used for the {@link OutboxEvent.id} property.
   */
  readonly idColumn: string;

  /**
   * The name of the column used for the {@link OutboxEvent.leaseExpiration} property.
   */
  readonly leaseExpirationColumn: string;

  /**
   * The index used to fetch events.
   */
  readonly index: string | undefined;

  /**
   * The SQL query used to fetch events from the outbox.
   */
  readonly fetchEventsSql: string;

  /**
   * The SQL query used to update events in the outbox after they have been successfully published.
   */
  readonly successfulUpdateSql: string;

  /**
   * The SQL query used to update events in the outbox after they have failed to be published.
   */
  readonly failedUpdateSql: string;

  /**
   * Creates a new {@link SpannerOutboxSender}.
   *
   * @param entityManager The {@link SpannerEntityManager} to use to access the outbox.
   * @param outboxEventType The type for the Spanner table used to store outbox events.
   * @param publisher The {@link EventPublisher} to use to publish events.
   * @param logger The {@link Logger} to use.
   * @param options Options for the {@link SpannerOutboxSender}.
   */
  constructor(
    readonly entityManager: SpannerEntityManager,
    readonly outboxEventType: Type<OutboxEvent>,
    publisher: EventPublisher,
    logger: Logger,
    options: SpannerOutboxSenderOptions = {},
  ) {
    super(publisher, logger, options);

    this.sharding = options.sharding;
    this.idColumn = options.idColumn ?? DEFAULT_ID_COLUMN;
    this.leaseExpirationColumn =
      options.leaseExpirationColumn ?? DEFAULT_LEASE_EXPIRATION_COLUMN;
    this.index = options.index;

    ({
      fetchEventsSql: this.fetchEventsSql,
      successfulUpdateSql: this.successfulUpdateSql,
      failedUpdateSql: this.failedUpdateSql,
    } = this.buildSql());
  }

  /**
   * Builds the SQL queries used to fetch and update events in the outbox based on the options.
   *
   * @returns The SQL queries used to fetch and update events in the outbox.
   */
  protected buildSql(): Pick<
    SpannerOutboxSender,
    'fetchEventsSql' | 'successfulUpdateSql' | 'failedUpdateSql'
  > {
    const table = this.entityManager.sqlTableName(this.outboxEventType);
    const tableWithIndex = this.entityManager.sqlTableName(
      this.outboxEventType,
      { index: this.index },
    );

    let filter = `${this.leaseExpirationColumn} IS NULL OR ${this.leaseExpirationColumn} < @currentTime`;
    if (this.sharding) {
      const { column, count } = this.sharding;
      filter = `${column} BETWEEN 0 AND ${count - 1} AND (${filter})`;
    }

    const fetchEventsSql = `
      UPDATE
        ${table}
      SET
        \`${this.leaseExpirationColumn}\` = @leaseExpiration
      WHERE
        \`${this.idColumn}\` IN (
          SELECT
            \`${this.idColumn}\`
          FROM
            ${tableWithIndex}
          WHERE
            ${filter}
          LIMIT
            @batchSize
        )
      THEN RETURN
        ${this.entityManager.sqlColumns(this.outboxEventType)}`;

    const successfulUpdateSql = `
      DELETE FROM
        ${table}
      WHERE
        \`${this.idColumn}\` IN UNNEST(@ids)`;

    const failedUpdateSql = `
      UPDATE
        ${table}
      SET
        \`${this.leaseExpirationColumn}\` = NULL
      WHERE
        \`${this.idColumn}\` IN UNNEST(@ids)`;

    return { fetchEventsSql, successfulUpdateSql, failedUpdateSql };
  }

  protected async fetchEvents(): Promise<OutboxEvent[]> {
    return await this.entityManager.transaction(async (transaction) => {
      const currentTime = new Date();
      const leaseExpiration = new Date(
        currentTime.getTime() + this.leaseDuration,
      );

      const params = {
        leaseExpiration,
        currentTime,
        batchSize: this.batchSize,
      };

      return await this.entityManager.query(
        { transaction, entityType: this.outboxEventType },
        { sql: this.fetchEventsSql, params },
      );
    });
  }

  protected async updateOutbox(
    result: OutboxEventPublishResult,
  ): Promise<void> {
    const successfulSends: string[] = [];
    const failedSends: string[] = [];
    Object.entries(result).forEach(([id, success]) =>
      (success ? successfulSends : failedSends).push(id),
    );

    const batchUpdates: Statement[] = [];
    if (successfulSends.length > 0) {
      batchUpdates.push({
        sql: this.successfulUpdateSql,
        params: { ids: successfulSends },
      });
    }
    if (failedSends.length > 0) {
      batchUpdates.push({
        sql: this.failedUpdateSql,
        params: { ids: failedSends },
      });
    }

    if (batchUpdates.length === 0) {
      return;
    }

    await this.entityManager.transaction((transaction) =>
      transaction.batchUpdate(batchUpdates),
    );
  }
}
