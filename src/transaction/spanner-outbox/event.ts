import type { EventAttributes, OutboxEvent } from '@causa/runtime';
import { SpannerColumn, SpannerTable } from '../../spanner/index.js';

/**
 * A Spanner table that implements the {@link OutboxEvent} interface, such that it can be used to store outbox event.
 *
 * The full DDL for the table to be used by the outbox transaction runner is:
 *
 * ```sql
 * CREATE TABLE OutboxEvent (
 *   id STRING(36) NOT NULL,
 *   topic STRING(MAX) NOT NULL,
 *   data BYTES(MAX) NOT NULL,
 *   attributes JSON NOT NULL,
 *   leaseExpiration TIMESTAMP,
 *   publishedAt TIMESTAMP,
 *   -- 20 is the number of shards.
 *   shard INT64 AS (MOD(ABS(FARM_FINGERPRINT(id)), 20)),
 * ) PRIMARY KEY (id)
 * ROW DELETION POLICY (OLDER_THAN(publishedAt, INTERVAL 0 DAY));
 * CREATE INDEX OutboxEventsByShardAndLeaseExpiration ON OutboxEvent(shard, leaseExpiration) STORING (publishedAt);
 * ```
 *
 * Compared to the {@link OutboxEvent} interface, this requires a `publishedAt` column to be defined, on which the row
 * deletion policy should be set. This allows updating published events rather than deleting them directly.
 * Updating published events allows setting the `leaseExpiration` column to a distant date, such that those events are
 * not scanned when fetching events to publish.
 * Because Spanner allows for version retention, recently deleted rows are still scanned (even as part of an efficient
 * index scan). This can affect performances if the rows are not updated before being deleted, because the
 * `leaseExpiration` date of published events would end up back in the scanned range - in the past (although rows are
 * deleted and not returned).
 */
@SpannerTable({ name: 'OutboxEvent', primaryKey: ['id'] })
export class SpannerOutboxEvent implements OutboxEvent {
  constructor(init: SpannerOutboxEvent) {
    Object.assign(this, init);
  }

  @SpannerColumn()
  readonly id!: string;

  @SpannerColumn()
  readonly topic!: string;

  @SpannerColumn()
  readonly data!: Buffer;

  @SpannerColumn({ isJson: true })
  readonly attributes!: EventAttributes;

  @SpannerColumn()
  readonly leaseExpiration!: Date | null;

  /**
   * The date at which the event was successfully published.
   */
  @SpannerColumn()
  readonly publishedAt!: Date | null;
}
