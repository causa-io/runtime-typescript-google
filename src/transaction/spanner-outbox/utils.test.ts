import type { Event, OutboxEvent } from '@causa/runtime';
import type { Type as NestjsType } from '@nestjs/common';
import { Type } from 'class-transformer';
import { setTimeout } from 'timers/promises';
import {
  SpannerColumn,
  SpannerEntityManager,
  SpannerTable,
} from '../../spanner/index.js';
import { SpannerOutboxEvent } from './event.js';

@SpannerTable({ primaryKey: ['id'] })
export class MyTable {
  constructor(data: MyTable) {
    Object.assign(this, data);
  }

  @SpannerColumn()
  readonly id!: string;

  @SpannerColumn()
  readonly value!: string;
}

export class MyEvent implements Event {
  constructor(data: MyEvent) {
    Object.assign(this, data);
  }

  readonly id!: string;

  @Type(() => Date)
  readonly producedAt!: Date;
  readonly name!: string;
  readonly data!: string;
}

export const SPANNER_SCHEMA = [
  `CREATE TABLE OutboxEvent (
    id STRING(36) NOT NULL,
    topic STRING(MAX) NOT NULL,
    data BYTES(MAX) NOT NULL,
    attributes JSON NOT NULL,
    leaseExpiration TIMESTAMP,
    publishedAt TIMESTAMP,
  ) PRIMARY KEY (id)`,
  `CREATE TABLE MyTable (
    id STRING(MAX) NOT NULL,
    value STRING(MAX) NOT NULL,
  ) PRIMARY KEY (id)`,
];

export function expectedOutboxEvent(
  event: MyEvent,
  options: Partial<SpannerOutboxEvent> & {
    published?: boolean;
    leased?: boolean;
  } = {},
): SpannerOutboxEvent {
  const { published, leased, ...data } = options;
  return new SpannerOutboxEvent({
    id: expect.any(String),
    topic: 'my-topic',
    data: Buffer.from(JSON.stringify(event)),
    attributes: {
      eventId: event.id,
      eventName: event.name,
      producedAt: event.producedAt.toISOString(),
    },
    leaseExpiration: published
      ? new Date('9999-12-31T00:00:00.000Z')
      : leased
        ? expect.toBeAfter(new Date())
        : null,
    publishedAt: published
      ? expect.toSatisfy((d) => d.getTime() < Date.now())
      : null,
    ...data,
  });
}

export async function expectOutboxToEqual(
  entityManager: SpannerEntityManager,
  expected: SpannerOutboxEvent[],
) {
  const maxAttempts = 10;
  let attempt = 0;

  while (attempt < maxAttempts) {
    attempt += 1;
    const events = await getSpannerOutboxEvents(entityManager);

    try {
      expect(events).toContainAllValues(expected);
      return;
    } catch (error) {
      if (attempt >= maxAttempts) {
        throw error;
      }
    }

    await setTimeout(100);
  }
}

export async function getSpannerOutboxEvents(
  entityManager: SpannerEntityManager,
  type: NestjsType<OutboxEvent> = SpannerOutboxEvent,
): Promise<OutboxEvent[]> {
  return entityManager.query(
    { entityType: type },
    {
      sql: `
        SELECT
          ${entityManager.sqlColumns(type)}
        FROM
          ${entityManager.sqlTableName(type)}`,
    },
  );
}
