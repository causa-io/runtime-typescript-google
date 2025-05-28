import type { EventAttributes, OutboxEventPublishResult } from '@causa/runtime';
import { Logger } from '@causa/runtime/nestjs';
import type { Database } from '@google-cloud/spanner';
import { PubSubPublisher } from '../../pubsub/index.js';
import {
  SpannerColumn,
  SpannerEntityManager,
  SpannerTable,
} from '../../spanner/index.js';
import { createDatabase, PubSubFixture } from '../../testing.js';
import { SpannerOutboxSender } from './sender.js';
import { getSpannerOutboxEvents, MyEvent } from './utils.test.js';

// The emulator has bugs with generated columns and indexes on them, so this schema declares the `shard` as a normal
// column. This would not be the case in a real setup.
const SPANNER_SCHEMA = [
  `CREATE TABLE OutboxEvent (
    id STRING(36) NOT NULL,
    topic STRING(MAX) NOT NULL,
    data BYTES(MAX) NOT NULL,
    attributes JSON NOT NULL,
    leaseExpiration TIMESTAMP,
    shard INT64 NOT NULL,
  ) PRIMARY KEY (id)`,
  `CREATE INDEX OutboxEventsByShardAndLeaseExpiration ON OutboxEvent(shard, leaseExpiration)`,
];

@SpannerTable({ name: 'OutboxEvent', primaryKey: ['id'] })
class SpannerOutboxEventWithShard {
  constructor(
    data: Omit<SpannerOutboxEventWithShard, 'shard'> &
      Partial<Pick<SpannerOutboxEventWithShard, 'shard'>>,
  ) {
    Object.assign(this, { shard: 0, ...data });
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

  @SpannerColumn({ isInt: true })
  readonly shard!: number;
}

type PublicSpannerOutboxSender = SpannerOutboxSender & {
  fetchEvents(): Promise<SpannerOutboxEventWithShard[]>;
  updateOutbox(result: OutboxEventPublishResult): Promise<void>;
};

describe('SpannerOutboxSender', () => {
  const defaultOptions = {
    pollingInterval: 0,
    batchSize: 3,
    leaseDuration: 2000,
  };
  let logger: Logger;
  let database: Database;
  let pubSubFixture: PubSubFixture;
  let entityManager: SpannerEntityManager;
  let publisher: PubSubPublisher;
  let sender: PublicSpannerOutboxSender;

  beforeAll(async () => {
    logger = new Logger({});
    database = await createDatabase();
    const [operation] = await database.updateSchema(SPANNER_SCHEMA);
    await operation.promise();
    pubSubFixture = new PubSubFixture();
    const pubSubConf = await pubSubFixture.create('my-topic', MyEvent);
    entityManager = new SpannerEntityManager(database);
    publisher = new PubSubPublisher(logger, {
      configurationGetter: (key) => pubSubConf[key],
    });
    sender = new SpannerOutboxSender(
      entityManager,
      SpannerOutboxEventWithShard,
      publisher,
      logger,
      defaultOptions,
    ) as any;
  });

  afterEach(async () => {
    pubSubFixture.clear();
    await entityManager.clear(SpannerOutboxEventWithShard);
  });

  afterAll(async () => {
    await pubSubFixture.deleteAll();
    await database.delete();
  });

  describe('configuration', () => {
    it('should use the passed options', () => {
      const sender = new SpannerOutboxSender(
        entityManager,
        SpannerOutboxEventWithShard,
        publisher,
        logger,
        {
          pollingInterval: 0,
          batchSize: 10,
          idColumn: 'myIdColumn',
          leaseExpirationColumn: 'myLeaseColumn',
          index: 'MyIndex',
        },
      );

      expect(sender.idColumn).toBe('myIdColumn');
      expect(sender.leaseExpirationColumn).toBe('myLeaseColumn');
      expect(sender.index).toBe('MyIndex');
      expect((sender as any).fetchEventsSql).toContain('`myIdColumn`');
      expect((sender as any).fetchEventsSql).toContain('`myLeaseColumn`');
      expect((sender as any).fetchEventsSql).toContain('FORCE_INDEX=`MyIndex`');
      expect((sender as any).acquireLeaseSql).toContain('`myLeaseColumn`');
    });
  });

  describe('fetchEvents', () => {
    it('should acquire a lease on events and return them', async () => {
      const event1 = new SpannerOutboxEventWithShard({
        id: '1',
        topic: 'my-topic',
        data: Buffer.from('🎉'),
        attributes: {},
        leaseExpiration: new Date('2021-01-01'),
      });
      const event2 = new SpannerOutboxEventWithShard({
        id: '2',
        topic: 'my-topic',
        data: Buffer.from('🎉'),
        attributes: {},
        leaseExpiration: null,
      });
      // Should not be retrieved, as the lease is not expired.
      const event3 = new SpannerOutboxEventWithShard({
        id: '3',
        topic: 'my-topic',
        data: Buffer.from('🎉'),
        attributes: {},
        leaseExpiration: new Date(Date.now() + 60000),
      });
      await entityManager.insert([event1, event2, event3]);

      const actualEvents = await sender.fetchEvents();

      const actualEvent1 = await entityManager.findOneByKeyOrFail(
        SpannerOutboxEventWithShard,
        '1',
      );
      const actualEvent2 = await entityManager.findOneByKeyOrFail(
        SpannerOutboxEventWithShard,
        '2',
      );
      const actualEvent3 = await entityManager.findOneByKeyOrFail(
        SpannerOutboxEventWithShard,
        '3',
      );
      const leaseExpectation = expect.toBeBetween(
        new Date(Date.now() + 1900),
        new Date(Date.now() + 2100),
      );
      expect(actualEvent1).toEqual({
        ...event1,
        leaseExpiration: leaseExpectation,
      });
      expect(actualEvent2).toEqual({
        ...event2,
        leaseExpiration: leaseExpectation,
      });
      expect(actualEvent3).toEqual(event3);
      expect(actualEvents).toIncludeAllMembers([actualEvent1, actualEvent2]);
    });

    it('should acquire at most the batch size', async () => {
      const expectedData = Buffer.from('🎉');
      const expectedAttributes = { att: '🏷️' };
      const events = Array.from(
        { length: 5 },
        (_, i) =>
          new SpannerOutboxEventWithShard({
            id: i.toString(),
            topic: 'my-topic',
            data: expectedData,
            attributes: expectedAttributes,
            leaseExpiration: null,
          }),
      );
      await entityManager.insert(events);

      const actualEvents = await sender.fetchEvents();

      const expectedEvent = {
        id: expect.any(String),
        topic: 'my-topic',
        data: Buffer.from('🎉'),
        attributes: expectedAttributes,
        leaseExpiration: expect.toBeBetween(
          new Date(Date.now() + 1900),
          new Date(Date.now() + 2100),
        ),
        shard: 0,
      };
      expect(actualEvents).toEqual([
        expectedEvent,
        expectedEvent,
        expectedEvent,
      ]);
      const acquiredEventIds = actualEvents.map(({ id }) => id);
      const storedEvents = await getSpannerOutboxEvents(
        entityManager,
        SpannerOutboxEventWithShard,
      );
      expect(storedEvents).toIncludeAllMembers([
        ...actualEvents,
        ...events.filter(({ id }) => !acquiredEventIds.includes(id)),
      ]);
    });

    it('should limit the fetch to the shard count', async () => {
      const senderWithSharding = new SpannerOutboxSender(
        entityManager,
        SpannerOutboxEventWithShard,
        publisher,
        logger,
        {
          ...defaultOptions,
          index: 'OutboxEventsByShardAndLeaseExpiration',
          sharding: {
            column: 'shard',
            count: 2,
          },
        },
      ) as any;
      const event = new SpannerOutboxEventWithShard({
        id: '1',
        topic: 'my-topic',
        data: Buffer.from('🎉'),
        attributes: {},
        leaseExpiration: new Date('2024-11-26T14:19:01.253Z'),
        shard: 13,
      });
      await entityManager.insert(event);

      const actualEvents = await senderWithSharding.fetchEvents();

      expect(actualEvents).toBeEmpty();
    });
  });

  describe('updateOutbox', () => {
    it('should delete successfully published events and update failed ones', async () => {
      const event1 = new SpannerOutboxEventWithShard({
        id: '1',
        topic: 'my-topic',
        data: Buffer.from('🎉'),
        attributes: {},
        leaseExpiration: new Date(Date.now() + 1000),
      });
      const event2 = new SpannerOutboxEventWithShard({
        id: '2',
        topic: 'my-topic',
        data: Buffer.from('🎉'),
        attributes: {},
        leaseExpiration: new Date(Date.now() + 1000),
      });
      const event3 = new SpannerOutboxEventWithShard({
        id: '3',
        topic: 'my-topic',
        data: Buffer.from('🎉'),
        attributes: {},
        leaseExpiration: new Date(Date.now() + 1000),
      });
      await entityManager.insert([event1, event2, event3]);

      await sender.updateOutbox({
        '1': true,
        '2': false,
      });

      const actualEvent1 = await entityManager.findOneByKey(
        SpannerOutboxEventWithShard,
        '1',
      );
      const actualEvent2 = await entityManager.findOneByKey(
        SpannerOutboxEventWithShard,
        '2',
      );
      const actualEvent3 = await entityManager.findOneByKey(
        SpannerOutboxEventWithShard,
        '3',
      );
      expect(actualEvent1).toBeUndefined();
      expect(actualEvent2).toEqual({
        ...event2,
        leaseExpiration: null,
      });
      expect(actualEvent3).toEqual(event3);
    });
  });
});
