import {
  OutboxEventTransaction,
  TransactionOldTimestampError,
} from '@causa/runtime';
import { AppFixture, LoggingFixture } from '@causa/runtime/nestjs/testing';
import { Database, Snapshot } from '@google-cloud/spanner';
import { jest } from '@jest/globals';
import { Module } from '@nestjs/common';
import { PubSubPublisher } from '../../pubsub/publisher.js';
import { PubSubPublisherModule } from '../../pubsub/publisher.module.js';
import { SpannerEntityManager, SpannerModule } from '../../spanner/index.js';
import { PubSubFixture, SpannerFixture } from '../../testing.js';
import { SpannerOutboxEvent } from './event.js';
import { SpannerOutboxTransactionModule } from './module.js';
import { SpannerReadOnlyStateTransaction } from './readonly-transaction.js';
import { SpannerOutboxTransactionRunner } from './runner.js';
import {
  expectOutboxToEqual,
  getSpannerOutboxEvents,
  MyEvent,
  MyTable,
  SPANNER_SCHEMA,
} from './utils.test.js';

@Module({
  imports: [
    SpannerModule.forRoot(),
    PubSubPublisherModule.forRoot(),
    SpannerOutboxTransactionModule.forRoot({ pollingInterval: 0 }),
  ],
})
class MyModule {}

describe('SpannerOutboxTransactionRunner', () => {
  let appFixture: AppFixture;
  let pubSubFixture: PubSubFixture;
  let entityManager: SpannerEntityManager;
  let runner: SpannerOutboxTransactionRunner;

  beforeAll(async () => {
    pubSubFixture = new PubSubFixture({ 'my-topic': MyEvent });
    appFixture = new AppFixture(MyModule, {
      fixtures: [
        new SpannerFixture({ types: [MyTable, SpannerOutboxEvent] }),
        pubSubFixture,
      ],
    });
    await appFixture.init();

    const database = appFixture.get(Database);
    const [operation] = await database.updateSchema(SPANNER_SCHEMA);
    await operation.promise();

    entityManager = appFixture.get(SpannerEntityManager);
    runner = appFixture.get(SpannerOutboxTransactionRunner);
  });

  afterEach(() => appFixture.clear());

  afterAll(() => appFixture.delete());

  it('should run the transaction, commit the events, and publish them', async () => {
    const expectedRow = new MyTable({ id: '1', value: '🗃️' });
    const expectedEvent = new MyEvent({
      id: '1',
      producedAt: new Date(),
      name: '📫',
      data: '💌',
    });

    const actualResult = await runner.run(
      { publishOptions: { attributes: { default: '🫥' } }, tag: '🔖' },
      async (transaction) => {
        expect(transaction.spannerTransaction).toBe(
          transaction.stateTransaction.spannerTransaction,
        );
        expect(
          transaction.spannerTransaction.requestOptions?.transactionTag,
        ).toBe('🔖');
        expect(transaction.entityManager).toBe(entityManager);
        expect(transaction.eventTransaction).toBeInstanceOf(
          OutboxEventTransaction,
        );

        await transaction.set(expectedRow);
        await transaction.publish('my-topic', expectedEvent, {
          attributes: { myAttr: '🏷️' },
        });

        return '🎉';
      },
    );

    expect(actualResult).toEqual('🎉');
    const actualRow = await entityManager.findOneByKey(MyTable, '1');
    expect(actualRow).toEqual(expectedRow);
    await pubSubFixture.expectEvent('my-topic', expectedEvent, {
      attributes: {
        default: '🫥',
        eventId: '1',
        eventName: '📫',
        producedAt: expectedEvent.producedAt.toISOString(),
        myAttr: '🏷️',
      },
    });
    await expectOutboxToEqual(entityManager, []);
  });

  it('should not commit the events nor publish them if an error is thrown within the transaction', async () => {
    const actualPromise = runner.run(async (transaction) => {
      await transaction.set(new MyTable({ id: '1', value: '🗃️' }));
      await transaction.publish(
        'my-topic',
        new MyEvent({
          id: '1',
          producedAt: new Date(),
          name: '📫',
          data: '💌',
        }),
      );

      throw new Error('💥');
    });

    await expect(actualPromise).rejects.toThrow('💥');
    const actualRow = await entityManager.findOneByKey(MyTable, '1');
    expect(actualRow).toBeUndefined();
    await pubSubFixture.expectNoMessage('my-topic');
    await expectOutboxToEqual(entityManager, []);
  });

  it('should leave the events in the outbox and remove the lease if publishing fails', async () => {
    const expectedRow = new MyTable({ id: '1', value: '🗃️' });
    const expectedEvent1 = new MyEvent({
      id: '1',
      producedAt: new Date(),
      name: '📫',
      data: '💌',
    });
    const expectedEvent2 = new MyEvent({
      id: '2',
      producedAt: new Date(),
      name: '📫',
      data: '💌',
    });
    const expectedOutboxEvents = [expectedEvent1, expectedEvent2].map(
      (event) =>
        new SpannerOutboxEvent({
          id: expect.any(String),
          topic: 'my-topic',
          data: Buffer.from(JSON.stringify(event)),
          attributes: {
            eventId: event.id,
            eventName: '📫',
            producedAt: event.producedAt.toISOString(),
          },
          leaseExpiration: expect.toBeAfter(new Date()),
        }),
    );
    const actualOutboxEvents = new Promise((resolve) => {
      jest
        .spyOn(appFixture.get(PubSubPublisher), 'publish')
        .mockImplementationOnce(async () => {
          // During publishing, events should still be in the outbox.
          resolve(await getSpannerOutboxEvents(entityManager));
          throw new Error('💥');
        });
    });

    const actualResult = await runner.run(async (transaction) => {
      await transaction.set(expectedRow);
      await transaction.publish('my-topic', expectedEvent1);
      await transaction.publish('my-topic', expectedEvent2);

      return '🎉';
    });

    expect(actualResult).toEqual('🎉');
    const actualRow = await entityManager.findOneByKey(MyTable, '1');
    expect(actualRow).toEqual(expectedRow);
    await pubSubFixture.expectEvent('my-topic', expectedEvent2);
    expect(await actualOutboxEvents).toIncludeSameMembers(expectedOutboxEvents);
    // The failed event should still be in the outbox, with the lease removed.
    await expectOutboxToEqual(entityManager, [
      { ...expectedOutboxEvents[0], leaseExpiration: null },
    ]);
    appFixture
      .get(LoggingFixture)
      .expectErrors({ message: 'Failed to publish an event.' });
  });

  it('should retry the transaction when a TransactionOldTimestampError is thrown', async () => {
    let numCalls = 0;

    const actualResult = await runner.run(async (transaction) => {
      numCalls += 1;
      const id = numCalls.toFixed();
      await transaction.set(new MyTable({ id, value: '🗃' }));
      await transaction.publish(
        'my-topic',
        new MyEvent({ id, producedAt: new Date(), name: '📫', data: '💌' }),
      );

      if (numCalls === 1) {
        throw new TransactionOldTimestampError(transaction.timestamp, 10);
      }

      return '🎉';
    });

    expect(actualResult).toEqual('🎉');
    expect(numCalls).toBe(2);
    const actualRow1 = await entityManager.findOneByKey(MyTable, '1');
    expect(actualRow1).toBeUndefined();
    const actualRow2 = await entityManager.findOneByKey(MyTable, '2');
    expect(actualRow2).toEqual(new MyTable({ id: '2', value: '🗃' }));
    await pubSubFixture.expectEvent(
      'my-topic',
      new MyEvent({
        id: '2',
        producedAt: expect.any(Date),
        name: '📫',
        data: '💌',
      }),
    );
    await expectOutboxToEqual(entityManager, []);
  });

  it('should use a new event transaction on each retry', async () => {
    let numCalls = 0;
    const observedNumStagedEvents: number[] = [];

    const actualResult = await runner.run(async (transaction) => {
      numCalls += 1;
      observedNumStagedEvents.push(transaction.eventTransaction.events.length);
      const id = numCalls.toFixed();
      await transaction.publish(
        'my-topic',
        new MyEvent({ id, producedAt: new Date(), name: '📫', data: '💌' }),
      );

      if (numCalls === 1) {
        throw new TransactionOldTimestampError(transaction.timestamp, 10);
      }

      return '🎉';
    });

    expect(actualResult).toEqual('🎉');
    expect(numCalls).toBe(2);
    expect(observedNumStagedEvents).toEqual([0, 0]);
    await pubSubFixture.expectEvent(
      'my-topic',
      new MyEvent({
        id: '2',
        producedAt: expect.any(Date),
        name: '📫',
        data: '💌',
      }),
    );
    // This could pass unexpectedly because we cannot guarantee all messages have been received.
    // However, this in addition to `observedNumStagedEvents` should be enough to ensure that a new event transaction is
    // used on each retry.
    expect(pubSubFixture.topics['my-topic'].messages).toHaveLength(1);
    await expectOutboxToEqual(entityManager, []);
  });

  it('should not retry the transaction when a TransactionOldTimestampError is thrown with a delay that is too high', async () => {
    const actualPromise = runner.run(async (transaction) => {
      await transaction.set(new MyTable({ id: '1', value: '🗃' }));
      await transaction.publish(
        'my-topic',
        new MyEvent({
          id: '1',
          producedAt: new Date(),
          name: '📫',
          data: '💌',
        }),
      );

      throw new TransactionOldTimestampError(transaction.timestamp, 100000);
    });

    await expect(actualPromise).rejects.toThrow(TransactionOldTimestampError);
    const actualRow = await entityManager.findOneByKey(MyTable, '1');
    expect(actualRow).toBeUndefined();
    await pubSubFixture.expectNoMessage('my-topic');
    await expectOutboxToEqual(entityManager, []);
  });

  it('should run a readonly transaction', async () => {
    const actualResult = await runner.run(
      { readOnly: true },
      async (transaction) => {
        expect(transaction).toBeInstanceOf(SpannerReadOnlyStateTransaction);
        expect(transaction.entityManager).toBe(entityManager);
        expect(transaction.spannerTransaction).toBeInstanceOf(Snapshot);
        return '🎉';
      },
    );

    expect(actualResult).toEqual('🎉');
  });
});
