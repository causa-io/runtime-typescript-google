import {
  type Event,
  IsDateType,
  IsNullable,
  TransactionOldTimestampError,
  ValidateNestedType,
  type VersionedEntity,
  VersionedEntityManager,
} from '@causa/runtime';
import { Logger } from '@causa/runtime/nestjs';
import { Database } from '@google-cloud/spanner';
import { IsString, IsUUID } from 'class-validator';
import { PubSubPublisher } from '../../pubsub/index.js';
import {
  SpannerColumn,
  SpannerEntityManager,
  SpannerTable,
} from '../../spanner/index.js';
import { PubSubFixture, createDatabase } from '../../testing.js';
import { SpannerTransaction } from '../spanner-transaction.js';
import { SpannerPubSubTransactionRunner } from './runner.js';

@SpannerTable({ primaryKey: ['id'] })
class MyEntity implements VersionedEntity {
  constructor(data: Partial<MyEntity> = {}) {
    Object.assign(this, data);
  }

  @SpannerColumn()
  @IsString()
  readonly id!: string;

  @SpannerColumn()
  @IsDateType()
  readonly createdAt!: Date;

  @SpannerColumn()
  @IsDateType()
  readonly updatedAt!: Date;

  @SpannerColumn()
  @IsDateType()
  @IsNullable()
  readonly deletedAt!: Date | null;

  @SpannerColumn()
  @IsString()
  readonly value!: string;
}

const SPANNER_SCHEMA = [
  `CREATE TABLE MyEntity (
  id STRING(36) NOT NULL,
  createdAt TIMESTAMP NOT NULL,
  updatedAt TIMESTAMP NOT NULL,
  deletedAt TIMESTAMP,
  value STRING(MAX) NOT NULL,
) PRIMARY KEY (id)`,
];

class MyEvent implements Event {
  @IsUUID()
  readonly id!: string;

  @IsDateType()
  readonly producedAt!: Date;

  @IsString()
  readonly name!: string;

  @ValidateNestedType(() => MyEntity)
  readonly data!: MyEntity;
}

describe('SpannerPubSubTransactionRunner', () => {
  let logger: Logger;
  let database: Database;
  let pubSubFixture: PubSubFixture;
  let entityManager: SpannerEntityManager;
  let publisher: PubSubPublisher;
  let runner: SpannerPubSubTransactionRunner;
  let myEntityManager: VersionedEntityManager<SpannerTransaction, MyEvent>;

  beforeAll(async () => {
    logger = new Logger({});
    database = await createDatabase();
    const [operation] = await database.updateSchema(SPANNER_SCHEMA);
    await operation.promise();
    pubSubFixture = new PubSubFixture();
    const pubSubConf = await pubSubFixture.create('my.entity.v1', MyEvent);
    entityManager = new SpannerEntityManager(database);
    publisher = new PubSubPublisher(logger, {
      configurationGetter: (key) => pubSubConf[key],
    });
  });

  beforeEach(() => {
    runner = new SpannerPubSubTransactionRunner(
      entityManager,
      publisher,
      logger,
    );
    myEntityManager = new VersionedEntityManager(
      'my.entity.v1',
      MyEvent,
      MyEntity,
      runner,
    );
  });

  afterEach(async () => {
    pubSubFixture.clear();
    await entityManager.clear(MyEntity);
  });

  afterAll(async () => {
    await pubSubFixture.deleteAll();
    await database.delete();
  });

  it('should commit the transaction and publish the events', async () => {
    const [actualEvent] = await runner.run(async (transaction) => {
      expect(transaction.spannerTransaction).toBe(
        transaction.stateTransaction.transaction,
      );
      expect(transaction.entityManager).toBe(entityManager);

      return await myEntityManager.create(
        '🎉',
        { id: 'id', value: '🌠' },
        { transaction },
      );
    });

    expect(await entityManager.findOneByKey(MyEntity, 'id')).toEqual(
      actualEvent.data,
    );
    await pubSubFixture.expectMessageInTopic(
      'my.entity.v1',
      expect.objectContaining({ event: actualEvent }),
    );
  });

  it('should not publish the events when an error is thrown within the transaction', async () => {
    const actualPromise = runner.run(async (transaction) => {
      await myEntityManager.create(
        '🎉',
        { id: 'id', value: '🌠' },
        { transaction },
      );

      throw new Error('💥');
    });

    await expect(actualPromise).rejects.toThrow('💥');
    expect(await entityManager.findOneByKey(MyEntity, 'id')).toBeUndefined();
    await pubSubFixture.expectNoMessageInTopic('my.entity.v1');
  });

  it('should retry the transaction when a TransactionOldTimestampError is thrown', async () => {
    const futureDate = new Date(Date.now() + 1000);
    await entityManager.insert(
      new MyEntity({
        id: 'id',
        createdAt: futureDate,
        updatedAt: futureDate,
        deletedAt: null,
        value: '🌠',
      }),
    );

    let numCalls = 0;
    const [actualEvent] = await runner.run(async (transaction) => {
      numCalls += 1;
      return await myEntityManager.update(
        '📝',
        { id: 'id' },
        { value: '🌔' },
        { transaction },
      );
    });

    // The transaction should have been retried, and then should have succeeded with a more recent timestamp.
    expect(numCalls).toEqual(2);
    expect(actualEvent.producedAt.getTime()).toBeGreaterThan(
      futureDate.getTime(),
    );
    expect(await entityManager.findOneByKey(MyEntity, 'id')).toEqual(
      actualEvent.data,
    );
    await pubSubFixture.expectMessageInTopic(
      'my.entity.v1',
      expect.objectContaining({ event: actualEvent }),
    );
  });

  it('should not retry the transaction when a TransactionOldTimestampError is thrown with a delay that is too high', async () => {
    const futureDate = new Date(Date.now() + 60 * 1000);
    const existingEntity = new MyEntity({
      id: 'id',
      createdAt: futureDate,
      updatedAt: futureDate,
      deletedAt: null,
      value: '🌠',
    });
    await entityManager.insert(existingEntity);

    const actualPromise = runner.run((transaction) =>
      myEntityManager.update(
        '📝',
        { id: 'id' },
        { value: '🌔' },
        { transaction },
      ),
    );

    await expect(actualPromise).rejects.toThrow(TransactionOldTimestampError);
    expect(await entityManager.findOneByKey(MyEntity, 'id')).toEqual(
      existingEntity,
    );
    await pubSubFixture.expectNoMessageInTopic('my.entity.v1');
  });
});
