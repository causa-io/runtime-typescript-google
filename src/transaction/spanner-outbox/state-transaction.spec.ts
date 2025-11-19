import { EntityNotFoundError } from '@causa/runtime';
import { Database } from '@google-cloud/spanner';
import {
  SpannerColumn,
  SpannerEntityManager,
  SpannerTable,
} from '../../spanner/index.js';
import { createDatabase } from '../../testing.js';
import { SpannerStateTransaction } from './state-transaction.js';

@SpannerTable({ primaryKey: ['id1', 'id2'] })
class MyEntity {
  constructor(data: Partial<MyEntity> = {}) {
    Object.assign(this, data);
  }

  @SpannerColumn()
  readonly id1!: string;

  @SpannerColumn()
  readonly id2!: string;

  @SpannerColumn()
  readonly value!: string | null;

  @SpannerColumn({ softDelete: true })
  readonly deletedAt!: Date | null;
}

const SPANNER_SCHEMA = [
  `CREATE TABLE MyEntity (
  id1 STRING(36) NOT NULL,
  id2 STRING(36) NOT NULL,
  value STRING(MAX),
  deletedAt TIMESTAMP,
) PRIMARY KEY (id1, id2)`,
];

describe('SpannerStateTransaction', () => {
  let database: Database;
  let entityManager: SpannerEntityManager;

  beforeAll(async () => {
    database = await createDatabase();
    const [operation] = await database.updateSchema(SPANNER_SCHEMA);
    await operation.promise();

    entityManager = new SpannerEntityManager(database);
  });

  afterEach(async () => {
    await entityManager.clear(MyEntity);
  });

  afterAll(async () => {
    await database.delete();
  });

  describe('set', () => {
    it('should set the entity', async () => {
      const entity = new MyEntity({
        id1: 'id1',
        id2: 'id2',
        value: '🌠',
        deletedAt: new Date(),
      });
      await entityManager.insert(entity);

      await entityManager.transaction(async (spannerTransaction) => {
        const transaction = new SpannerStateTransaction(
          entityManager,
          spannerTransaction,
        );
        await transaction.set(new MyEntity({ id1: 'id1', id2: 'id2' }));
      });

      const actualEntity = await entityManager.findOneByKey(MyEntity, [
        'id1',
        'id2',
      ]);
      expect(actualEntity).toEqual({
        id1: 'id1',
        id2: 'id2',
        value: null,
        deletedAt: null,
      });
    });

    it('should use the transaction', async () => {
      const entity = new MyEntity({
        id1: 'id1',
        id2: 'id2',
        value: '🌠',
        deletedAt: null,
      });
      await entityManager.insert(entity);

      await database.runTransactionAsync(async (spannerTransaction) => {
        const transaction = new SpannerStateTransaction(
          entityManager,
          spannerTransaction,
        );
        await transaction.set(new MyEntity({ id1: 'id1', id2: 'id2' }));
        spannerTransaction.end();
      });

      const actualEntity = await entityManager.findOneByKey(MyEntity, [
        'id1',
        'id2',
      ]);
      expect(actualEntity).toEqual(entity);
    });
  });

  describe('delete', () => {
    it('should delete the entity', async () => {
      const entity = new MyEntity({ id1: 'id1', id2: 'id2', value: '🌠' });
      await entityManager.insert(entity);

      await entityManager.transaction(async (spannerTransaction) => {
        const transaction = new SpannerStateTransaction(
          entityManager,
          spannerTransaction,
        );
        await transaction.delete(MyEntity, { id1: 'id1', id2: 'id2' });
      });

      const actualEntity = await entityManager.findOneByKey(MyEntity, [
        'id1',
        'id2',
      ]);
      expect(actualEntity).toBeUndefined();
    });

    it('should delete the entity when provided with the entity', async () => {
      const entity = new MyEntity({ id1: 'id1', id2: 'id2', value: '🌠' });
      await entityManager.insert(entity);

      await entityManager.transaction(async (spannerTransaction) => {
        const transaction = new SpannerStateTransaction(
          entityManager,
          spannerTransaction,
        );
        await transaction.delete(entity);
      });

      const actualEntity = await entityManager.findOneByKey(MyEntity, [
        'id1',
        'id2',
      ]);
      expect(actualEntity).toBeUndefined();
    });

    it('should throw if the entity does not exist', async () => {
      const actualPromise = entityManager.transaction(
        async (spannerTransaction) => {
          const transaction = new SpannerStateTransaction(
            entityManager,
            spannerTransaction,
          );
          await transaction.delete(MyEntity, { id1: 'id1', id2: 'id2' });
        },
      );

      expect(actualPromise).rejects.toThrow(EntityNotFoundError);
    });

    it('should delete a soft deleted entity', async () => {
      const entity = new MyEntity({
        id1: 'id1',
        id2: 'id2',
        value: '🌠',
        deletedAt: new Date(),
      });
      await entityManager.insert(entity);

      await entityManager.transaction(async (spannerTransaction) => {
        const transaction = new SpannerStateTransaction(
          entityManager,
          spannerTransaction,
        );
        await transaction.delete(MyEntity, { id1: 'id1', id2: 'id2' });
      });

      const actualEntity = await entityManager.findOneByKey(MyEntity, [
        'id1',
        'id2',
      ]);
      expect(actualEntity).toBeUndefined();
    });

    it('should use the transaction', async () => {
      const entity = new MyEntity({
        id1: 'id1',
        id2: 'id2',
        value: '🌠',
        deletedAt: null,
      });
      await entityManager.insert(entity);

      await database.runTransactionAsync(async (spannerTransaction) => {
        const transaction = new SpannerStateTransaction(
          entityManager,
          spannerTransaction,
        );
        await transaction.delete(MyEntity, { id1: 'id1', id2: 'id2' });
        await spannerTransaction.rollback();
      });

      const actualEntity = await entityManager.findOneByKey(MyEntity, [
        'id1',
        'id2',
      ]);
      expect(actualEntity).toEqual(entity);
    });
  });
});
