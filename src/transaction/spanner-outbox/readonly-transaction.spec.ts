import { PreciseDate } from '@google-cloud/precise-date';
import { Database } from '@google-cloud/spanner';
import { setTimeout } from 'timers/promises';
import {
  SpannerColumn,
  SpannerEntityManager,
  SpannerTable,
} from '../../spanner/index.js';
import { createDatabase } from '../../testing.js';
import { SpannerReadOnlyStateTransaction } from './readonly-transaction.js';

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

describe('SpannerReadOnlyStateTransaction', () => {
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

  describe('get', () => {
    it('should find the entity', async () => {
      const entity = new MyEntity({
        id1: 'id1',
        id2: 'id2',
        value: '🌠',
        deletedAt: null,
      });
      await entityManager.insert(entity);

      const actualEntity = await entityManager.snapshot(
        async (spannerTransaction) => {
          const transaction = new SpannerReadOnlyStateTransaction(
            entityManager,
            spannerTransaction,
          );
          return await transaction.get(MyEntity, { id1: 'id1', id2: 'id2' });
        },
      );

      expect(actualEntity).toEqual(entity);
    });

    it('should return undefined if the entity does not exist', async () => {
      const actualEntity = await entityManager.snapshot(
        async (spannerTransaction) => {
          const transaction = new SpannerReadOnlyStateTransaction(
            entityManager,
            spannerTransaction,
          );
          return await transaction.get(MyEntity, { id1: 'id1', id2: 'id2' });
        },
      );

      expect(actualEntity).toBeUndefined();
    });

    it('should return a soft deleted entity', async () => {
      const entity = new MyEntity({
        id1: 'id1',
        id2: 'id2',
        value: '🌠',
        deletedAt: new Date(),
      });
      await entityManager.insert(entity);

      const actualEntity = await entityManager.snapshot(
        async (spannerTransaction) => {
          const transaction = new SpannerReadOnlyStateTransaction(
            entityManager,
            spannerTransaction,
          );
          return await transaction.get(MyEntity, { id1: 'id1', id2: 'id2' });
        },
      );

      expect(actualEntity).toEqual(entity);
    });

    it('should use the transaction', async () => {
      // Ensures `beforeInsertDate` is after the database creation...
      const beforeInsertDate = new PreciseDate();
      // ...but is in the distant past relative to the insert.
      await setTimeout(100);

      const entity = new MyEntity({ id1: '🕰️', id2: '🔮', value: '🌠' });
      await entityManager.insert(entity);

      const actualEntity = await entityManager.snapshot(
        // Making it look like the entity does not exist.
        { timestampBounds: { readTimestamp: beforeInsertDate } },
        async (snapshot) => {
          const transaction = new SpannerReadOnlyStateTransaction(
            entityManager,
            snapshot,
          );
          return await transaction.get(MyEntity, { id1: '🕰️', id2: '🔮' });
        },
      );

      expect(actualEntity).toBeUndefined();
    });
  });
});
