import { EntityAlreadyExistsError, EntityNotFoundError } from '@causa/runtime';
import { PreciseDate } from '@google-cloud/precise-date';
import { Database, Snapshot, Transaction } from '@google-cloud/spanner';
import { jest } from '@jest/globals';
import { grpc } from 'google-gax';
import 'jest-extended';
import * as uuid from 'uuid';
import { SpannerColumn } from './column.decorator.js';
import {
  SpannerEntityManager,
  SpannerReadOnlyTransaction,
} from './entity-manager.js';
import {
  EntityMissingPrimaryKeyError,
  InvalidArgumentError,
  InvalidEntityDefinitionError,
  InvalidQueryError,
  TemporarySpannerError,
  TransactionFinishedError,
} from './errors.js';
import { SpannerTable } from './table.decorator.js';
import { createDatabase } from './testing.js';

@SpannerTable({ name: 'MyEntity', primaryKey: ['id'] })
class SomeEntity {
  constructor(data: Partial<SomeEntity> = {}) {
    Object.assign(this, data);
  }

  @SpannerColumn()
  id!: string;

  @SpannerColumn()
  value!: string;
}

@SpannerTable({ primaryKey: ['id'] })
class IntEntity {
  constructor(data: Partial<IntEntity> = {}) {
    Object.assign(this, data);
  }

  @SpannerColumn()
  id!: number;

  @SpannerColumn({ isBigInt: true })
  value!: bigint;
}

@SpannerTable({ primaryKey: ['id'] })
class IndexedEntity {
  constructor(data: Partial<IndexedEntity> = {}) {
    Object.assign(this, data);
  }

  @SpannerColumn()
  id!: string;

  @SpannerColumn({ isInt: true })
  value!: number;

  @SpannerColumn()
  otherValue!: string;

  @SpannerColumn()
  notStored!: string | null;

  static readonly ByValue = 'IndexedEntitiesByValue';
}

const TEST_SCHEMA = [
  `CREATE TABLE MyEntity (
    id STRING(MAX) NOT NULL,
    value STRING(MAX) NOT NULL
  ) PRIMARY KEY (id)`,
  `CREATE TABLE IntEntity (
    id STRING(MAX) NOT NULL,
    value INT64 NOT NULL
  ) PRIMARY KEY (id)`,
  `CREATE TABLE IndexedEntity (
    id STRING(MAX) NOT NULL,
    value INT64 NOT NULL,
    otherValue STRING(MAX) NOT NULL,
    notStored STRING(MAX)
  ) PRIMARY KEY (id)`,
  `CREATE INDEX IndexedEntitiesByValue ON IndexedEntity(value) STORING (otherValue)`,
];

describe('SpannerEntityManager', () => {
  let database: Database;
  let manager: SpannerEntityManager;

  beforeAll(async () => {
    database = await createDatabase();

    const [operation] = await database.updateSchema(TEST_SCHEMA);
    await operation.promise();
  });

  beforeEach(() => {
    manager = new SpannerEntityManager(database);
  });

  afterEach(async () => {
    await manager.transaction(async (transaction) => {
      await manager.clear(SomeEntity, { transaction });
      await manager.clear(IntEntity, { transaction });
      await manager.clear(IndexedEntity, { transaction });
    });
  });

  afterAll(async () => {
    await database.delete();
  });

  describe('transaction', () => {
    it('should return a transaction that can be used to perform read and writes', async () => {
      const expectedReturnValue = { value: '✅' };
      const expectedEntities = [
        { id: '1', value: '🎁' },
        { id: '2', value: '🎉' },
      ];

      const actualReturnValue = await manager.transaction(
        async (transaction) => {
          transaction.insert('MyEntity', expectedEntities);
          return expectedReturnValue;
        },
      );

      const [actualRows] = await database.table('MyEntity').read({
        keys: ['1', '2'],
        columns: ['id', 'value'],
        json: true,
      });
      expect(actualRows).toEqual(expectedEntities);
      expect(actualReturnValue).toEqual(expectedReturnValue);
    });

    it('should catch and rethrow an already exists error', async () => {
      const actualPromise = manager.transaction(async (transaction) => {
        const duplicate = { id: '1', value: 'value' };
        transaction.insert('MyEntity', duplicate);
        transaction.insert('MyEntity', duplicate);
      });

      await expect(actualPromise).rejects.toThrow(EntityAlreadyExistsError);
    });

    it('should catch and rethrow a temporary Spanner error due to a deadline exceeded gRPC status', async () => {
      const actualPromise = manager.transaction(async () => {
        const error = new Error('⌛');
        (error as any).code = grpc.status.DEADLINE_EXCEEDED;
        throw error;
      });

      await expect(actualPromise).rejects.toThrow(TemporarySpannerError);
    });

    it('should catch and rethrow a temporary Spanner error thrown by runTransactionAsync itself', async () => {
      const error = new Error('⌛');
      (error as any).code = grpc.status.DEADLINE_EXCEEDED;
      jest.spyOn(database, 'runTransactionAsync').mockRejectedValueOnce(error);

      const actualPromise = manager.transaction(async () => {
        // No-op.
      });

      await expect(actualPromise).rejects.toThrow(TemporarySpannerError);
    });

    it('should catch and rethrow a temporary Spanner error due to a session acquisition timeout', async () => {
      const actualPromise = manager.transaction(async () => {
        throw new Error('Timeout occurred while acquiring session.');
      });

      await expect(actualPromise).rejects.toThrow(TemporarySpannerError);
    });

    it('should rethrow unknown errors and rollback', async () => {
      let actualTransaction!: Transaction;
      const actualPromise = manager.transaction(async (transaction) => {
        actualTransaction = transaction;

        // This ensures the transaction is started and should be rolled back.
        await transaction.read('MyEntity', { keys: ['1'], columns: ['id'] });

        const notInserted = { id: '12', value: 'value' };
        transaction.insert('MyEntity', notInserted);

        throw new Error('💥');
      });

      await expect(actualPromise).rejects.toThrow('💥');
      const [actualRows] = await database
        .table('MyEntity')
        .read({ keys: ['12'], columns: ['id'], json: true });
      expect(actualRows).toBeEmpty();
      expect(actualTransaction.ended).toBeTrue();
    });

    it('should rethrow unknown errors and end the transaction', async () => {
      let actualTransaction!: Transaction;
      const actualPromise = manager.transaction(async (transaction) => {
        actualTransaction = transaction;

        // When no read is made, the transaction is not started and should be "ended" rather than rolled back.

        const notInserted = { id: '12', value: 'value' };
        transaction.insert('MyEntity', notInserted);

        throw new Error('💥');
      });

      await expect(actualPromise).rejects.toThrow('💥');
      const [actualRows] = await database
        .table('MyEntity')
        .read({ keys: ['12'], columns: ['id'], json: true });
      expect(actualRows).toBeEmpty();
      expect(actualTransaction.ended).toBeTrue();
    });

    it('should throw if the transaction is ended by the provided function', async () => {
      const actualPromise = manager.transaction(async (transaction) => {
        const row = { id: '1', value: 'value' };
        transaction.insert('MyEntity', row);
        await transaction.commit();
      });

      await expect(actualPromise).rejects.toThrow(TransactionFinishedError);
    });
  });

  describe('snapshot', () => {
    it('should return a snapshot that can be used to perform reads', async () => {
      await database.table('MyEntity').insert({ id: '5', value: '❄️' });

      const result = await manager.snapshot(async (snapshot) => {
        const [rows] = await snapshot.read('MyEntity', {
          keys: ['5'],
          columns: ['id', 'value'],
          json: true,
        });
        return rows[0];
      });

      expect(result).toEqual({ id: '5', value: '❄️' });
    });

    it('should catch and rethrow Spanner errors', async () => {
      let actualSnapshot!: Snapshot;
      const actualPromise = manager.snapshot(async (snapshot) => {
        actualSnapshot = snapshot;
        await snapshot.read('nope', { columns: ['nope'], keys: ['nope'] });
      });

      await expect(actualPromise).rejects.toThrow(InvalidQueryError);
      expect(actualSnapshot.ended).toBeTrue();
    }, 10000);

    it('should rethrow unknown errors', async () => {
      let actualSnapshot!: Snapshot;
      const actualPromise = manager.snapshot(async (snapshot) => {
        actualSnapshot = snapshot;
        throw new Error('💣');
      });

      await expect(actualPromise).rejects.toThrow('💣');
      expect(actualSnapshot.ended).toBeTrue();
    });

    it('should catch and rethrow an error thrown by getSnapshot itself', async () => {
      const error = new Error('💤');
      (error as any).code = grpc.status.DEADLINE_EXCEEDED;
      jest.spyOn(database as any, 'getSnapshot').mockRejectedValueOnce(error);

      const actualPromise = manager.snapshot(async () => {
        // No-op.
      });

      await expect(actualPromise).rejects.toThrow(TemporarySpannerError);
    });

    it('should accept the timestamp options', async () => {
      await database.table('MyEntity').insert({ id: '6', value: '🔮' });
      const getSnapshotSpy = jest.spyOn(database, 'getSnapshot');
      // A timestamp in the future ensures the previous write/insert is read.
      const readTimestamp = new PreciseDate(new Date().getTime() + 2000);

      const result = await manager.snapshot(
        { timestampBounds: { readTimestamp } },
        async (snapshot) => {
          const [rows] = await snapshot.read('MyEntity', {
            keys: ['6'],
            columns: ['id', 'value'],
            json: true,
          });
          return rows[0];
        },
      );

      expect(result).toEqual({ id: '6', value: '🔮' });
      expect(getSnapshotSpy).toHaveBeenCalledExactlyOnceWith({ readTimestamp });
    });
  });

  describe('runInExistingOrNewTransaction', () => {
    it('should run the provided function in the provided transaction', async () => {
      const expectedReturnValue = { value: '✅' };

      const actualReturnValue = await database.runTransactionAsync(
        async (transaction) => {
          const returnValue = await manager.runInExistingOrNewTransaction(
            transaction,
            async (transaction) => {
              transaction.insert('MyEntity', { id: '1', value: '🎁' });
              return expectedReturnValue;
            },
          );

          transaction.end();

          return returnValue;
        },
      );

      expect(actualReturnValue).toEqual(expectedReturnValue);
      const [actualRows] = await database
        .table('MyEntity')
        .read({ keys: ['1'], columns: ['id'] });
      expect(actualRows).toBeEmpty();
    });

    it('should run the provided function in a new transaction if none is provided', async () => {
      const expectedReturnValue = { value: '✅' };

      const actualReturnValue = await manager.runInExistingOrNewTransaction(
        undefined,
        async (transaction) => {
          transaction.insert('MyEntity', { id: '1', value: '🎁' });
          return expectedReturnValue;
        },
      );

      expect(actualReturnValue).toEqual(expectedReturnValue);
      const [actualRows] = await database
        .table('MyEntity')
        .read({ keys: ['1'], columns: ['id'], json: true });
      expect(actualRows).toEqual([{ id: '1' }]);
    });

    it('should convert a Spanner error', async () => {
      let actualPromise!: Promise<void>;
      await database.runTransactionAsync(async (transaction) => {
        actualPromise = manager.runInExistingOrNewTransaction(
          transaction,
          async () => {
            const error = new Error('⌛');
            (error as any).code = grpc.status.DEADLINE_EXCEEDED;
            throw error;
          },
        );

        await actualPromise.catch(() => {
          // Ignore the error, just to finish the transaction.
        });

        transaction.end();
      });

      await expect(actualPromise).rejects.toThrow(TemporarySpannerError);
    });
  });

  describe('runInExistingOrNewReadOnlyTransaction', () => {
    it('should run the provided function in the provided transaction', async () => {
      const expectedReturnValue = { value: '✅' };

      let expectedTransaction!: Transaction;
      let actualTransaction!: SpannerReadOnlyTransaction;
      const actualReturnValue = await database.runTransactionAsync(
        async (transaction) => {
          expectedTransaction = transaction;

          return await manager.runInExistingOrNewReadOnlyTransaction(
            transaction,
            async (transaction) => {
              actualTransaction = transaction;
              return expectedReturnValue;
            },
          );
        },
      );

      expect(actualTransaction).toBe(expectedTransaction);
      expect(actualReturnValue).toEqual(expectedReturnValue);
    });

    it('should run the provided function in a new snapshot if none is provided', async () => {
      const expectedReturnValue = { value: '✅' };

      let actualTransaction!: SpannerReadOnlyTransaction;
      const actualReturnValue =
        await manager.runInExistingOrNewReadOnlyTransaction(
          undefined,
          async (transaction) => {
            actualTransaction = transaction;
            return expectedReturnValue;
          },
        );

      expect(actualTransaction).toBeInstanceOf(Snapshot);
      expect(actualTransaction.ended).toBeTrue();
      expect(actualReturnValue).toEqual(expectedReturnValue);
    });

    it('should convert a Spanner error', async () => {
      const [snapshot] = await database.getSnapshot();

      const actualPromise = manager.runInExistingOrNewReadOnlyTransaction(
        snapshot,
        async () => {
          const error = new Error('⌛');
          (error as any).code = grpc.status.DEADLINE_EXCEEDED;
          throw error;
        },
      );
      await actualPromise.catch(() => {
        // Ignore the error, just to make sure the snapshot can be ended.
      });
      snapshot.end();

      await expect(actualPromise).rejects.toThrow(TemporarySpannerError);
    });
  });

  describe('clear', () => {
    it('should remove all rows', async () => {
      await database.table('MyEntity').insert([
        { id: '1', value: '🎁' },
        { id: '2', value: '🎉' },
      ]);

      await manager.clear(SomeEntity);

      const [actualRows] = await database.table('MyEntity').read({
        ranges: [{ startClosed: [], endClosed: [] }],
        limit: 1,
        columns: ['id'],
      });
      expect(actualRows).toBeEmpty();
    });

    it('should use the provided transaction', async () => {
      await database.table('MyEntity').insert([
        { id: '1', value: '🎁' },
        { id: '2', value: '🎉' },
      ]);

      await database.runTransactionAsync(async (transaction) => {
        await manager.clear(SomeEntity, { transaction });
        await transaction.rollback();
      });

      const [actualRows] = await database.table('MyEntity').read({
        ranges: [{ startClosed: [], endClosed: [] }],
        limit: 3,
        columns: ['id'],
      });
      expect(actualRows).toHaveLength(2);
    });
  });

  describe('query', () => {
    it('should run the query and return the typed entities', async () => {
      await database.table('MyEntity').insert([
        { id: '1', value: '🎁' },
        { id: '2', value: '🎉' },
      ]);

      const actualEntities = await manager.query(
        { entityType: SomeEntity },
        { sql: 'SELECT id, value FROM MyEntity ORDER BY id' },
      );

      expect(actualEntities).toEqual([
        { id: '1', value: '🎁' },
        { id: '2', value: '🎉' },
      ]);
      expect(actualEntities[0]).toBeInstanceOf(SomeEntity);
      expect(actualEntities[1]).toBeInstanceOf(SomeEntity);
    });

    it('should wrap numbers when converting results to class instances', async () => {
      const value = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
      await database
        .table('IntEntity')
        .insert([{ id: '1', value: value.toString() }]);

      const actualEntities = await manager.query(
        { entityType: IntEntity },
        { sql: 'SELECT id, value FROM IntEntity' },
      );

      expect(actualEntities).toEqual([{ id: '1', value }]);
      expect(actualEntities[0]).toBeInstanceOf(IntEntity);
    });

    it('should run the query without typed entities', async () => {
      await database.table('IntEntity').insert([{ id: '1', value: 10 }]);

      const actualEntities = await manager.query({
        sql: 'SELECT id, value FROM IntEntity',
      });

      expect(actualEntities).toEqual([{ id: '1', value: 10 }]);
      expect(actualEntities[0]).not.toBeInstanceOf(IntEntity);
    });

    it('should use a read only transaction by default', async () => {
      const actualPromise = manager.query({
        sql: `INSERT INTO IntEntity(id, value) VALUES ('1', 10)`,
      });

      await expect(actualPromise).rejects.toThrow(InvalidArgumentError);
    }, 10000);

    it('should use the provided transaction', async () => {
      await manager.transaction(async (transaction) => {
        await manager.query(
          { transaction },
          { sql: `INSERT INTO IntEntity(id, value) VALUES ('1', 10)` },
        );
      });

      const [actualRows] = await database.table('IntEntity').read({
        keys: ['1'],
        columns: ['id', 'value'],
        json: true,
      });
      expect(actualRows).toEqual([{ id: '1', value: 10 }]);
    });
  });

  describe('getPrimaryKey', () => {
    it('should throw when the object has no type and the entity type is not provided', () => {
      expect(() => manager.getPrimaryKey({})).toThrow(
        InvalidEntityDefinitionError,
      );
    });

    it('should default to the object constructor when the entity type is not provided', () => {
      const obj = new SomeEntity({ id: '1' });

      const actualPrimaryKey = manager.getPrimaryKey(obj);

      expect(actualPrimaryKey).toEqual(['1']);
    });

    it('should return a composite primary key as a list of strings', () => {
      @SpannerTable({ primaryKey: ['first', 'second', 'third', 'fourth'] })
      class MyWeirdEntity {
        @SpannerColumn({ isBigInt: true })
        first!: bigint;
        @SpannerColumn()
        second!: string | null;
        @SpannerColumn()
        third!: string;
        @SpannerColumn()
        fourth!: Date;
      }
      const expectedDate = new Date();

      const actualPrimaryKey = manager.getPrimaryKey(
        { first: 1n, second: null, third: '3', fourth: expectedDate },
        MyWeirdEntity,
      );

      expect(actualPrimaryKey).toEqual(['1', null, '3', expectedDate.toJSON()]);
    });

    it('should throw when a primary key column is missing', () => {
      const obj = new SomeEntity({ value: 'value' });

      expect(() => manager.getPrimaryKey(obj)).toThrow(
        EntityMissingPrimaryKeyError,
      );
    });
  });

  describe('findOneByKey', () => {
    it('should return undefined when the entity does not exist', async () => {
      const actualEntity = await manager.findOneByKey(SomeEntity, '1');

      expect(actualEntity).toBeUndefined();
    });

    it('should return the entity when it exists', async () => {
      await database.table('MyEntity').insert({ id: '1', value: '🎁' });

      const actualEntity = await manager.findOneByKey(SomeEntity, '1');

      expect(actualEntity).toEqual({ id: '1', value: '🎁' });
      expect(actualEntity).toBeInstanceOf(SomeEntity);
    });

    it('should only return the specified columns', async () => {
      await database.table('MyEntity').insert({ id: '1', value: '🎁' });

      const actualEntity = await manager.findOneByKey(SomeEntity, '1', {
        columns: ['value'],
      });

      expect(actualEntity).toEqual({ value: '🎁' });
      expect(actualEntity).toBeInstanceOf(SomeEntity);
    });

    it('should look up the entity using the provided index', async () => {
      await database
        .table('IndexedEntity')
        .insert({ id: '1', value: 10, otherValue: '🎁' });

      const actualEntity = await manager.findOneByKey(IndexedEntity, ['10'], {
        index: IndexedEntity.ByValue,
        columns: ['id', 'otherValue'],
      });

      expect(actualEntity).toEqual({ id: '1', otherValue: '🎁' });
      expect(actualEntity).toBeInstanceOf(IndexedEntity);
    });

    it('should use the provided transaction', async () => {
      const id = uuid.v4();
      await database
        .table('IndexedEntity')
        .insert({ id, value: 10, otherValue: '🎁' });

      // Uses a snapshot reading at a past timestamp to make it look like the row does not exist.
      const actualEntity = await manager.snapshot(
        { timestampBounds: { exactStaleness: { seconds: 1 } } },
        async (transaction) =>
          manager.findOneByKey(IndexedEntity, id, { transaction }),
      );

      expect(actualEntity).toBeUndefined();
    });

    it('should fetch the entire entity when using an index and columns are not specified', async () => {
      await database
        .table('IndexedEntity')
        .insert({ id: '1', value: 10, otherValue: '🎁', notStored: '🙈' });

      const actualEntity = await manager.findOneByKey(IndexedEntity, ['10'], {
        index: IndexedEntity.ByValue,
      });

      expect(actualEntity).toEqual({
        id: '1',
        value: 10,
        otherValue: '🎁',
        notStored: '🙈',
      });
      expect(actualEntity).toBeInstanceOf(IndexedEntity);
    });
  });

  describe('findOneByKeyOrFail', () => {
    it('should throw when the entity does not exist', async () => {
      const actualPromise = manager.findOneByKeyOrFail(SomeEntity, '1');

      await expect(actualPromise).rejects.toThrow(EntityNotFoundError);
      await expect(actualPromise).rejects.toMatchObject({
        entityType: SomeEntity,
        key: '1',
      });
    });

    it('should return the entity when it exists', async () => {
      await database.table('MyEntity').insert({ id: '1', value: '🎁' });

      const actualEntity = await manager.findOneByKeyOrFail(SomeEntity, '1');

      expect(actualEntity).toEqual({ id: '1', value: '🎁' });
      expect(actualEntity).toBeInstanceOf(SomeEntity);
    });
  });

  describe('insert', () => {
    it('should insert the entity', async () => {
      const entityToInsert = new SomeEntity({ id: '1', value: '🎁' });

      await manager.insert(entityToInsert);

      const [actualRows] = await database.table('MyEntity').read({
        keys: ['1'],
        columns: ['id', 'value'],
        json: true,
      });
      expect(actualRows).toEqual([{ id: '1', value: '🎁' }]);
    });

    it('should throw if the entity already exists', async () => {
      await database.table('MyEntity').insert({ id: '1', value: '💥' });

      const actualPromise = manager.insert(
        new SomeEntity({ id: '1', value: '🙅' }),
      );

      await expect(actualPromise).rejects.toThrow(EntityAlreadyExistsError);
    });

    it('should use the provided transaction', async () => {
      const entityToInsert = new SomeEntity({ id: '1', value: '🎁' });

      await database.runTransactionAsync(async (transaction) => {
        await manager.insert(entityToInsert, { transaction });
        transaction.end();
      });

      const [actualRows] = await database.table('MyEntity').read({
        keys: ['1'],
        columns: ['id'],
      });
      expect(actualRows).toBeEmpty();
    });
  });

  describe('delete', () => {
    it('should delete the entity', async () => {
      await database.table('MyEntity').insert({ id: '1', value: '🎁' });

      const actualEntity = await manager.delete(SomeEntity, '1');

      expect(actualEntity).toEqual({ id: '1', value: '🎁' });
      const [actualRows] = await database.table('MyEntity').read({
        keys: ['1'],
        columns: ['id'],
        limit: 1,
      });
      expect(actualRows).toBeEmpty();
    });

    it('should throw if the entity does not exist', async () => {
      const actualPromise = manager.delete(SomeEntity, '1');

      await expect(actualPromise).rejects.toThrow(EntityNotFoundError);
      await expect(actualPromise).rejects.toMatchObject({
        entityType: SomeEntity,
        key: ['1'],
      });
    });

    it('should use the provided transaction', async () => {
      await database.table('MyEntity').insert({ id: '1', value: '🎁' });

      await database.runTransactionAsync(async (transaction) => {
        await manager.delete(SomeEntity, '1', { transaction });
        await transaction.rollback();
      });

      const [actualRows] = await database.table('MyEntity').read({
        keys: ['1'],
        columns: ['id'],
        limit: 1,
      });
      expect(actualRows).not.toBeEmpty();
    });

    it('should throw an error thrown by the validation function', async () => {
      await database.table('MyEntity').insert({ id: '1', value: '🎁' });
      const fn = jest.fn(() => {
        throw new Error('💥');
      });

      const actualPromise = manager.delete(SomeEntity, '1', { validateFn: fn });

      await expect(actualPromise).rejects.toThrow('💥');
      expect(fn).toHaveBeenCalledExactlyOnceWith({ id: '1', value: '🎁' });
      const [actualRows] = await database.table('MyEntity').read({
        keys: ['1'],
        columns: ['id'],
        limit: 1,
        json: true,
      });
      expect(actualRows).toEqual([{ id: '1' }]);
    });
  });
});