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

class ChildEntity {
  constructor(data: Partial<ChildEntity> = {}) {
    Object.assign(this, data);
  }

  @SpannerColumn({ isJson: true })
  someJson!: any | null;

  @SpannerColumn({ name: 'otherValue' })
  other!: string | null;
}

@SpannerTable({ primaryKey: ['id'] })
class ParentEntity {
  constructor(data: Partial<ParentEntity> = {}) {
    Object.assign(this, data);
  }

  @SpannerColumn()
  id!: string;

  @SpannerColumn({ nestedType: ChildEntity })
  child!: ChildEntity | null;
}

@SpannerTable({ primaryKey: ['id'] })
class SoftDeleteEntity {
  constructor(data: Partial<SoftDeleteEntity> = {}) {
    Object.assign(this, data);
  }

  @SpannerColumn()
  id!: string;

  @SpannerColumn({ softDelete: true })
  deletedAt!: Date | null;
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
  `CREATE TABLE ParentEntity (
    id STRING(MAX) NOT NULL,
    child_someJson JSON,
    child_otherValue STRING(MAX)
  ) PRIMARY KEY (id)`,
  `CREATE TABLE SoftDeleteEntity (
    id STRING(MAX) NOT NULL,
    deletedAt TIMESTAMP
  ) PRIMARY KEY (id)`,
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
      await manager.clear(ParentEntity, { transaction });
      await manager.clear(SoftDeleteEntity, { transaction });
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

    it('should return undefined when the entity is soft deleted', async () => {
      await database
        .table('SoftDeleteEntity')
        .insert({ id: '1', deletedAt: new Date() });

      const actualEntity = await manager.findOneByKey(SoftDeleteEntity, '1');

      expect(actualEntity).toBeUndefined();
    });

    it('should return the entity when the entity is soft deleted and soft deletes are included', async () => {
      const expectedEntity = new SoftDeleteEntity({
        id: '1',
        deletedAt: new Date(),
      });
      await database.table('SoftDeleteEntity').insert(expectedEntity);

      const actualEntity = await manager.findOneByKey(SoftDeleteEntity, '1', {
        includeSoftDeletes: true,
      });

      expect(actualEntity).toEqual(expectedEntity);
      expect(actualEntity).toBeInstanceOf(SoftDeleteEntity);
    });

    it('should only return the specified columns', async () => {
      await database.table('MyEntity').insert({ id: '1', value: '🎁' });

      const actualEntity = await manager.findOneByKey(SomeEntity, '1', {
        columns: ['value'],
      });

      expect(actualEntity).toEqual({ value: '🎁' });
      expect(actualEntity).toBeInstanceOf(SomeEntity);
    });

    it('should throw the the specified columns do not include the soft delete column', async () => {
      await database
        .table('SoftDeleteEntity')
        .insert({ id: '1', deletedAt: new Date() });

      const actualPromise = manager.findOneByKey(SoftDeleteEntity, '1', {
        columns: ['id'],
      });

      await expect(actualPromise).rejects.toThrow(InvalidArgumentError);
    });

    it('should return the columns for the entity when it is soft deleted and soft deletes are included', async () => {
      await database.table('SoftDeleteEntity').insert({
        id: '1',
        deletedAt: new Date(),
      });

      const actualEntity = await manager.findOneByKey(SoftDeleteEntity, '1', {
        includeSoftDeletes: true,
        columns: ['id'],
      });

      expect(actualEntity).toEqual({ id: '1' });
      expect(actualEntity).toBeInstanceOf(SoftDeleteEntity);
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

  describe('replace', () => {
    it('should insert the entity', async () => {
      const entityToInsert = new SomeEntity({ id: '1', value: '🎁' });

      await manager.replace(entityToInsert);

      const [actualRows] = await database.table('MyEntity').read({
        keys: ['1'],
        columns: ['id', 'value'],
        json: true,
      });
      expect(actualRows).toEqual([{ id: '1', value: '🎁' }]);
    });

    it('should replace an existing entity', async () => {
      const expectedEntity = new IndexedEntity({
        id: '1',
        value: 10,
        otherValue: '🎁',
      });
      await database
        .table('IndexedEntity')
        .insert({ id: '1', value: 9, otherValue: '🎉', notStored: '🙈' });

      await manager.replace(expectedEntity);

      const [actualRows] = await database.table('IndexedEntity').read({
        keys: ['1'],
        columns: ['id', 'value', 'otherValue', 'notStored'],
        json: true,
      });
      expect(actualRows).toEqual([{ ...expectedEntity, notStored: null }]);
    });

    it('should use the provided transaction', async () => {
      const expectedEntity = {
        id: '1',
        value: 9,
        otherValue: '🎉',
        notStored: '🙈',
      };
      await database.table('IndexedEntity').insert(expectedEntity);

      await database.runTransactionAsync(async (transaction) => {
        await manager.replace(
          new IndexedEntity({ id: '1', value: 10, otherValue: '📝' }),
          { transaction },
        );
        transaction.end();
      });

      const [actualRows] = await database.table('IndexedEntity').read({
        keys: ['1'],
        columns: ['id', 'value', 'otherValue', 'notStored'],
        json: true,
      });
      expect(actualRows).toEqual([expectedEntity]);
    });
  });

  describe('update', () => {
    it('should update the entity', async () => {
      await database.table('IndexedEntity').insert({
        id: '1',
        value: 10,
        otherValue: '🎁',
        notStored: '🙈',
      });

      const actualEntity = await manager.update(IndexedEntity, {
        id: '1',
        otherValue: '🎉',
      });

      expect(actualEntity).toEqual({
        id: '1',
        value: 10,
        otherValue: '🎉',
        notStored: '🙈',
      });
      expect(actualEntity).toBeInstanceOf(IndexedEntity);
      const [actualRows] = await database.table('IndexedEntity').read({
        keys: ['1'],
        columns: ['id', 'value', 'otherValue', 'notStored'],
        json: true,
      });
      expect(actualRows).toEqual([actualEntity]);
    });

    it('should throw if the entity does not exist', async () => {
      const actualPromise = manager.update(IndexedEntity, {
        id: '1',
        otherValue: '🎉',
      });

      await expect(actualPromise).rejects.toThrow(EntityNotFoundError);
      await expect(actualPromise).rejects.toMatchObject({
        entityType: IndexedEntity,
        key: ['1'],
      });
    });

    it('should throw if the entity is soft deleted', async () => {
      await database
        .table('SoftDeleteEntity')
        .insert({ id: '1', deletedAt: new Date() });

      const actualPromise = manager.update(SoftDeleteEntity, {
        id: '1',
        deletedAt: new Date(),
      });

      await expect(actualPromise).rejects.toThrow(EntityNotFoundError);
    });

    it('should update the entity when the entity is soft deleted and soft deletes are included', async () => {
      await database.table('SoftDeleteEntity').insert({
        id: '1',
        deletedAt: new Date('2000-01-01'),
      });
      const expectedDeletedAt = new Date('2999-01-01');

      const actualEntity = await manager.update(
        SoftDeleteEntity,
        { id: '1', deletedAt: expectedDeletedAt },
        { includeSoftDeletes: true },
      );

      expect(actualEntity).toEqual({ id: '1', deletedAt: expectedDeletedAt });
      expect(actualEntity).toBeInstanceOf(SoftDeleteEntity);
      const [actualRows] = await database.table('SoftDeleteEntity').read({
        keys: ['1'],
        columns: ['id', 'deletedAt'],
        json: true,
      });
      expect(actualRows).toEqual([actualEntity]);
    });

    it('should use the provided transaction', async () => {
      const expectedEntity = {
        id: '1',
        value: 10,
        otherValue: '🎁',
        notStored: '🙈',
      };
      await database.table('IndexedEntity').insert(expectedEntity);

      await database.runTransactionAsync(async (transaction) => {
        await manager.update(
          IndexedEntity,
          { id: '1', otherValue: '🎉' },
          { transaction },
        );
        await transaction.rollback();
      });

      const [actualRows] = await database.table('IndexedEntity').read({
        keys: ['1'],
        columns: ['id', 'value', 'otherValue', 'notStored'],
        json: true,
      });
      expect(actualRows).toEqual([expectedEntity]);
    });

    it('should throw an error thrown by the validation function', async () => {
      const expectedEntity = {
        id: '1',
        value: 10,
        otherValue: '🎁',
        notStored: '🙈',
      };
      await database.table('IndexedEntity').insert(expectedEntity);
      const fn: jest.Mock<(entity: IndexedEntity) => void> = jest.fn(() => {
        throw new Error('💥');
      });

      const actualPromise = manager.update(
        IndexedEntity,
        { id: '1', otherValue: '🎉' },
        { validateFn: fn },
      );

      await expect(actualPromise).rejects.toThrow('💥');
      expect(fn).toHaveBeenCalledExactlyOnceWith({
        id: '1',
        value: 10,
        otherValue: '🎁',
        notStored: '🙈',
      });
      expect(fn.mock.calls[0][0]).toBeInstanceOf(IndexedEntity);
      const [actualRows] = await database.table('IndexedEntity').read({
        keys: ['1'],
        columns: ['id', 'value', 'otherValue', 'notStored'],
        json: true,
      });
      expect(actualRows).toEqual([expectedEntity]);
    });

    it('should throw an error when the update does not contain the primary key', async () => {
      const actualPromise = manager.update(IndexedEntity, {
        otherValue: '🎉',
      });

      await expect(actualPromise).rejects.toThrow(EntityMissingPrimaryKeyError);
    });

    it('should update the entity when using the upsert option', async () => {
      await database.table('IndexedEntity').insert({
        id: '1',
        value: 10,
        otherValue: '🎁',
        notStored: '🙈',
      });

      const actualEntity = await manager.update(
        IndexedEntity,
        { id: '1', value: 11, otherValue: '🎉' },
        { upsert: true },
      );

      expect(actualEntity).toEqual({
        id: '1',
        value: 11,
        otherValue: '🎉',
        notStored: '🙈',
      });
      expect(actualEntity).toBeInstanceOf(IndexedEntity);
      const [actualRows] = await database.table('IndexedEntity').read({
        keys: ['1'],
        columns: ['id', 'value', 'otherValue', 'notStored'],
        json: true,
      });
      expect(actualRows).toEqual([actualEntity]);
    });

    it('should insert the entity when using the upsert option and the entity does not exist', async () => {
      const actualEntity = await manager.update(
        IndexedEntity,
        { id: '1', value: 11, otherValue: '🎉' },
        { upsert: true },
      );

      expect(actualEntity).toEqual({
        id: '1',
        value: 11,
        otherValue: '🎉',
        notStored: null,
      });
      expect(actualEntity).toBeInstanceOf(IndexedEntity);
      const [actualRows] = await database.table('IndexedEntity').read({
        keys: ['1'],
        columns: ['id', 'value', 'otherValue', 'notStored'],
        json: true,
      });
      expect(actualRows).toEqual([actualEntity]);
    });

    it('should update a partial nested field', async () => {
      await database.table('ParentEntity').insert({
        id: '1',
        child_someJson: JSON.stringify([{ value: '🎁' }, { other: '😍' }]),
        child_otherValue: '🙈',
      });

      const actualEntity = await manager.update(ParentEntity, {
        id: '1',
        child: { someJson: [{ new: '🎉' }] },
      });

      expect(actualEntity).toEqual({
        id: '1',
        child: { someJson: [{ new: '🎉' }], other: '🙈' },
      });
      expect(actualEntity).toBeInstanceOf(ParentEntity);
      expect(actualEntity.child).toBeInstanceOf(ChildEntity);
      const [actualRows] = await database.table('ParentEntity').read({
        keys: ['1'],
        columns: ['id', 'child_someJson', 'child_otherValue'],
        json: true,
      });
      expect(actualRows).toEqual([
        { id: '1', child_someJson: [{ new: '🎉' }], child_otherValue: '🙈' },
      ]);
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

    it('should throw if the entity is soft deleted', async () => {
      await database
        .table('SoftDeleteEntity')
        .insert({ id: '1', deletedAt: new Date() });

      const actualPromise = manager.delete(SoftDeleteEntity, '1');

      await expect(actualPromise).rejects.toThrow(EntityNotFoundError);
    });

    it('should delete the entity when the entity is soft deleted and soft deletes are included', async () => {
      const expectedEntity = {
        id: '1',
        deletedAt: new Date(),
      };
      await database.table('SoftDeleteEntity').insert(expectedEntity);

      const actualEntity = await manager.delete(SoftDeleteEntity, '1', {
        includeSoftDeletes: true,
      });

      expect(actualEntity).toEqual(expectedEntity);
      expect(actualEntity).toBeInstanceOf(SoftDeleteEntity);
      const [actualRows] = await database.table('SoftDeleteEntity').read({
        keys: ['1'],
        columns: ['id'],
        json: true,
      });
      expect(actualRows).toBeEmpty();
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

  describe('sqlTableName', () => {
    it('should return the quoted table name for the entity type', () => {
      const actualTableName = manager.sqlTableName(SomeEntity);

      expect(actualTableName).toEqual('`MyEntity`');
    });

    it('should return the quoted table name for a string', () => {
      const actualTableName = manager.sqlTableName('MyTable');

      expect(actualTableName).toEqual('`MyTable`');
    });

    it('should add the index to the table name', () => {
      const actualTableName = manager.sqlTableName('MyTable', {
        index: 'MyIndex',
      });

      expect(actualTableName).toEqual('`MyTable`@{FORCE_INDEX=`MyIndex`}');
    });

    it('should add the index and the Spanner emulator hint', () => {
      const actualTableName = manager.sqlTableName('MyTable', {
        index: 'MyIndex',
        disableQueryNullFilteredIndexEmulatorCheck: true,
      });

      expect(actualTableName).toEqual(
        '`MyTable`@{FORCE_INDEX=`MyIndex`,spanner_emulator.disable_query_null_filtered_index_check=true}',
      );
    });

    it('should throw when the type is not a valid entity type', () => {
      expect(() => manager.sqlTableName({} as any)).toThrow(
        InvalidEntityDefinitionError,
      );
    });
  });

  describe('sqlColumns', () => {
    it('should return the quoted column names for the entity type', () => {
      const actualColumns = manager.sqlColumns(SomeEntity);

      expect(actualColumns).toEqual('`id`, `value`');
    });

    it('should return all columns for a table with a nested type', () => {
      const actualColumns = manager.sqlColumns(ParentEntity);

      expect(actualColumns).toEqual(
        '`id`, `child_someJson`, `child_otherValue`',
      );
    });

    it('should return the quoted column names for a list of columns', () => {
      const actualColumns = manager.sqlColumns(['id', 'value']);

      expect(actualColumns).toEqual('`id`, `value`');
    });

    it('should throw when the type is not a valid entity type', () => {
      expect(() => manager.sqlColumns({} as any)).toThrow(
        InvalidEntityDefinitionError,
      );
    });
  });
});
