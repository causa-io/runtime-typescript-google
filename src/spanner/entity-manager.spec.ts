import { EntityAlreadyExistsError } from '@causa/runtime';
import { PreciseDate } from '@google-cloud/precise-date';
import { Database, Snapshot, Transaction } from '@google-cloud/spanner';
import { jest } from '@jest/globals';
import { grpc } from 'google-gax';
import 'jest-extended';
import { SpannerColumn } from './column.decorator.js';
import { SpannerEntityManager } from './entity-manager.js';
import {
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

const TEST_SCHEMA = [
  `CREATE TABLE MyEntity (
    id STRING(MAX) NOT NULL,
    value STRING(MAX) NOT NULL
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
    await manager.clear(SomeEntity);
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
      });

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
});
