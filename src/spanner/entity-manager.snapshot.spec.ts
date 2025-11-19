import { PreciseDate } from '@google-cloud/precise-date';
import { Database, Transaction } from '@google-cloud/spanner';
import { status } from '@grpc/grpc-js';
import { jest } from '@jest/globals';
import 'jest-extended';
import { SpannerEntityManager } from './entity-manager.js';
import {
  clearAllTestEntities,
  setupTestDatabase,
} from './entity-manager.test.js';
import { InvalidQueryError, TemporarySpannerError } from './errors.js';
import type { SpannerReadOnlyTransaction } from './types.js';

describe('SpannerEntityManager', () => {
  let database: Database;
  let manager: SpannerEntityManager;

  beforeAll(async () => {
    ({ database } = await setupTestDatabase());
  });

  beforeEach(() => {
    manager = new SpannerEntityManager(database);
  });

  afterEach(() => clearAllTestEntities(manager));

  afterAll(() => database.delete());

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
      let actualSnapshot!: SpannerReadOnlyTransaction;
      const actualPromise = manager.snapshot(async (snapshot) => {
        actualSnapshot = snapshot;
        await snapshot.read('nope', { columns: ['nope'], keys: ['nope'] });
      });

      await expect(actualPromise).rejects.toThrow(InvalidQueryError);
      expect(actualSnapshot.ended).toBeTrue();
    });

    it('should rethrow unknown errors', async () => {
      let actualSnapshot!: SpannerReadOnlyTransaction;
      const actualPromise = manager.snapshot(async (snapshot) => {
        actualSnapshot = snapshot;
        throw new Error('💣');
      });

      await expect(actualPromise).rejects.toThrow('💣');
      expect(actualSnapshot.ended).toBeTrue();
    });

    it('should catch and rethrow an error thrown by getSnapshot itself', async () => {
      const error = new Error('💤');
      (error as any).code = status.DEADLINE_EXCEEDED;
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

    it('should run the provided function in the provided transaction', async () => {
      const expectedReturnValue = { value: '✅' };

      let expectedTransaction!: Transaction;
      let actualTransaction!: SpannerReadOnlyTransaction;
      const actualReturnValue = await database.runTransactionAsync(
        async (transaction) => {
          jest.spyOn(transaction, 'end');
          expectedTransaction = transaction;

          const value = await manager.snapshot(
            { transaction },
            async (transaction) => {
              actualTransaction = transaction;
              return expectedReturnValue;
            },
          );

          expect(transaction.end).not.toHaveBeenCalled();
          return value;
        },
      );

      expect(actualTransaction).toBe(expectedTransaction);
      expect(actualReturnValue).toEqual(expectedReturnValue);
    });
  });
});
