import { EntityAlreadyExistsError } from '@causa/runtime';
import { Database } from '@google-cloud/spanner';
import { status } from '@grpc/grpc-js';
import { jest } from '@jest/globals';
import 'jest-extended';
import {
  SpannerEntityManager,
  type SpannerReadWriteTransaction,
} from './entity-manager.js';
import {
  clearAllTestEntities,
  setupTestDatabase,
} from './entity-manager.test.js';
import { TemporarySpannerError, TransactionFinishedError } from './errors.js';

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
        (error as any).code = status.DEADLINE_EXCEEDED;
        throw error;
      });

      await expect(actualPromise).rejects.toThrow(TemporarySpannerError);
    });

    it('should catch and rethrow a temporary Spanner error thrown by runTransactionAsync itself', async () => {
      const error = new Error('⌛');
      (error as any).code = status.DEADLINE_EXCEEDED;
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
      let actualTransaction!: SpannerReadWriteTransaction;
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
      let actualTransaction!: SpannerReadWriteTransaction;
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

    it('should run the provided function in the provided transaction', async () => {
      const expectedReturnValue = { value: '✅' };

      const actualReturnValue = await database.runTransactionAsync(
        async (transaction) => {
          const returnValue = await manager.transaction(
            { transaction },
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

    it('should pass the transaction tag', async () => {
      jest.spyOn(database, 'runTransactionAsync');
      const tag = '🔖';

      const actual = await manager.transaction({ tag }, async () => '🎉');

      expect(actual).toEqual('🎉');
      expect(database.runTransactionAsync).toHaveBeenCalledWith(
        { requestOptions: { transactionTag: tag } },
        expect.any(Function),
      );
    });
  });
});
