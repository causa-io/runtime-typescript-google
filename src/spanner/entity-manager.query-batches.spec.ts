import { Database, Transaction } from '@google-cloud/spanner';
import { jest } from '@jest/globals';
import 'jest-extended';
import { SpannerEntityManager } from './entity-manager.js';
import {
  clearAllTestEntities,
  setupTestDatabase,
  SomeEntity,
} from './entity-manager.test.js';
import { SpannerRequestPriority } from './types.js';

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

  describe('queryBatches', () => {
    it('should run the query and return batches of the specified size', async () => {
      await database.table('MyEntity').insert([
        { id: '1', value: '🎁' },
        { id: '2', value: '🎉' },
        { id: '3', value: '🎊' },
        { id: '4', value: '🎈' },
        { id: '5', value: '🎀' },
      ]);

      const actualBatches: SomeEntity[][] = [];
      for await (const batch of manager.queryBatches(
        { entityType: SomeEntity, batchSize: 2 },
        { sql: 'SELECT id, value FROM MyEntity ORDER BY id' },
      )) {
        actualBatches.push(batch);
      }

      expect(actualBatches).toEqual([
        [
          { id: '1', value: '🎁' },
          { id: '2', value: '🎉' },
        ],
        [
          { id: '3', value: '🎊' },
          { id: '4', value: '🎈' },
        ],
        [{ id: '5', value: '🎀' }],
      ]);
      expect(actualBatches[0][0]).toBeInstanceOf(SomeEntity);
    });

    it('should work with batch size equal to total results', async () => {
      await database.table('MyEntity').insert([
        { id: '1', value: '🎁' },
        { id: '2', value: '🎉' },
      ]);

      const actualBatches: SomeEntity[][] = [];
      for await (const batch of manager.queryBatches(
        { entityType: SomeEntity, batchSize: 2 },
        { sql: 'SELECT id, value FROM MyEntity ORDER BY id' },
      )) {
        actualBatches.push(batch);
      }

      expect(actualBatches).toEqual([
        [
          { id: '1', value: '🎁' },
          { id: '2', value: '🎉' },
        ],
      ]);
    });

    it('should work with batch size larger than total results', async () => {
      await database.table('MyEntity').insert([
        { id: '1', value: '🎁' },
        { id: '2', value: '🎉' },
      ]);

      const actualBatches: SomeEntity[][] = [];
      for await (const batch of manager.queryBatches(
        { entityType: SomeEntity, batchSize: 10 },
        { sql: 'SELECT id, value FROM MyEntity ORDER BY id' },
      )) {
        actualBatches.push(batch);
      }

      expect(actualBatches).toEqual([
        [
          { id: '1', value: '🎁' },
          { id: '2', value: '🎉' },
        ],
      ]);
    });

    it('should use the provided transaction', async () => {
      await manager.transaction(async (transaction) => {
        const batches: any[] = [];
        for await (const batch of manager.queryBatches(
          { transaction, batchSize: 1 },
          { sql: `INSERT INTO IntEntity(id, value) VALUES ('1', 10)` },
        )) {
          batches.push(batch);
        }
      });

      const [actualRows] = await database.table('IntEntity').read({
        keys: ['1'],
        columns: ['id', 'value'],
        json: true,
      });
      expect(actualRows).toEqual([{ id: '1', value: 10 }]);
    });

    it('should pass the request options to the query', async () => {
      let spy!: jest.SpiedFunction<Transaction['runStream']>;
      const requestOptions = { priority: SpannerRequestPriority.PRIORITY_LOW };
      await database.table('MyEntity').insert([{ id: '1', value: '🎁' }]);

      await manager.snapshot(async (transaction) => {
        spy = jest.spyOn(transaction, 'runStream');
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        for await (const _ of manager.queryBatches(
          { transaction, requestOptions, batchSize: 1 },
          { sql: 'SELECT id FROM MyEntity' },
        )) {
        }
      });

      expect(spy).toHaveBeenCalledWith(
        expect.objectContaining({ requestOptions }),
      );
    });

    it('should return an empty iterable when no results are found', async () => {
      const actualBatches: SomeEntity[][] = [];
      for await (const batch of manager.queryBatches(
        { entityType: SomeEntity, batchSize: 2 },
        { sql: 'SELECT id, value FROM MyEntity' },
      )) {
        actualBatches.push(batch);
      }

      expect(actualBatches).toBeEmpty();
    });
  });
});
