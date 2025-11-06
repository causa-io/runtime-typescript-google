import { Database, Transaction } from '@google-cloud/spanner';
import { jest } from '@jest/globals';
import 'jest-extended';
import { SpannerEntityManager } from './entity-manager.js';
import {
  clearAllTestEntities,
  IntEntity,
  setupTestDatabase,
  SomeEntity,
} from './entity-manager.test.js';
import { InvalidArgumentError } from './errors.js';
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
    });

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

    it('should pass the request options to the query', async () => {
      let spy!: jest.SpiedFunction<Transaction['runStream']>;
      const requestOptions = { priority: SpannerRequestPriority.PRIORITY_LOW };

      await manager.snapshot(async (transaction) => {
        spy = jest.spyOn(transaction, 'runStream');
        await manager.query(
          { transaction, requestOptions },
          { sql: 'SELECT 1' },
        );
      });

      expect(spy).toHaveBeenCalledWith(
        expect.objectContaining({ requestOptions }),
      );
    });
  });
});
