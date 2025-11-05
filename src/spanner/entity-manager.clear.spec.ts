import { Database } from '@google-cloud/spanner';
import 'jest-extended';
import { SpannerEntityManager } from './entity-manager.js';
import {
  clearAllTestEntities,
  setupTestDatabase,
  SomeEntity,
} from './entity-manager.test.js';

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
