import { Database } from '@google-cloud/spanner';
import { jest } from '@jest/globals';
import 'jest-extended';
import { SpannerEntityManager } from './entity-manager.js';
import {
  clearAllTestEntities,
  setupTestDatabase,
  SoftDeleteEntity,
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

  describe('blindDelete', () => {
    it('should delete the entity', async () => {
      await database.table('MyEntity').insert({ id: '1', value: '🎁' });

      await manager.blindDelete(SomeEntity, '1');

      const [actualRows] = await database.table('MyEntity').read({
        keys: ['1'],
        columns: ['id'],
        limit: 1,
      });
      expect(actualRows).toBeEmpty();
    });

    it('should not throw if the entity does not exist', async () => {
      const actualPromise = manager.blindDelete(SomeEntity, '1');

      await expect(actualPromise).toResolve();
    });

    it('should delete a soft deleted entity', async () => {
      await database
        .table('SoftDeleteEntity')
        .insert({ id: '1', deletedAt: new Date() });

      await manager.blindDelete(SoftDeleteEntity, '1');

      const [actualRows] = await database.table('SoftDeleteEntity').read({
        keys: ['1'],
        columns: ['id'],
        json: true,
      });
      expect(actualRows).toBeEmpty();
    });

    it('should use the provided transaction without reading the entity', async () => {
      await database.table('MyEntity').insert({ id: '1', value: '🎁' });

      await database.runTransactionAsync(async (transaction) => {
        jest.spyOn(transaction, 'read');

        await manager.blindDelete(SomeEntity, '1', { transaction });

        expect(transaction.read).not.toHaveBeenCalled();
        await transaction.rollback();
      });

      const [actualRows] = await database.table('MyEntity').read({
        keys: ['1'],
        columns: ['id'],
        limit: 1,
      });
      expect(actualRows).not.toBeEmpty();
    });
  });
});
