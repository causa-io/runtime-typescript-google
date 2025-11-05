import { EntityNotFoundError } from '@causa/runtime';
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
});
