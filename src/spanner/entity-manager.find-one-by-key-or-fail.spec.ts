import { EntityNotFoundError } from '@causa/runtime';
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
});
