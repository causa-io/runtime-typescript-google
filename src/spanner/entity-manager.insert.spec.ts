import { EntityAlreadyExistsError } from '@causa/runtime';
import { Database } from '@google-cloud/spanner';
import 'jest-extended';
import { SpannerEntityManager } from './entity-manager.js';
import {
  clearAllTestEntities,
  IntEntity,
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

    it('should insert several entities', async () => {
      const entitiesToInsert = [
        new SomeEntity({ id: '1', value: '🎁' }),
        new SomeEntity({ id: '2', value: '🎉' }),
        new IntEntity({ id: '3', value: 10n }),
      ];

      await manager.insert(entitiesToInsert);

      const [actualMyEntityRows] = await database.table('MyEntity').read({
        keys: [['1'], ['2']],
        columns: ['id', 'value'],
        json: true,
      });
      expect(actualMyEntityRows).toEqual([
        { id: '1', value: '🎁' },
        { id: '2', value: '🎉' },
      ]);
      const [actualIntEntityRows] = await database.table('IntEntity').read({
        keys: ['3'],
        columns: ['id', 'value'],
        json: true,
      });
      expect(actualIntEntityRows).toEqual([{ id: '3', value: 10 }]);
    });
  });
});
