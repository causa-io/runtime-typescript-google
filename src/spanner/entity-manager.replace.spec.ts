import { Database } from '@google-cloud/spanner';
import 'jest-extended';
import { SpannerEntityManager } from './entity-manager.js';
import {
  clearAllTestEntities,
  IndexedEntity,
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

  describe('replace', () => {
    it('should insert the entity', async () => {
      const entityToInsert = new SomeEntity({ id: '1', value: '🎁' });

      await manager.replace(entityToInsert);

      const [actualRows] = await database.table('MyEntity').read({
        keys: ['1'],
        columns: ['id', 'value'],
        json: true,
      });
      expect(actualRows).toEqual([{ id: '1', value: '🎁' }]);
    });

    it('should replace an existing entity', async () => {
      const expectedEntity = new IndexedEntity({
        id: '1',
        value: 10,
        otherValue: '🎁',
      });
      await database
        .table('IndexedEntity')
        .insert({ id: '1', value: 9, otherValue: '🎉', notStored: '🙈' });

      await manager.replace(expectedEntity);

      const [actualRows] = await database.table('IndexedEntity').read({
        keys: ['1'],
        columns: ['id', 'value', 'otherValue', 'notStored'],
        json: true,
      });
      expect(actualRows).toEqual([{ ...expectedEntity, notStored: null }]);
    });

    it('should use the provided transaction', async () => {
      const expectedEntity = {
        id: '1',
        value: 9,
        otherValue: '🎉',
        notStored: '🙈',
      };
      await database.table('IndexedEntity').insert(expectedEntity);

      await database.runTransactionAsync(async (transaction) => {
        await manager.replace(
          new IndexedEntity({ id: '1', value: 10, otherValue: '📝' }),
          { transaction },
        );
        transaction.end();
      });

      const [actualRows] = await database.table('IndexedEntity').read({
        keys: ['1'],
        columns: ['id', 'value', 'otherValue', 'notStored'],
        json: true,
      });
      expect(actualRows).toEqual([expectedEntity]);
    });

    it('should replace several entities', async () => {
      await database
        .table('IndexedEntity')
        .insert({ id: '1', value: 9, otherValue: '🎉', notStored: '🙈' });

      await manager.replace([
        new IndexedEntity({
          id: '1',
          value: 10,
          otherValue: '📝',
          notStored: null,
        }),
        new IndexedEntity({
          id: '2',
          value: 11,
          otherValue: '📝',
          notStored: '😇',
        }),
        new IntEntity({ id: '3', value: 10n }),
      ]);

      const [actualIndexedEntityRows] = await database
        .table('IndexedEntity')
        .read({
          keys: [['1'], ['2']],
          columns: ['id', 'value', 'otherValue', 'notStored'],
          json: true,
        });
      expect(actualIndexedEntityRows).toEqual([
        { id: '1', value: 10, otherValue: '📝', notStored: null },
        { id: '2', value: 11, otherValue: '📝', notStored: '😇' },
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
