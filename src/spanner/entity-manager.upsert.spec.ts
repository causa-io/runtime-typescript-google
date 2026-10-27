import { Database } from '@google-cloud/spanner';
import { jest } from '@jest/globals';
import 'jest-extended';
import { SpannerEntityManager } from './entity-manager.js';
import {
  clearAllTestEntities,
  IndexedEntity,
  IntEntity,
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

  describe('upsert', () => {
    it('should insert the entity', async () => {
      const entityToInsert = new SomeEntity({ id: '1', value: '🎁' });

      await manager.upsert(entityToInsert);

      const [actualRows] = await database.table('MyEntity').read({
        keys: ['1'],
        columns: ['id', 'value'],
        json: true,
      });
      expect(actualRows).toEqual([{ id: '1', value: '🎁' }]);
    });

    it('should update an existing entity and keep its interleaved rows', async () => {
      await database.table('MyEntity').insert({ id: '1', value: '🎁' });
      await database.table('MyInterleavedEntity').insert({ id: '1' });

      await manager.upsert(new SomeEntity({ id: '1', value: '🎉' }));

      const [actualRows] = await database.table('MyEntity').read({
        keys: ['1'],
        columns: ['id', 'value'],
        json: true,
      });
      expect(actualRows).toEqual([{ id: '1', value: '🎉' }]);
      const [actualInterleavedRows] = await database
        .table('MyInterleavedEntity')
        .read({ keys: ['1'], columns: ['id'], json: true });
      expect(actualInterleavedRows).toEqual([{ id: '1' }]);
    });

    it('should keep the value of columns for undefined properties', async () => {
      await database
        .table('IndexedEntity')
        .insert({ id: '1', value: 9, otherValue: '🎉', notStored: '🙈' });

      await manager.upsert(
        new IndexedEntity({ id: '1', value: 10, otherValue: '🎁' }),
      );

      const [actualRows] = await database.table('IndexedEntity').read({
        keys: ['1'],
        columns: ['id', 'value', 'otherValue', 'notStored'],
        json: true,
      });
      expect(actualRows).toEqual([
        { id: '1', value: 10, otherValue: '🎁', notStored: '🙈' },
      ]);
    });

    it('should update a soft deleted entity', async () => {
      await database
        .table('SoftDeleteEntity')
        .insert({ id: '1', deletedAt: new Date() });

      await manager.upsert(new SoftDeleteEntity({ id: '1', deletedAt: null }));

      const [actualRows] = await database.table('SoftDeleteEntity').read({
        keys: ['1'],
        columns: ['id', 'deletedAt'],
        json: true,
      });
      expect(actualRows).toEqual([{ id: '1', deletedAt: null }]);
    });

    it('should upsert several entities', async () => {
      await database
        .table('IndexedEntity')
        .insert({ id: '1', value: 9, otherValue: '🎉', notStored: '🙈' });

      await manager.upsert([
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

    it('should use the provided transaction without reading the entity', async () => {
      await database.table('MyEntity').insert({ id: '1', value: '🎁' });

      await database.runTransactionAsync(async (transaction) => {
        jest.spyOn(transaction, 'read');

        await manager.upsert(new SomeEntity({ id: '1', value: '🎉' }), {
          transaction,
        });

        expect(transaction.read).not.toHaveBeenCalled();
        await transaction.rollback();
      });

      const [actualRows] = await database.table('MyEntity').read({
        keys: ['1'],
        columns: ['id', 'value'],
        json: true,
      });
      expect(actualRows).toEqual([{ id: '1', value: '🎁' }]);
    });
  });
});
