import { EntityNotFoundError } from '@causa/runtime';
import { Database } from '@google-cloud/spanner';
import { jest } from '@jest/globals';
import 'jest-extended';
import { SpannerEntityManager } from './entity-manager.js';
import { EntityMissingPrimaryKeyError } from './errors.js';
import {
  clearAllTestEntities,
  IndexedEntity,
  NestedKeyEntity,
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

  describe('update', () => {
    it('should update the entity', async () => {
      await database.table('IndexedEntity').insert({
        id: '1',
        value: 10,
        otherValue: '🎁',
        notStored: '🙈',
      });

      const actualEntity = await manager.update(IndexedEntity, {
        id: '1',
        otherValue: '🎉',
      });

      expect(actualEntity).toEqual({
        id: '1',
        value: 10,
        otherValue: '🎉',
        notStored: '🙈',
      });
      expect(actualEntity).toBeInstanceOf(IndexedEntity);
      const [actualRows] = await database.table('IndexedEntity').read({
        keys: ['1'],
        columns: ['id', 'value', 'otherValue', 'notStored'],
        json: true,
      });
      expect(actualRows).toEqual([actualEntity]);
    });

    it('should not remove interleaved child entities', async () => {
      await database.table('MyEntity').insert({ id: '1', value: '🎁' });
      await database.table('MyInterleavedEntity').insert({ id: '1' });

      const actualEntity = await manager.update(SomeEntity, {
        id: '1',
        value: '🎉',
      });

      expect(actualEntity).toEqual({ id: '1', value: '🎉' });
      const [actualRows] = await database.table('MyInterleavedEntity').read({
        keys: ['1'],
        columns: ['id'],
        json: true,
      });
      expect(actualRows).toEqual([{ id: '1' }]);
    });

    it('should throw if the entity does not exist', async () => {
      const actualPromise = manager.update(IndexedEntity, {
        id: '1',
        otherValue: '🎉',
      });

      await expect(actualPromise).rejects.toThrow(EntityNotFoundError);
      await expect(actualPromise).rejects.toMatchObject({
        entityType: IndexedEntity,
        key: ['1'],
      });
    });

    it('should throw if the entity is soft deleted', async () => {
      await database
        .table('SoftDeleteEntity')
        .insert({ id: '1', deletedAt: new Date() });

      const actualPromise = manager.update(SoftDeleteEntity, {
        id: '1',
        deletedAt: new Date(),
      });

      await expect(actualPromise).rejects.toThrow(EntityNotFoundError);
    });

    it('should update the entity when the entity is soft deleted and soft deletes are included', async () => {
      await database.table('SoftDeleteEntity').insert({
        id: '1',
        deletedAt: new Date('2000-01-01'),
      });
      const expectedDeletedAt = new Date('2999-01-01');

      const actualEntity = await manager.update(
        SoftDeleteEntity,
        { id: '1', deletedAt: expectedDeletedAt },
        { includeSoftDeletes: true },
      );

      expect(actualEntity).toEqual({ id: '1', deletedAt: expectedDeletedAt });
      expect(actualEntity).toBeInstanceOf(SoftDeleteEntity);
      const [actualRows] = await database.table('SoftDeleteEntity').read({
        keys: ['1'],
        columns: ['id', 'deletedAt'],
        json: true,
      });
      expect(actualRows).toEqual([actualEntity]);
    });

    it('should use the provided transaction', async () => {
      const expectedEntity = {
        id: '1',
        value: 10,
        otherValue: '🎁',
        notStored: '🙈',
      };
      await database.table('IndexedEntity').insert(expectedEntity);

      await database.runTransactionAsync(async (transaction) => {
        await manager.update(
          IndexedEntity,
          { id: '1', otherValue: '🎉' },
          { transaction },
        );
        await transaction.rollback();
      });

      const [actualRows] = await database.table('IndexedEntity').read({
        keys: ['1'],
        columns: ['id', 'value', 'otherValue', 'notStored'],
        json: true,
      });
      expect(actualRows).toEqual([expectedEntity]);
    });

    it('should throw an error thrown by the validation function', async () => {
      const expectedEntity = {
        id: '1',
        value: 10,
        otherValue: '🎁',
        notStored: '🙈',
      };
      await database.table('IndexedEntity').insert(expectedEntity);
      const fn: jest.Mock<(entity: IndexedEntity) => void> = jest.fn(() => {
        throw new Error('💥');
      });

      const actualPromise = manager.update(
        IndexedEntity,
        { id: '1', otherValue: '🎉' },
        { validateFn: fn },
      );

      await expect(actualPromise).rejects.toThrow('💥');
      expect(fn).toHaveBeenCalledExactlyOnceWith({
        id: '1',
        value: 10,
        otherValue: '🎁',
        notStored: '🙈',
      });
      expect(fn.mock.calls[0][0]).toBeInstanceOf(IndexedEntity);
      const [actualRows] = await database.table('IndexedEntity').read({
        keys: ['1'],
        columns: ['id', 'value', 'otherValue', 'notStored'],
        json: true,
      });
      expect(actualRows).toEqual([expectedEntity]);
    });

    it('should throw an error when the update does not contain the primary key', async () => {
      const actualPromise = manager.update(IndexedEntity, {
        otherValue: '🎉',
      });

      await expect(actualPromise).rejects.toThrow(EntityMissingPrimaryKeyError);
    });

    it('should update the entity when using the upsert option', async () => {
      await database.table('IndexedEntity').insert({
        id: '1',
        value: 10,
        otherValue: '🎁',
        notStored: '🙈',
      });

      const actualEntity = await manager.update(
        IndexedEntity,
        { id: '1', value: 11, otherValue: '🎉' },
        { upsert: true },
      );

      expect(actualEntity).toEqual({
        id: '1',
        value: 11,
        otherValue: '🎉',
        notStored: '🙈',
      });
      expect(actualEntity).toBeInstanceOf(IndexedEntity);
      const [actualRows] = await database.table('IndexedEntity').read({
        keys: ['1'],
        columns: ['id', 'value', 'otherValue', 'notStored'],
        json: true,
      });
      expect(actualRows).toEqual([actualEntity]);
    });

    it('should insert the entity when using the upsert option and the entity does not exist', async () => {
      const actualEntity = await manager.update(
        IndexedEntity,
        { id: '1', value: 11, otherValue: '🎉' },
        { upsert: true },
      );

      expect(actualEntity).toEqual({
        id: '1',
        value: 11,
        otherValue: '🎉',
        notStored: null,
      });
      expect(actualEntity).toBeInstanceOf(IndexedEntity);
      const [actualRows] = await database.table('IndexedEntity').read({
        keys: ['1'],
        columns: ['id', 'value', 'otherValue', 'notStored'],
        json: true,
      });
      expect(actualRows).toEqual([actualEntity]);
    });

    it('should update entity with nested properties in primary key', async () => {
      await database.table('NestedKeyEntity').insert({
        id: 77,
        address: { city: 'Seattle', zip: '98101' },
        value: '☕',
      });

      const actualEntity = await manager.update(NestedKeyEntity, {
        id: 77,
        address: { city: 'Seattle', zip: '98101' },
        value: '🌧️',
      });

      expect(actualEntity).toEqual({
        id: 77,
        address: { city: 'Seattle', zip: '98101' },
        value: '🌧️',
      });
      expect(actualEntity).toBeInstanceOf(NestedKeyEntity);
      const [actualRows] = await database.table('NestedKeyEntity').read({
        keys: [['Seattle', '98101', '77']],
        columns: ['id', 'address', 'value'],
        json: true,
      });
      expect(actualRows).toEqual([actualEntity]);
    });
  });
});
