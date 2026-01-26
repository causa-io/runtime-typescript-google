import { Database, Transaction } from '@google-cloud/spanner';
import { jest } from '@jest/globals';
import 'jest-extended';
import { setTimeout } from 'node:timers/promises';
import * as uuid from 'uuid';
import { SpannerEntityManager } from './entity-manager.js';
import {
  clearAllTestEntities,
  IndexedEntity,
  NestedKeyEntity,
  setupTestDatabase,
  SoftDeleteEntity,
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

  describe('findOneByKey', () => {
    it('should return undefined when the entity does not exist', async () => {
      const actualEntity = await manager.findOneByKey(SomeEntity, '1');

      expect(actualEntity).toBeUndefined();
    });

    it('should return the entity when it exists', async () => {
      await database.table('MyEntity').insert({ id: '1', value: '🎁' });

      const actualEntity = await manager.findOneByKey(SomeEntity, '1');

      expect(actualEntity).toEqual({ id: '1', value: '🎁' });
      expect(actualEntity).toBeInstanceOf(SomeEntity);
    });

    it('should return undefined when the entity is soft deleted', async () => {
      await database
        .table('SoftDeleteEntity')
        .insert({ id: '1', deletedAt: new Date() });

      const actualEntity = await manager.findOneByKey(SoftDeleteEntity, '1');

      expect(actualEntity).toBeUndefined();
    });

    it('should return the entity when the entity is soft deleted and soft deletes are included', async () => {
      const expectedEntity = new SoftDeleteEntity({
        id: '1',
        deletedAt: new Date(),
      });
      await database.table('SoftDeleteEntity').insert(expectedEntity);

      const actualEntity = await manager.findOneByKey(SoftDeleteEntity, '1', {
        includeSoftDeletes: true,
      });

      expect(actualEntity).toEqual(expectedEntity);
      expect(actualEntity).toBeInstanceOf(SoftDeleteEntity);
    });

    it('should only return the specified columns', async () => {
      await database.table('MyEntity').insert({ id: '1', value: '🎁' });

      const actualEntity = await manager.findOneByKey(SomeEntity, '1', {
        columns: ['value'],
      });

      expect(actualEntity).toEqual({ value: '🎁' });
      expect(actualEntity).toBeInstanceOf(SomeEntity);
    });

    it('should throw when the specified columns do not include the soft delete column', async () => {
      await database
        .table('SoftDeleteEntity')
        .insert({ id: '1', deletedAt: new Date() });

      const actualPromise = manager.findOneByKey(SoftDeleteEntity, '1', {
        columns: ['id'],
      });

      await expect(actualPromise).rejects.toThrow(InvalidArgumentError);
    });

    it('should return the columns for the entity when it is soft deleted and soft deletes are included', async () => {
      await database.table('SoftDeleteEntity').insert({
        id: '1',
        deletedAt: new Date(),
      });

      const actualEntity = await manager.findOneByKey(SoftDeleteEntity, '1', {
        includeSoftDeletes: true,
        columns: ['id'],
      });

      expect(actualEntity).toEqual({ id: '1' });
      expect(actualEntity).toBeInstanceOf(SoftDeleteEntity);
    });

    it('should look up the entity using the provided index', async () => {
      await database
        .table('IndexedEntity')
        .insert({ id: '1', value: 10, otherValue: '🎁' });

      const actualEntity = await manager.findOneByKey(IndexedEntity, ['10'], {
        index: IndexedEntity.ByValue,
        columns: ['id', 'otherValue'],
      });

      expect(actualEntity).toEqual({ id: '1', otherValue: '🎁' });
      expect(actualEntity).toBeInstanceOf(IndexedEntity);
    });

    it('should use the provided transaction', async () => {
      // Ensures the database has been existing for this long.
      await setTimeout(1500);

      const id = uuid.v4();
      await database
        .table('IndexedEntity')
        .insert({ id, value: 10, otherValue: '🎁' });

      // Uses a snapshot reading at a past timestamp to make it look like the row does not exist.
      const actualEntity = await manager.snapshot(
        { timestampBounds: { exactStaleness: { seconds: 1 } } },
        async (transaction) =>
          manager.findOneByKey(IndexedEntity, id, { transaction }),
      );

      expect(actualEntity).toBeUndefined();
    });

    it('should fetch the entire entity when using an index and columns are not specified', async () => {
      await database
        .table('IndexedEntity')
        .insert({ id: '1', value: 10, otherValue: '🎁', notStored: '🙈' });
      await database.table('NestedKeyEntity').insert({
        id: 1,
        address: JSON.stringify({ city: 'City', zip: '12345' }),
        value: 'test',
      });

      const actualEntity = await manager.findOneByKey(IndexedEntity, ['10'], {
        index: IndexedEntity.ByValue,
      });
      const actualNestedEntity = await manager.findOneByKey(
        NestedKeyEntity,
        '12345',
        { index: 'NestedKeyEntityByZip' },
      );

      expect(actualEntity).toEqual({
        id: '1',
        value: 10,
        otherValue: '🎁',
        notStored: '🙈',
      });
      expect(actualEntity).toBeInstanceOf(IndexedEntity);
      expect(actualNestedEntity).toEqual({
        id: 1,
        address: { city: 'City', zip: '12345' },
        value: 'test',
      });
      expect(actualNestedEntity).toBeInstanceOf(NestedKeyEntity);
    });

    it('should pass the request options to the read method', async () => {
      let spy!: jest.SpiedFunction<Transaction['read']>;
      const requestOptions = { priority: SpannerRequestPriority.PRIORITY_LOW };

      await manager.snapshot(async (transaction) => {
        spy = jest.spyOn(transaction, 'read');
        await manager.findOneByKey(SomeEntity, '1', {
          transaction,
          requestOptions,
        });
      });

      expect(spy).toHaveBeenCalledExactlyOnceWith(
        'MyEntity',
        expect.objectContaining({ requestOptions }),
      );
    });
  });
});
