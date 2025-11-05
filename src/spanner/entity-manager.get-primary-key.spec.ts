import { Database } from '@google-cloud/spanner';
import 'jest-extended';
import { SpannerColumn } from './column.decorator.js';
import { SpannerEntityManager } from './entity-manager.js';
import {
  EntityMissingPrimaryKeyError,
  InvalidEntityDefinitionError,
} from './errors.js';
import { SpannerTable } from './table.decorator.js';
import {
  clearAllTestEntities,
  NestedKeyEntity,
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

  describe('getPrimaryKey', () => {
    it('should throw when the object has no type and the entity type is not provided', () => {
      expect(() => manager.getPrimaryKey({})).toThrow(
        InvalidEntityDefinitionError,
      );
    });

    it('should default to the object constructor when the entity type is not provided', () => {
      const obj = new SomeEntity({ id: '1' });

      const actualPrimaryKey = manager.getPrimaryKey(obj);

      expect(actualPrimaryKey).toEqual(['1']);
    });

    it('should return a composite primary key as a list of strings', () => {
      @SpannerTable({ primaryKey: ['first', 'second', 'third', 'fourth'] })
      class MyWeirdEntity {
        @SpannerColumn({ isBigInt: true })
        first!: bigint;
        @SpannerColumn()
        second!: string | null;
        @SpannerColumn()
        third!: string;
        @SpannerColumn()
        fourth!: Date;
      }
      const expectedDate = new Date();

      const actualPrimaryKey = manager.getPrimaryKey(
        { first: 1n, second: null, third: '3', fourth: expectedDate },
        MyWeirdEntity,
      );

      expect(actualPrimaryKey).toEqual(['1', null, '3', expectedDate.toJSON()]);
    });

    it('should throw when a primary key column is missing', () => {
      const obj = new SomeEntity({ value: 'value' });

      expect(() => manager.getPrimaryKey(obj)).toThrow(
        EntityMissingPrimaryKeyError,
      );
    });

    it('should extract primary key from nested properties using dot notation', () => {
      const obj = new NestedKeyEntity({
        id: 42,
        address: { city: 'San Francisco', zip: '94102' },
        value: 'test',
      });

      const actualPrimaryKey = manager.getPrimaryKey(obj);

      expect(actualPrimaryKey).toEqual(['San Francisco', '94102', '42']);
    });

    it('should throw when a nested primary key property is undefined', () => {
      const obj = new NestedKeyEntity({
        id: 42,
        address: { city: 'San Francisco' } as any,
        value: 'test',
      });

      expect(() => manager.getPrimaryKey(obj)).toThrow(
        EntityMissingPrimaryKeyError,
      );
    });

    it('should throw when a nested primary key parent object is undefined', () => {
      const obj = new NestedKeyEntity({
        id: 42,
        value: 'test',
      } as any);

      expect(() => manager.getPrimaryKey(obj)).toThrow(
        EntityMissingPrimaryKeyError,
      );
    });

    it('should convert numbers in nested properties to strings for primary keys', () => {
      @SpannerTable({ primaryKey: ['metadata.count'] })
      class EntityWithNumericNestedKey {
        @SpannerColumn()
        metadata!: { count: number };
      }

      const actualPrimaryKey = manager.getPrimaryKey(
        { metadata: { count: 123 } },
        EntityWithNumericNestedKey,
      );

      expect(actualPrimaryKey).toEqual(['123']);
    });
  });
});
