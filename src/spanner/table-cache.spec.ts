import { Float, Float32, Int, Numeric } from '@google-cloud/spanner';
import { SpannerColumn } from './column.decorator.js';
import {
  EntityMissingPrimaryKeyError,
  InvalidEntityDefinitionError,
} from './errors.js';
import { SpannerTableCache } from './table-cache.js';
import { SpannerTable } from './table.decorator.js';

@SpannerTable({ name: 'MyCustomEntity', primaryKey: ['id'] })
class MyEntity {
  @SpannerColumn()
  id!: string;

  @SpannerColumn({ name: 'customName' })
  someName!: string;

  @SpannerColumn({ softDelete: true })
  deletedAt!: Date | null;
}

describe('SpannerTableCache', () => {
  let cache: SpannerTableCache;

  beforeEach(() => {
    cache = new SpannerTableCache();
  });

  describe('getMetadata', () => {
    it('should return the metadata for the given entity type', () => {
      const metadata = cache.getMetadata(MyEntity);

      expect(metadata).toEqual({
        tableName: 'MyCustomEntity',
        primaryKeyColumns: ['id'],
        columnNames: {
          id: 'id',
          someName: 'customName',
          deletedAt: 'deletedAt',
        },
        softDeleteColumn: 'deletedAt',
        primaryKeyGetter: expect.any(Function),
      });
    });

    it('should cache the metadata for the given entity type', () => {
      const metadata1 = cache.getMetadata(MyEntity);
      const metadata2 = cache.getMetadata(MyEntity);

      expect(metadata1).toBe(metadata2);
    });

    it('should throw if the entity type is not decorated', () => {
      class UndecoratedEntity {}

      expect(() => cache.getMetadata(UndecoratedEntity)).toThrow(
        InvalidEntityDefinitionError,
      );
    });

    it('should return null as the soft delete column if none is defined', () => {
      @SpannerTable({ primaryKey: ['id'] })
      class MyEntity {
        @SpannerColumn()
        id!: string;
      }

      const metadata = cache.getMetadata(MyEntity);

      expect(metadata).toEqual({
        tableName: 'MyEntity',
        primaryKeyColumns: ['id'],
        columnNames: { id: 'id' },
        softDeleteColumn: null,
        primaryKeyGetter: expect.any(Function),
      });
    });

    it('should throw if more than one column is marked as soft delete', () => {
      @SpannerTable({ primaryKey: ['id'] })
      class MyEntity {
        @SpannerColumn()
        id!: string;

        @SpannerColumn({ softDelete: true })
        deletedAt1!: Date | null;

        @SpannerColumn({ softDelete: true })
        deletedAt2!: Date | null;
      }

      expect(() => cache.getMetadata(MyEntity)).toThrow(
        InvalidEntityDefinitionError,
      );
    });

    it('should support nested property paths in primary keys', () => {
      @SpannerTable({ primaryKey: ['address.city', 'address.zip', 'id'] })
      class NestedEntity {
        @SpannerColumn()
        id!: string;

        @SpannerColumn()
        address!: {
          city: string;
          zip: string;
        };
      }

      const metadata = cache.getMetadata(NestedEntity);

      expect(metadata).toEqual({
        tableName: 'NestedEntity',
        primaryKeyColumns: expect.toIncludeSameMembers(['address', 'id']),
        columnNames: { id: 'id', address: 'address' },
        softDeleteColumn: null,
        primaryKeyGetter: expect.any(Function),
      });
    });

    it('should throw if a primary key column does not exist', () => {
      @SpannerTable({ primaryKey: ['nonExistent'] })
      class BadEntity {
        @SpannerColumn()
        id!: string;
      }

      expect(() => cache.getMetadata(BadEntity)).toThrow(
        InvalidEntityDefinitionError,
      );
    });
  });

  describe('primaryKeyGetter', () => {
    it('should extract simple primary key from entity', () => {
      const metadata = cache.getMetadata(MyEntity);
      const entity = { id: 'test-id', someName: 'test', deletedAt: null };

      const primaryKey = metadata.primaryKeyGetter(entity);

      expect(primaryKey).toEqual(['test-id']);
    });

    it('should extract nested property paths in primary key', () => {
      @SpannerTable({ primaryKey: ['address.city', 'address.zip', 'id'] })
      class NestedEntity {
        @SpannerColumn()
        id!: string;

        @SpannerColumn()
        address!: {
          city: string;
          zip: string;
        };
      }
      const metadata = cache.getMetadata(NestedEntity);
      const entity = {
        id: '123',
        address: { city: 'San Francisco', zip: '94102' },
      };

      const primaryKey = metadata.primaryKeyGetter(entity);

      expect(primaryKey).toEqual(['San Francisco', '94102', '123']);
    });

    it.each([
      [42, ['42']],
      [BigInt(9007199254740991), ['9007199254740991']],
      [new Date('2023-01-01T00:00:00.000Z'), ['2023-01-01T00:00:00.000Z']],
      [new Int('42'), ['42']],
      [new Float(3.14), ['3.14']],
      [new Float32(2.5), ['2.5']],
      [new Numeric('123.45'), ['123.45']],
    ])(
      'should convert other types to strings in primary key',
      (value, expected) => {
        @SpannerTable({ primaryKey: ['id'] })
        class SomeEntity {
          @SpannerColumn()
          id!: any;
        }
        const metadata = cache.getMetadata(SomeEntity);

        const primaryKey = metadata.primaryKeyGetter({ id: value });

        expect(primaryKey).toEqual(expected);
      },
    );

    it('should handle null values in composite primary key', () => {
      @SpannerTable({ primaryKey: ['first', 'second'] })
      class NullableEntity {
        @SpannerColumn()
        first!: string | null;

        @SpannerColumn()
        second!: string;
      }
      const metadata = cache.getMetadata(NullableEntity);
      const entity = { first: null, second: 'value' };

      const primaryKey = metadata.primaryKeyGetter(entity);

      expect(primaryKey).toEqual([null, 'value']);
    });

    it('should throw when primary key value is undefined', () => {
      const metadata = cache.getMetadata(MyEntity);
      const entity = { someName: 'test', deletedAt: null };

      expect(() => metadata.primaryKeyGetter(entity)).toThrow(
        EntityMissingPrimaryKeyError,
      );
    });

    it('should throw when nested parent object is undefined', () => {
      @SpannerTable({ primaryKey: ['address.city'] })
      class NestedEntity {
        @SpannerColumn()
        address!: { city: string };
      }
      const metadata = cache.getMetadata(NestedEntity);
      const entity = {};

      expect(() => metadata.primaryKeyGetter(entity)).toThrow(
        EntityMissingPrimaryKeyError,
      );
    });
  });
});
