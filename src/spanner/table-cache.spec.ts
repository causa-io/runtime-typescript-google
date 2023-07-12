import { SpannerColumn } from './column.decorator.js';
import { InvalidEntityDefinitionError } from './errors.js';
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
        quotedTableName: '`MyCustomEntity`',
        primaryKeyColumns: ['id'],
        columns: ['id', 'customName', 'deletedAt'],
        quotedColumns: '`id`, `customName`, `deletedAt`',
        softDeleteColumn: 'deletedAt',
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
        quotedTableName: '`MyEntity`',
        primaryKeyColumns: ['id'],
        columns: ['id'],
        quotedColumns: '`id`',
        softDeleteColumn: null,
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

    it('should throw if the soft delete column is nested', () => {
      @SpannerTable({ primaryKey: ['id'] })
      class MyEntity {
        @SpannerColumn()
        id!: string;

        @SpannerColumn({ softDelete: true, nestedType: Date })
        deletedAt!: Date | null;
      }

      expect(() => cache.getMetadata(MyEntity)).toThrow(
        InvalidEntityDefinitionError,
      );
    });
  });
});
