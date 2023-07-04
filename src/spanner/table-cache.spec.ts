import { SpannerColumn } from './column.decorator.js';
import { SpannerTableCache } from './table-cache.js';
import { SpannerTable } from './table.decorator.js';

@SpannerTable({ name: 'MyCustomEntity', primaryKey: ['id'] })
class MyEntity {
  @SpannerColumn()
  id!: string;

  @SpannerColumn({ name: 'customName' })
  someName!: string;
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
        columns: ['id', 'customName'],
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
        `The definition of the Spanner entity class 'UndecoratedEntity' is not valid.`,
      );
    });
  });
});
