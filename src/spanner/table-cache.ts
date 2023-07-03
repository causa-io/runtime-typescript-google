import { getSpannerColumns } from './column.decorator.js';
import { InvalidEntityDefinitionError } from './errors.js';
import { getSpannerTableMetadataFromType } from './table.decorator.js';

/**
 * An object providing metadata about a Spanner table and its columns.
 */
export type SpannerTableMetadata = {
  /**
   * The name of the table.
   */
  tableName: string;

  /**
   * The name of the table, quoted with backticks for use in queries.
   */
  quotedTableName: string;

  /**
   * The (ordered) list of columns that are part of the primary key.
   */
  primaryKeyColumns: string[];

  /**
   * The list of all columns in the table.
   */
  columns: string[];
};

/**
 * A cache storing the {@link SpannerTableMetadata} for each entity type (class).
 */
export class SpannerTableCache {
  /**
   * The cache of {@link SpannerTableMetadata} objects, where keys are entity types (class constructors).
   */
  private readonly cache: Map<{ new (): any }, SpannerTableMetadata> =
    new Map();

  /**
   * Builds the {@link SpannerTableMetadata} for the given entity type, by reading decorator metadata.
   *
   * @param entityType The entity type for which to build the metadata.
   * @returns The metadata.
   */
  private buildMetadata(entityType: { new (): any }): SpannerTableMetadata {
    const tableMetadata = getSpannerTableMetadataFromType(entityType);
    if (!tableMetadata) {
      throw new InvalidEntityDefinitionError(entityType);
    }

    const columns = getSpannerColumns(entityType);

    return {
      tableName: tableMetadata.name,
      quotedTableName: `\`${tableMetadata.name}\``,
      primaryKeyColumns: tableMetadata.primaryKey as string[],
      columns,
    };
  }

  /**
   * Returns the {@link SpannerTableMetadata} for the given entity type, either from the cache or by building it.
   *
   * @param entityType The entity type for which to get the metadata.
   * @returns The metadata.
   */
  getMetadata<T>(entityType: { new (): T }): SpannerTableMetadata {
    let metadata = this.cache.get(entityType);

    if (!metadata) {
      metadata = this.buildMetadata(entityType);
      this.cache.set(entityType, metadata);
    }

    return metadata;
  }
}
