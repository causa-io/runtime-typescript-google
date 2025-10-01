import type { Type } from '@nestjs/common';
import { getSpannerColumnsMetadata } from './column.decorator.js';
import { InvalidEntityDefinitionError } from './errors.js';
import { getSpannerTableMetadataFromType } from './table.decorator.js';

/**
 * An object providing metadata about a Spanner table and its columns.
 */
export type CachedSpannerTableMetadata = {
  /**
   * The name of the table.
   */
  tableName: string;

  /**
   * The (ordered) list of columns that are part of the primary key.
   */
  primaryKeyColumns: string[];

  /**
   * A map from class property names to Spanner column names.
   */
  columnNames: Record<string, string>;

  /**
   * The name of the column used to mark soft deletes, if any.
   */
  softDeleteColumn: string | null;
};

/**
 * A cache storing the {@link CachedSpannerTableMetadata} for each entity type (class).
 */
export class SpannerTableCache {
  /**
   * The cache of {@link CachedSpannerTableMetadata} objects, where keys are entity types (class constructors).
   */
  private readonly cache: Map<Type, CachedSpannerTableMetadata> = new Map();

  /**
   * Builds the {@link CachedSpannerTableMetadata} for the given entity type, by reading decorator metadata.
   *
   * @param entityType The entity type for which to build the metadata.
   * @returns The metadata.
   */
  private buildMetadata(entityType: Type): CachedSpannerTableMetadata {
    const tableMetadata = getSpannerTableMetadataFromType(entityType);
    if (!tableMetadata) {
      throw new InvalidEntityDefinitionError(entityType);
    }

    const tableName = tableMetadata.name;
    const primaryKeyColumns = tableMetadata.primaryKey;

    const columnsMetadata = getSpannerColumnsMetadata(entityType);
    const softDeleteColumns = Object.values(columnsMetadata).filter(
      (metadata) => metadata.softDelete,
    );
    if (softDeleteColumns.length > 1) {
      throw new InvalidEntityDefinitionError(
        entityType,
        `Only one column can be marked as soft delete.`,
      );
    }
    const softDeleteColumn = softDeleteColumns[0]?.name ?? null;

    const columnNames = Object.fromEntries(
      Object.entries(columnsMetadata).map(([prop, { name }]) => [prop, name]),
    );

    return {
      tableName,
      primaryKeyColumns,
      softDeleteColumn,
      columnNames,
    };
  }

  /**
   * Returns the {@link CachedSpannerTableMetadata} for the given entity type, either from the cache or by building it.
   *
   * @param entityType The entity type for which to get the metadata.
   * @returns The metadata.
   */
  getMetadata(entityType: Type): CachedSpannerTableMetadata {
    let metadata = this.cache.get(entityType);

    if (!metadata) {
      metadata = this.buildMetadata(entityType);
      this.cache.set(entityType, metadata);
    }

    return metadata;
  }
}
