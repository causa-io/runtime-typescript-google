import type { Type } from '@nestjs/common';
import {
  getSpannerColumns,
  getSpannerColumnsMetadata,
} from './column.decorator.js';
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

  /**
   * The list of all columns in the table, quoted with backticks and joined for use in queries.
   */
  quotedColumns: string;

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
    const quotedTableName = `\`${tableName}\``;
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
    if (softDeleteColumns[0]?.nestedType) {
      throw new InvalidEntityDefinitionError(
        entityType,
        `Soft delete columns cannot be nested.`,
      );
    }
    const softDeleteColumn = softDeleteColumns[0]?.name ?? null;

    const columns = getSpannerColumns(entityType);
    const quotedColumns = columns.map((c) => `\`${c}\``).join(', ');

    return {
      tableName,
      quotedTableName,
      primaryKeyColumns,
      columns,
      quotedColumns,
      softDeleteColumn,
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
