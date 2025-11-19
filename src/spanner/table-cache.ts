import { Float, Float32, Int, Numeric } from '@google-cloud/spanner';
import type { Type } from '@nestjs/common';
import { getSpannerColumnsMetadata } from './column.decorator.js';
import {
  EntityMissingPrimaryKeyError,
  InvalidEntityDefinitionError,
} from './errors.js';
import { getSpannerTableMetadataFromType } from './table.decorator.js';
import type { SpannerKey } from './types.js';

/**
 * A function that, given an entity, returns its primary key as a {@link SpannerKey}.
 */
type SpannerPrimaryKeyGetter = (entity: any) => SpannerKey;

/**
 * An object providing metadata about a Spanner table and its columns.
 */
export type CachedSpannerTableMetadata = {
  /**
   * The name of the table.
   */
  tableName: string;

  /**
   * The list of columns that are used to compute the primary key.
   * For primary keys with generated columns, this includes the columns off of which the key is composed.
   */
  primaryKeyColumns: string[];

  /**
   * A function that, given an entity, returns its primary key as a {@link SpannerKey}.
   */
  primaryKeyGetter: SpannerPrimaryKeyGetter;

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
 * Constructs a {@link SpannerPrimaryKeyGetter} for the given property paths.
 *
 * @param paths The paths to resolve in the entity to get primary key values.
 * @returns The {@link SpannerPrimaryKeyGetter}.
 */
function makePrimaryKeyGetter(paths: string[][]): SpannerPrimaryKeyGetter {
  return (entity) =>
    paths.map((path): SpannerKey[number] => {
      const value = path.reduce<any>((v, k) => v?.[k], entity);

      if (value === undefined) {
        throw new EntityMissingPrimaryKeyError();
      }

      if (
        value instanceof Int ||
        value instanceof Float ||
        value instanceof Float32 ||
        value instanceof Numeric
      ) {
        return value.value.toString();
      }

      if (value instanceof Date) {
        return value.toJSON();
      }

      if (typeof value === 'bigint' || typeof value === 'number') {
        return value.toString();
      }

      return value;
    });
}

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

    const primaryKeyColumnsSet = new Set<string>();
    const primaryPropertyPaths = tableMetadata.primaryKey.map((path) => {
      const [columnName, ...nested] = path.split('.');
      primaryKeyColumnsSet.add(columnName);

      const property = Object.entries(columnNames).find(
        ([, name]) => name === columnName,
      );
      if (!property) {
        throw new InvalidEntityDefinitionError(
          entityType,
          `Primary key column '${columnName}' does not exist.`,
        );
      }

      return [property[0], ...nested];
    });

    const primaryKeyColumns = [...primaryKeyColumnsSet];
    const primaryKeyGetter = makePrimaryKeyGetter(primaryPropertyPaths);

    return {
      tableName,
      primaryKeyColumns,
      primaryKeyGetter,
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
