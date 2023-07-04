import 'reflect-metadata';

/**
 * The metadata key used to store the Spanner table information.
 */
const SPANNER_TABLE_METADATA_KEY = 'CAUSA_SPANNER_TABLE';

/**
 * Metadata for a Spanner table.
 */
export interface SpannerTableMetadata {
  /**
   * The name of the Spanner table.
   */
  name: string;

  /**
   * The (ordered) list of columns in the class defining the primary key.
   */
  primaryKey: string[];
}

/**
 * Defines the decorated class as a Spanner table.
 *
 * @param metadata Options that applies to the Spanner table represented by this class.
 */
export function SpannerTable(
  metadata: Partial<SpannerTableMetadata> &
    Pick<SpannerTableMetadata, 'primaryKey'>,
) {
  return (target: { new (): any }) => {
    const value: SpannerTableMetadata = {
      name: target.name,
      ...metadata,
    };

    Reflect.defineMetadata(SPANNER_TABLE_METADATA_KEY, value, target);
  };
}

/**
 * Returns the Spanner metadata for the table associated with a class.
 *
 * @param tableType The class for the table.
 * @returns The metadata for the Spanner table, or `null` if the class is not decorated.
 */
export function getSpannerTableMetadataFromType(tableType: {
  new (): any;
}): SpannerTableMetadata | null {
  return Reflect.getOwnMetadata(SPANNER_TABLE_METADATA_KEY, tableType) ?? null;
}
