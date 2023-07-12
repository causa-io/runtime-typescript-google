import 'reflect-metadata';

/**
 * The metadata key used to store the Spanner column information.
 */
const SPANNER_COLUMN_METADATA_KEY = 'CAUSA_SPANNER_COLUMNS';

/**
 * Metadata for a single column/property of a Spanner table.
 */
export type SpannerColumnMetadata = {
  /**
   * The name of the Spanner column for this property.
   */
  name: string;

  /**
   * If the type of the property is a nested type, this is the class for this property.
   */
  nestedType?: { new (): any };

  /**
   * When `true`, sets the property to null if all properties is the nested object are null.
   * Defaults to `false`.
   */
  nullifyNested: boolean;

  /**
   * When `true`, the column is assumed to be of type `INT64` and the value will be safely stored in a `bigint`.
   */
  isBigInt: boolean;

  /**
   * When `true`, the column is assumed to be of type `INT64` but the type of the property is a JavaScript `number`.
   */
  isInt: boolean;

  /**
   * When `true`, the `PreciseDate` returned by Spanner will be passed instead of converting it to a `Date`.
   */
  isPreciseDate: boolean;

  /**
   * When `true`, the column is of JSON type and will be stringified before being sent to Spanner.
   */
  isJson: boolean;

  /**
   * If `true`, declares this column as a soft delete column, which has a truthy value when the row is (soft) deleted.
   * It cannot be a column in a nested object.
   */
  softDelete: boolean;
};

/**
 * A dictionary of column metadata, where keys are class property names and values are the metadata.
 */
export type SpannerColumnMetadataDictionary = Record<
  string,
  SpannerColumnMetadata
>;

/**
 * Defines this property as a column in the Spanner table for the class.
 *
 * @param options Options that applies to the column for this property.
 */
export function SpannerColumn(options: Partial<SpannerColumnMetadata> = {}) {
  return (target: any, propertyKey: string) => {
    const metadata = getSpannerColumnsMetadata(target.constructor);

    metadata[propertyKey] = {
      name: options.name ?? propertyKey,
      nestedType: options.nestedType,
      nullifyNested: options.nullifyNested ?? false,
      isInt: options.isInt ?? false,
      isBigInt: options.isBigInt ?? false,
      isPreciseDate: options.isPreciseDate ?? false,
      isJson: options.isJson ?? false,
      softDelete: options.softDelete ?? false,
    };

    Reflect.defineMetadata(
      SPANNER_COLUMN_METADATA_KEY,
      metadata,
      target.constructor,
    );
  };
}

/**
 * Retrieves metadata for all the columns defined in a class.
 *
 * @param classType The class for the table.
 * @returns The metadata for all columns of this table.
 */
export function getSpannerColumnsMetadata(classType: {
  new (): any;
}): SpannerColumnMetadataDictionary {
  return (
    Reflect.getOwnMetadata(SPANNER_COLUMN_METADATA_KEY, classType) ?? {
      ...Reflect.getMetadata(SPANNER_COLUMN_METADATA_KEY, classType),
    }
  );
}

/**
 * Lists all the columns in the given class, based on {@link SpannerColumn} decorators.
 *
 * @param classType The type for the table.
 * @param namePrefix Used for recursion with nested types, do not use directly.
 * @returns The list of columns for the given class.
 */
export function getSpannerColumns(
  classType: { new (): any },
  namePrefix = '',
): string[] {
  const columnsMetadata = getSpannerColumnsMetadata(classType);

  return Object.values(columnsMetadata).flatMap((c) =>
    c.nestedType
      ? getSpannerColumns(c.nestedType, `${c.name}_`)
      : `${namePrefix}${c.name}`,
  );
}
