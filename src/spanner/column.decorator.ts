import { PreciseDate } from '@google-cloud/precise-date';
import type { Type } from '@nestjs/common';
import { Transform } from 'class-transformer';
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

    const isPreciseDate = options.isPreciseDate ?? false;

    metadata[propertyKey] = {
      name: options.name ?? propertyKey,
      isInt: options.isInt ?? false,
      isBigInt: options.isBigInt ?? false,
      isPreciseDate,
      isJson: options.isJson ?? false,
      softDelete: options.softDelete ?? false,
    };

    Reflect.defineMetadata(
      SPANNER_COLUMN_METADATA_KEY,
      metadata,
      target.constructor,
    );

    if (isPreciseDate) {
      // `class-transformer` interferes with the `Date` type by copying the value to a new `Date` object if it is an
      // instance of `Date`. Unfortunately, `PreciseDate` inherits from `Date` therefore precision is lost during the
      // copy. `obj` must be used to retrieve the original value for the property. It is then copied manually. (Note
      // that the `PreciseDate` constructor itself also loses precision if passed a `Date` or `PreciseDate` object.)
      Transform(({ obj }) => {
        const value = obj[propertyKey];
        if (!value) {
          return value;
        }

        return new PreciseDate((value as PreciseDate).getFullTime());
      })(target, propertyKey);
    }
  };
}

/**
 * Retrieves metadata for all the columns defined in a class.
 *
 * @param classType The class for the table.
 * @returns The metadata for all columns of this table.
 */
export function getSpannerColumnsMetadata(
  classType: Type,
): SpannerColumnMetadataDictionary {
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
 * @returns The list of columns for the given class.
 */
export function getSpannerColumns(classType: Type): string[] {
  const columnsMetadata = getSpannerColumnsMetadata(classType);
  return Object.values(columnsMetadata).flatMap((c) => c.name);
}
