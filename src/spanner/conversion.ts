import { PreciseDate } from '@google-cloud/precise-date';
import { Float, Int } from '@google-cloud/spanner';
import type { Type } from '@nestjs/common';
import { plainToInstance } from 'class-transformer';
import {
  type SpannerColumnMetadata,
  getSpannerColumnsMetadata,
} from './column.decorator.js';
import type { RecursivePartialEntity } from './types.js';

/**
 * Creates a typed class instance from an object returned by the Spanner API.
 *
 * @param spannerObject The object returned by Spanner that should be converted back to a class instance.
 * @param type The class for the object.
 * @param options Used for recursion and should not be set directly.
 * @returns The created object.
 */
function spannerObjectToInstanceWithOptions<T>(
  spannerObject: Record<string, any>,
  type: Type<T>,
  options: {
    /**
     * The prefix to add to column names when converting the object.
     */
    columnNamePrefix?: string;

    /**
     * Whether the returned instance should be `null` if all its properties are null.
     * Default to `false`.
     */
    nullifyInstance?: boolean;
  } = {},
): T | null {
  const columnsMetadata = getSpannerColumnsMetadata(type);
  const columnNamePrefix = options.columnNamePrefix ?? '';
  const plain: any = {};

  let hasAtLeastOneNonNullValue = false;
  Object.entries(columnsMetadata).forEach(([property, columnMetadata]) => {
    const columnName = `${columnNamePrefix}${columnMetadata.name}`;

    if (columnMetadata.nestedType) {
      plain[property] = spannerObjectToInstanceWithOptions(
        spannerObject,
        columnMetadata.nestedType,
        {
          columnNamePrefix: `${columnName}_`,
          nullifyInstance: columnMetadata.nullifyNested,
        },
      );
    } else if (columnMetadata.isJson) {
      plain[property] = spannerObject[columnName];
    } else if (Array.isArray(spannerObject[columnName])) {
      plain[property] = spannerObject[columnName].map((v: any) =>
        spannerValueToJavaScript(v, columnMetadata),
      );
    } else {
      plain[property] = spannerValueToJavaScript(
        spannerObject[columnName],
        columnMetadata,
      );
    }

    if (plain[property] != null) {
      hasAtLeastOneNonNullValue = true;
    }
  });

  return hasAtLeastOneNonNullValue || !options.nullifyInstance
    ? plainToInstance(type, plain)
    : null;
}

/**
 * Converts a scalar value returned by Spanner to a regular JavaScript value.
 * Numeric and date values are converted to their plain JavaScript counterparts.
 * Other types of values are left as is.
 *
 * @param value The Spanner value to process.
 * @param columnMetadata The metadata for the column in which the value is stored.
 * @returns The processed value.
 */
function spannerValueToJavaScript(
  value: any,
  columnMetadata: SpannerColumnMetadata,
): any {
  if (value instanceof Float) {
    // Floats are always safe and can always be unwrapped.
    return value.valueOf();
  } else if (value instanceof Int) {
    if (columnMetadata.isBigInt) {
      return BigInt(value.value);
    } else {
      // This unwraps the string to an integer number, but might throw if the integer is above what can be represented
      // safely as a float.
      return value.valueOf();
    }
  } else if (
    value instanceof Date &&
    // `PreciseDate` extends `Date`. This was previously used to handle conflicting versions of `PreciseDate`.
    value.constructor.name === PreciseDate.name &&
    !columnMetadata.isPreciseDate
  ) {
    // PreciseDate adds micro and nano seconds but leaves the `time` (up to milliseconds) unchanged.
    const time = value.getTime();
    return new Date(time);
  }

  return value;
}

/**
 * Creates a typed class instance from an object returned by the Spanner API.
 *
 * @param spannerObject The object returned by Spanner that should be converted back to a class instance.
 * @param type The class for the object.
 * @returns The created object.
 */
export function spannerObjectToInstance<T>(
  spannerObject: Record<string, any>,
  type: Type<T>,
): T {
  // This is okay as `null` can only be returned when the internal option `nullifyInstance` is set.
  return spannerObjectToInstanceWithOptions(spannerObject, type)!;
}

/**
 * Converts a value such that it is safe to pass to the Spanner client.
 * Numeric values and arrays of numeric values are wrapped using Spanner classes.
 * A column marked as JSON is stringified.
 *
 * @param value The value to process.
 * @param metadata The metadata for the column that has this value.
 * @returns The value that can be passed to the Spanner client.
 */
function makeSpannerValue(value: any, metadata: SpannerColumnMetadata): any {
  if (value === undefined || value === null) {
    return value;
  }

  if (metadata.isBigInt || metadata.isInt) {
    return Array.isArray(value)
      ? value.map((v) => new Int(v.toString()))
      : new Int(value.toString());
  } else if (metadata.isJson) {
    return JSON.stringify(value);
  } else if (typeof value === 'number') {
    return new Float(value);
  } else if (
    Array.isArray(value) &&
    value.length > 0 &&
    typeof value[0] === 'number'
  ) {
    return value.map((v) => new Float(v));
  }

  return value;
}

/**
 * Converts a class object to a plain JavaScript object that can be passed to the Spanner API.
 *
 * @param instance The object to convert to a Spanner object.
 * @param type The type of the object to convert.
 * @param columnNamePrefix The prefix to add to column names. Used for recursion and should not be set directly.
 * @returns A generic JavaScript object that can be passed to the Spanner API.
 */
export function instanceToSpannerObjectInternal<T>(
  instance: T | RecursivePartialEntity<T> | null,
  type: Type<T>,
  columnNamePrefix = '',
): Record<string, any> {
  const columnsMetadata = getSpannerColumnsMetadata(type);

  let spannerObject: Record<string, any> = {};

  Object.entries(columnsMetadata).forEach(([property, columnMetadata]) => {
    const columnName = `${columnNamePrefix}${columnMetadata.name}`;

    // When the current property value is `undefined`, column(s) values should not be set.
    if (instance !== null && (instance as any)[property] === undefined) {
      return;
    }

    // When the whole instance is `null`, all its child properties should be set to null.
    const propertyValue =
      instance === null ? null : (instance as any)[property];

    if (columnMetadata.nestedType) {
      spannerObject = {
        ...spannerObject,
        ...instanceToSpannerObjectInternal(
          propertyValue,
          columnMetadata.nestedType,
          `${columnName}_`,
        ),
      };
    } else {
      spannerObject[columnName] = makeSpannerValue(
        propertyValue,
        columnMetadata,
      );
    }
  });

  return spannerObject;
}

/**
 * Converts a class object to a plain JavaScript object that can be passed to the Spanner API.
 *
 * @param instance The object to convert to a Spanner object.
 * @param type The type of the object to convert.
 * @returns A generic JavaScript object that can be passed to the Spanner API.
 */
export function instanceToSpannerObject<T>(
  instance: T | RecursivePartialEntity<T>,
  type: Type<T>,
): Record<string, any> {
  return instanceToSpannerObjectInternal(instance, type);
}

/**
 * Copies an instance, recursively setting all columns that are not defined in the instance to `null`.
 * Columns with the {@link SpannerColumnMetadata.nullifyNested} option set to `true` are also set to `null`.
 *
 * @param instance The instance to copy.
 * @param type The type of the instance.
 * @returns The copied instance.
 */
export function copyInstanceWithMissingColumnsToNull<T>(
  instance: T | RecursivePartialEntity<T>,
  type: Type<T>,
): T {
  const columnsMetadata = getSpannerColumnsMetadata(type);

  const plain: any = {};

  Object.entries(columnsMetadata).forEach(([property, columnMetadata]) => {
    const instanceValue = instance == null ? null : (instance as any)[property];

    if (columnMetadata.nestedType) {
      if (columnMetadata.nullifyNested && instanceValue == null) {
        plain[property] = null;
      } else {
        plain[property] = copyInstanceWithMissingColumnsToNull(
          instanceValue,
          columnMetadata.nestedType,
        );
      }
    } else if (instanceValue !== undefined) {
      plain[property] = instanceValue;
    } else {
      plain[property] = null;
    }
  });

  return plainToInstance(type, plain);
}

/**
 * Recursively updates an instance with the values from the update.
 * Updates are applied "column-wise", which means that recursion stops at properties decorated as columns.
 * For example, JSON values are not affected.
 *
 * @param instance The instance to update. It should be a full, typed, instance, unless `type` is passed as well.
 * @param update The update to apply to the instance.
 * @param type The type of the instance. If not provided, it will be inferred from the instance.
 * @returns The updated instance.
 */
export function updateInstanceByColumn<T>(
  instance: T,
  update: RecursivePartialEntity<T>,
  type?: Type<T>,
): T {
  type ??= (instance as any).constructor as Type<T>;
  const columnsMetadata = getSpannerColumnsMetadata(type);

  const plain: any = {};

  Object.entries(columnsMetadata).forEach(([property, columnMetadata]) => {
    const instanceValue = instance == null ? null : (instance as any)[property];
    const updateValue = update == null ? update : (update as any)[property];

    if (updateValue === undefined) {
      plain[property] = instanceValue;
      return;
    }

    if (columnMetadata.nestedType) {
      if (columnMetadata.nullifyNested && updateValue === null) {
        plain[property] = null;
      } else {
        plain[property] = updateInstanceByColumn(
          instanceValue,
          updateValue,
          columnMetadata.nestedType,
        );
      }
    } else if (updateValue !== undefined) {
      plain[property] = updateValue;
    }
  });

  return plainToInstance(type, plain);
}
