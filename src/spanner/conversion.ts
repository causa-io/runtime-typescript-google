import { PreciseDate } from '@google-cloud/precise-date';
import { Float, Int } from '@google-cloud/spanner';
import type { Type } from '@nestjs/common';
import { plainToInstance } from 'class-transformer';
import {
  type SpannerColumnMetadata,
  getSpannerColumnsMetadata,
} from './column.decorator.js';

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
  const columnsMetadata = getSpannerColumnsMetadata(type);

  const plain = Object.fromEntries(
    Object.entries(columnsMetadata).map(([property, columnMetadata]) => {
      const columnName = columnMetadata.name;
      const value = spannerObject[columnName];

      if (columnMetadata.isJson) {
        return [property, value];
      }

      if (Array.isArray(value)) {
        const values = value.map((v) =>
          spannerValueToJavaScript(v, columnMetadata),
        );
        return [property, values];
      }

      return [property, spannerValueToJavaScript(value, columnMetadata)];
    }),
  );

  return plainToInstance(type, plain);
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
  }

  if (value instanceof Int) {
    if (columnMetadata.isBigInt) {
      return BigInt(value.value);
    }

    // This unwraps the string to an integer number, but might throw if the integer is above what can be represented
    // safely as a float.
    return value.valueOf();
  }

  if (
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
 * Converts a number, bigint, or string value to a Spanner `Int`, throwing if the value is not a safe integer.
 *
 * @param value The value to convert.
 * @returns The Spanner `Int` value.
 */
function toSafeSpannerInt(value: unknown): Int {
  const valueType = typeof value;
  if (
    valueType !== 'number' &&
    valueType !== 'string' &&
    valueType !== 'bigint'
  ) {
    throw new TypeError(
      `Expected a number, bigint, or string, but received ${valueType}.`,
    );
  }

  const num = valueType !== 'number' ? Number(value) : value;
  if (!Number.isSafeInteger(num)) {
    throw new RangeError(
      'Value is not a safe integer for column marked as integer.',
    );
  }

  return new Int((value as any).toString());
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

  if (metadata.isBigInt) {
    return Array.isArray(value)
      ? value.map((v) => new Int(v.toString()))
      : new Int(value.toString());
  }

  if (metadata.isInt) {
    return Array.isArray(value)
      ? value.map(toSafeSpannerInt)
      : toSafeSpannerInt(value);
  }

  if (metadata.isJson) {
    return JSON.stringify(value);
  }

  if (typeof value === 'number') {
    return new Float(value);
  }

  if (
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
 * @returns A generic JavaScript object that can be passed to the Spanner API.
 */
export function instanceToSpannerObject<T>(
  instance: T | Partial<T>,
  type: Type<T>,
): Record<string, any> {
  const columnsMetadata = getSpannerColumnsMetadata(type);

  return Object.fromEntries(
    Object.entries(columnsMetadata)
      .map(([p, m]) => [m, (instance as any)[p]] as const)
      // When the current property value is `undefined`, the column value should not be set.
      .filter(([, v]) => v !== undefined)
      .map(([metadata, value]) => [
        metadata.name,
        makeSpannerValue(value, metadata),
      ]),
  );
}

/**
 * Copies an instance, setting all columns that are not defined in the instance to `null`.
 *
 * @param instance The instance to copy.
 * @param type The type of the instance.
 * @returns The copied instance.
 */
export function copyInstanceWithMissingColumnsToNull<T>(
  instance: T | Partial<T>,
  type: Type<T>,
): T {
  const columnsMetadata = getSpannerColumnsMetadata(type);

  const plain = Object.fromEntries(
    Object.keys(columnsMetadata).map((property) => {
      const value = (instance as any)[property];
      return [property, value === undefined ? null : value];
    }),
  );

  return plainToInstance(type, plain);
}

/**
 * Updates an instance with the values from the update.
 *
 * @param instance The instance to update. It should be a full, typed, instance, unless `type` is passed as well.
 * @param update The update to apply to the instance.
 * @param type The type of the instance. If not provided, it will be inferred from the instance.
 * @returns The updated instance.
 */
export function updateInstanceByColumn<T>(
  instance: T,
  update: Partial<T>,
  type?: Type<T>,
): T {
  type ??= (instance as any).constructor as Type<T>;
  const columnsMetadata = getSpannerColumnsMetadata(type);

  const plain = Object.fromEntries(
    Object.keys(columnsMetadata).map((property) => {
      const instanceValue = (instance as any)[property];
      const updateValue = (update as any)[property];
      return [
        property,
        updateValue === undefined ? instanceValue : updateValue,
      ];
    }),
  );

  return plainToInstance(type, plain);
}
