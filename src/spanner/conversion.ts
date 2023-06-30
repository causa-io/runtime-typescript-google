import { PreciseDate } from '@google-cloud/precise-date';
import { Float, Int } from '@google-cloud/spanner';
import {
  SpannerColumnMetadata,
  getSpannerColumnsMetadata,
} from './column.decorator.js';
import { RecursivePartialEntity } from './types.js';

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
  type: { new (): T },
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
  const instance: any = new type();

  let hasAtLeastOneNonNullValue = false;
  Object.entries(columnsMetadata).forEach(([property, columnMetadata]) => {
    const columnName = `${columnNamePrefix}${columnMetadata.name}`;

    if (columnMetadata.nestedType) {
      instance[property] = spannerObjectToInstanceWithOptions(
        spannerObject,
        columnMetadata.nestedType,
        {
          columnNamePrefix: `${columnName}_`,
          nullifyInstance: columnMetadata.nullifyNested,
        },
      );
    } else if (columnMetadata.isJson) {
      instance[property] = spannerObject[columnName];
    } else if (Array.isArray(spannerObject[columnName])) {
      instance[property] = spannerObject[columnName].map((v: any) =>
        spannerValueToJavaScript(v, columnMetadata),
      );
    } else {
      instance[property] = spannerValueToJavaScript(
        spannerObject[columnName],
        columnMetadata,
      );
    }

    if (instance[property] != null) {
      hasAtLeastOneNonNullValue = true;
    }
  });

  return hasAtLeastOneNonNullValue || !options.nullifyInstance
    ? instance
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
    // `PreciseDate` extends `Date`. This was previously used to handle conflicting versions of `PreciseDate.
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
  type: { new (): T },
): T {
  // This is okay as `null` can only be returned when the internal option `nullifyInstance` is set.
  // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
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
  type: { new (): T },
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
  type: { new (): T },
): Record<string, any> {
  return instanceToSpannerObjectInternal(instance, type);
}
