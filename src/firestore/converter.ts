import type { Type } from '@nestjs/common';
import { instanceToPlain, plainToInstance } from 'class-transformer';
import {
  type FirestoreDataConverter,
  type PartialWithFieldValue,
  QueryDocumentSnapshot,
  Timestamp,
} from 'firebase-admin/firestore';

/**
 * The cache of converters, ensuring a single converter instance exists per document type.
 */
const converters = new Map<Type, FirestoreDataConverter<any>>();

/**
 * Makes a converter that transforms Firestore data from and to the given class type.
 *
 * @returns The converter.
 */
export function makeFirestoreDataConverter<T>(
  classType: Type<T>,
): FirestoreDataConverter<T> {
  const existing = converters.get(classType);
  if (existing) {
    return existing;
  }

  const converter = {
    toFirestore: (data: PartialWithFieldValue<T>) => instanceToPlain(data),
    fromFirestore: (snapshot: QueryDocumentSnapshot) => {
      const converted = convertFirestoreTimestampsToDates(snapshot.data());
      const typed = plainToInstance(classType, converted);
      return typed;
    },
  };
  converters.set(classType, converter);

  return converter;
}

/**
 * Recursively converts Firestore's {@link Timestamp}s to `Date`s in the given object.
 * Arrays and plain objects are modified in place. Other objects (e.g. `GeoPoint`s) are copied to plain objects.
 * The same type is defined as input and output to this function, even though `Date` types are assumed to be
 * {@link Timestamp}s in the input object.
 *
 * @param obj The object in which {@link Timestamp}s should be recursively converted.
 * @returns The object with `Date`s, which is the input object itself if it is an array or a plain object.
 */
export function convertFirestoreTimestampsToDates<T>(obj: T): T {
  if (!obj || typeof obj !== 'object') {
    return obj;
  }

  if (obj instanceof Timestamp) {
    return obj.toDate() as unknown as T;
  }

  if (obj instanceof Array) {
    for (let i = 0; i < obj.length; i++) {
      obj[i] = convertFirestoreTimestampsToDates(obj[i]);
    }
    return obj;
  }

  const converted: Record<string, any> =
    Object.getPrototypeOf(obj) === Object.prototype ? obj : {};
  for (const key of Object.keys(obj)) {
    converted[key] = convertFirestoreTimestampsToDates((obj as any)[key]);
  }

  return converted as T;
}
