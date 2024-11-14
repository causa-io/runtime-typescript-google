import type { Type } from '@nestjs/common';
import { instanceToPlain, plainToInstance } from 'class-transformer';
import {
  type FirestoreDataConverter,
  type PartialWithFieldValue,
  QueryDocumentSnapshot,
  Timestamp,
} from 'firebase-admin/firestore';

/**
 * Makes a converter that transforms Firestore data from and to the given class type.
 *
 * @returns The converter.
 */
export function makeFirestoreDataConverter<T>(
  classType: Type<T>,
): FirestoreDataConverter<T> {
  return {
    toFirestore: (data: PartialWithFieldValue<T>) => instanceToPlain(data),
    fromFirestore: (snapshot: QueryDocumentSnapshot) => {
      const data = snapshot.data();
      const converted = convertFirestoreTimestampsToDates(data);
      const typed = plainToInstance(classType, converted);
      return typed;
    },
  };
}

/**
 * Recursively converts Firestore's {@link Timestamp}s to `Date`s in the given object.
 * The same type is defined as input and output to this function, even though `Date` types are assumed to be
 * {@link Timestamp}s in the input object.
 *
 * @param obj The object in which {@link Timestamp}s should be recursively converted.
 * @returns The object with `Date`s.
 */
export function convertFirestoreTimestampsToDates<T>(obj: T): T {
  if (!obj) {
    return obj;
  }

  if (typeof obj !== 'object') {
    return obj as unknown as T;
  }

  if (obj instanceof Array) {
    return obj.map(convertFirestoreTimestampsToDates) as unknown as T;
  }

  if (obj instanceof Timestamp) {
    return obj.toDate() as unknown as T;
  }

  const serialized: Record<string, any> = {};

  Object.entries(obj).forEach(([key, value]) => {
    serialized[key] = convertFirestoreTimestampsToDates(value);
  });

  return serialized as unknown as T;
}
