import {
  isString,
  registerDecorator,
  ValidationOptions,
} from 'class-validator';

/**
 * Checks whether the value is a valid Firestore document ID.
 * A document ID cannot be the literals `.` or `..`, and cannot contain forward slashes.
 * For more information, see https://firebase.google.com/docs/firestore/best-practices#document_ids.
 *
 * @param value The value to check.
 * @returns Whether the value is a valid Firestore document ID.
 */
export function isValidFirestoreId(value: unknown): value is string {
  if (!isString(value)) {
    return false;
  }

  if (value === '.' || value === '..') {
    return false;
  }

  if (value.includes('/')) {
    return false;
  }

  return true;
}

/**
 * Checks that the decorated property is a valid Firestore document ID.
 * A document ID cannot be the literals `.` or `..`, and cannot contain forward slashes.
 * For more information, see https://firebase.google.com/docs/firestore/best-practices#document_ids.
 *
 * @param options Validation options.
 */
export function IsValidFirestoreId(options: ValidationOptions = {}) {
  return function IsValidFirestoreIdDecorator(
    prototype: object,
    propertyName: string,
  ) {
    registerDecorator({
      name: 'isValidFirestoreId',
      target: prototype.constructor,
      propertyName,
      constraints: [],
      options,
      validator: {
        defaultMessage() {
          return `'${propertyName}' cannot be '.', '..', or contain forward slashes.`;
        },
        validate(value: unknown) {
          return isValidFirestoreId(value);
        },
      },
    });
  };
}
