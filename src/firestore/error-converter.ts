import {
  EntityAlreadyExistsError,
  EntityNotFoundError,
  IncorrectEntityVersionError,
} from '@causa/runtime';
import { status } from '@grpc/grpc-js';
import { TemporaryFirestoreError } from './errors.js';

/**
 * Converts a Firestore error to an entity error or a {@link TemporaryFirestoreError}.
 * Entity errors do not provide details about the entity that caused the error, because it cannot be retrieved from the
 * gRPC errors.
 *
 * @param error The error thrown by Firestore.
 * @returns The converted error, or `undefined` if it could not be converted.
 */
export function convertFirestoreError(error: any): Error | undefined {
  switch (error.code) {
    case status.NOT_FOUND:
      return new EntityNotFoundError(null, null);
    case status.ALREADY_EXISTS:
      return new EntityAlreadyExistsError(null, null);
    case status.FAILED_PRECONDITION:
      return new IncorrectEntityVersionError(
        null,
        null,
        new Date(NaN),
        new Date(NaN),
      );
    // The following is based on the logic in the Firestore client:
    // https://github.com/googleapis/nodejs-firestore/blob/e598b9daf628cbc54dc10dab80bb0f46e2a3e2a2/dev/src/transaction.ts#L750
    case status.ABORTED:
    case status.CANCELLED:
    case status.UNKNOWN:
    case status.DEADLINE_EXCEEDED:
    case status.INTERNAL:
    case status.UNAVAILABLE:
    case status.UNAUTHENTICATED:
    case status.RESOURCE_EXHAUSTED:
      return new TemporaryFirestoreError(error.message, error.code);
    case status.INVALID_ARGUMENT:
      if (error.message.match(/transaction has expired/)) {
        return new TemporaryFirestoreError(error.message, error.code);
      }
      return;
    default:
      return;
  }
}

/**
 * Runs a Firestore operation and converts its errors to entity errors or {@link TemporaryFirestoreError}s.
 * Unrecognized errors are thrown as-is.
 *
 * @param operation The operation to wrap.
 * @returns The result of the operation.
 */
export async function wrapFirestoreOperation<T>(
  operation: () => Promise<T>,
): Promise<T> {
  try {
    return await operation();
  } catch (error) {
    throw convertFirestoreError(error) ?? error;
  }
}
