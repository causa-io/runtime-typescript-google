import { Inject } from '@nestjs/common';

/**
 * Returns a unique key that references a Firestore collection that should be injected.
 *
 * @param documentType The type of the document stored in the collection.
 * @returns The key.
 */
export function getFirestoreCollectionInjectionName(documentType: {
  new (): any;
}) {
  return `CAUSA_FIRESTORE_COLLECTION#${documentType.name}`;
}

/**
 * Decorates a parameter or property to specify the type of Firestore collection to inject.
 *
 * @param documentType The type of the document stored in the collection.
 */
export const InjectFirestoreCollection = (documentType: { new (): any }) =>
  Inject(getFirestoreCollectionInjectionName(documentType));
