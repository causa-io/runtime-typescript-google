import { CollectionReference, Firestore } from 'firebase-admin/firestore';
import * as uuid from 'uuid';
import { getFirestoreCollectionNameForType } from './collection-name.decorator.js';
import { makeFirestoreDataConverter } from './converter.js';

/**
 * Creates a new collection prefixed with a random ID.
 *
 * @param firestore The {@link Firestore} instance to use.
 * @param documentType The type of the document stored in the collection.
 *   It should be decorated with `FirestoreCollectionName`.
 * @returns The {@link CollectionReference} of the created collection.
 */
export function createFirestoreTemporaryCollection<T>(
  firestore: Firestore,
  documentType: { new (): T },
): CollectionReference<T> {
  const prefix = `${uuid.v4().slice(-10)}-`;
  return firestore
    .collection(getFirestoreCollectionNameForType(documentType, prefix))
    .withConverter(makeFirestoreDataConverter(documentType));
}

/**
 * Clears a Firestore collection of all its documents.
 *
 * @param firestore The {@link Firestore} instance to use.
 * @param collectionRef The reference to the collection that should be cleared.
 */
export async function clearFirestoreCollection(
  firestore: Firestore,
  collectionRef: CollectionReference,
): Promise<void> {
  const batch = firestore.batch();

  const documents = await collectionRef.listDocuments();

  documents.forEach((document) => batch.delete(document));

  await batch.commit();
}
