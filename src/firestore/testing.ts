import { CollectionReference, Firestore } from 'firebase-admin/firestore';
import * as uuid from 'uuid';

/**
 * Creates a new collection suffixed with a random ID.
 *
 * @param firestore The {@link Firestore} instance to use.
 * @param prefix A human-readable prefix for the collection.
 * @returns The {@link CollectionReference} of the created collection.
 */
export function createFirestoreTemporaryCollection(
  firestore: Firestore,
  prefix = 'test',
): CollectionReference {
  const collectionName = `${prefix}-${uuid.v4().slice(-10)}`;
  return firestore.collection(collectionName);
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
