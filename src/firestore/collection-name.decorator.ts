/**
 * The name of the metadata key used to store the Firestore collection name.
 */
const FIRESTORE_COLLECTION_NAME_METADATA_KEY = 'firestoreCollectionName';

/**
 * Defines this class as a type of document stored in the given Firestore collection.
 *
 * @param collectionName The name of the Firestore collection.
 */
export function FirestoreCollectionName(collectionName: string) {
  return (target: any) => {
    Reflect.defineMetadata(
      FIRESTORE_COLLECTION_NAME_METADATA_KEY,
      collectionName,
      target,
    );
  };
}

/**
 * Returns the name of the Firestore collection corresponding to the given class.
 * Throws if the class is not decorated with {@link FirestoreCollectionName}.
 *
 * @param documentType The type of document.
 * @param prefix A prefix to prepend to the collection name.
 * @returns The name of the Firestore collection.
 */
export function getFirestoreCollectionNameForType(
  documentType: any,
  prefix = '',
): string {
  const collectionName = Reflect.getOwnMetadata(
    FIRESTORE_COLLECTION_NAME_METADATA_KEY,
    documentType,
  );

  if (!collectionName) {
    throw new Error(
      `Class '${documentType.name}' is not declared as a Firestore collection.`,
    );
  }

  return `${prefix}${collectionName}`;
}
