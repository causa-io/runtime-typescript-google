import 'reflect-metadata';

/**
 * The name of the metadata key used to store the Firestore collection metadata.
 */
const FIRESTORE_COLLECTION_METADATA_KEY = 'firestoreCollection';

/**
 * The metadata for a Firestore collection.
 */
export type FirestoreCollectionMetadata<T> = {
  /**
   * The name of the Firestore collection.
   */
  name: string;

  /**
   * Returns the path of the document (relative to the collection) for the given partial document.
   *
   * @param document The (partial) document for which the path should be returned.
   * @returns The path of the document (relative to the collection), or `undefined` if the path cannot be derived from
   *   the partial document.
   */
  path: (document: Partial<T>) => string | undefined;
};

/**
 * Defines this class as a type of document stored in the given Firestore collection.
 *
 * @param metadata The metadata for the Firestore collection.
 */
export function FirestoreCollection<T>(
  metadata: FirestoreCollectionMetadata<T>,
) {
  return (target: { new (): T }) => {
    Reflect.defineMetadata(FIRESTORE_COLLECTION_METADATA_KEY, metadata, target);
  };
}

/**
 * Returns the metadata for the Firestore collection corresponding to the given class.
 * Throws if the class is not decorated with {@link FirestoreCollection}.
 *
 * @param documentType The type of document.
 * @returns The metadata for the Firestore collection.
 */
export function getFirestoreCollectionMetadataForType<T>(documentType: {
  new (): T;
}): FirestoreCollectionMetadata<T> {
  const metadata = Reflect.getOwnMetadata(
    FIRESTORE_COLLECTION_METADATA_KEY,
    documentType,
  );

  if (!metadata) {
    throw new Error(
      `Class '${documentType.name}' is not declared as a Firestore collection.`,
    );
  }

  return metadata;
}
