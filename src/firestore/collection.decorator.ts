import type { Type } from '@nestjs/common';
import {
  CollectionReference,
  DocumentReference,
  Firestore,
} from 'firebase-admin/firestore';
import 'reflect-metadata';
import { makeFirestoreDataConverter } from './converter.js';

/**
 * The name of the metadata key used to store the Firestore collection metadata.
 */
const FIRESTORE_COLLECTION_METADATA_KEY = 'firestoreCollection';

/**
 * The metadata for a Firestore collection.
 */
export type FirestoreCollectionMetadata<T> = {
  /**
   * Returns the full path of the document from the root of the database, as an array of segments.
   * The array must have an even number of segments, alternating between collection names and document IDs, and ending
   * with the document's ID.
   * Segments can be `undefined` or `null` when they cannot be computed from the given partial document, in which case
   * obtaining a reference for the document will throw an error.
   *
   * @param document The (partial) document for which the path should be returned.
   * @returns The path of the document, as an array of segments.
   */
  path: (document: Partial<T>) => (string | undefined | null)[];
};

/**
 * Defines this class as a type of document stored in a Firestore collection.
 *
 * @param metadata The metadata for the Firestore collection.
 */
export function FirestoreCollection<T>(
  metadata: FirestoreCollectionMetadata<T>,
) {
  return (target: Type<T>) => {
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
export function getFirestoreCollectionMetadataForType<T>(
  documentType: Type<T>,
): FirestoreCollectionMetadata<T> {
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

/**
 * Returns the path segments for the given document, validating their number.
 *
 * @param documentType The type of the document.
 * @param document The (partial) document for which the path segments should be returned.
 * @returns The path segments for the document.
 */
function getPathSegmentsForFirestoreDocument<T>(
  documentType: Type<T>,
  document: Partial<T>,
): (string | undefined | null)[] {
  const { path } = getFirestoreCollectionMetadataForType(documentType);

  const segments = path(document);
  if (segments.length < 2 || segments.length % 2 !== 0) {
    throw new Error(
      `The path of the '${documentType.name}' document should have an even number of at least 2 segments.`,
    );
  }

  return segments;
}

/**
 * Returns the reference for the Firestore document corresponding to the given object.
 * If {@link FirestoreCollectionMetadata.path} returns any nullish or empty segment for the given object, an error is
 * thrown.
 *
 * @param firestore The {@link Firestore} instance to use.
 * @param document The (partial) document for which the reference should be returned.
 * @param documentType The type of the document. This can be omitted if the `document` is not a partial document.
 * @returns The reference for the Firestore document corresponding to the given object.
 */
export function getReferenceForFirestoreDocument<T>(
  firestore: Firestore,
  document: T | Partial<T>,
  documentType?: Type<T>,
): DocumentReference<T> {
  documentType ??= (document as any).constructor as Type<T>;
  const segments = getPathSegmentsForFirestoreDocument(documentType, document);

  if (segments.some((s) => !s)) {
    throw new Error(
      `The path of the '${documentType.name}' document cannot be obtained from the given object.`,
    );
  }

  return firestore
    .doc(segments.join('/'))
    .withConverter(makeFirestoreDataConverter(documentType));
}

/**
 * Returns the Firestore collection in which documents of the given type are stored.
 * For documents stored in nested collections, the (partial) `document` must be provided such that the IDs of the
 * parent documents can be computed.
 *
 * @param firestore The {@link Firestore} instance to use.
 * @param documentType The type of the document.
 * @param document The (partial) document used to compute the collection path.
 *   This can be omitted for documents stored in root collections.
 * @returns The {@link CollectionReference} for the given document type.
 */
export function getFirestoreCollection<T>(
  firestore: Firestore,
  documentType: Type<T>,
  document: Partial<T> = {},
): CollectionReference<T> {
  const segments = getPathSegmentsForFirestoreDocument(documentType, document);

  const collectionSegments = segments.slice(0, -1);
  if (collectionSegments.some((s) => !s)) {
    throw new Error(
      `The collection path of the '${documentType.name}' document cannot be obtained from the given object.`,
    );
  }

  return firestore
    .collection(collectionSegments.join('/'))
    .withConverter(makeFirestoreDataConverter(documentType));
}
