import { VersionedEntity } from '@causa/runtime';
import { Type } from '@nestjs/common';
import 'reflect-metadata';

/**
 * The metadata for a Firestore collection in which the objects that are soft-deleted are moved to a separate
 * collection, and eventually deleted.
 */
export type SoftDeletedFirestoreCollectionMetadata = {
  /**
   * The delay (in milliseconds) after which the soft-deleted documents are permanently (hard) deleted.
   * This delay is computed from the `deletedAt` / `updatedAt` field of the document.
   * Default is 1 day.
   */
  expirationDelay: number;

  /**
   * The name of the field in the Firestore document that contains the timestamp at which it should be hard-deleted.
   * Default is `_expirationDate`.
   */
  expirationField: string;

  /**
   * The suffix to append to the name of the Firestore collection to create the name of the collection in which the
   * soft-deleted documents are moved.
   * Default is `$deleted`.
   */
  deletedDocumentsCollectionSuffix: string;
};

/**
 * The name of the metadata key used to store the soft-deleted Firestore collection metadata.
 */
const FIRESTORE_SOFT_DELETED_COLLECTION_METADATA_KEY =
  'firestoreSoftDeletedCollection';

/**
 * Defines this class as a type of Firestore document stored that can be soft-deleted.
 * Documents should be {@link VersionedEntity} that are considered soft-deleted when their `deletedAt` field is set.
 * When a document is soft-deleted, it is moved to a separate collection, and eventually deleted.
 *
 * @param metadata The metadata for the soft-deleted Firestore collection.
 */
export function SoftDeletedFirestoreCollection(
  metadata: Partial<SoftDeletedFirestoreCollectionMetadata> = {},
) {
  return (target: Type<VersionedEntity>) => {
    Reflect.defineMetadata(
      FIRESTORE_SOFT_DELETED_COLLECTION_METADATA_KEY,
      {
        expirationDelay: 24 * 60 * 60 * 1000,
        expirationField: '_expirationDate',
        deletedDocumentsCollectionSuffix: '$deleted',
        ...metadata,
      },
      target,
    );
  };
}

/**
 * Returns the metadata for the soft-deleted Firestore collection corresponding to the given class.
 *
 * @param documentType The type of document.
 * @returns The metadata for the soft-deleted Firestore collection, or `null` if the type is not decorated.
 */
export function getSoftDeletedFirestoreCollectionMetadataForType(
  documentType: Type,
): SoftDeletedFirestoreCollectionMetadata | null {
  const metadata = Reflect.getOwnMetadata(
    FIRESTORE_SOFT_DELETED_COLLECTION_METADATA_KEY,
    documentType,
  );

  return metadata ?? null;
}
