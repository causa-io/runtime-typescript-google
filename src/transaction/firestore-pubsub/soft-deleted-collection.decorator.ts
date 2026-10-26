import type { VersionedEntity } from '@causa/runtime';
import type { Type } from '@nestjs/common';
import 'reflect-metadata';
import { makeFirestoreDataConverter } from '../../firestore/index.js';

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
 * Information about soft-deletion for a given document.
 */
export type SoftDeleteInfo<T extends object> = Omit<
  SoftDeletedFirestoreCollectionMetadata,
  'deletedDocumentsCollectionSuffix'
> & {
  /**
   * The reference to the soft-deleted document.
   */
  ref: FirebaseFirestore.DocumentReference<T>;
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

/**
 * Returns the soft-delete information for a given document, if the document type supports soft-deletion.
 *
 * @param activeDocRef The reference to the active document.
 * @param type The type of the document.
 * @returns The soft-delete information for the document, or `null` if the document type does not support
 *   soft-deletion.
 */
export function getSoftDeleteInfo<T extends object>(
  activeDocRef: FirebaseFirestore.DocumentReference<T>,
  type: Type<T>,
): SoftDeleteInfo<T> | null {
  const softDeleteMetadata =
    getSoftDeletedFirestoreCollectionMetadataForType(type);
  if (!softDeleteMetadata) {
    return null;
  }

  const { deletedDocumentsCollectionSuffix: suffix, ...info } =
    softDeleteMetadata;
  const softDeleteCollection = activeDocRef.firestore
    .collection(`${activeDocRef.parent.path}${suffix}`)
    .withConverter(makeFirestoreDataConverter(type));
  const ref = softDeleteCollection.doc(activeDocRef.id);

  return { ...info, ref };
}

/**
 * Whether the active and soft-deleted versions of a document exist, as observed in a transaction.
 */
export type SoftDeletableDocumentState = {
  /**
   * Whether the document exists in the active collection.
   */
  readonly active: boolean;

  /**
   * Whether the document exists in the soft-deleted collection.
   * This is `undefined` if the soft-deleted document was not read.
   */
  readonly deleted?: boolean;
};

/**
 * Options when reading a document that may be soft-deleted.
 */
export type SoftDeletableDocumentReadOptions = {
  /**
   * Whether the soft-deleted document should only be read if the active document does not exist, in a second request.
   * By default, both documents are read in a single request.
   */
  readonly sequentialSoftDeleteLookups?: boolean;
};

/**
 * Reads a document in a transaction, from either the active or the soft-deleted collection.
 * For types supporting soft-deletion, both documents are read in a single request, unless
 * {@link SoftDeletableDocumentReadOptions.sequentialSoftDeleteLookups} is set.
 *
 * @param transaction The Firestore transaction to use.
 * @param activeDocRef The reference to the active document.
 * @param type The type of the document.
 * @param options Options when reading the document.
 * @returns The document (or `null` if it does not exist), and the {@link SoftDeletableDocumentState} if the document
 *   type supports soft-deletion.
 */
export async function getSoftDeletableDocument<T extends object>(
  transaction: FirebaseFirestore.Transaction,
  activeDocRef: FirebaseFirestore.DocumentReference<T>,
  type: Type<T>,
  options: SoftDeletableDocumentReadOptions = {},
): Promise<{
  document: T | null;
  state: SoftDeletableDocumentState | null;
}> {
  const softDeleteInfo = getSoftDeleteInfo(activeDocRef, type);
  if (!softDeleteInfo) {
    const snapshot = await transaction.get(activeDocRef);
    return { document: snapshot.data() ?? null, state: null };
  }

  let activeSnapshot: FirebaseFirestore.DocumentSnapshot<T>;
  let deletedSnapshot: FirebaseFirestore.DocumentSnapshot<T> | undefined;
  if (options.sequentialSoftDeleteLookups) {
    activeSnapshot = await transaction.get(activeDocRef);
    if (!activeSnapshot.exists) {
      deletedSnapshot = await transaction.get(softDeleteInfo.ref);
    }
  } else {
    [activeSnapshot, deletedSnapshot] = await transaction.getAll(
      activeDocRef,
      softDeleteInfo.ref,
    );
  }
  const state = {
    active: activeSnapshot.exists,
    deleted: deletedSnapshot?.exists,
  };

  const activeDocument = activeSnapshot.data();
  if (activeDocument) {
    return { document: activeDocument, state };
  }

  const deletedDocument = deletedSnapshot?.data();
  if (!deletedDocument) {
    return { document: null, state };
  }

  delete (deletedDocument as any)[softDeleteInfo.expirationField];
  return { document: deletedDocument, state };
}
