import type { Type } from '@nestjs/common';
import type { CollectionReference } from 'firebase-admin/firestore';
import type { SoftDeletedFirestoreCollectionMetadata } from './soft-deleted-collection.decorator.js';

/**
 * The Firestore collections that should be used to create document references for a given document type.
 */
export type FirestoreCollectionsForDocumentType<T> = {
  /**
   * The regular collection, where documents are stored when they are not deleted.
   */
  readonly activeCollection: CollectionReference<T>;

  /**
   * Configuration about the soft-delete collection, where documents are stored when their `deletedAt` field is not
   * `null`. This can be `null` if the document type does not declare a soft-delete collection.
   *
   * @deprecated Use `SoftDeleteInfo` in `FirestoreReadOnlyStateTransaction.getSoftDeleteInfo` instead.
   */
  readonly softDelete:
    | ({
        /**
         * The collection where soft-deleted documents are stored.
         */
        collection: CollectionReference<T>;
      } & Pick<
        SoftDeletedFirestoreCollectionMetadata,
        'expirationDelay' | 'expirationField'
      >)
    | null;
};

/**
 * A resolver that returns the Firestore collections for a given document type.
 * This allows using the {@link FirestoreStateTransaction} in various contexts, such as in a NestJS module. It also
 * enables testing with temporary collections.
 */
export interface FirestoreCollectionResolver {
  /**
   * Returns the Firestore collections for a given document type.
   *
   * @param documentType The type of document.
   * @returns The Firestore collections for the given document type.
   */
  getCollectionsForType<T>(
    documentType: Type<T>,
  ): FirestoreCollectionsForDocumentType<T>;
}
