import type {
  ReadOnlyStateTransaction,
  ReadOnlyTransactionOption,
} from '@causa/runtime';
import type { Type } from '@nestjs/common';
import { Transaction } from 'firebase-admin/firestore';
import {
  getReferenceForFirestoreDocument,
  makeFirestoreDataConverter,
} from '../../firestore/index.js';
import {
  getSoftDeletedFirestoreCollectionMetadataForType,
  type SoftDeletedFirestoreCollectionMetadata,
} from './soft-deleted-collection.decorator.js';
import type { FirestoreCollectionResolver } from './types.js';

/**
 * Option for a function that accepts a {@link FirestoreReadOnlyStateTransaction}.
 */
export type FirestoreReadOnlyStateTransactionOption =
  ReadOnlyTransactionOption<FirestoreReadOnlyStateTransaction>;

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
 * A {@link ReadOnlyStateTransaction} that uses Firestore for state storage.
 *
 * This transaction handles soft-deleted documents if the class is decorated with `SoftDeletedFirestoreCollection`,
 * which means that documents with a `deletedAt` field set to a non-null value are considered deleted.
 *
 * {@link FirestoreStateTransaction.get} will return the document from either the regular or soft-delete collection, as
 * expected by {@link ReadOnlyStateTransaction.get}.
 */
export class FirestoreReadOnlyStateTransaction
  implements ReadOnlyStateTransaction
{
  constructor(
    /**
     * The Firestore transaction to use.
     */
    readonly firestoreTransaction: Transaction,

    /**
     * The resolver that provides the Firestore collections for a given document type.
     */
    readonly collectionResolver: FirestoreCollectionResolver,
  ) {}

  /**
   * Returns the soft-delete information for a given document, if the document type supports soft-deletion.
   *
   * @param activeDocRef The reference to the active document.
   * @param type The type of the document.
   * @returns The soft-delete information for the document, or `null` if the document type does not support
   *   soft-deletion.
   */
  protected getSoftDeleteInfo<T extends object>(
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

  async get<T extends object>(
    type: Type<T>,
    entity: Partial<T>,
  ): Promise<T | null> {
    const { activeCollection } =
      this.collectionResolver.getCollectionsForType(type);

    const activeDocRef = getReferenceForFirestoreDocument(
      activeCollection,
      entity,
      type,
    );
    const activeSnapshot = await this.firestoreTransaction.get(activeDocRef);
    const activeDocument = activeSnapshot.data();
    if (activeDocument) {
      return activeDocument;
    }

    const softDeleteInfo = this.getSoftDeleteInfo(activeDocRef, type);
    if (!softDeleteInfo) {
      return null;
    }

    const deletedSnapshot = await this.firestoreTransaction.get(
      softDeleteInfo.ref,
    );
    const deletedDocument = deletedSnapshot.data();
    if (!deletedDocument) {
      return null;
    }

    delete (deletedDocument as any)[softDeleteInfo.expirationField];
    return deletedDocument;
  }
}
