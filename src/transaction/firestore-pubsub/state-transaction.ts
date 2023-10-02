import { FindReplaceStateTransaction } from '@causa/runtime';
import { Type } from '@nestjs/common';
import { plainToInstance } from 'class-transformer';
import { CollectionReference, Transaction } from 'firebase-admin/firestore';
import { getReferenceForFirestoreDocument } from '../../firestore/index.js';
import { getSoftDeletedFirestoreCollectionMetadataForType } from './soft-deleted-collection.decorator.js';

/**
 * The Firestore collections that should be used to create document references for a given document type.
 */
export type FirestoreCollectionsForDocumentType<T> = {
  /**
   * The regular collection, where documents are stored when they are not deleted.
   */
  activeCollection: CollectionReference<T>;

  /**
   * The soft-delete collection, where documents are stored when their `deletedAt` field is not `null`.
   */
  deletedCollection: CollectionReference<T>;
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

/**
 * A {@link FindReplaceStateTransaction} that uses Firestore for state storage.
 *
 * This transaction handles soft-deleted documents, which means that documents with a `deletedAt` field set to a
 * non-null value are considered deleted.
 * Soft-deleted documents are moved to a separate collection, where they are kept for a configurable amount of time
 * before being permanently deleted. See the `SoftDeletedFirestoreCollection` decorator for more information.
 *
 * {@link FirestoreStateTransaction.deleteWithSameKeyAs} will delete the document from any of the regular or soft-delete
 * collections. It does not throw an error if the document does not exist.
 *
 * {@link FirestoreStateTransaction.findOneWithSameKeyAs} will return the document from either the regular or
 * soft-delete collection, as expected by {@link FindReplaceStateTransaction.findOneWithSameKeyAs}.
 *
 * {@link FirestoreStateTransaction.replace} will set the document in the relevant collection, either the regular or
 * soft-delete collection depending on the value of the `deletedAt` field.
 */
export class FirestoreStateTransaction implements FindReplaceStateTransaction {
  constructor(
    readonly transaction: Transaction,
    readonly collectionResolver: FirestoreCollectionResolver,
  ) {}

  async deleteWithSameKeyAs<T extends object>(
    type: Type<T>,
    key: Partial<T>,
  ): Promise<void> {
    const { activeCollection, deletedCollection } =
      this.collectionResolver.getCollectionsForType(type);

    const activeDocRef = getReferenceForFirestoreDocument(
      activeCollection,
      key,
      type,
    );
    const deletedDocRef = getReferenceForFirestoreDocument(
      deletedCollection,
      key,
      type,
    );

    this.transaction.delete(activeDocRef);
    this.transaction.delete(deletedDocRef);
  }

  async findOneWithSameKeyAs<T extends object>(
    type: Type<T>,
    entity: Partial<T>,
  ): Promise<T | undefined> {
    const { activeCollection, deletedCollection } =
      this.collectionResolver.getCollectionsForType(type);

    const activeDocRef = getReferenceForFirestoreDocument(
      activeCollection,
      entity,
      type,
    );
    const activeSnapshot = await this.transaction.get(activeDocRef);
    if (activeSnapshot.exists) {
      return activeSnapshot.data();
    }

    const deletedDocRef = getReferenceForFirestoreDocument(
      deletedCollection,
      entity,
      type,
    );
    const deletedSnapshot = await this.transaction.get(deletedDocRef);
    if (deletedSnapshot.exists) {
      const deletedDocument = deletedSnapshot.data();
      const { expirationField } =
        getSoftDeletedFirestoreCollectionMetadataForType(type);
      delete (deletedDocument as any)[expirationField];
      return deletedDocument;
    }

    return undefined;
  }

  async replace<T extends object>(entity: T): Promise<void> {
    const documentType = entity.constructor as Type<T>;
    const { activeCollection, deletedCollection } =
      this.collectionResolver.getCollectionsForType(documentType);

    const activeDocRef = getReferenceForFirestoreDocument(
      activeCollection,
      entity,
    );
    const deletedDocRef = getReferenceForFirestoreDocument(
      deletedCollection,
      entity,
    );

    if ('deletedAt' in entity && entity.deletedAt instanceof Date) {
      const { expirationDelay, expirationField } =
        getSoftDeletedFirestoreCollectionMetadataForType(documentType);
      const expiresAt = new Date(entity.deletedAt.getTime() + expirationDelay);
      const deletedDoc = plainToInstance(documentType, {
        ...entity,
        [expirationField]: expiresAt,
      });

      this.transaction.delete(activeDocRef);
      this.transaction.set(deletedDocRef, deletedDoc);
    } else {
      this.transaction.set(activeDocRef, entity);
      this.transaction.delete(deletedDocRef);
    }
  }
}
