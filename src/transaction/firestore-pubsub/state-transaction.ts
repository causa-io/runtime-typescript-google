import type { StateTransaction } from '@causa/runtime';
import type { Type } from '@nestjs/common';
import { plainToInstance } from 'class-transformer';
import { CollectionReference, Transaction } from 'firebase-admin/firestore';
import { getReferenceForFirestoreDocument } from '../../firestore/index.js';
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

/**
 * A {@link StateTransaction} that uses Firestore for state storage.
 *
 * This transaction handles soft-deleted documents if the class is decorated with `SoftDeletedFirestoreCollection`,
 * which means that documents with a `deletedAt` field set to a non-null value are considered deleted.
 * Soft-deleted documents are moved to a separate collection, where they are kept for a configurable amount of time
 * before being permanently deleted. See the `SoftDeletedFirestoreCollection` decorator for more information.
 *
 * {@link FirestoreStateTransaction.delete} will delete the document from any of the regular or soft-delete collections.
 * It does not throw an error if the document does not exist.
 *
 * {@link FirestoreStateTransaction.get} will return the document from either the regular or soft-delete collection, as
 * expected by {@link StateTransaction.get}.
 *
 * {@link FirestoreStateTransaction.set} will set the document in the relevant collection, either the regular or
 * soft-delete collection depending on the value of the `deletedAt` field.
 */
export class FirestoreStateTransaction implements StateTransaction {
  constructor(
    readonly transaction: Transaction,
    readonly collectionResolver: FirestoreCollectionResolver,
  ) {}

  async delete<T extends object>(
    typeOrEntity: Type<T> | T,
    key?: Partial<T>,
  ): Promise<void> {
    const type = (
      key === undefined ? typeOrEntity.constructor : typeOrEntity
    ) as Type<T>;
    key ??= typeOrEntity as Partial<T>;

    const { activeCollection, softDelete } =
      this.collectionResolver.getCollectionsForType(type);

    const activeDocRef = getReferenceForFirestoreDocument(
      activeCollection,
      key,
      type,
    );
    this.transaction.delete(activeDocRef);

    if (!softDelete) {
      return;
    }

    const deletedDocRef = getReferenceForFirestoreDocument(
      softDelete.collection,
      key,
      type,
    );
    this.transaction.delete(deletedDocRef);
  }

  async get<T extends object>(
    type: Type<T>,
    entity: Partial<T>,
  ): Promise<T | undefined> {
    const { activeCollection, softDelete } =
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

    if (!softDelete) {
      return undefined;
    }

    const deletedDocRef = getReferenceForFirestoreDocument(
      softDelete.collection,
      entity,
      type,
    );
    const deletedSnapshot = await this.transaction.get(deletedDocRef);
    if (!deletedSnapshot.exists) {
      return undefined;
    }

    const deletedDocument = deletedSnapshot.data();
    delete (deletedDocument as any)[softDelete.expirationField];
    return deletedDocument;
  }

  async set<T extends object>(entity: T): Promise<void> {
    const documentType = entity.constructor as Type<T>;
    const { activeCollection, softDelete } =
      this.collectionResolver.getCollectionsForType(documentType);

    const activeDocRef = getReferenceForFirestoreDocument(
      activeCollection,
      entity,
    );
    if (!softDelete) {
      this.transaction.set(activeDocRef, entity);
      return;
    }

    const deletedDocRef = getReferenceForFirestoreDocument(
      softDelete.collection,
      entity,
    );

    if ('deletedAt' in entity && entity.deletedAt instanceof Date) {
      const { expirationDelay, expirationField } = softDelete;
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
