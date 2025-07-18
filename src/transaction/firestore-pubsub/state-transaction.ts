import type { StateTransaction } from '@causa/runtime';
import type { Type } from '@nestjs/common';
import { plainToInstance } from 'class-transformer';
import { getReferenceForFirestoreDocument } from '../../firestore/index.js';
import { FirestoreReadOnlyStateTransaction } from './readonly-state-transaction.js';

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
export class FirestoreStateTransaction
  extends FirestoreReadOnlyStateTransaction
  implements StateTransaction
{
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
    this.firestoreTransaction.delete(activeDocRef);

    if (!softDelete) {
      return;
    }

    const deletedDocRef = getReferenceForFirestoreDocument(
      softDelete.collection,
      key,
      type,
    );
    this.firestoreTransaction.delete(deletedDocRef);
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
      this.firestoreTransaction.set(activeDocRef, entity);
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

      this.firestoreTransaction.delete(activeDocRef);
      this.firestoreTransaction.set(deletedDocRef, deletedDoc);
    } else {
      this.firestoreTransaction.set(activeDocRef, entity);
      this.firestoreTransaction.delete(deletedDocRef);
    }
  }
}
