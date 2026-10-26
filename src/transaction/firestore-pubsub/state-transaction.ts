import type { StateTransaction } from '@causa/runtime';
import type { Type } from '@nestjs/common';
import { plainToInstance } from 'class-transformer';
import { getReferenceForFirestoreDocument } from '../../firestore/index.js';
import { FirestoreReadOnlyStateTransaction } from './readonly-state-transaction.js';
import {
  getSoftDeletableDocument,
  getSoftDeleteInfo,
  type SoftDeletableDocumentState,
} from './soft-deleted-collection.decorator.js';

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
  /**
   * Whether the active and soft-deleted versions of documents exist, as read or written in this transaction, keyed by
   * the path of the active document.
   * This is used to avoid deleting documents that are known not to exist. This is safe because documents read in a
   * Firestore transaction cannot change before it is committed, and because a new state transaction is created for each
   * attempt of the Firestore transaction.
   */
  private readonly softDeleteStates = new Map<
    string,
    SoftDeletableDocumentState
  >();

  async get<T extends object>(
    type: Type<T>,
    entity: Partial<T>,
  ): Promise<T | null> {
    const activeDocRef = getReferenceForFirestoreDocument(
      this.firestore,
      entity,
      type,
    );
    const { document, state } = await getSoftDeletableDocument(
      this.firestoreTransaction,
      activeDocRef,
      type,
      this.options,
    );
    if (state) {
      this.softDeleteStates.set(activeDocRef.path, state);
    }

    return document;
  }

  async delete<T extends object>(
    typeOrEntity: Type<T> | T,
    key?: Partial<T>,
  ): Promise<void> {
    const type = (
      key === undefined ? typeOrEntity.constructor : typeOrEntity
    ) as Type<T>;
    key ??= typeOrEntity as Partial<T>;

    const activeDocRef = getReferenceForFirestoreDocument(
      this.firestore,
      key,
      type,
    );

    const softDeleteInfo = getSoftDeleteInfo(activeDocRef, type);
    if (!softDeleteInfo) {
      this.firestoreTransaction.delete(activeDocRef);
      return;
    }

    const state = this.softDeleteStates.get(activeDocRef.path);
    if (state?.active !== false) {
      this.firestoreTransaction.delete(activeDocRef);
    }
    if (state?.deleted !== false) {
      this.firestoreTransaction.delete(softDeleteInfo.ref);
    }
    this.softDeleteStates.set(activeDocRef.path, {
      active: false,
      deleted: false,
    });
  }

  async set<T extends object>(entity: T): Promise<void> {
    const documentType = entity.constructor as Type<T>;

    const activeDocRef = getReferenceForFirestoreDocument(
      this.firestore,
      entity,
    );

    const softDeleteInfo = getSoftDeleteInfo(activeDocRef, documentType);
    if (!softDeleteInfo) {
      this.firestoreTransaction.set(activeDocRef, entity);
      return;
    }

    const {
      ref: deletedDocRef,
      expirationDelay,
      expirationField,
    } = softDeleteInfo;
    const state = this.softDeleteStates.get(activeDocRef.path);

    if ('deletedAt' in entity && entity.deletedAt instanceof Date) {
      const expiresAt = new Date(entity.deletedAt.getTime() + expirationDelay);
      const deletedDoc = plainToInstance(documentType, {
        ...entity,
        [expirationField]: expiresAt,
      });

      if (state?.active !== false) {
        this.firestoreTransaction.delete(activeDocRef);
      }
      this.firestoreTransaction.set(deletedDocRef, deletedDoc);
      this.softDeleteStates.set(activeDocRef.path, {
        active: false,
        deleted: true,
      });
    } else {
      this.firestoreTransaction.set(activeDocRef, entity);
      if (state?.deleted !== false) {
        this.firestoreTransaction.delete(deletedDocRef);
      }
      this.softDeleteStates.set(activeDocRef.path, {
        active: true,
        deleted: false,
      });
    }
  }
}
