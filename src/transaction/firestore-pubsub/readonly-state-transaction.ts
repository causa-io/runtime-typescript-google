import type {
  ReadOnlyStateTransaction,
  ReadOnlyTransactionOption,
} from '@causa/runtime';
import type { Type } from '@nestjs/common';
import { Transaction } from 'firebase-admin/firestore';
import { getReferenceForFirestoreDocument } from '../../firestore/index.js';
import type { FirestoreCollectionResolver } from './types.js';

/**
 * Option for a function that accepts a {@link FirestoreReadOnlyStateTransaction}.
 */
export type FirestoreReadOnlyStateTransactionOption =
  ReadOnlyTransactionOption<FirestoreReadOnlyStateTransaction>;

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
    const activeSnapshot = await this.firestoreTransaction.get(activeDocRef);
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
    const deletedSnapshot = await this.firestoreTransaction.get(deletedDocRef);
    if (!deletedSnapshot.exists) {
      return undefined;
    }

    const deletedDocument = deletedSnapshot.data();
    delete (deletedDocument as any)[softDelete.expirationField];
    return deletedDocument;
  }
}
