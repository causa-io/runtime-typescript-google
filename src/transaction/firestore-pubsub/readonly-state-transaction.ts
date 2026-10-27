import type {
  ReadOnlyStateTransaction,
  ReadOnlyTransactionOption,
} from '@causa/runtime';
import type { Type } from '@nestjs/common';
import { Firestore, Transaction } from 'firebase-admin/firestore';
import { getReferenceForFirestoreDocument } from '../../firestore/index.js';
import {
  getSoftDeletableDocument,
  type SoftDeletableDocumentReadOptions,
} from './soft-deleted-collection.decorator.js';

/**
 * Options for the {@link FirestoreReadOnlyStateTransaction} and the `FirestoreStateTransaction`.
 */
export type FirestoreStateTransactionOptions = SoftDeletableDocumentReadOptions;

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
export class FirestoreReadOnlyStateTransaction implements ReadOnlyStateTransaction {
  constructor(
    /**
     * The Firestore transaction to use.
     */
    readonly firestoreTransaction: Transaction,

    /**
     * The {@link Firestore} instance to use.
     */
    readonly firestore: Firestore,

    /**
     * Options for the transaction.
     */
    readonly options: FirestoreStateTransactionOptions = {},
  ) {}

  async get<T extends object>(
    type: Type<T>,
    entity: Partial<T>,
  ): Promise<T | null> {
    const activeDocRef = getReferenceForFirestoreDocument(
      this.firestore,
      entity,
      type,
    );
    const { document } = await getSoftDeletableDocument(
      this.firestoreTransaction,
      activeDocRef,
      type,
      this.options,
    );
    return document;
  }
}
