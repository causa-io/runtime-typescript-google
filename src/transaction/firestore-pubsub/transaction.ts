import { BufferEventTransaction, Transaction } from '@causa/runtime';
import { Transaction as FirestoreTransaction } from 'firebase-admin/firestore';
import { FirestoreStateTransaction } from './state-transaction.js';

/**
 * A {@link Transaction} that uses Firestore for state storage and Pub/Sub for event publishing.
 */
export class FirestorePubSubTransaction extends Transaction<
  FirestoreStateTransaction,
  BufferEventTransaction
> {
  /**
   * The underlying {@link FirestoreTransaction} used by the state transaction.
   */
  get firestoreTransaction(): FirestoreTransaction {
    return this.stateTransaction.transaction;
  }
}
