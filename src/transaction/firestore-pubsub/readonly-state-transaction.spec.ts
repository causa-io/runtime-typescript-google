import type { VersionedEntity } from '@causa/runtime';
import { initializeApp } from 'firebase-admin/app';
import {
  CollectionReference,
  Firestore,
  getFirestore,
} from 'firebase-admin/firestore';
import 'jest-extended';
import { randomUUID } from 'node:crypto';
import {
  FirestoreCollection,
  getFirestoreCollection,
  makeFirestoreDataConverter,
} from '../../firestore/index.js';
import { clearFirestoreDatabase } from '../../firestore/testing.js';
import { FirestoreReadOnlyStateTransaction } from './readonly-state-transaction.js';
import { SoftDeletedFirestoreCollection } from './soft-deleted-collection.decorator.js';

@FirestoreCollection({ path: (doc) => ['myDocument', doc.id] })
@SoftDeletedFirestoreCollection()
class MyDocument implements VersionedEntity {
  constructor(data: Partial<MyDocument> = {}) {
    Object.assign(this, {
      id: '1234',
      createdAt: new Date(),
      updatedAt: new Date(),
      deletedAt: null,
      ...data,
    });
  }

  readonly id!: string;
  readonly createdAt!: Date;
  readonly updatedAt!: Date;
  readonly deletedAt!: Date | null;
}

@FirestoreCollection({ path: (doc) => ['myOtherDocument', doc.id] })
class MyNonSoftDeletedDocument implements VersionedEntity {
  constructor(data: Partial<MyNonSoftDeletedDocument> = {}) {
    Object.assign(this, {
      id: '1234',
      createdAt: new Date(),
      updatedAt: new Date(),
      deletedAt: null,
      ...data,
    });
  }

  readonly id!: string;
  readonly createdAt!: Date;
  readonly updatedAt!: Date;
  readonly deletedAt!: Date | null;
}

@FirestoreCollection({ path: (doc) => ['parent', doc.id1, 'child', doc.id2] })
@SoftDeletedFirestoreCollection()
class MyNestedDocument {
  constructor(data: Partial<MyNestedDocument> = {}) {
    Object.assign(this, {
      createdAt: new Date(),
      updatedAt: new Date(),
      deletedAt: null,
      ...data,
    });
  }

  readonly id1!: string;
  readonly id2!: string;
  readonly createdAt!: Date;
  readonly updatedAt!: Date;
  readonly deletedAt!: Date | null;
}

describe('FirestoreReadOnlyStateTransaction', () => {
  let firestore: Firestore;
  let activeCollection: CollectionReference<MyDocument>;
  let deletedCollection: CollectionReference<MyDocument>;
  let nonSoftDeleteCollection: CollectionReference<MyNonSoftDeletedDocument>;
  let parentCollection: CollectionReference<MyNestedDocument>;

  beforeAll(() => {
    firestore = getFirestore(initializeApp(), `test-${randomUUID()}`);
    activeCollection = getFirestoreCollection(firestore, MyDocument);
    deletedCollection = firestore
      .collection(`${activeCollection.path}$deleted`)
      .withConverter(makeFirestoreDataConverter(MyDocument));
    nonSoftDeleteCollection = getFirestoreCollection(
      firestore,
      MyNonSoftDeletedDocument,
    );
    parentCollection = firestore
      .collection('parent')
      .withConverter(makeFirestoreDataConverter(MyNestedDocument));
  });

  afterEach(() => clearFirestoreDatabase(firestore));

  afterAll(() => firestore.terminate());

  describe('constructor', () => {
    it('should expose the transaction and the Firestore instance', () => {
      const transaction = {} as any;

      const stateTransaction = new FirestoreReadOnlyStateTransaction(
        transaction,
        firestore,
      );

      expect(stateTransaction.firestoreTransaction).toBe(transaction);
      expect(stateTransaction.firestore).toBe(firestore);
    });
  });

  describe('get', () => {
    it('should return the document from the active collection', async () => {
      const document = new MyDocument();
      await activeCollection.doc(document.id).set(document);

      const actualDocument = await firestore.runTransaction(
        async (transaction) => {
          const stateTransaction = new FirestoreReadOnlyStateTransaction(
            transaction,
            firestore,
          );

          return stateTransaction.get(MyDocument, {
            id: document.id,
          });
        },
        { readOnly: true },
      );

      expect(actualDocument).toEqual(document);
      expect(actualDocument).toBeInstanceOf(MyDocument);
    });

    it('should return the document from the deleted collection', async () => {
      const document = new MyDocument({ deletedAt: new Date() });
      await deletedCollection.doc(document.id).set({
        ...document,
        _expirationDate: new Date(),
      } as any);

      const actualDocument = await firestore.runTransaction(
        async (transaction) => {
          const stateTransaction = new FirestoreReadOnlyStateTransaction(
            transaction,
            firestore,
          );

          return stateTransaction.get(MyDocument, {
            id: document.id,
          });
        },
        { readOnly: true },
      );

      expect(actualDocument).toEqual(document);
      expect(actualDocument).toBeInstanceOf(MyDocument);
    });

    it('should favor the document from the active collection', async () => {
      const activeDocument = new MyDocument();
      await activeCollection.doc(activeDocument.id).set(activeDocument);
      const deletedDocument = new MyDocument({
        id: activeDocument.id,
        deletedAt: new Date(),
      });
      await deletedCollection.doc(deletedDocument.id).set({
        ...deletedDocument,
        _expirationDate: new Date(),
      } as any);

      const actualDocument = await firestore.runTransaction(
        async (transaction) => {
          const stateTransaction = new FirestoreReadOnlyStateTransaction(
            transaction,
            firestore,
          );

          return stateTransaction.get(MyDocument, {
            id: activeDocument.id,
          });
        },
        { readOnly: true },
      );

      expect(actualDocument).toEqual(activeDocument);
      expect(actualDocument).toBeInstanceOf(MyDocument);
    });

    it('should return null if the document does not exist', async () => {
      const actualDocument = await firestore.runTransaction(
        async (transaction) => {
          const stateTransaction = new FirestoreReadOnlyStateTransaction(
            transaction,
            firestore,
          );

          return stateTransaction.get(MyDocument, {
            id: '🎁',
          });
        },
        { readOnly: true },
      );

      expect(actualDocument).toBeNull();
    });

    it('should return a document without a soft delete collection', async () => {
      const document = new MyNonSoftDeletedDocument();
      await nonSoftDeleteCollection.doc(document.id).set(document);

      const actualDocument = await firestore.runTransaction(
        async (transaction) => {
          const stateTransaction = new FirestoreReadOnlyStateTransaction(
            transaction,
            firestore,
          );

          return stateTransaction.get(MyNonSoftDeletedDocument, {
            id: document.id,
          });
        },
        { readOnly: true },
      );

      expect(actualDocument).toEqual(document);
      expect(actualDocument).toBeInstanceOf(MyNonSoftDeletedDocument);
    });

    it('should handle nested collections', async () => {
      const document = new MyNestedDocument({ id1: 'parent1', id2: 'child1' });
      await parentCollection
        .doc(`${document.id1}/child/${document.id2}`)
        .set(document);
      const deletedDocument = new MyNestedDocument({
        id1: 'parent1',
        id2: 'child2',
        deletedAt: new Date(),
      });
      await parentCollection
        .doc(`${deletedDocument.id1}/child$deleted/${deletedDocument.id2}`)
        .set({ ...deletedDocument, _expirationDate: new Date() } as any);

      const { actualDocument, actualDeletedDocument } =
        await firestore.runTransaction(
          async (transaction) => {
            const stateTransaction = new FirestoreReadOnlyStateTransaction(
              transaction,
              firestore,
            );

            const actualDocument = await stateTransaction.get(
              MyNestedDocument,
              { id1: document.id1, id2: document.id2 },
            );
            const actualDeletedDocument = await stateTransaction.get(
              MyNestedDocument,
              { id1: deletedDocument.id1, id2: deletedDocument.id2 },
            );

            return { actualDocument, actualDeletedDocument };
          },
          { readOnly: true },
        );

      expect(actualDocument).toEqual(document);
      expect(actualDocument).toBeInstanceOf(MyNestedDocument);
      expect(actualDeletedDocument).toEqual(deletedDocument);
      expect(actualDeletedDocument).toBeInstanceOf(MyNestedDocument);
    });
  });
});
