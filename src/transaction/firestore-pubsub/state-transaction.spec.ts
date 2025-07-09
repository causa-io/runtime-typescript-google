import type { VersionedEntity } from '@causa/runtime';
import {
  CollectionReference,
  Firestore,
  getFirestore,
} from 'firebase-admin/firestore';
import 'jest-extended';
import { getDefaultFirebaseApp } from '../../firebase/index.js';
import { FirestoreCollection } from '../../firestore/index.js';
import {
  clearFirestoreCollection,
  createFirestoreTemporaryCollection,
} from '../../firestore/testing.js';
import { SoftDeletedFirestoreCollection } from './soft-deleted-collection.decorator.js';
import {
  type FirestoreCollectionResolver,
  type FirestoreCollectionsForDocumentType,
  FirestoreStateTransaction,
} from './state-transaction.js';

@FirestoreCollection({ name: 'myDocument', path: (doc) => doc.id })
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

@FirestoreCollection({ name: 'myOtherDocument', path: (doc) => doc.id })
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

describe('FirestoreStateTransaction', () => {
  let firestore: Firestore;
  let activeCollection: CollectionReference<MyDocument>;
  let deletedCollection: CollectionReference<MyDocument>;
  let nonSoftDeleteCollection: CollectionReference<MyNonSoftDeletedDocument>;
  let resolver: FirestoreCollectionResolver;

  beforeAll(() => {
    firestore = getFirestore(getDefaultFirebaseApp());
    activeCollection = createFirestoreTemporaryCollection(
      firestore,
      MyDocument,
    );
    deletedCollection = createFirestoreTemporaryCollection(
      firestore,
      MyDocument,
    );
    nonSoftDeleteCollection = createFirestoreTemporaryCollection(
      firestore,
      MyNonSoftDeletedDocument,
    );
    resolver = {
      getCollectionsForType<T>(documentType: {
        new (): T;
      }): FirestoreCollectionsForDocumentType<any> {
        if (documentType === MyDocument) {
          return {
            activeCollection,
            softDelete: {
              collection: deletedCollection,
              expirationField: '_expirationDate',
              expirationDelay: 24 * 3600 * 1000,
            },
          };
        }

        if (documentType === MyNonSoftDeletedDocument) {
          return {
            activeCollection: nonSoftDeleteCollection,
            softDelete: null,
          };
        }

        throw new Error('Unexpected document type.');
      },
    };
  });

  afterEach(async () => {
    await clearFirestoreCollection(activeCollection);
  });

  describe('constructor', () => {
    it('should expose the transaction and the collection resolver', () => {
      const transaction = {} as any;

      const stateTransaction = new FirestoreStateTransaction(
        transaction,
        resolver,
      );

      expect(stateTransaction.transaction).toBe(transaction);
      expect(stateTransaction.collectionResolver).toBe(resolver);
    });
  });

  describe('delete', () => {
    it('should delete the document from the active collection', async () => {
      const document = new MyDocument();
      await activeCollection.doc(document.id).set(document);

      await firestore.runTransaction(async (transaction) => {
        const stateTransaction = new FirestoreStateTransaction(
          transaction,
          resolver,
        );

        await stateTransaction.delete(MyDocument, {
          id: document.id,
        });
      });

      const actualActiveDocument = await activeCollection
        .doc(document.id)
        .get();
      expect(actualActiveDocument.exists).toBeFalse();
      const actualDeletedDocument = await deletedCollection
        .doc(document.id)
        .get();
      expect(actualDeletedDocument.exists).toBeFalse();
    });

    it('should delete the document from the deleted collection', async () => {
      const document = new MyDocument({ deletedAt: new Date() });
      await deletedCollection.doc(document.id).set(document);

      await firestore.runTransaction(async (transaction) => {
        const stateTransaction = new FirestoreStateTransaction(
          transaction,
          resolver,
        );

        await stateTransaction.delete(MyDocument, {
          id: document.id,
        });
      });

      const actualActiveDocument = await activeCollection
        .doc(document.id)
        .get();
      expect(actualActiveDocument.exists).toBeFalse();
      const actualDeletedDocument = await deletedCollection
        .doc(document.id)
        .get();
      expect(actualDeletedDocument.exists).toBeFalse();
    });

    it('should ignore a document that does not exist', async () => {
      await firestore.runTransaction(async (transaction) => {
        const stateTransaction = new FirestoreStateTransaction(
          transaction,
          resolver,
        );

        await stateTransaction.delete(MyDocument, {
          id: '🎁',
        });
      });

      const actualActiveDocument = await activeCollection.doc('🎁').get();
      expect(actualActiveDocument.exists).toBeFalse();
      const actualDeletedDocument = await deletedCollection.doc('🎁').get();
      expect(actualDeletedDocument.exists).toBeFalse();
    });

    it('should handle a document without a soft delete collection', async () => {
      const document = new MyNonSoftDeletedDocument();
      await nonSoftDeleteCollection.doc(document.id).set(document);

      await firestore.runTransaction(async (transaction) => {
        const stateTransaction = new FirestoreStateTransaction(
          transaction,
          resolver,
        );

        await stateTransaction.delete(MyNonSoftDeletedDocument, {
          id: document.id,
        });
      });

      const actualDocument = await nonSoftDeleteCollection
        .doc(document.id)
        .get();
      expect(actualDocument.exists).toBeFalse();
    });

    it('should delete the document when provided with the full entity', async () => {
      const document = new MyNonSoftDeletedDocument();
      await nonSoftDeleteCollection.doc(document.id).set(document);

      await firestore.runTransaction(async (transaction) => {
        const stateTransaction = new FirestoreStateTransaction(
          transaction,
          resolver,
        );

        await stateTransaction.delete(document);
      });

      const actualDocument = await nonSoftDeleteCollection
        .doc(document.id)
        .get();
      expect(actualDocument.exists).toBeFalse();
    });
  });

  describe('get', () => {
    it('should return the document from the active collection', async () => {
      const document = new MyDocument();
      await activeCollection.doc(document.id).set(document);

      const actualDocument = await firestore.runTransaction(
        async (transaction) => {
          const stateTransaction = new FirestoreStateTransaction(
            transaction,
            resolver,
          );

          return stateTransaction.get(MyDocument, {
            id: document.id,
          });
        },
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
          const stateTransaction = new FirestoreStateTransaction(
            transaction,
            resolver,
          );

          return stateTransaction.get(MyDocument, {
            id: document.id,
          });
        },
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
          const stateTransaction = new FirestoreStateTransaction(
            transaction,
            resolver,
          );

          return stateTransaction.get(MyDocument, {
            id: activeDocument.id,
          });
        },
      );

      expect(actualDocument).toEqual(activeDocument);
      expect(actualDocument).toBeInstanceOf(MyDocument);
    });

    it('should return undefined if the document does not exist', async () => {
      const actualDocument = await firestore.runTransaction(
        async (transaction) => {
          const stateTransaction = new FirestoreStateTransaction(
            transaction,
            resolver,
          );

          return stateTransaction.get(MyDocument, {
            id: '🎁',
          });
        },
      );

      expect(actualDocument).toBeUndefined();
    });

    it('should return a document without a soft delete collection', async () => {
      const document = new MyNonSoftDeletedDocument();
      await nonSoftDeleteCollection.doc(document.id).set(document);

      const actualDocument = await firestore.runTransaction(
        async (transaction) => {
          const stateTransaction = new FirestoreStateTransaction(
            transaction,
            resolver,
          );

          return stateTransaction.get(MyNonSoftDeletedDocument, {
            id: document.id,
          });
        },
      );

      expect(actualDocument).toEqual(document);
      expect(actualDocument).toBeInstanceOf(MyNonSoftDeletedDocument);
    });
  });

  describe('set', () => {
    it('should insert the document into the active collection', async () => {
      const document = new MyDocument();

      await firestore.runTransaction(async (transaction) => {
        const stateTransaction = new FirestoreStateTransaction(
          transaction,
          resolver,
        );

        await stateTransaction.set(document);
      });

      const actualDocument = await activeCollection.doc(document.id).get();
      expect(actualDocument.data()).toEqual(document);
      const actualDeletedDocument = await deletedCollection
        .doc(document.id)
        .get();
      expect(actualDeletedDocument.exists).toBeFalse();
    });

    it('should insert the document into the deleted collection', async () => {
      const deletedAt = new Date();
      const document = new MyDocument({ deletedAt });

      await firestore.runTransaction(async (transaction) => {
        const stateTransaction = new FirestoreStateTransaction(
          transaction,
          resolver,
        );

        await stateTransaction.set(document);
      });

      const actualDocument = await deletedCollection.doc(document.id).get();
      expect(actualDocument.data()).toEqual({
        ...document,
        _expirationDate: new Date(deletedAt.getTime() + 24 * 3600 * 1000),
      });
      const actualActiveDocument = await activeCollection
        .doc(document.id)
        .get();
      expect(actualActiveDocument.exists).toBeFalse();
    });

    it('should replace a deleted document with an active document', async () => {
      const deletedAt = new Date();
      const document = new MyDocument();
      await deletedCollection.doc(document.id).set({
        ...document,
        deletedAt,
        _expirationDate: new Date(deletedAt.getTime() + 24 * 3600 * 1000),
      } as any);

      await firestore.runTransaction(async (transaction) => {
        const stateTransaction = new FirestoreStateTransaction(
          transaction,
          resolver,
        );

        await stateTransaction.set(document);
      });

      const actualDocument = await activeCollection.doc(document.id).get();
      expect(actualDocument.data()).toEqual(document);
      const actualDeletedDocument = await deletedCollection
        .doc(document.id)
        .get();
      expect(actualDeletedDocument.exists).toBeFalse();
    });

    it('should replace an active document with a deleted document', async () => {
      const deletedAt = new Date();
      const document = new MyDocument({ deletedAt });
      await activeCollection
        .doc(document.id)
        .set({ ...document, deletedAt: null });

      await firestore.runTransaction(async (transaction) => {
        const stateTransaction = new FirestoreStateTransaction(
          transaction,
          resolver,
        );

        await stateTransaction.set(document);
      });

      const actualDocument = await deletedCollection.doc(document.id).get();
      expect(actualDocument.data()).toEqual({
        ...document,
        _expirationDate: new Date(deletedAt.getTime() + 24 * 3600 * 1000),
      });
      const actualActiveDocument = await activeCollection
        .doc(document.id)
        .get();
      expect(actualActiveDocument.exists).toBeFalse();
    });

    it('should insert a document without a soft delete collection', async () => {
      const document = new MyNonSoftDeletedDocument();

      await firestore.runTransaction(async (transaction) => {
        const stateTransaction = new FirestoreStateTransaction(
          transaction,
          resolver,
        );

        await stateTransaction.set(document);
      });

      const actualDocument = await nonSoftDeleteCollection
        .doc(document.id)
        .get();
      expect(actualDocument.data()).toEqual(document);
    });
  });
});
