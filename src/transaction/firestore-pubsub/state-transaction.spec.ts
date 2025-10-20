import type { VersionedEntity } from '@causa/runtime';
import {
  CollectionReference,
  Firestore,
  getFirestore,
} from 'firebase-admin/firestore';
import 'jest-extended';
import { getDefaultFirebaseApp } from '../../firebase/index.js';
import {
  FirestoreCollection,
  makeFirestoreDataConverter,
} from '../../firestore/index.js';
import {
  clearFirestoreCollection,
  createFirestoreTemporaryCollection,
} from '../../firestore/testing.js';
import { SoftDeletedFirestoreCollection } from './soft-deleted-collection.decorator.js';
import { FirestoreStateTransaction } from './state-transaction.js';
import type {
  FirestoreCollectionResolver,
  FirestoreCollectionsForDocumentType,
} from './types.js';

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

@FirestoreCollection({
  name: 'parent',
  path: (doc) => `${doc.id1}/child/${doc.id2}`,
})
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

describe('FirestoreStateTransaction', () => {
  let firestore: Firestore;
  let activeCollection: CollectionReference<MyDocument>;
  let deletedCollection: CollectionReference<MyDocument>;
  let nonSoftDeleteCollection: CollectionReference<MyNonSoftDeletedDocument>;
  let parentCollection: CollectionReference<MyNestedDocument>;
  let resolver: FirestoreCollectionResolver;

  beforeAll(() => {
    firestore = getFirestore(getDefaultFirebaseApp());
    activeCollection = createFirestoreTemporaryCollection(
      firestore,
      MyDocument,
    );
    deletedCollection = firestore
      .collection(`${activeCollection.path}$deleted`)
      .withConverter(makeFirestoreDataConverter(MyDocument));
    nonSoftDeleteCollection = createFirestoreTemporaryCollection(
      firestore,
      MyNonSoftDeletedDocument,
    );
    parentCollection = createFirestoreTemporaryCollection(
      firestore,
      MyNestedDocument,
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

        if (documentType === MyNestedDocument) {
          return {
            activeCollection: parentCollection,
            softDelete: {
              // This should not be used anyway, as it is not correct for nested collections.
              collection: parentCollection,
              expirationField: '_expirationDate',
              expirationDelay: 24 * 3600 * 1000,
            },
          };
        }

        throw new Error('Unexpected document type.');
      },
    };
  });

  afterEach(async () => {
    await clearFirestoreCollection(activeCollection);
    await clearFirestoreCollection(deletedCollection);
    await clearFirestoreCollection(nonSoftDeleteCollection);
    await clearFirestoreCollection(parentCollection);
  });

  describe('constructor', () => {
    it('should expose the transaction and the collection resolver', () => {
      const transaction = {} as any;

      const stateTransaction = new FirestoreStateTransaction(
        transaction,
        resolver,
      );

      expect(stateTransaction.firestoreTransaction).toBe(transaction);
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

    it('should handle nested collections', async () => {
      const document = new MyNestedDocument({ id1: 'parent1', id2: 'child1' });
      const documentPath = parentCollection.doc(
        `${document.id1}/child/${document.id2}`,
      );
      await documentPath.set(document);
      const deletedDocument = new MyNestedDocument({
        id1: 'parent1',
        id2: 'child2',
        deletedAt: new Date(),
      });
      const deletedDocumentPath = parentCollection.doc(
        `${deletedDocument.id1}/child$deleted/${deletedDocument.id2}`,
      );
      await deletedDocumentPath.set({
        ...deletedDocument,
        _expirationDate: new Date(),
      } as any);

      await firestore.runTransaction(async (transaction) => {
        const stateTransaction = new FirestoreStateTransaction(
          transaction,
          resolver,
        );

        await stateTransaction.delete(document);
        await stateTransaction.delete(MyNestedDocument, {
          id1: deletedDocument.id1,
          id2: deletedDocument.id2,
        });
      });

      const actualDocument = await documentPath.get();
      expect(actualDocument.exists).toBeFalse();
      const actualDeletedDocument = await deletedDocumentPath.get();
      expect(actualDeletedDocument.exists).toBeFalse();
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

    it('should handle nested collections', async () => {
      const document = new MyNestedDocument({ id1: 'parent1', id2: 'child1' });
      const deletedDocument = new MyNestedDocument({
        id1: 'parent1',
        id2: 'child2',
        deletedAt: new Date(),
      });

      await firestore.runTransaction(async (transaction) => {
        const stateTransaction = new FirestoreStateTransaction(
          transaction,
          resolver,
        );

        await stateTransaction.set(document);
        await stateTransaction.set(deletedDocument);
      });

      const actualDocument = await parentCollection
        .doc(`${document.id1}/child/${document.id2}`)
        .get();
      expect(actualDocument.data()).toEqual(document);
      const actualDeletedDocument = await parentCollection
        .doc(`${deletedDocument.id1}/child$deleted/${deletedDocument.id2}`)
        .get();
      expect(actualDeletedDocument.data()).toEqual({
        ...deletedDocument,
        _expirationDate: new Date(
          deletedDocument.deletedAt!.getTime() + 24 * 3600 * 1000,
        ),
      });
    });
  });
});
