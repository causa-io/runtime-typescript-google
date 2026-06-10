import { initializeApp } from 'firebase-admin/app';
import { Firestore, getFirestore } from 'firebase-admin/firestore';
import 'reflect-metadata';
import {
  FirestoreCollection,
  getFirestoreCollection,
  getFirestoreCollectionMetadataForType,
  getReferenceForFirestoreDocument,
} from './collection.decorator.js';

@FirestoreCollection({
  path: (doc) => ['🔖', doc.id],
})
class MyDocument {
  constructor(readonly id: string = '🐑') {}
}

@FirestoreCollection({
  path: (doc) => ['📂', doc.parentId, '🔖', doc.id],
})
class MyNestedDocument {
  constructor(
    readonly parentId: string | null = '🗃️',
    readonly id: string = '🐑',
    readonly unused?: string,
  ) {}
}

@FirestoreCollection({
  path: (doc) => [doc.id],
})
class MyInvalidDocument {
  constructor(readonly id: string = '🐑') {}
}

describe('FirestoreCollection', () => {
  let firestore: Firestore;

  beforeAll(() => {
    firestore = getFirestore(initializeApp());
  });

  describe('getFirestoreCollectionMetadataForType', () => {
    it('should return the metadata of the Firestore collection corresponding to the given class', () => {
      const actualMetadata = getFirestoreCollectionMetadataForType(MyDocument);

      expect(actualMetadata.path({ id: '🎁' })).toEqual(['🔖', '🎁']);
    });

    it('should throw if the class is not decorated with FirestoreCollection', () => {
      class MyDocument {}

      expect(() => getFirestoreCollectionMetadataForType(MyDocument)).toThrow(
        `Class 'MyDocument' is not declared as a Firestore collection.`,
      );
    });
  });

  describe('getReferenceForFirestoreDocument', () => {
    it('should return the reference for the Firestore document corresponding to the given document', () => {
      const document = new MyDocument();

      const actualReference = getReferenceForFirestoreDocument(
        firestore,
        document,
      );

      expect(actualReference.path).toEqual('🔖/🐑');
    });

    it('should return the reference for the Firestore document corresponding to the given partial document', () => {
      const document = { id: '🪆' };

      const actualReference = getReferenceForFirestoreDocument(
        firestore,
        document,
        MyDocument,
      );

      expect(actualReference.path).toEqual('🔖/🪆');
    });

    it('should return the reference for a document in a nested collection', () => {
      const document = new MyNestedDocument();

      const actualReference = getReferenceForFirestoreDocument(
        firestore,
        document,
      );

      expect(actualReference.path).toEqual('📂/🗃️/🔖/🐑');
    });

    it('should throw if the document is not decorated with FirestoreCollection', () => {
      class MyDocument {}

      expect(() =>
        getReferenceForFirestoreDocument(firestore, new MyDocument()),
      ).toThrow(
        `Class 'MyDocument' is not declared as a Firestore collection.`,
      );
    });

    it('should throw if the returned path contains an undefined segment', () => {
      expect(() =>
        getReferenceForFirestoreDocument(firestore, {}, MyDocument),
      ).toThrow(
        `The path of the 'MyDocument' document cannot be obtained from the given object.`,
      );
    });

    it('should throw if the returned path contains a null segment', () => {
      expect(() =>
        getReferenceForFirestoreDocument(
          firestore,
          { parentId: null, id: '🐑' },
          MyNestedDocument,
        ),
      ).toThrow(
        `The path of the 'MyNestedDocument' document cannot be obtained from the given object.`,
      );
    });

    it('should throw if the returned path has an odd number of segments', () => {
      expect(() =>
        getReferenceForFirestoreDocument(firestore, new MyInvalidDocument()),
      ).toThrow(
        `The path of the 'MyInvalidDocument' document should have an even number of at least 2 segments.`,
      );
    });
  });

  describe('getFirestoreCollection', () => {
    it('should return the collection for a document type in a root collection', () => {
      const actualCollection = getFirestoreCollection(firestore, MyDocument);

      expect(actualCollection.path).toEqual('🔖');
    });

    it('should return the collection for a document type in a nested collection', () => {
      const actualCollection = getFirestoreCollection(
        firestore,
        MyNestedDocument,
        { parentId: '🎁' },
      );

      expect(actualCollection.path).toEqual('📂/🎁/🔖');
    });

    it('should throw if the class is not decorated with FirestoreCollection', () => {
      class MyDocument {}

      expect(() => getFirestoreCollection(firestore, MyDocument)).toThrow(
        `Class 'MyDocument' is not declared as a Firestore collection.`,
      );
    });

    it('should throw if a parent document ID cannot be computed', () => {
      expect(() => getFirestoreCollection(firestore, MyNestedDocument)).toThrow(
        `The collection path of the 'MyNestedDocument' document cannot be obtained from the given object.`,
      );
    });

    it('should throw if the returned path has an odd number of segments', () => {
      expect(() =>
        getFirestoreCollection(firestore, MyInvalidDocument),
      ).toThrow(
        `The path of the 'MyInvalidDocument' document should have an even number of at least 2 segments.`,
      );
    });
  });
});
